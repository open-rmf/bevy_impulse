/*
 * Copyright (C) 2023 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

use crate::{Promise, PromiseState, Cancellation, CancellationCause};

use std::{
    task::Waker,
    sync::{
        Arc, Weak, Mutex, MutexGuard, Condvar, atomic::{AtomicBool, Ordering}
    },
};

pub(crate) struct Sender<Response> {
    target: Weak<PromiseTarget<Response>>,
    sent: bool,
}

impl<T> Sender<T> {
    pub(super) fn new(target: Weak<PromiseTarget<T>>) -> Self {
        Self { target, sent: false }
    }

    pub(crate) fn send(mut self, value: T) -> Result<(), PromiseResult<T>> {
        let result = self.set(PromiseResult::Available(value));
        result
    }

    pub(crate) fn cancel(mut self, cause: Cancellation) -> Result<(), PromiseResult<T>> {
        let result = self.set(PromiseResult::Cancelled(cause));
        result
    }

    pub(crate) fn set(&mut self, result: PromiseResult<T>) -> Result<(), PromiseResult<T>> {
        let Some(target) = self.target.upgrade() else {
            return Err(result);
        };
        let mut inner = match target.inner.lock() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Some(on_result_arrival) = inner.on_result_arrival.take() {
            (on_result_arrival)(result);
        } else {
            inner.result = Some(result);
            if let Some(waker) = inner.waker.take() {
                waker.wake();
            }
            target.cv.notify_all();
        }
        self.sent = true;
        Ok(())
    }

    pub(crate) fn on_promise_drop(&mut self, f: impl FnOnce() + 'static + Send) {
        match self.target.upgrade() {
            Some(target) => {
                let mut guard = match target.inner.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };

                guard.on_promise_drop = Some(Box::new(f));
            }
            None => f(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if !self.sent {
            self.set(PromiseResult::Disposed).ok();
        }
    }
}

impl<T> Promise<T> {
    pub(crate) fn new() -> (Self, Sender<T>) {
        let target = Arc::new(PromiseTarget::new());
        let sender = Sender::new(Arc::downgrade(&target));
        let promise = Self { state: PromiseState::Pending, target, dependencies: Vec::new() };
        (promise, sender)
    }

    pub(super) fn impl_wait<'a>(
        target: &'a PromiseTarget<T>,
        interrupt: Option<Arc<AtomicBool>>
    ) -> Option<MutexGuard<'a, PromiseTargetInner<T>>> {
        let guard = match target.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return None;
            }
        };

        if guard.result.is_some() {
            // The result arrived but ownership has not been transferred to this
            // promise.
            return None;
        }

        target.cv.wait_while(guard, |inner| {
            if interrupt.as_ref().is_some_and(
                |interrupt| interrupt.load(Ordering::Relaxed)
            ) {
                return false;
            }
            inner.result.is_none()
        }).ok()
    }

    pub(super) fn impl_try_take_result(
        state: &mut PromiseState<T>,
        result: &mut Option<PromiseResult<T>>,
    ) -> bool {
        match result.take() {
            Some(PromiseResult::Available(response)) => {
                *state = PromiseState::Available(response);
                return false;
            }
            Some(PromiseResult::Cancelled(cause)) => {
                *state = PromiseState::Cancelled(cause);
                return false;
            }
            Some(PromiseResult::Disposed) => {
                *state = PromiseState::Disposed;
                return false;
            }
            None => {
                return true;
            }
        }
    }
}

impl<T: 'static + Send + Sync> Promise<Promise<T>> {
    pub(super) fn impl_flatten(mut self) -> Promise<T> {
        let (mut flat_promise, mut flat_sender) = Promise::new();

        let mut outer_promise_dependency = false;
        flat_promise.state = match self.target.inner.lock() {
            Ok(mut outer_target) => {
                if self.state.is_pending() {
                    self.state.update(outer_target.result.take());
                }

                let flat_state = match self.state.take() {
                    PromiseState::Available(mut inner_promise) => {
                        let mut inner_promise_dependency = false;
                        let flat_state = match inner_promise.target.inner.lock() {
                            Ok(mut inner_target) => {
                                if inner_promise.state.is_pending() {
                                    inner_promise.state.update(inner_target.result.take());
                                }

                                let inner_state = inner_promise.state.take();
                                if inner_state.is_pending() {
                                    inner_promise_dependency = true;
                                    inner_target.on_result_arrival = Some(Box::new(move |result| {
                                        let _ = flat_sender.set(result);
                                    }));
                                }
                                inner_state
                            }
                            Err(_) => {
                                PromiseState::make_poisoned()
                            }
                        };

                        if inner_promise_dependency {
                            flat_promise.dependencies.push(Box::new(inner_promise));
                        }

                        flat_state
                    }
                    PromiseState::Pending => {
                        outer_promise_dependency = true;
                        let future_dependency = Arc::new(Mutex::new(None));
                        self.dependencies.push(Box::new(future_dependency.clone()));

                        outer_target.on_result_arrival = Some(Box::new(move |inner_result| {
                            match inner_result {
                                PromiseResult::Available(mut inner_promise) => {
                                    let mut inner_promise_dependency = false;
                                    match inner_promise.target.inner.lock() {
                                        Ok(mut inner_target) => {
                                            if inner_promise.state.is_pending() {
                                                inner_promise.state.update(inner_target.result.take());
                                            }

                                            match inner_promise.state.take() {
                                                PromiseState::Available(x) => {
                                                    let _ = flat_sender.send(x);
                                                }
                                                PromiseState::Pending => {
                                                    inner_promise_dependency = true;
                                                    inner_target.on_result_arrival = Some(Box::new(move |result| {
                                                        let _ = flat_sender.set(result);
                                                    }));
                                                }
                                                PromiseState::Cancelled(cause) => {
                                                    let _ = flat_sender.cancel(cause);
                                                }
                                                PromiseState::Disposed | PromiseState::Taken => {
                                                    drop(flat_sender);
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            let _ = flat_sender.cancel(
                                                Cancellation::from_cause(CancellationCause::PoisonedMutex)
                                            );
                                        }
                                    }

                                    if inner_promise_dependency {
                                        *future_dependency.lock().unwrap() = Some(inner_promise);
                                    }
                                }
                                PromiseResult::Cancelled(cause) => {
                                    let _ = flat_sender.cancel(cause);
                                }
                                PromiseResult::Disposed => {
                                    drop(flat_sender);
                                }
                            }
                        }));

                        PromiseState::Pending
                    }
                    PromiseState::Cancelled(cause) => PromiseState::Cancelled(cause),
                    PromiseState::Disposed => PromiseState::Disposed,
                    PromiseState::Taken => PromiseState::Taken,
                };

                flat_state
            }
            Err(_) => {
                PromiseState::Cancelled(
                    Cancellation::from_cause(CancellationCause::PoisonedMutex)
                )
            }
        };

        if outer_promise_dependency {
            flat_promise.dependencies.push(Box::new(self));
        }

        flat_promise
    }
}

#[derive(Debug)]
pub(crate) enum PromiseResult<T> {
    Available(T),
    Cancelled(Cancellation),
    Disposed,
}

pub(super) struct PromiseTargetInner<T> {
    pub(super) result: Option<PromiseResult<T>>,
    pub(super) waker: Option<Waker>,
    pub(super) on_promise_drop: Option<Box<dyn FnOnce() + 'static + Send>>,
    pub(super) on_result_arrival: Option<Box<dyn FnOnce(PromiseResult<T>) + 'static + Send>>,
}

impl<T> PromiseTargetInner<T> {
    pub(super) fn new() -> Self {
        Self { result: None, waker: None, on_promise_drop: None, on_result_arrival: None }
    }
}

pub(super) struct PromiseTarget<T> {
    pub(super) inner: Mutex<PromiseTargetInner<T>>,
    pub(super) cv: Condvar,
}

impl<T> PromiseTarget<T> {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(PromiseTargetInner::new()),
            cv: Condvar::new(),
        }
    }
}

pub(super) trait Interruptible {
    fn interrupt(&self);
}

impl<T> Interruptible for PromiseTarget<T> {
    fn interrupt(&self) {
        self.cv.notify_all();
    }
}

pub(super) struct Interruptee {
    pub(super) interrupt: Arc<AtomicBool>,
    pub(super) interruptible: Arc<dyn Interruptible>,
}

pub(super) struct InterrupterInner {
    pub(super) triggered: bool,
    pub(super) waiters: Vec<Interruptee>,
}

impl InterrupterInner {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

impl Default for InterrupterInner {
    fn default() -> Self {
        Self { triggered: false, waiters: Vec::new() }
    }
}
