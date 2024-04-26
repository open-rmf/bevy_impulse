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

use std::{
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}},
    future::Future, task::{Context, Poll}, pin::Pin
};

use crate::Cancellation;

pub(crate) mod private;
use private::*;

/// A promise expects to receive a value in the future.
#[must_use]
pub struct Promise<T> {
    state: PromiseState<T>,
    target: Arc<PromiseTarget<T>>,
}

impl<T> Promise<T> {
    /// Check the last known state of the promise without performing any update.
    /// This will never block, but it might provide a state that is out of date.
    ///
    /// To borrow a view of the most current state at the cost of synchronizing
    /// you can use [`peek`].
    pub fn sneak_peek(&self) -> &PromiseState<T> {
        &self.state
    }

    /// View the state of the promise. If a response is available, you will
    /// borrow it, but it will continue to be stored inside the promise.
    ///
    /// This requires a mutable reference to the promise because it may try to
    /// update the current state if needed. To peek at that last known state
    /// without trying to synchronize you can use [`sneak_peek()`].
    pub fn peek(&mut self) -> &PromiseState<T> {
        self.update();
        &self.state
    }

    /// Try to take the response of the promise. If the response is available,
    /// it will be contained within the returned state, and the internal state
    /// of this promise will permanently change to [`PromiseState::Taken`].
    pub fn take(&mut self) -> PromiseState<T> {
        self.update();
        self.state.take()
    }

    /// Wait for the promise to be resolved. The internal state of the
    /// [`Promise`] will not be updated; that requires a follow-up call to one
    /// of the mutable methods.
    ///
    /// To both wait for a result and update the Promise's internal state once
    /// it is available, use [`wait_mut`].
    pub fn wait(&self) -> &Self {
        if !self.state.is_pending() {
            // The result arrived and ownership has been transferred to this
            // promise.
            return self;
        }

        Self::impl_wait(&self.target, None);
        self
    }

    pub fn interruptible_wait(&self, interrupter: &Interrupter) -> &Self
    where
        T: 'static,
    {
        if !self.state.is_pending() {
            // The result arrived and ownership has been transferred to this
            // promise.
            return self;
        }

        if let Some(interrupt) = interrupter.push(self.target.clone()) {
            Self::impl_wait(&self.target, Some(interrupt));
        }

        self
    }

    /// Wait for the promise to be resolved and update the internal state with
    /// the result.
    pub fn wait_mut(&mut self) -> &mut Self {
        if !self.state.is_pending() {
            return self;
        }

        if let Some(mut guard) = Self::impl_wait(&self.target, None) {
            Self::impl_try_take_result(&mut self.state, &mut guard.result);
        }

        self
    }

    pub fn interruptible_wait_mut(
        &mut self,
        interrupter: &Interrupter
    ) -> &mut Self
    where
        T: 'static,
    {
        if !self.state.is_pending() {
            return self;
        }

        if let Some(interrupt) = interrupter.push(self.target.clone()) {
            if let Some(mut guard) = Self::impl_wait(&self.target, Some(interrupt)) {
                Self::impl_try_take_result(&mut self.state, &mut guard.result);
            }
        }

        return self;
    }

    /// Update the internal state of the promise if it is still pending. This
    /// will automatically be done by [`peek`] and [`take`] so there is no
    /// need to call this explicitly unless you want a specific timing for when
    /// to synchronize the internal state.
    pub fn update(&mut self) {
        if self.state.is_pending() {
            match self.target.inner.lock() {
                Ok(mut guard) => {
                    match guard.result.take() {
                        Some(PromiseResult::Available(response)) => {
                            self.state = PromiseState::Available(response);
                        }
                        Some(PromiseResult::Cancelled(cause)) => {
                            self.state = PromiseState::Cancelled(cause);
                        }
                        Some(PromiseResult::Disposed) => {
                            self.state = PromiseState::Disposed;
                        }
                        None => {
                            // Do nothing
                        }
                    }
                }
                Err(_) => {
                    // If the mutex is poisoned, that has to mean the sender
                    // crashed while trying to send the value, so we should
                    // treat it as cancelled.
                    self.state = PromiseState::Disposed;
                }
            }
        }
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        if self.state.is_pending() {
            // We never received the result from the sender so we will trigger
            // the cancellation.
            let f = match self.target.inner.lock() {
                Ok(mut guard) => guard.on_promise_drop.take(),
                Err(_) => None,
            };

            if let Some(f) = f {
                f();
            }
        }
    }
}

impl<T: Unpin> Future for Promise<T> {
    type Output = PromiseState<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_mut = self.get_mut();
        let state = self_mut.take();
        if state.is_pending() {
            match self_mut.target.inner.lock() {
                Ok(mut inner) => {
                    inner.waker = Some(cx.waker().clone());
                }
                Err(_) => { }
            }
            Poll::Pending
        } else {
            Poll::Ready(state)
        }
    }
}

/// The state of a promise.
#[derive(Debug, Clone)]
pub enum PromiseState<T> {
    /// The promise received its result and can be seen in this state.
    Available(T),
    /// The promise is still pending, so you need to keep waiting for the state.
    Pending,
    /// The promise has been cancelled and will never receive a response.
    Cancelled(Cancellation),
    /// The sender was disposed of, so the promise will never receive a response.
    Disposed,
    /// The promise was delivered and has been taken. It will never be available
    /// to take again.
    Taken,
}

impl<T> PromiseState<T> {
    pub fn as_ref(&self) -> PromiseState<&T> {
        match self {
            Self::Available(value) => PromiseState::Available(value),
            Self::Pending => PromiseState::Pending,
            Self::Cancelled(cancellation) => PromiseState::Cancelled(cancellation.clone()),
            Self::Disposed => PromiseState::Disposed,
            Self::Taken => PromiseState::Taken,
        }
    }

    pub fn available(&self) -> Option<&T> {
        match self {
            Self::Available(value) => Some(value),
            _ => None,
        }
    }

    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available(_))
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled(_))
    }

    pub fn cancellation(&self) -> Option<&Cancellation> {
        match self {
            Self::Cancelled(cause) => Some(cause),
            _ => None,
        }
    }

    pub fn is_disposed(&self) -> bool {
        matches!(self, Self::Disposed)
    }

    pub fn is_taken(&self) -> bool {
        matches!(self, Self::Taken)
    }

    pub fn take(&mut self) -> PromiseState<T> {
        let next_value = match self {
            Self::Available(_) => {
                Self::Taken
            }
            Self::Pending => {
                Self::Pending
            }
            Self::Cancelled(cancellation) => {
                Self::Cancelled(cancellation.clone())
            }
            Self::Disposed => {
                Self::Disposed
            }
            Self::Taken => {
                Self::Taken
            }
        };

        std::mem::replace(self, next_value)
    }
}

pub struct Interrupter {
    inner: Arc<Mutex<InterrupterInner>>,
}

impl Interrupter {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(InterrupterInner::new())) }
    }

    /// Tell all waiters that are listening to this Interrupter to interrupt
    /// their waiting.
    ///
    /// Any new waiters added to this Interrupter after this is triggered will
    /// not wait at all until [`Interrupter::reset`] is called for this
    /// Interrupter.
    pub fn interrupt(&self) {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                let mut inner = poisoned.into_inner();
                *inner = InterrupterInner::new();
                return;
            }
        };
        guard.triggered = true;
        for waiter in &*guard.waiters {
            waiter.interrupt.store(true, Ordering::SeqCst);
            waiter.interruptible.interrupt();
        }
        guard.waiters.clear();
    }

    /// If interrupt() has been called on this Interrupter in the past, calling
    /// this function will clear out the after-effect of that, allowing new
    /// waiters to wait for a new call to interrupt() to happen.
    pub fn reset(&self) {
        match self.inner.lock() {
            Ok(mut guard) => {
                guard.triggered = false;
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = InterrupterInner::new();
            }
        }
    }

    fn push<T: 'static>(
        &self,
        target: Arc<PromiseTarget<T>>
    ) -> Option<Arc<AtomicBool>> {
        let mut guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                *guard = InterrupterInner::new();
                guard
            }
        };

        if guard.triggered {
            return None;
        }

        let interruptee = Interruptee {
            interrupt: Arc::new(AtomicBool::new(false)),
            interruptible: target,
        };
        let interrupt = interruptee.interrupt.clone();

        guard.waiters.push(interruptee);
        Some(interrupt)
    }
}

impl Default for Interrupter {
    fn default() -> Self {
        Interrupter::new()
    }
}
