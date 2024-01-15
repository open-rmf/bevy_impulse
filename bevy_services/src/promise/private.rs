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

use crate::{Promise, PromiseState};

use std::{
    task::Waker,
    sync::{Arc, Weak, Mutex, Condvar, atomic::{AtomicBool, Ordering}},
};

impl<T> Promise<T> {
    pub(super) fn impl_wait(&self, interrupt: Option<Arc<AtomicBool>>) {
        if !self.state.is_pending() {
            return;
        }

        let guard = match self.target.inner.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return;
            }
        };
        self.target.cv.wait_while(guard, |inner| {
            if interrupt.is_some_and(
                |interrupt| interrupt.load(Ordering::Relaxed)
            ) {
                return false;
            }
            inner.result.is_none()
        });
    }

    pub(super) fn impl_try_take_result(
        state: &mut PromiseState<T>,
        result: &mut Option<PromiseResult<T>>,
    ) -> bool {
        match result.take() {
            Some(PromiseResult::Finished(response)) => {
                *state = PromiseState::Received(response);
                return false;
            }
            Some(PromiseResult::Canceled) => {
                *state = PromiseState::Canceled;
                return false;
            }
            None => {
                return true;
            }
        }
    }
}

pub(super) trait Interruptible {
    fn interrupt(&self);
}

pub(super) struct Sender<Response> {
    target: Weak<PromiseTarget<Response>>,
    sent: bool,
}

impl<T> Sender<T> {
    pub(super) fn new(target: Weak<PromiseTarget<T>>) -> Self {
        Self { target, sent: false }
    }

    pub(super) fn send(mut self, value: T) {
        self.set(PromiseResult::Finished(value));
        self.sent = true;
    }

    pub(super) fn set(&mut self, result: PromiseResult<T>) {
        let Some(target) = self.target.upgrade() else {
            return;
        };
        let Ok(mut inner) = target.inner.lock() else {
            return;
        };
        inner.result = Some(result);
        if let Some(waker) = inner.waker {
            waker.wake();
        }
        target.cv.notify_all();
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.sent {
            return;
        }
        self.set(PromiseResult::Canceled);
    }
}

impl<T> Promise<T> {
    pub(super) fn new() -> (Self, Sender<T>) {
        let target = Arc::new(PromiseTarget::new());
        let sender = Sender::new(Arc::downgrade(&target));
        let promise = Self { state: PromiseState::Pending, target };
        (promise, sender)
    }
}

pub(super) enum PromiseResult<T> {
    Finished(T),
    Canceled,
}

pub(super) struct PromiseTargetInner<T> {
    pub(super) result: Option<PromiseResult<T>>,
    pub(super) waker: Option<Waker>,
}

impl<T> PromiseTargetInner<T> {
    pub(super) fn new() -> Self {
        Self { result: None, waker: None }
    }
}

pub(super) struct PromiseTarget<T> {
    pub(super) inner: Mutex<PromiseTargetInner<T>>,
    /// Condition variable used to notify waiting Promises that a result is
    /// available or that an interrupt has been triggered.
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

impl<T> Interruptible for PromiseTarget<T> {
    fn interrupt(&self) {
        self.cv.notify_all();
    }
}

pub(super) struct Interruptee {
    pub(super) interrupt: Arc<AtomicBool>,
    pub(super) interruptible: Arc<dyn Interruptible>,
}
