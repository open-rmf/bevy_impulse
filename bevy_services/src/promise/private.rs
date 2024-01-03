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

use std::{task::Waker, sync::{Arc, Weak, Mutex, Condvar}, any::Any};

pub(crate) struct Sender<Response> {
    target: Weak<PromiseTarget<Response>>,
    sent: bool,
}

/// Tracks whether there is any expectation for the Sender to deliver on its
/// promise (i.e. whether the promise still has a target) without any generics.
pub(crate) struct Expectation {
    target: Weak<dyn Any + Send + Sync + 'static>,
}

impl<T> Sender<T> {
    pub(crate) fn new(target: Weak<PromiseTarget<T>>) -> Self {
        Self { target, sent: false }
    }

    pub(crate) fn send(mut self, value: T) -> Result<(), T> {
        let result = self.set(PromiseResult::Finished(value));
        self.sent = true;
        result
    }

    pub(crate) fn set(&mut self, result: PromiseResult<T>) -> Result<(), T> {
        let Some(target) = self.target.upgrade() else {
            match result {
                PromiseResult::Finished(value) => return Err(value),
                PromiseResult::Canceled => return Ok(()),
            }
        };
        let mut inner = match target.inner.lock() {
            Ok(inner) => inner,
            Err(poisoned) => poisoned.into_inner(),
        };
        inner.result = Some(result);
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
        target.cv.notify_all();
        Ok(())
    }

    pub(crate) fn on_cancel(&mut self, f: impl FnOnce() + 'static + Send) {
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

    pub(crate) fn expectation(&self) -> Expectation
    where
        T: Send + 'static
    {
        Expectation { target: self.target.clone() }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if !self.sent {
            self.set(PromiseResult::Canceled);
        }
    }
}

impl<T> Promise<T> {
    pub(crate) fn new() -> (Self, Sender<T>) {
        let target = Arc::new(PromiseTarget::new());
        let sender = Sender::new(Arc::downgrade(&target));
        let promise = Self { state: PromiseState::Pending, target };
        (promise, sender)
    }
}

pub(crate) enum PromiseResult<T> {
    Finished(T),
    Canceled,
}

pub(crate) struct PromiseTargetInner<T> {
    pub(crate) result: Option<PromiseResult<T>>,
    pub(crate) waker: Option<Waker>,
    pub(crate) on_promise_drop: Option<Box<dyn FnOnce() + 'static + Send>>,
}

impl<T> PromiseTargetInner<T> {
    pub(crate) fn new() -> Self {
        Self { result: None, waker: None, on_promise_drop: None }
    }
}

pub(crate) struct PromiseTarget<T> {
    pub(crate) inner: Mutex<PromiseTargetInner<T>>,
    pub(crate) cv: Condvar,
}

impl<T> PromiseTarget<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(PromiseTargetInner::new()),
            cv: Condvar::new(),
        }
    }
}
