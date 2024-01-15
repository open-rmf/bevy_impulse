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

use crate::{
    Provider, UnusedTarget, Terminate, PerformOperation,
    Fork, Map, Chosen, ApplyLabel, Servable, Serve, ServeOnce, Stream,
};

use bevy::prelude::{Entity, Commands};

use std::{
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}},
    future::Future, task::{Context, Poll}, pin::Pin
};

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
    pub fn sneak_peek(&self) -> PromiseState<&T> {
        self.state.as_ref()
    }

    /// View the state of the promise. If a response is available, you will
    /// borrow it, but it will continue to be stored inside the promise.
    ///
    /// This requires a mutable reference to the promise because it may try to
    /// update the current state if needed. To peek at that last known state
    /// without trying to synchronize you can use [`sneak_peek()`].
    pub fn peek(&mut self) -> PromiseState<&T> {
        self.update();
        self.state.as_ref()
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
                        Some(PromiseResult::Finished(response)) => {
                            self.state = PromiseState::Available(response);
                        }
                        Some(PromiseResult::Canceled) => {
                            self.state = PromiseState::Canceled;
                        }
                        None => {
                            // Do nothing
                        }
                    }
                }
                Err(_) => {
                    // If the mutex is poisoned, that has to mean the sender
                    // crashed while trying to send the value, so we should
                    // treat it as canceled.
                    self.state = PromiseState::Canceled;
                }
            }
        }
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        if self.state.is_pending() {
            // We never received the result from the sender so we will trigger
            // the cancelation.
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
pub enum PromiseState<T> {
    /// The promise received its result and can be seen in this state.
    Available(T),
    /// The promise is still pending, so you need to keep waiting for the state.
    Pending,
    /// The promise has been canceled and will never receive a response.
    Canceled,
    /// The promise was delivered and has been taken. It will never be available
    /// to take again.
    Taken,
}

impl<T> PromiseState<T> {
    pub fn as_ref(&self) -> PromiseState<&T> {
        match self {
            Self::Available(value) => PromiseState::Available(value),
            Self::Pending => PromiseState::Pending,
            Self::Canceled => PromiseState::Canceled,
            Self::Taken => PromiseState::Taken,
        }
    }

    pub fn is_received(&self) -> bool {
        matches!(self, Self::Available(_))
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    pub fn is_canceled(&self) -> bool {
        matches!(self, Self::Canceled)
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
            Self::Canceled => {
                Self::Canceled
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

/// After submitting a service request, use [`PromiseCommands`] to describe how
/// the response should be handled. At a minimum, for the response to be
/// delivered, you must choose one of:
/// - `.detach()`: Let the service run to completion and then discard the
///   response data.
/// - `.take()`: As long as the [`Promise`] or one of its clones is alive,
///   the service will continue running to completion and you will be able to
///   view the response (or take the response, but only once). If all clones of
///   the [`Promise`] are dropped before the service is delivered, it will
///   be canceled.
/// - `detached_take()`: As long as the [`Promise`] or one of its clones is
///   alive, you will be able to view the response (or take the response, but
///   only once). The service will run to completion even if every clone of the
///   [`Promise`] is dropped.
///
/// If you do not select one of the above then the service request will be
/// canceled without ever attempting to run.
#[must_use]
pub struct PromiseCommands<'w, 's, 'a, Response, Streams, L> {
    provider: Entity,
    target: Entity,
    commands: &'a mut Commands<'w, 's>,
    response: std::marker::PhantomData<Response>,
    streams: std::marker::PhantomData<Streams>,
    labeling: std::marker::PhantomData<L>,
}

impl<'w, 's, 'a, Response: 'static + Send + Sync, Streams, L> PromiseCommands<'w, 's, 'a, Response, Streams, L> {
    /// Have the service run until it is finished without holding onto any
    /// promise. Immediately after the service is finished, the storage for the
    /// promise will automatically be freed up.
    pub fn detach(self) {
        self.commands.add(PerformOperation::new(
            self.target,
            Terminate::<Response>::new(None, true),
        ));
    }

    /// Take the promise so you can reference it later. If all copies of the
    /// [`Promise`] are dropped then the service request will automatically
    /// be canceled and the storage for the promise will be freed up.
    pub fn take(self) -> Promise<Response> {
        let (promise, sender) = Promise::new();
        self.commands.add(PerformOperation::new(
            self.target,
            Terminate::new(Some(sender), false),
        ));
        promise
    }

    /// Take the promise so you can reference it later. The service request
    /// will continue to be fulfilled even if you drop all copies of the
    /// [`Promise`]. The storage for the promise will remain available until
    /// all copies of [`Promise`] are dropped.
    ///
    /// This is effectively equivalent to running both [`detach`] and [`hold`].
    pub fn detached_take(self) -> Promise<Response> {
        let (promise, sender) = Promise::new();
        self.commands.add(PerformOperation::new(
            self.target,
            Terminate::new(Some(sender), true),
        ));
        promise
    }

    /// When the response is delivered, we will make a clone of it and
    /// simultaneously pass that clone along two different delivery chains: one
    /// determined by the `f` callback provided to this function and the other
    /// determined by the [`PromiseCommands`] that gets returned by this function.
    ///
    /// This can only be applied when the Response can be cloned.
    ///
    /// You cannot hook into streams or apply a label after using this function,
    /// so perform those operations before calling this.
    pub fn fork(
        self,
        f: impl FnOnce(PromiseCommands<'w, 's, '_, Response, (), ()>),
    ) -> PromiseCommands<'w, 's, 'a, Response, (), Chosen>
    where
        Response: Clone,
    {
        let source = self.target;
        let left_target = self.commands.spawn(UnusedTarget).id();
        let right_target = self.commands.spawn(UnusedTarget).id();

        self.commands.add(PerformOperation::new(
            source,
            Fork::<Response>::new([left_target, right_target]),
        ));

        f(PromiseCommands::new(self.target, left_target, self.commands));
        PromiseCommands::new(self.target, right_target, self.commands)
    }

    /// Apply a simple callback to the response to change its type. Unlike `.then`
    /// the callback is not a system. This is more efficient for cases where
    /// system parameters don't need to be fetched to perform the transformation.
    ///
    /// You cannot hook into streams or apply a label after using this function,
    /// so perform those operations before calling this.
    pub fn map<MappedResponse: 'static + Send + Sync>(
        self,
        f: impl FnOnce(Response) -> MappedResponse + Send + Sync + 'static,
    ) -> PromiseCommands<'w, 's, 'a, MappedResponse, (), Chosen> {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(PerformOperation::new(source, Map::new(target, Box::new(f))));
        PromiseCommands::new(source, target, self.commands)
    }

    /// Use the response of the service as a new service request as soon as the
    /// response is delivered. If you apply a label or hook into streams after
    /// calling this function, then those will be applied to this new service
    /// request.
    pub fn then<ThenResponse: 'static + Send + Sync, ThenStreams: Stream>(
        self,
        service_provider: Provider<Response, ThenResponse, ThenStreams>
    ) -> PromiseCommands<'w, 's, 'a, ThenResponse, ThenStreams, ()> {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(PerformOperation::new(source, Serve::new(service_provider, target)));
        PromiseCommands::new(source, target, self.commands)
    }

    /// Use the response of the service as a request into a [`Servable`] object
    /// as soon as the response is delivered.
    ///
    /// [`Servable`] objects cannot accept labels because they are not able to
    /// do any queueing.
    pub fn then_serve<M, ThenServe>(
        self,
        servable: ThenServe,
    ) -> PromiseCommands<'w, 's, 'a, ThenServe::Response, ThenServe::Streams, Chosen>
    where
        M: 'static + Send + Sync,
        ThenServe: 'static + Send + Sync + Servable<M>,
        ThenServe::Request: 'static + Send + Sync,
        ThenServe::Response: 'static + Send + Sync,
    {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(PerformOperation::new(source, ServeOnce::new(servable, target)));
        PromiseCommands::new(source, target, self.commands)
    }
}

impl<'w, 's, 'a, Response: 'static + Send + Sync, Streams> PromiseCommands<'w, 's, 'a, Response, Streams, ()> {
    /// Apply a label to the request. For more information about request labels
    /// see [`crate::LabelBuilder`].
    pub fn label(
        self,
        label: impl ApplyLabel,
    ) -> PromiseCommands<'w, 's, 'a, Response, Streams, Chosen> {
        label.apply(&mut self.commands.entity(self.target));
        PromiseCommands::new(self.provider, self.target, self.commands)
    }
}

impl<'w, 's, 'a, Response: 'static + Send + Sync, Streams, L> PromiseCommands<'w, 's, 'a, Response, Streams, L> {
    /// Used internally to create a [`PromiseCommands`] that can accept a label
    /// and hook into streams.
    pub(crate) fn new(
        provider: Entity,
        target: Entity,
        commands: &'a mut Commands<'w, 's>,
    ) -> Self {
        Self {
            provider,
            target,
            commands,
            response: Default::default(),
            streams: Default::default(),
            labeling: Default::default(),
        }
    }
}
