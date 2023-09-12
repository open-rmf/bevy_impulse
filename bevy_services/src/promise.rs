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
    Detached, Held, DispatchCommand, Provider, UnusedTarget, MakeThen,
    MakeFork, MakeMap, Chosen, ApplyLabel,
};

use bevy::prelude::{Entity, Commands};
use std::sync::Arc;

/// After submitting a service request, use [`PromiseCommands`] to describe how
/// the response should be handled. At a minimum, for the response to be
/// delivered, you must choose one of:
/// - `.detach()`: Let the service run to completion and then discard the
///   response data.
/// - `.hold()`: As long as the [`HeldPromise`] or one of its clones is alive,
///   the service will continue running to completion and you will be able to
///   view the response (or take the response, but only once). If all clones of
///   the [`HeldPromise`] are dropped before the service is delivered, it will
///   be canceled.
/// - `detached_hold()`: As long as the [`HeldPromise`] or one of its clones is
///   alive, you will be able to view the response (or take the response, but
///   only once). The service will run to completion even if every clone of the
///   [`HeldPromise`] is dropped.
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
        self.commands.entity(self.target)
            .remove::<UnusedTarget>()
            .insert(Detached);
        self.commands.add(DispatchCommand::new(self.provider, self.target));
    }

    /// Hold onto the promise so you can reference it later. If all copies of
    /// the [`HeldPromise`] are dropped then the service request will
    /// automatically be canceled and the storage for the promise will be freed
    /// up.
    pub fn hold(self) -> HeldPromise<Response> {
        let holding = Arc::new(());
        self.commands.entity(self.target)
            .remove::<UnusedTarget>()
            .insert(Held(Arc::downgrade(&holding)));
        self.commands.add(DispatchCommand::new(self.provider, self.target));
        HeldPromise::new(self.target, holding)
    }

    /// Hold onto the promise so you can reference it later. The service request
    /// will continue to be fulfilled even if you drop all copies of the
    /// [`HeldPromise`]. The storage for the promise will remain available until
    /// all copies of [`HeldPromise`] are dropped.
    ///
    /// This is effectively equivalent to running both [`detach`] and [`hold`].
    pub fn detached_hold(self) -> HeldPromise<Response> {
        self.commands.entity(self.target).insert(Detached);

        let holding = Arc::new(());
        self.commands.entity(self.target)
            .remove::<UnusedTarget>()
            .insert(Held(Arc::downgrade(&holding)));
        self.commands.add(DispatchCommand::new(self.provider, self.target));
        HeldPromise::new(self.target, holding)
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
        let left_target = self.commands.spawn(UnusedTarget).id();
        let right_target = self.commands.spawn(UnusedTarget).id();

        self.commands.add(MakeFork::<Response>::new(
            self.target,
            [left_target, right_target],
        ));
        self.commands.add(DispatchCommand::new(self.provider, self.target));
        f(PromiseCommands::new(self.target, left_target, self.commands));

        PromiseCommands::new(self.target, right_target, self.commands)
    }

    /// Apply a simple callback to the response to change its type. Unlike `.then`
    /// the callback is not a system. This is more efficient for cases where
    /// system parameters don't need to be fetched to perform the transformation.
    ///
    /// You cannot hook into streams or apply a label after using this function,
    /// so perform those operations before calling this.
    pub fn map<U: 'static + Send + Sync>(
        self,
        f: impl FnOnce(Response) -> U + Send + Sync + 'static,
    ) -> PromiseCommands<'w, 's, 'a, U, (), Chosen> {
        self.commands.add(MakeMap::new(self.target, Box::new(f)));
        let map_target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(DispatchCommand::new(self.provider, self.target));
        PromiseCommands::new(self.target, map_target, self.commands)
    }

    /// Use the response of the service as a new service request as soon as the
    /// response is delivered. If you apply a label or hook into streams after
    /// calling this function, then those will be applied to this new service
    /// request.
    pub fn then<U: 'static + Send + Sync, ThenStreams>(
        self,
        service_provider: Provider<Response, U, ThenStreams>
    ) -> PromiseCommands<'w, 's, 'a, U, ThenStreams, ()> {
        let then_target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(MakeThen::<Response>::new(self.target, service_provider.get()));
        self.commands.add(DispatchCommand::new(self.provider, self.target));
        PromiseCommands::new(self.target, then_target, self.commands)
    }

    pub fn then_serve<M>()
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

#[must_use]
pub struct HeldPromise<Response> {
    /// Where will the promised value be stored once it is delivered.
    target: Entity,
    /// Keeps track of whether the promise is being watched for.
    claim: Arc<()>,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response> HeldPromise<Response> {
    /// This is only used internally. To obtain a HeldPromise as a user, use
    /// [`Commands`]`::request(~)` and then call [`PromiseCommands`]`::hold()`.
    fn new(target: Entity, claim: Arc<()>) -> Self {
        Self { target, claim, _ignore: Default::default() }
    }
}

