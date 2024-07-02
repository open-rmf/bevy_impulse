/*
 * Copyright (C) 2024 Open Source Robotics Foundation
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

use bevy::prelude::{
    Commands, Entity, Bundle, Component, BuildChildren, Event,
};

use std::future::Future;

use crate::{
    Promise, Provider, StreamPack, IntoBlockingMap, IntoAsyncMap, UnusedTarget,
};

mod detach;
pub(crate) use detach::*;

mod insert;
pub(crate) use insert::*;

mod internal;
pub(crate) use internal::*;

mod push;
pub(crate) use push::*;

mod send_event;
pub(crate) use send_event::*;

mod store;
pub(crate) use store::*;

mod taken;
pub(crate) use taken::*;

/// Impulses can be chained as a simple sequence of [providers](Provider).
pub struct Impulse<'w, 's, 'a, Response, Streams> {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
    pub(crate) commands: &'a mut Commands<'w, 's>,
    pub(crate) _ignore: std::marker::PhantomData<(Response, Streams)>
}

impl<'w, 's, 'a, Response, Streams> Impulse<'w, 's, 'a, Response, Streams>
where
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    /// Keep carrying out the impulse chain up to here even if a downstream
    /// dependent was dropped.
    pub fn detach(self) -> Impulse<'w, 's, 'a, Response, Streams> {
        self.commands.add(Detach { session: self.target });
        self
    }

    /// Take the data that comes out of the request, including both the response
    /// and the streams.
    #[must_use]
    pub fn take(self) -> Recipient<Response, Streams> {
        let (response_sender, response_promise) = Promise::<Response>::new();
        self.commands.add(AddImpulse::new(
            self.target,
            TakenResponse::<Response>::new(response_sender),
        ));
        let (bundle, stream_receivers) = Streams::take_streams(self.target, self.commands);
        self.commands.entity(self.source).insert(bundle);

        Recipient {
            response: response_promise,
            streams: stream_receivers,
        }
    }

    /// Take only the response data that comes out of the request.
    #[must_use]
    pub fn take_response(self) -> Promise<Response> {
        let (response_sender, response_promise) = Promise::<Response>::new();
        self.commands.add(AddImpulse::new(
            self.target,
            TakenResponse::<Response>::new(response_sender),
        ));
        response_promise
    }

    /// Pass the outcome of the request to another provider.
    #[must_use]
    pub fn then<P: Provider<Request = Response>>(
        self,
        provider: P,
    ) -> Impulse<'w, 's, 'a, P::Response, P::Streams> {
        let source = self.target;
        let session = self.commands.spawn((
            Detached::default(),
            UnusedTarget,
        )).id();

        // We should automatically delete the previous step in the chain once
        // this one is finished.
        self.commands.entity(source).set_parent(session);
        provider.connect(source, session, self.commands);
        Impulse {
            source,
            target: session,
            commands: self.commands,
            _ignore: Default::default(),
        }
    }

    /// Apply a one-time callback whose input is the Response of the current
    /// target. The output of the map will become the Response of the returned
    /// target.
    ///
    /// This takes in a regular blocking function, which means all systems will
    /// be blocked from running while the function gets executed.
    #[must_use]
    pub fn map_block<U>(
        self,
        f: impl FnOnce(Response) -> U + 'static + Send + Sync,
    ) -> Impulse<'w, 's, 'a, U, ()>
    where
        U: 'static + Send + Sync,
    {
        self.then(f.into_blocking_map())
    }

    /// Apply a one-time callback whose output is a [`Future`] that will be run
    /// in the [`AsyncComputeTaskPool`][1]. The output of the [`Future`] will be
    /// the Response of the returned Impulse.
    ///
    /// [1]: bevy::tasks::AsyncComputeTaskPool
    #[must_use]
    pub fn map_async<Task>(
        self,
        f: impl FnOnce(Response) -> Task + 'static + Send + Sync,
    ) -> Impulse<'w, 's, 'a, Task::Output, ()>
    where
        Task: Future + 'static + Send + Sync,
        Task::Output: 'static + Send + Sync,
    {
        self.then(f.into_async_map())
    }

    /// Store the response in a [`Storage`] component in the specified entity.
    ///
    /// Each stream will be collected into [`Collection`] components in the
    /// specified entity, one for each stream type. To store the streams in a
    /// different entity, call [`Self::collect_streams`] before this.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    pub fn store(self, target: Entity) {
        self.commands.add(AddImpulse::new(
            self.target,
            Store::<Response>::new(target),
        ));

        let stream_targets = Streams::collect_streams(
            self.source, target, self.commands,
        );
        self.commands.entity(self.source).insert(stream_targets);
    }

    /// Collect the stream data into [`Collection<T>`] components in the
    /// specified target, one collection for each stream data type. You must
    /// still decide what to do with the final response data.
    #[must_use]
    pub fn collect_streams(self, target: Entity) -> Impulse<'w, 's, 'a, Response, ()> {
        let stream_targets = Streams::collect_streams(
            self.source, target, self.commands,
        );
        self.commands.entity(self.source).insert(stream_targets);

        Impulse {
            source: self.source,
            target: self.target,
            commands: self.commands,
            _ignore: Default::default(),
        }
    }

    /// Push the response to the back of a [`Collection<T>`] component in an
    /// entity.
    ///
    /// Similar to [`Self::store`] this will also collect streams into this
    /// entity.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    pub fn push(self, target: Entity) {
        self.commands.add(AddImpulse::new(
            self.target,
            Push::<Response>::new(target, false),
        ));

        let stream_targets = Streams::collect_streams(
            self.source, target, self.commands,
        );
        self.commands.entity(self.source).insert(stream_targets);
    }

    // TODO(@mxgrey): Consider offering ways for users to respond to cancellations.
    // For example, offer an on_cancel method that lets users provide a callback
    // to be triggered when a cancellation happens. Or focus on terminal impulses,
    // like offer store_or_else(~), push_or_else(~) etc which accept a callback
    // that will be triggered after a cancellation.
}

impl<'w, 's, 'a, Response, Streams> Impulse<'w, 's, 'a, Response, Streams>
where
    Response: Bundle,
{
    /// Insert the response as a bundle in the specified entity. Stream data
    /// will be dropped unless you use [`Self::collect_streams`] before this.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    ///
    /// If the response is not a bundle then you can store it in an entity using
    /// [`Self::store`] or [`Self::append`]. Alternatively you can transform it
    /// into a bundle using [`Self::map_block`] or [`Self::map_async`].
    pub fn insert(self, target: Entity) {
        self.commands.add(AddImpulse::new(
            self.target,
            Insert::<Response>::new(target),
        ));
    }
}

impl<'w, 's, 'a, Response, Streams> Impulse<'w, 's, 'a, Response, Streams>
where
    Response: Event,
{
    /// Send the response out as an event once it is ready. Stream data will be
    /// dropped unless you use [`Self::collect_streams`] before this.
    ///
    /// Using this will also effectively [detach](Self::detach) the impulse.
    pub fn send_event(self) {
        self.commands.add(AddImpulse::new(
            self.target,
            SendEvent::<Response>::new(),
        ));
    }
}

pub struct Recipient<Response, Streams: StreamPack> {
    pub response: Promise<Response>,
    pub streams: Streams::Receiver,
}

/// Used to store a response of an impulse as a component of an entity.
#[derive(Component)]
pub struct Storage<T> {
    pub data: T,
    pub session: Entity,
}

/// Used to collect responses from multiple impulse chains into a container
/// attached to an entity.
//
// TODO(@mxgrey): Consider allowing the user to choose the container type.
#[derive(Component)]
pub struct Collection<T> {
    /// The items that have been collected.
    pub items: Vec<Storage<T>>,
}

impl<T> Default for Collection<T> {
    fn default() -> Self {
        Self { items: Default::default() }
    }
}