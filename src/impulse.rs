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
    Promise, Provider, StreamPack, Detach, TakenResponse, AddOperation,
    IntoBlockingMap, IntoAsyncMap, ImpulseProperties, UnusedTarget, StoreResponse,
    PushResponse,
};

pub mod traits;
pub use traits::*;

/// Impulses can be chained as a simple sequence of [providers](Provider).
pub struct Impulse<'w, 's, 'a, Response, Streams> {
    pub(crate) source: Entity,
    pub(crate) session: Entity,
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
        self.commands.add(Detach { session: self.session });
        self
    }

    /// Take the data that comes out of the request.
    #[must_use]
    pub fn take(self) -> Recipient<Response, Streams> {
        let (response_sender, response_promise) = Promise::<Response>::new();
        self.commands.add(AddOperation::new(
            self.session,
            TakenResponse::<Response>::new(response_sender),
        ));
        let (bundle, stream_receivers) = Streams::make_receiver(self.session, self.commands);
        self.commands.entity(self.source).insert(bundle);

        Recipient {
            response: response_promise,
            streams: stream_receivers,
        }
    }

    /// Pass the outcome of the request to another provider.
    #[must_use]
    pub fn then<P: Provider<Request = Response>>(
        self,
        provider: P,
    ) -> Impulse<'w, 's, 'a, P::Response, P::Streams> {
        let source = self.session;
        let session = self.commands.spawn((
            ImpulseProperties::new(),
            UnusedTarget,
        )).id();

        // We should automatically delete the previous step in the chain once
        // this one is finished.
        self.commands.entity(source).set_parent(session);
        provider.connect(source, session, self.commands);
        Impulse {
            source,
            session,
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
    /// the Response of the returned target.
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
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    pub fn store(self, target: Entity) {
        self.commands.add(AddOperation::new(
            self.session,
            StoreResponse::<Response>::new(target),
        ));
    }

    /// Push the response to the back of a [`Storage<Vec<T>>`] component in an
    /// entity.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    pub fn push(self, target: Entity) {
        self.commands.add(AddOperation::new(
            self.session,
            PushResponse::<Response>::new(target),
        ));
    }

    // TODO(@mxgrey): Offer an on_cancel method that lets users provide a
    // callback to be triggered when a cancellation happens.
}

impl<'w, 's, 'a, Response, Streams> Impulse<'w, 's, 'a, Response, Streams>
where
    Response: Bundle,
{
    /// Insert the response as a bundle in the specified entity.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    ///
    /// If the response is not a bundle then you can store it in an entity using
    /// [`Self::store`] or [`Self::append`]. Alternatively you can transform it
    /// into a bundle using [`Self::map_block`] or [`Self::map_async`].
    pub fn insert(self, target: Entity) {

    }
}

impl<'w, 's, 'a, Response, Streams> Impulse<'w, 's, 'a, Response, Streams>
where
    Response: Event,
{
    /// Send the response out as an event once it is ready. Using this will also
    /// effectively [detach](Self::detach) the impulse.
    pub fn send(self) {

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

/// Used to collect responses from multiple impulse chains into a container.
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
