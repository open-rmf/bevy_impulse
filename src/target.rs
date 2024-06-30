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

use bevy::prelude::{Commands, Entity, Bundle, Component, Deref, DerefMut};

use std::future::Future;

use crate::{
    Promise, Provider, StreamPack, Detach, TakenResponse, AddOperation,
    IntoBlockingMap, IntoAsyncMap,
};


pub struct Target<'w, 's, 'a, Response, Streams> {
    session: Entity,
    commands: &'a mut Commands<'w, 's>,
    _ignore: std::marker::PhantomData<(Response, Streams)>
}

impl<'w, 's, 'a, Response, Streams> Target<'w, 's, 'a, Response, Streams>
where
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    /// If the target is dropped, the request will not be cancelled.
    pub fn detach(self) -> Target<'w, 's, 'a, Response, Streams> {
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
        self.commands.entity(self.session).insert(bundle);

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
    ) -> Target<'w, 's, 'a, P::Response, P::Streams> {

    }

    /// Apply a one-time callback whose input is the Response of the current
    /// target. The output of the map will become the Response of the returned
    /// target.
    ///
    /// This takes in a regular blocking function, which means all systems will
    /// be blocked from running while the function gets executed.
    pub fn map_block<U>(
        self,
        f: impl FnOnce(Response) -> U + 'static + Send + Sync,
    ) -> Target<'w, 's, 'a, U, ()>
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
    pub fn map_async<Task>(
        self,
        f: impl FnOnce(Response) -> Task + 'static + Send + Sync,
    ) -> Target<'w, 's, 'a, Task::Output, ()>
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

    }

    /// Append the response to the back of a [`Storage<Vec<T>>`] component in an
    /// entity.
    ///
    /// If the entity despawns then the request gets cancelled unless you used
    /// [`Self::detach`] before calling this.
    pub fn append(self, target: Entity) {

    }
}

impl<'w, 's, 'a, Response, Streams> Target<'w, 's, 'a, Response, Streams>
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

pub struct Recipient<Response, Streams: StreamPack> {
    pub response: Promise<Response>,
    pub streams: Streams::Receiver,
}

/// Used to store the response of a
#[derive(Component, Deref, DerefMut)]
pub struct Storage<T>(pub T);
