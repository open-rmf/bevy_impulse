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
    OutputChain, UnusedTarget, InputBundle, StreamPack, Provider,
    ModifiersUnset, AddOperation, Noop, IntoAsyncMap, Promise,
};

use bevy::prelude::{Commands, Entity, Bundle};

use std::future::Future;

mod internal;
pub use internal::{ApplyLabel, BuildLabel};

pub trait RequestExt<'w, 's> {
    /// Call this on [`Commands`] to begin building a impulse chain by submitting
    /// a request to a provider.
    ///
    /// ```
    /// use bevy_impulse::{*, testing::*};
    /// let mut context = TestingContext::headless_plugins();
    /// let mut promise = context.build(|commands| {
    ///     let request = SpawnCube { position: Vec3::ZERO, size: 0.1 };
    ///     let service = commands.spawn_service(spawn_cube.into_blocking_service());
    ///
    ///     commands
    ///     .request(request, service)
    ///     .take()
    /// });
    ///
    /// context.run_while_pending(&mut promise);
    /// assert!(promise.peek().is_available());
    /// ```
    fn request<'a, P: Provider>(
        &'a mut self,
        request: P::Request,
        provider: P,
    ) -> Segment<'w, 's, 'a, P::Response, P::Streams, ModifiersUnset>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: StreamPack;

    /// Call this on [`Commands`] to begin building an impulse chain from a value
    /// without calling any provider.
    fn provide<'a, T: 'static + Send + Sync>(
        &'a mut self,
        value: T,
    ) -> OutputChain<'w, 's, 'a, T>;

    /// Call this on [`Commands`] to begin building an impulse chain from a [`Future`]
    /// whose [`Future::Output`] will be the first item provided in the chain.
    fn serve<'a, T: 'static + Send + Sync + Future>(
        &'a mut self,
        future: T,
    ) -> OutputChain<'w, 's, 'a, T::Output>
    where
        T::Output: 'static + Send + Sync;
}

impl<'w, 's> RequestExt<'w, 's> for Commands<'w, 's> {
    fn request<'a, P: Provider>(
        &'a mut self,
        request: P::Request,
        provider: P,
    ) -> Segment<'w, 's, 'a, P::Response, P::Streams, ModifiersUnset>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: StreamPack,
    {
        let source = self.spawn(InputBundle::new(request)).id();
        let target = self.spawn(UnusedTarget).id();
        provider.provide(source, target, self);

        Segment::new(source, target, self)
    }

    fn provide<'a, T: 'static + Send + Sync>(
        &'a mut self,
        value: T,
    ) -> OutputChain<'w, 's, 'a, T> {
        let source = self.spawn(InputBundle::new(value)).id();
        let target = self.spawn(UnusedTarget).id();

        self.add(AddOperation::new(
            source, Noop::<T>::new(target),
        ));
        Segment::new(source, target, self)
    }

    fn serve<'a, T: 'static + Send + Sync + Future>(
        &'a mut self,
        future: T,
    ) -> OutputChain<'w, 's, 'a, T::Output>
    where
        T::Output: 'static + Send + Sync,
    {
        self.request(future, async_server.into_async_map()).output()
    }
}

async fn async_server<T: Future>(value: T) -> T::Output {
    value.await
}

pub struct Target<Response, Streams> {
    session: Entity,
    _ignore: std::marker::PhantomData<(Response, Streams)>
}

impl<Response, Streams> Target<Response, Streams> {
    /// If the target is dropped, the request will not be cancelled.
    pub fn detach(self) -> Target<Response, Streams> {

    }

    /// Take the data that comes out of the request.
    #[must_use]
    pub fn take(self) -> Recipient<Response, Streams> {

    }

    /// Pass the outcome of the request to another provider.
    #[must_use]
    pub fn then<P: Provider<Request = Response>>(
        self,
        provider: Provider,
    ) -> Target<P::Response, P::Streams> {

    }
}

impl<Response, Streams> Target<Response, Streams>
where
    Response: Bundle,
{
    pub fn store(self, target: Entity) {

    }
}

pub struct Recipient<Response, Streams: StreamPack> {
    pub response: Promise<Response>,
    pub streams: Streams::Receiver,
}

#[cfg(test)]
mod tests {
    use crate::{*, testing::*};

    #[test]
    fn simple_spawn() {
        let mut context = TestingContext::headless_plugins();
        let mut promise = context.build(|commands| {
            let request = SpawnCube { position: Vec3::ZERO, size: 0.1 };
            let service = commands.spawn_service(spawn_cube.into_blocking_service());

            commands
                .request(request, service)
                .take()
        });

        context.run_with_conditions(
            &mut promise,
            FlushConditions::new().with_update_count(2),
        );
        assert!(promise.peek().is_available());
    }

    #[test]
    fn simple_serve() {
        use async_std::future;
        use std::time::Duration;

        let mut context = TestingContext::minimal_plugins();
        let mut promise = context.build(|commands| {
            let future = async {
                let never = future::pending::<()>();
                let _ = future::timeout(Duration::from_secs_f32(0.01), never);
                "hello"
            };

            commands.serve(future).take()
        });

        context.run_with_conditions(
            &mut promise,
            FlushConditions::new().with_timeout(Duration::from_secs_f32(5.0)),
        );
        assert_eq!(promise.peek().available().copied(), Some("hello"));
    }
}
