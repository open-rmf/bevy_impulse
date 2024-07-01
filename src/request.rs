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
    UnusedTarget, InputCommand, StreamPack, Provider, IntoAsyncMap, Impulse,
    ImpulseProperties,
};

use bevy::prelude::{Commands, BuildChildren};

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
    #[must_use]
    fn request<'a, P: Provider>(
        &'a mut self,
        request: P::Request,
        provider: P,
    ) -> Impulse<'w, 's, 'a, P::Response, P::Streams>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: StreamPack;

    /// Call this on [`Commands`] to begin building an impulse chain from a value
    /// without calling any provider.
    fn provide<'a, T: 'static + Send + Sync>(
        &'a mut self,
        value: T,
    ) -> Impulse<'w, 's, 'a, T, ()>;

    /// Call this on [`Commands`] to begin building an impulse chain from a
    /// [`Future`] whose [`Future::Output`] will be the item provided to the
    /// target.
    fn serve<'a, T: 'static + Send + Sync + Future>(
        &'a mut self,
        future: T,
    ) -> Impulse<'w, 's, 'a, T::Output, ()>
    where
        T::Output: 'static + Send + Sync;
}

impl<'w, 's> RequestExt<'w, 's> for Commands<'w, 's> {
    fn request<'a, P: Provider>(
        &'a mut self,
        request: P::Request,
        provider: P,
    ) -> Impulse<'w, 's, 'a, P::Response, P::Streams>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: StreamPack,
    {
        let session = self.spawn((
            ImpulseProperties::new(),
            UnusedTarget,
        )).id();

        let source = self.spawn(())
            // We set the parent of this source to the target so that when the
            // target gets despawned, this will also be despawned.
            .set_parent(session)
            .id();

        provider.connect(source, session, self);

        self.add(InputCommand { session, target: source, data: request });

        Impulse {
            source,
            session,
            commands: self,
            _ignore: Default::default(),
        }
    }

    fn provide<'a, T: 'static + Send + Sync>(
        &'a mut self,
        value: T,
    ) -> Impulse<'w, 's, 'a, T, ()> {
        let session = self.spawn((
            ImpulseProperties::new(),
            UnusedTarget,
        )).id();

        self.add(InputCommand { session, target: session, data: value });

        Impulse {
            session,
            // The source field won't actually matter for impulse produced by
            // this provide method, so we'll just use the session value as a
            // placeholder
            source: session,
            commands: self,
            _ignore: Default::default(),
        }
    }

    fn serve<'a, T: 'static + Send + Sync + Future>(
        &'a mut self,
        future: T,
    ) -> Impulse<'w, 's, 'a, T::Output, ()>
    where
        T::Output: 'static + Send + Sync,
    {
        self.request(future, async_server.into_async_map())
    }
}

async fn async_server<T: Future>(value: T) -> T::Output {
    value.await
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
            &mut promise.response,
            FlushConditions::new().with_update_count(2),
        );
        assert!(promise.response.peek().is_available());
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
            &mut promise.response,
            FlushConditions::new().with_timeout(Duration::from_secs_f32(5.0)),
        );
        assert_eq!(promise.response.peek().available().copied(), Some("hello"));
    }
}
