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

use bevy::prelude::{Commands, BuildChildren};

use std::future::Future;

use crate::{
    UnusedTarget, StreamPack, ProvideOnce, IntoBlockingMapOnce, IntoAsyncMap,
    Impulse, Detached, InputCommand, Cancellable, cancel_impulse,
};

/// Extensions for creating impulse chains by making a request to a provider or
/// serving a value. This is implemented for [`Commands`].
pub trait RequestExt<'w, 's> {
    /// Call this on [`Commands`] to begin building a impulse chain by submitting
    /// a request to a provider.
    ///
    /// ```
    /// use bevy_impulse::{*, testing::*};
    /// let mut context = TestingContext::headless_plugins();
    /// let mut promise = context.command(|commands| {
    ///     let service = commands.spawn_service(spawn_cube.into_blocking_service());
    ///
    ///     commands
    ///     .request(SpawnCube { position: Vec3::ZERO, size: 0.1 }, service)
    ///     .take()
    ///     .response
    /// });
    ///
    /// context.run_while_pending(&mut promise);
    /// assert!(promise.peek().is_available());
    /// ```
    #[must_use]
    fn request<'a, P: ProvideOnce>(
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
    fn request<'a, P: ProvideOnce>(
        &'a mut self,
        request: P::Request,
        provider: P,
    ) -> Impulse<'w, 's, 'a, P::Response, P::Streams>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: StreamPack,
    {
        let target = self.spawn((
            Detached::default(),
            UnusedTarget,
        )).id();

        let source = self
            .spawn(Cancellable::new(cancel_impulse))
            // We set the parent of this source to the target so that when the
            // target gets despawned, this will also be despawned.
            .set_parent(target)
            .id();

        provider.connect(None, source, target, self);

        self.add(InputCommand { session: source, target: source, data: request });

        Impulse {
            source,
            target,
            commands: self,
            _ignore: Default::default(),
        }
    }

    fn provide<'a, T: 'static + Send + Sync>(
        &'a mut self,
        value: T,
    ) -> Impulse<'w, 's, 'a, T, ()> {
        self.request(value, provide_value.into_blocking_map_once())
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

fn provide_value<T>(value: T) -> T {
    value
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
        let mut promise = context.command(|commands| {
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
        let mut promise = context.command(|commands| {
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
