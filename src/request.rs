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

use bevy_ecs::{
    prelude::{Commands, World},
    world::CommandQueue,
};
use bevy_hierarchy::BuildChildren;

use std::future::Future;

use crate::{
    cancel_impulse, Cancellable, Detached, Impulse, ImpulseMarker, InputCommand, IntoAsyncMap,
    IntoBlockingMapOnce, ProvideOnce, SessionStatus, StreamPack, UnusedTarget,
};

/// Extensions for creating impulse chains by making a request to a provider or
/// serving a value. This is implemented for [`Commands`].
pub trait RequestExt<'w, 's> {
    /// Call this on [`Commands`] to trigger an [impulse](Impulse) to fire.
    ///
    /// Impulses are one-time requests that you can send to a provider. Impulses
    /// can be chained together as a sequence, but you should end the sequence
    /// by [taking][1] the response, [detaching][2] the impulse, or using one of
    /// the other terminating operations mentioned in [the chart](Impulse::detach).
    ///
    /// ```
    /// use bevy_impulse::{prelude::*, testing::*};
    /// let mut context = TestingContext::minimal_plugins();
    /// let mut promise = context.command(|commands| {
    ///     let service = commands.spawn_service(spawn_test_entities);
    ///     commands.request(5, service).take().response
    /// });
    ///
    /// context.run_while_pending(&mut promise);
    /// assert!(promise.peek().is_available());
    /// ```
    ///
    /// [1]: Impulse::take
    /// [2]: Impulse::detach
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

    /// Call this on [`Commands`] to begin building an impulse chain from a
    /// value without calling any provider.
    fn provide<'a, T: 'static + Send + Sync>(&'a mut self, value: T) -> Impulse<'w, 's, 'a, T, ()> {
        self.request(value, provide_value.into_blocking_map_once())
    }

    /// Call this on [`Commands`] to begin building an impulse chain from a
    /// [`Future`] whose [`Future::Output`] will be the item provided to the
    /// target.
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
        let target = self
            .spawn((Detached::default(), UnusedTarget, ImpulseMarker))
            .id();

        let source = self
            .spawn((
                Cancellable::new(cancel_impulse),
                ImpulseMarker,
                SessionStatus::Active,
            ))
            // We set the parent of this source to the target so that when the
            // target gets despawned, this will also be despawned.
            .set_parent(target)
            .id();

        provider.connect(None, source, target, self);
        self.add(InputCommand {
            session: source,
            target: source,
            data: request,
        });

        Impulse {
            source,
            target,
            commands: self,
            _ignore: Default::default(),
        }
    }
}

pub trait RunCommandsOnWorldExt {
    fn command<U>(&mut self, f: impl FnOnce(&mut Commands) -> U) -> U;
}

impl RunCommandsOnWorldExt for World {
    fn command<U>(&mut self, f: impl FnOnce(&mut Commands) -> U) -> U {
        let mut command_queue = CommandQueue::default();
        let mut commands = Commands::new(&mut command_queue, self);
        let u = f(&mut commands);
        command_queue.apply(self);
        u
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
    use crate::{prelude::*, testing::*};

    #[test]
    fn simple_spawn() {
        let mut context = TestingContext::minimal_plugins();
        let mut promise = context.command(|commands| {
            let service = commands.spawn_service(spawn_test_entities);
            commands.request(3, service).take()
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
        assert_eq!(promise.response.take().available(), Some("hello"));
        assert!(context.no_unhandled_errors());
    }
}
