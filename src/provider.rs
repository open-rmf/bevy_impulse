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

use bevy_ecs::prelude::{Commands, Entity};

/// The `Provider` trait encapsulates the idea of providing a response to a
/// request. There are three different provider types with different advantages:
/// - [`Service`](crate::Service)
///   - Stored in the [`World`][4], associated with its own unique [`Entity`]
///   - You can add [`Components`][1] to the [`Entity`] of the service to "configure" the service
///   - You can lookup services in the `World` using the [`ServiceDiscovery`](crate::ServiceDiscovery) parameter
///   - Services can view and modify anything in the `World` while running
/// - [`Callback`](crate::Callback)
///   - Exist as objects that can be shared and passed around, not associated with an [`Entity`]
///   - Can be stored inside [`Components`][1] or [`Resources`][2]
///   - Callbacks can view and modify anything in the `World` while running
/// - [`Map`](crate::AsMap)
///   - Simple function to transform data
///   - Any single-input function, regular or async, can be used as a map
///   - Cannot view or modify the world (except by using the [async channel][3]) but has less overhead than `Services` or `Callbacks`
///
/// Types that implement this trait can be used to create nodes in a workflow or
/// impulses in an impulse chain.
///
/// Across these three types of providers there are three flavors to choose from:
///
/// | Type        | Description  | Advantage | Service | Callback | Map |
/// | ------------|--------------|-----------|---------|----------|-----|
/// | Blocking    | Execute one-at-a-time, preventing the execution of any other systems while running. | No thread-switching overhead. <br> A sequence of blocking providers will finish in one flush. | ✅ | ✅ | ✅ |
/// | Async       | Executed in an async thread pool. <br> Able to modify the [`World`][4] using a [`Channel`][5] while running in the thread pool. | Long-running tasks won't block other systems from running. <br> Can use `.await`, making them good for filesystem or network i/o. | ✅ | ✅ | ✅ |
/// | Continuous  | Run every update cycle inside a specified [`App`](bevy_app::App) schedule. <br> Receive a queue of ongoing requests to fulfill. | Can run in parallel with other Bevy systems according to normal schedule logic. <br> Good for services that need to incrementally fulfill requests. | ✅ | ❌ | ❌ |
///
/// [1]: bevy_ecs::prelude::Component
/// [2]: bevy_ecs::prelude::Resource
/// [3]: crate::AsyncMap::channel
/// [4]: bevy_ecs::prelude::World
/// [5]: crate::Channel
pub trait Provider: ProvideOnce {}

/// Similar to [`Provider`] but can be used for functions that are only able to
/// run once, e.g. that use [`FnOnce`]. Because of this, [`ProvideOnce`] is
/// suitable for impulses, but not for workflows.
pub trait ProvideOnce {
    type Request;
    type Response;
    type Streams;

    /// Take a request from a source (stream target information will also be
    /// extracted from the source) and connect it to a target.
    fn connect(
        self,
        scope: Option<Entity>,
        source: Entity,
        target: Entity,
        commands: &mut Commands,
    );
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*};
    use bevy_ecs::{prelude::*, system::SystemState};
    use std::future::Future;

    #[test]
    fn test_exclusive_systems_as_services() {
        let mut context = TestingContext::minimal_plugins();

        let _blocking_exclusive_system_srv = context
            .app
            .spawn_service(blocking_exclusive_system.into_blocking_service());
        let _blocking_exclusive_system_with_params_srv = context
            .app
            .spawn_service(blocking_exclusive_system_with_param.into_blocking_service());
        let _blocking_exclusive_service = context.app.spawn_service(blocking_exclusive_service);
        let _blocking_exclusive_service_with_params = context
            .app
            .spawn_service(blocking_exclusive_service_with_params);

        let _blocking_exclusive_system_cb = blocking_exclusive_system.into_blocking_callback();
        let _blocking_exclusive_system_with_params_cb =
            blocking_exclusive_system_with_param.into_blocking_callback();
        let _blocking_exclusive_callback = blocking_exclusive_callback.as_callback();
        let _blocking_exclusive_callback_with_param =
            blocking_exclusive_callback_with_param.as_callback();

        let _async_exclusive_system_srv = context
            .app
            .spawn_service(async_exclusive_system.into_async_service());
        let _async_exclusive_system_with_param_srv = context
            .app
            .spawn_service(async_exclusive_system_with_param.into_async_service());
        let _async_exclusive_service = context.app.spawn_service(async_exclusive_service);
        let _async_exclusive_service_with_param = context
            .app
            .spawn_service(async_exclusive_service_with_param);

        let _async_exclusive_system_cb = async_exclusive_system.into_async_callback();
        let _async_exclusive_system_with_param_cb =
            async_exclusive_system_with_param.into_async_callback();
        let _async_exclusive_callback = async_exclusive_callback.as_callback();
        let _async_exclusive_callback_with_param =
            async_exclusive_callback_with_param.as_callback();

        let _exclusive_continuous_service = context
            .app
            .spawn_continuous_service(Update, exclusive_continuous_service);
        let _exclusive_continuous_service_with_param = context
            .app
            .spawn_continuous_service(Update, exclusive_continuous_service_with_param);

        let exclusive_closure = |_: In<()>, _: &mut World| {};
        let _exclusive_closure_blocking_srv = context
            .app
            .spawn_service(exclusive_closure.into_blocking_service());

        let exclusive_closure = |_: In<()>, _: &mut World| async move {};
        let _exclusive_closure_async_srv = context
            .app
            .spawn_service(exclusive_closure.into_async_service());

        let exclusive_closure = |_: ContinuousServiceInput<(), ()>, _: &mut World| {};
        let _exclusive_closure_continuous_srv = context
            .app
            .spawn_continuous_service(Update, exclusive_closure);

        let exclusive_closure = |_: In<()>, _: &mut World| {};
        let _exclusive_closure_blocking_cb = exclusive_closure.into_blocking_callback();

        let exclusive_closure = |_: In<()>, _: &mut World| async move {};
        let _exclusive_closure_async_cb = exclusive_closure.into_async_callback();
    }

    fn blocking_exclusive_system(In(_): In<i32>, _: &mut World) {}

    fn blocking_exclusive_system_with_param(
        In(_): In<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) {
    }

    fn blocking_exclusive_service(In(_): BlockingServiceInput<i32>, _: &mut World) {}

    fn blocking_exclusive_service_with_params(
        In(_): BlockingServiceInput<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) {
    }

    fn blocking_exclusive_callback(In(_): BlockingCallbackInput<i32>, _: &mut World) {}

    fn blocking_exclusive_callback_with_param(
        In(_): BlockingCallbackInput<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) {
    }

    fn async_exclusive_system(In(_): In<i32>, _: &mut World) -> impl Future<Output = ()> {
        async {}
    }

    fn async_exclusive_system_with_param(
        In(_): In<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) -> impl Future<Output = ()> {
        async {}
    }

    fn async_exclusive_service(
        In(_): AsyncServiceInput<i32>,
        _: &mut World,
    ) -> impl Future<Output = ()> {
        async {}
    }

    fn async_exclusive_service_with_param(
        In(_): AsyncServiceInput<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) -> impl Future<Output = ()> {
        async {}
    }

    fn async_exclusive_callback(
        In(_): AsyncCallbackInput<i32>,
        _: &mut World,
    ) -> impl Future<Output = ()> {
        async {}
    }

    fn async_exclusive_callback_with_param(
        In(_): AsyncCallbackInput<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) -> impl Future<Output = ()> {
        async {}
    }

    fn exclusive_continuous_service(
        In(_): ContinuousServiceInput<i32, ()>,
        _: &mut World,
        _: &mut SystemState<ContinuousQuery<i32, ()>>,
    ) {
    }

    fn exclusive_continuous_service_with_param(
        In(_): ContinuousServiceInput<i32, ()>,
        _: &mut World,
        _: &mut SystemState<ContinuousQuery<i32, ()>>,
        _: &mut QueryState<&mut TestComponent>,
    ) {
    }
}
