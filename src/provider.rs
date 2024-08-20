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

use bevy_ecs::prelude::{Entity, Commands};

/// The `Provider` trait encapsulates the idea of providing a response to a
/// request. There are three different provider types with different advantages:
/// - [`Service`](crate::Service)
/// - [`Callback`](crate::Callback)
/// - [`AsyncMap`](crate::AsyncMap)
/// - [`BlockingMap`](crate::BlockingMap)
///
/// Types that implement this trait can be used to create nodes in a workflow or
/// impulses in an impulse chain.
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
    fn connect(self, scope: Option<Entity>, source: Entity, target: Entity, commands: &mut Commands);
}

#[cfg(test)]
mod tests {
    use bevy_ecs::{
        prelude::*,
        system::SystemState,
    };
    use crate::{*, testing::*};
    use std::future::Future;

    #[test]
    fn test_exclusive_systems_as_services() {
        let mut context = TestingContext::minimal_plugins();

        let _blocking_exclusive_system_srv = context.app.spawn_service(
            blocking_exclusive_system.into_blocking_service()
        );
        let _blocking_exclusive_system_with_params_srv = context.app.spawn_service(
            blocking_exclusive_system_with_param.into_blocking_service()
        );
        let _blocking_exclusive_service = context.app.spawn_service(
            blocking_exclusive_service
        );
        let _blocking_exclusive_service_with_params = context.app.spawn_service(
            blocking_exclusive_service_with_params
        );

        let _blocking_exclusive_system_cb = blocking_exclusive_system
            .into_blocking_callback();
        let _blocking_exclusive_system_with_params_cb = blocking_exclusive_system_with_param
            .into_blocking_callback();
        let _blocking_exclusive_callback = blocking_exclusive_callback
            .as_callback();
        let _blocking_exclusive_callback_with_param = blocking_exclusive_callback_with_param
            .as_callback();

        let _async_exclusive_system_srv = context.app.spawn_service(
            async_exclusive_system.into_async_service()
        );
        let _async_exclusive_system_with_param_srv = context.app.spawn_service(
            async_exclusive_system_with_param.into_async_service()
        );
        let _async_exclusive_service = context.app.spawn_service(
            async_exclusive_service
        );
        let _async_exclusive_service_with_param = context.app.spawn_service(
            async_exclusive_service_with_param
        );

        let _async_exclusive_system_cb = async_exclusive_system
            .into_async_callback();
        let _async_exclusive_system_with_param_cb = async_exclusive_system_with_param
            .into_async_callback();
        let _async_exclusive_callback = async_exclusive_callback
            .as_callback();
        let _async_exclusive_callback_with_param = async_exclusive_callback_with_param
            .as_callback();

        let _exclusive_continuous_service = context.app.spawn_continuous_service(
            Update,
            exclusive_continuous_service,
        );
        let _exclusive_continuous_service_with_param = context.app.spawn_continuous_service(
            Update,
            exclusive_continuous_service_with_param,
        );

        let exclusive_closure = |_: In<()>, _: &mut World| {
        };
        let _exclusive_closure_blocking_srv = context.app.spawn_service(exclusive_closure.into_blocking_service());

        let exclusive_closure = |_: In<()>, _: &mut World| {
            async move { }
        };
        let _exclusive_closure_async_srv = context.app.spawn_service(exclusive_closure.into_async_service());

        let exclusive_closure = |_: ContinuousServiceInput<(), ()>, _: &mut World| {
        };
        let _exclusive_closure_continuous_srv = context.app.spawn_continuous_service(Update, exclusive_closure);

        let exclusive_closure = |_: In<()>, _: &mut World| {
        };
        let _exclusive_closure_blocking_cb = exclusive_closure.into_blocking_callback();

        let exclusive_closure = |_: In<()>, _: &mut World| {
            async move { }
        };
        let _exclusive_closure_async_cb = exclusive_closure.into_async_callback();
    }

    fn blocking_exclusive_system(
        In(_): In<i32>,
        _: &mut World,
    ) {

    }

    fn blocking_exclusive_system_with_param(
        In(_): In<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) {

    }

    fn blocking_exclusive_service(
        In(_): BlockingServiceInput<i32>,
        _: &mut World,
    ) {

    }

    fn blocking_exclusive_service_with_params(
        In(_): BlockingServiceInput<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) {

    }

    fn blocking_exclusive_callback(
        In(_): BlockingCallbackInput<i32>,
        _: &mut World,
    ) {

    }

    fn blocking_exclusive_callback_with_param(
        In(_): BlockingCallbackInput<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) {

    }

    fn async_exclusive_system(
        In(_): In<i32>,
        _: &mut World,
    ) -> impl Future<Output = ()> {
        async { }
    }

    fn async_exclusive_system_with_param(
        In(_): In<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) -> impl Future<Output = ()> {
        async { }
    }

    fn async_exclusive_service(
        In(_): AsyncServiceInput<i32>,
        _: &mut World,
    ) -> impl Future<Output = ()> {
        async { }
    }

    fn async_exclusive_service_with_param(
        In(_): AsyncServiceInput<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) -> impl Future<Output = ()> {
        async { }
    }

    fn async_exclusive_callback(
        In(_): AsyncCallbackInput<i32>,
        _: &mut World
    ) -> impl Future<Output = ()> {
        async { }
    }

    fn async_exclusive_callback_with_param(
        In(_): AsyncCallbackInput<i32>,
        _: &mut World,
        _: &mut QueryState<&mut TestComponent>,
    ) -> impl Future<Output = ()> {
        async { }
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
