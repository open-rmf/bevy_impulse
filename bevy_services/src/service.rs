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

use crate::OperationRoster;

use bevy::{
    prelude::{Entity, App, Commands, World, Component, Bundle},
    ecs::{
        world::EntityMut,
        system::EntityCommands,
    },
    tasks::Task as BevyTask,
};

mod async_srv;
pub use async_srv::*;

mod blocking;
pub use blocking::*;

mod builder;
pub use builder::ServiceBuilder;

mod traits;
pub use traits::*;

/// Marker trait to indicate when an input is ready without knowing the type of
/// input.
#[derive(Component)]
pub(crate) struct InputReady;

#[derive(Component)]
pub(crate) struct InputStorage<Input>(pub(crate) Input);

impl<Input> InputStorage<Input> {
    pub(crate) fn take(self) -> Input {
        self.0
    }
}

#[derive(Bundle)]
pub(crate) struct InputBundle<Input: 'static + Send + Sync> {
    value: InputStorage<Input>,
    ready: InputReady,
}

impl<Input: 'static + Send + Sync> InputBundle<Input> {
    pub(crate) fn new(value: Input) -> Self {
        Self { value: InputStorage(value), ready: InputReady }
    }
}

pub struct ServiceRequest<'a> {
    /// The entity that holds the service that is being used.
    pub provider: Entity,
    /// The entity that holds the [`InputStorage`].
    pub source: Entity,
    /// The entity where the response should be placed as [`InputStorage`].
    pub target: Entity,
    /// The world that the service must operate on
    pub world: &'a mut World,
    /// The operation roster which lets the service queue up more operations to immediately perform
    pub roster: &'a mut OperationRoster,
}

impl<'a> ServiceRequest<'a> {
    pub fn from_source<B>(&mut self) -> Option<B> {
        if let Some(mut source_mut) = self.world.get_entity_mut(self.source) {
            let Some(request) = source_mut.take::<B>() else {
                self.roster.cancel(self.source);
                return None;
            };
            Some(request.0)
        } else {
            self.roster.cancel(self.source);
            return None;
        }
    }
}

pub(crate) struct ServiceMarker<Request, Response> {
    hook: fn(ServiceRequest),
    _ignore: std::marker::PhantomData<(Request, Response)>,
}

impl<Request, Response> ServiceMarker<Request, Response> {
    pub(crate) fn new<Srv: ServiceTrait>() -> Self {
        Self {
            hook: service_hook::<Srv>,
            _ignore: Default::default(),
        }
    }
}

fn service_hook<Srv: ServiceTrait>(request: ServiceRequest) {
    Srv::serve(request);
}

/// Provider is the public API handle for referring to an existing service
/// provider. Downstream users can obtain a Provider using
/// - [`crate::ServiceDiscovery`].iter()
/// - [`bevy::prelude::App`].add_*_service(~)
/// - [`bevy::prelude::Commands`].spawn_*_service(~)
///
/// To use a provider, call [`bevy::prelude::Commands`].request(provider, request).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceRef<Request, Response, Streams = ()> {
    entity: Entity,
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<Req, Res, S> Clone for ServiceRef<Req, Res, S> {
    fn clone(&self) -> Self {
        Self { entity: self.entity, _ignore: Default::default() }
    }
}

impl<Req, Res, S> Copy for ServiceRef<Req, Res, S> { }

impl<Request, Response, Streams> ServiceRef<Request, Response, Streams> {
    /// Get the underlying entity that the service provider is associated with.
    pub fn get(&self) -> Entity {
        self.entity
    }

    /// This can only be used internally. To obtain a Provider, use one of the
    /// following:
    /// - App::add_*_service
    /// - Commands::spawn_*_service
    /// - ServiceDiscovery::iter()
    fn new(entity: Entity) -> Self {
        Self { entity, _ignore: Default::default() }
    }
}

/// This trait extends the Commands interface so that services can spawned from
/// any system.
pub trait SpawnServicesExt<'w, 's> {
    /// Call this with Commands to create a new async service from any system.
    fn spawn_service<'a, M, S: ServiceSpawn<M>>(
        &'a mut self,
        service: S,
    ) -> ServiceRef<S::Request, S::Response, S::Streams>;
}

impl<'w, 's> SpawnServicesExt<'w, 's> for Commands<'w, 's> {
    fn spawn_service<'a, M, S: ServiceSpawn<M>>(
        &'a mut self,
        service: S,
    ) -> ServiceRef<S::Request, S::Response, S::Streams> {
        service.spawn_service(self)
    }
}

/// This trait extends the App interface so that services can be added while
/// configuring an App.
pub trait AddServicesExt {
    /// Call this on an App to create a service that is available immediately.
    fn add_service<M, S: ServiceAdd<M>>(&mut self, service: S) -> &mut Self;
}

impl AddServicesExt for App {
    fn add_service<M, S: ServiceAdd<M>>(&mut self, service: S) -> &mut Self {
        service.add_service(self);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlockingReq, InBlockingReq, AsyncReq, InAsyncReq, Channel};
    use bevy::{
        prelude::*,
        ecs::world::EntityMut,
    };
    use std::future::Future;

    #[derive(Component)]
    struct TestPeople {
        name: String,
        age: u64,
    }

    #[derive(Component)]
    struct Multiplier(u64);

    #[derive(Resource)]
    struct TestSystemRan(bool);

    #[derive(Resource)]
    struct MyServiceProvider {
        #[allow(unused)]
        provider: ServiceRef<String, u64>,
    }

    #[test]
    fn test_spawn_async_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_systems(Startup, sys_spawn_async_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_async_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_service(sys_async_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_async_service_serial() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_service(sys_async_service.serial())
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_built_async_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_service(
                sys_async_service
                .also(|app: &mut App, provider| {
                    app.insert_resource(MyServiceProvider { provider });
                })
            )
            .add_systems(Update, sys_use_my_service_provider);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_spawn_blocking_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_systems(Startup, sys_spawn_blocking_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_simple_blocking_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_service(sys_blocking_system.into_blocking_service())
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_self_aware_blocking_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_service(
                sys_blocking_service
                .with(|mut entity_mut: EntityMut| {
                    entity_mut.insert(Multiplier(2));
                })
            )
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    fn sys_async_service(
        In(AsyncReq{ request, .. }): InAsyncReq<String>,
        people: Query<&TestPeople>,
    ) -> impl Future<Output=u64> {
        let mut matching_people = Vec::new();
        for person in &people {
            if person.name == request {
                matching_people.push(person.age);
            }
        }

        async move {
            matching_people.into_iter().fold(0, |sum, age| sum + age)
        }
    }

    fn sys_spawn_async_service(
        mut commands: Commands,
    ) {
        commands.spawn_service(sys_async_service);
    }

    fn sys_blocking_service(
        In(BlockingReq{ request, provider }): InBlockingReq<String>,
        people: Query<&TestPeople>,
        multipliers: Query<&Multiplier>,
    ) -> u64 {
        let mut sum = 0;
        let multiplier = multipliers.get(provider).unwrap().0;
        for person in &people {
            if person.name == request {
                sum += multiplier * person.age;
            }
        }
        sum
    }

    fn sys_blocking_system(
        In(name): In<String>,
        people: Query<&TestPeople>,
    ) -> u64 {
        let mut sum = 0;
        for person in &people {
            if person.name == name {
                sum += person.age;
            }
        }
        sum
    }

    fn sys_spawn_blocking_service(
        mut commands: Commands,
    ) {
        commands.spawn_service(sys_blocking_service);
    }

    fn sys_find_service(
        query: Query<&ServiceMarker<String, u64>>,
        mut ran: ResMut<TestSystemRan>,
    ) {
        assert!(!query.is_empty());
        ran.0 = true;
    }

    fn sys_use_my_service_provider(
        my_provider: Option<Res<MyServiceProvider>>,
        mut ran: ResMut<TestSystemRan>,
    ) {
        assert!(my_provider.is_some());
        ran.0 = true;
    }
}
