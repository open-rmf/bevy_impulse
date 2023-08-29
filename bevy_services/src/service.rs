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
    Assistant, GenericAssistant,
    stream::*,
};

use bevy::{
    prelude::{Entity, Component, App},
    ecs::system::{BoxedSystem, IntoSystem, EntityCommands, Commands},
    utils::define_label,
};

mod building;
pub use building::traits::*;

mod delivery;
pub(crate) use delivery::*;

/// Provider is the public API handle for referring to an existing service
/// provider. Downstream users can obtain a Provider using
/// - [`crate::ServiceDiscovery`].iter()
/// - [`bevy::prelude::App`].add_*_service(~)
/// - [`bevy::prelude::Commands`].spawn_*_service(~)
///
/// To use a provider, call [`bevy::prelude::Commands`].request(provider, request).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Provider<Request, Response, Streams = ()> {
    entity: Entity,
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<Req, Res, S> Clone for Provider<Req, Res, S> {
    fn clone(&self) -> Self {
        Self { entity: self.entity, _ignore: Default::default() }
    }
}

impl<Req, Res, S> Copy for Provider<Req, Res, S> { }

impl<Request, Response, Streams> Provider<Request, Response, Streams> {
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
    fn spawn_async_service<'a, M, S: AsyncServiceSpawn<M>>(
        &'a mut self,
        service: S,
    ) -> Provider<S::Request, S::Response, S::Streams>;

    /// Call this with Commands to create a new blocking service from any system.
    fn spawn_blocking_service<'a, Request, Response, M, Sys>(
        &'a mut self,
        service: Sys
    ) -> EntityCommands<'w, 's, 'a>
    where
        Sys: IntoSystem<Request, Response, M>,
        Request: 'static,
        Response: 'static;
}

impl<'w, 's> SpawnServicesExt<'w, 's> for Commands<'w, 's> {
    fn spawn_async_service<'a, M, S: AsyncServiceSpawn<M>>(
        &'a mut self,
        service: S,
    ) -> Provider<S::Request, S::Response, S::Streams> {
        service.spawn_service(self)
    }

    fn spawn_blocking_service<'a, Request, Response, M, Sys>(
        &'a mut self,
        service: Sys
    ) -> EntityCommands<'w, 's, 'a>
    where
        Sys: IntoSystem<Request, Response, M>,
        Request: 'static,
        Response: 'static,
    {
        self.spawn(Service::Blocking(Box::new(IntoSystem::into_system(service))))
    }
}

/// This trait extends the App interface so that services can be added while
/// configuring an App.
pub trait AddServicesExt {
    /// Call this on an App to create an async service that is available
    /// immediately.
    fn add_async_service<M, S>(&mut self, service: S) -> &mut Self
    where
        S: AsyncServiceAdd<M>;

    /// Call this on an App to create a blocking service that is available
    /// immediately.
    fn add_blocking_service<'a, Request, Response, M, Sys>(
        &mut self,
        service: Sys,
    ) -> &mut Self
    where
        Sys: IntoSystem<Request, Response, M>,
        Request: 'static,
        Response: 'static;
}

impl AddServicesExt for App {
    fn add_async_service<M, S>(&mut self, service: S) -> &mut Self
    where
        S: AsyncServiceAdd<M>,
    {
        service.add_service(self);
        self
    }

    fn add_blocking_service<'a, Request, Response, M, Sys>(
        &mut self,
        service: Sys,
    ) -> &mut Self
    where
        Sys: IntoSystem<Request, Response, M>,
        Request: 'static,
        Response: 'static,
    {
        self.world.spawn(Service::Blocking(Box::new(IntoSystem::into_system(service))));
        self
    }
}

/// A service is a type of system that takes in a request and produces a
/// response, optionally emitting events from its streams using the provided
/// assistant.
#[derive(Component)]
pub(crate) enum Service<Request, Response> {
    /// The service takes in the request and blocks execution until the response
    /// is produced.
    Blocking(BoxedSystem<Request, Response>),
    /// The service produces a task that runs asynchronously in the bevy thread
    /// pool.
    Async(BoxedSystem<Request, Box<dyn FnOnce(GenericAssistant) -> Option<Response>>>),
}

define_label!(
    /// A strongly-typed class of labels used to identify requests that have been
    /// issued to a service.
    RequestLabel,
    /// Strongly-typed identifier for a [`RequestLabel`].
    RequestLabelId,
);

#[cfg(test)]
mod tests {
    use super::*;
    use bevy::prelude::*;

    #[derive(Component)]
    struct TestPeople {
        name: String,
        age: u64,
    }

    #[derive(Resource)]
    struct TestSystemRan(bool);

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
    fn test_add_async_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_async_service(sys_async_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[derive(Resource)]
    struct MyServiceProvider {
        provider: Provider<String, u64>,
    }

    #[test]
    fn test_add_built_async_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_async_service(
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
    fn test_add_blocking_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_blocking_service(sys_blocking_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    fn sys_async_service(
        In(name): In<String>,
        people: Query<&TestPeople>,
    ) -> impl FnOnce(Assistant<()>) -> Option<u64> {
        let mut matching_people = Vec::new();
        for person in &people {
            if person.name == name {
                matching_people.push(person.age);
            }
        }

        move |_: Assistant<()>| {
            Some(matching_people.into_iter().fold(0, |sum, age| sum + age))
        }
    }

    fn sys_spawn_async_service(
        mut commands: Commands,
    ) {
        commands.spawn_async_service(sys_async_service);
    }

    fn sys_blocking_service(
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
        commands.spawn_blocking_service(sys_blocking_service);
    }

    fn sys_find_service(
        query: Query<&Service<String, u64>>,
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
