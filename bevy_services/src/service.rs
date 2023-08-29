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

use bevy::{
    prelude::{Entity, App, In, Commands},
    utils::define_label,
};

mod building;
pub use building::{ServiceBuilder, traits::*};

mod delivery;
pub(crate) use delivery::*;

mod internal;
pub(crate) use internal::*;

/// Use Req to indicate the request data structure that your service's system
/// takes as input. For example this signature can be used for simple services
/// that only need the request data as input:
///
/// ```rust
/// fn my_service(
///     In(Req(request)): In<Req<MyRequestData>>,
///     other: Query<&OtherComponents>,
/// ) -> impl FnOnce(Assistant) -> Resp<MyResponseData> {
///     /* ... */
/// }
/// ```
///
/// On the other hand, the systems of more complex services might also need to
/// know what entity is providing the service, e.g. if the service provider is
/// configured with additional components that need to be queried when a request
/// comes in. For that you can use the self-aware signature:
///
/// ```rust
/// fn my_self_aware_service(
///     In((me, Req(request))): In<(Entity, Req<MyRequestData>)>,
///     query_service_params: Query<&MyServiceParams>,
///     other: Query<&OtherComponents>,
/// ) -> Job<impl FnOnce(Assistant<()>) -> MyResponseData> {
///     let my_params = query_service_params.get(me).unwrap();
///     /* ... */
/// }
/// ```
pub struct Req<Request>(pub Request);

/// Wrap [`Resp`] around the return value of your service's system to indicate
/// it will immediately return a response to the request. This means your
/// service is blocking and all other system execution will halt while it is
/// running. It should only be used for services that execute very quickly.
///
/// To define an async service use [`Job`].
pub struct Resp<Response>(pub Response);

/// Wrap [`Job`] around the return value of your service's system to provide a
/// function that will be passed along as a task.
pub struct Job<Task>(pub Task);

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
    fn spawn_service<'a, M, S: ServiceSpawn<M>>(
        &'a mut self,
        service: S,
    ) -> Provider<S::Request, S::Response, S::Streams>;
}

impl<'w, 's> SpawnServicesExt<'w, 's> for Commands<'w, 's> {
    fn spawn_service<'a, M, S: ServiceSpawn<M>>(
        &'a mut self,
        service: S,
    ) -> Provider<S::Request, S::Response, S::Streams> {
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
    use crate::Assistant;
    use bevy::{
        prelude::*,
        ecs::world::EntityMut,
    };

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
        provider: Provider<String, u64>,
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
            .add_service(sys_blocking_service)
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
                sys_self_aware_blocking_service
                .with(|mut entity_mut: EntityMut| {
                    entity_mut.insert(Multiplier(2));
                })
            )
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    fn sys_async_service(
        In(Req(name)): In<Req<String>>,
        people: Query<&TestPeople>,
    ) -> Job<impl FnOnce(Assistant) -> Option<u64>> {
        let mut matching_people = Vec::new();
        for person in &people {
            if person.name == name {
                matching_people.push(person.age);
            }
        }

        let job = move |_: Assistant| {
            Some(matching_people.into_iter().fold(0, |sum, age| sum + age))
        };
        Job(job)
    }

    fn sys_spawn_async_service(
        mut commands: Commands,
    ) {
        commands.spawn_service(sys_async_service);
    }

    fn sys_self_aware_blocking_service(
        In((me, Req(name))): In<(Entity, Req<String>)>,
        people: Query<&TestPeople>,
        multipliers: Query<&Multiplier>,
    ) -> Resp<u64> {
        let mut sum = 0;
        let multiplier = multipliers.get(me).unwrap().0;
        for person in &people {
            if person.name == name {
                sum += multiplier * person.age;
            }
        }
        Resp(sum)
    }

    fn sys_blocking_service(
        In(Req(name)): In<Req<String>>,
        people: Query<&TestPeople>,
    ) -> Resp<u64> {
        let mut sum = 0;
        for person in &people {
            if person.name == name {
                sum += person.age;
            }
        }
        Resp(sum)
    }

    fn sys_spawn_blocking_service(
        mut commands: Commands,
    ) {
        commands.spawn_service(sys_self_aware_blocking_service);
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
