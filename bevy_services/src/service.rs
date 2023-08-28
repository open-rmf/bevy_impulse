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
    prelude::{Entity, Component, In, App},
    ecs::{
        world::EntityMut,
        system::{
            BoxedSystem, IntoSystem, EntityCommands, Commands,
        }
    },
    utils::define_label,
};

use std::collections::{VecDeque, HashMap};

pub struct AsyncServiceBuilder<Request, Response, Streams, Deliver, With, Then> {
    service: Service<Request, Response>,
    streams: std::marker::PhantomData<Streams>,
    deliver: Deliver,
    with: With,
    then: Then,
}

pub struct BuilderMarker;

pub trait AsyncServiceAdd<Marker> {
    type Request;
    type Response;
    type Streams;
    fn add_service(self, app: &mut App);
}

impl<Request, Response, Streams, Deliver, With, Then>
AsyncServiceAdd<BuilderMarker>
for AsyncServiceBuilder<Request, Response, Streams, Deliver, With, Then>
where
    Streams: IntoStreamOutComponents,
    Deliver: DeliveryChoice,
    With: WithEntityMut,
    Then: ThenAdd<Request, Response, Streams>,
    Request: 'static,
    Response: 'static,
    Streams: 'static,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn add_service(self, app: &mut App) {
        let mut entity_mut = app.world.spawn(self.service);
        let provider = Provider::<Request, Response, Streams>::new(entity_mut.id());
        Streams::mut_stream_out_components(&mut entity_mut);
        self.deliver.apply_entity_mut(&mut entity_mut);
        self.with.apply(entity_mut);
        self.then.apply(app, provider);
    }
}

impl<Request, Response, Streams, Task, M, Sys>
AsyncServiceAdd<(Request, Response, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<Request, Task, M>,
    Task: FnOnce(Assistant<Streams>) -> Option<Response> + 'static,
    Streams: IntoStreamOutComponents + 'static,
    Request: 'static,
    Response: 'static,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn add_service(self, app: &mut App) {
        // AsyncServiceBuilder::new(self).add_service(app)
        AsyncServiceAdd::<BuilderMarker>::add_service(
            AsyncServiceBuilder::new(self), app
        );
    }
}

pub trait AsyncServiceSpawn<Marker> {
    type Request;
    type Response;
    type Streams;
    fn spawn_service(self, commands: &mut Commands) -> Provider<Self::Request, Self::Response, Self::Streams>;
}

impl<Request, Response, Streams, Deliver, With>
AsyncServiceSpawn<BuilderMarker>
for AsyncServiceBuilder<Request, Response, Streams, Deliver, With, ()>
where
    Streams: IntoStreamOutComponents,
    Deliver: DeliveryChoice,
    With: WithEntityCommands,
    Request: 'static,
    Response: 'static,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn spawn_service(self, commands: &mut Commands) -> Provider<Request, Response, Streams> {
        let mut entity_cmds = commands.spawn(self.service);
        let provider = Provider::<Request, Response, Streams>::new(entity_cmds.id());
        Streams::cmd_stream_out_components(&mut entity_cmds);
        self.deliver.apply_entity_commands(&mut entity_cmds);
        self.with.apply(&mut entity_cmds);
        provider
    }
}

impl<Request, Response, Streams, Task, M, Sys> AsyncServiceSpawn<(Request, Response, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<Request, Task, M>,
    Task: FnOnce(Assistant<Streams>) -> Option<Response> + 'static,
    Streams: IntoStreamOutComponents + 'static,
    Request: 'static,
    Response: 'static,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn spawn_service(self, commands: &mut Commands) -> Provider<Self::Request, Self::Response, Self::Streams> {
        AsyncServiceBuilder::new(self).spawn_service(commands)
    }
}

impl<Request, Response, Streams> AsyncServiceBuilder<Request, Response, Streams, (), (), ()> {
    /// Start building a new async service by providing the system that will
    /// provide the service.
    pub fn new<M, Sys, Task>(service: Sys) -> Self
    where
        Sys: IntoSystem<Request, Task, M>,
        Task: FnOnce(Assistant<Streams>) -> Option<Response>,
        Request: 'static,
        Response: 'static,
        Streams: 'static,
        Task: 'static,
    {
        let service = Service::Async(
            Box::new(IntoSystem::into_system(
                service.pipe(
                    |In(task): In<Task>| {
                        let task: Box<dyn FnOnce(GenericAssistant) -> Option<Response>> = Box::new(
                            move |assistant: GenericAssistant| {
                                task(assistant.into_specific())
                            }
                        );
                        task
                    }
                )
            ))
        );

        Self {
            service,
            streams: Default::default(),
            deliver: (),
            with: (),
            then: (),
        }
    }
}

impl<Request, Response, Streams, With, Then> AsyncServiceBuilder<Request, Response, Streams, (), With, Then> {
    /// Make this service always fulfill requests in serial. The system that
    /// provides the service will not be executed until any prior run of this
    /// service is finished (delivered or cancelled).
    pub fn serial(self) -> AsyncServiceBuilder<Request, Response, Streams, SerialChosen, With, Then> {
        AsyncServiceBuilder {
            service: self.service,
            streams: Default::default(),
            deliver: SerialChosen,
            with: self.with,
            then: self.then,
        }
    }

    /// Allow the service to run in parallel. Requests that shared the same
    /// RequestLabel will still be run in serial or interrupt each other
    /// depending on settings.
    pub fn parallel(self) -> AsyncServiceBuilder<Request, Response, Streams, ParallelChosen, With, Then> {
        AsyncServiceBuilder {
            service: self.service,
            streams: Default::default(),
            deliver: ParallelChosen,
            with: self.with,
            then: self.then,
        }
    }
}

impl<Request, Response, Streams, Deliver, Then> AsyncServiceBuilder<Request, Response, Streams, Deliver, (), Then> {
    pub fn with<With>(self, with: With) -> AsyncServiceBuilder<Request, Response, Streams, Deliver, With, Then> {
        AsyncServiceBuilder {
            service: self.service,
            streams: Default::default(),
            deliver: self.deliver,
            with,
            then: self.then,
        }
    }
}

impl<Request, Response, Streams, Deliver, With> AsyncServiceBuilder<Request, Response, Streams, Deliver, With, ()> {
    pub fn then<Then>(self, then: Then) -> AsyncServiceBuilder<Request, Response, Streams, Deliver, With, Then> {
        AsyncServiceBuilder {
            service: self.service,
            streams: Default::default(),
            deliver: self.deliver,
            with: self.with,
            then,
        }
    }
}

/// This trait is used to accept a callback that
pub trait DeliveryChoice {
    fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>);
    fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
}

/// When this is used in the Deliver type parameter of AsyncServiceBuilder, the
/// user has indicated that the service should be executed in serial.
pub struct SerialChosen;

impl DeliveryChoice for SerialChosen {
    fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        entity_mut.insert(Delivery::serial());
    }

    fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        entity_commands.insert(Delivery::serial());
    }
}

/// When this is used in the Deliver type parameter of AsyncServiceBuilder, the
/// user has indicated that the service should be executed in parallel.
pub struct ParallelChosen;

impl DeliveryChoice for ParallelChosen {
    fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        entity_mut.insert(Delivery::parallel());
    }

    fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        entity_commands.insert(Delivery::parallel());
    }
}

impl DeliveryChoice for () {
    fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        ParallelChosen.apply_entity_commands(entity_commands)
    }
    fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        ParallelChosen.apply_entity_mut(entity_mut)
    }
}

/// This trait is used to accept anything that can be executed on an EntityMut,
/// used when adding a service with the App interface.
pub trait WithEntityMut {
    fn apply<'w>(self, entity_mut: EntityMut<'w>);
}

impl<T: FnOnce(EntityMut)> WithEntityMut for T {
    fn apply<'w>(self, entity_mut: EntityMut<'w>) {
        self(entity_mut);
    }
}

impl WithEntityMut for () {
    fn apply<'w>(self, _: EntityMut<'w>) {
        // Do nothing
    }
}

/// This trait is used to accept anything that can be executed on an
/// EntityCommands
pub trait WithEntityCommands {
    fn apply<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
}

impl<T: FnOnce(&mut EntityCommands)> WithEntityCommands for T {
    fn apply<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        self(entity_commands);
    }
}

impl WithEntityCommands for () {
    fn apply<'w, 's, 'a>(self, _: &mut EntityCommands<'w, 's ,'a>) {
        // Do nothing
    }
}

pub trait ThenAdd<Request, Response, Streams> {
    fn apply<'w>(self, app: &mut App, provider: Provider<Request, Response, Streams>);
}

impl<Request, Response, Streams, T> ThenAdd<Request, Response, Streams> for T
where
    T: FnOnce(&mut App, Provider<Request, Response, Streams>)
{
    fn apply<'w>(self, app: &mut App, provider: Provider<Request, Response, Streams>) {
        self(app, provider)
    }
}

impl<Request, Response, Streams> ThenAdd<Request, Response, Streams> for () {
    fn apply<'w>(self, _: &mut App, _: Provider<Request, Response, Streams>) {
        // Do nothing
    }
}

pub trait SpawnServicesExt<'w, 's> {
    fn spawn_async_service<'a, M, S: AsyncServiceSpawn<M>>(
        &'a mut self,
        service: S,
    ) -> Provider<S::Request, S::Response, S::Streams>;

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

pub trait AddServicesExt {
    /// Call this on an App to create an async service that is available
    /// immediately.
    fn add_async_service<M, S>(&mut self, service: S) -> &mut Self
    where
        S: AsyncServiceAdd<M>;

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

///
pub(crate) struct DeliveryInstructions {
    label: RequestLabelId,
    interrupting: bool,
    interruptible: bool,
}

pub(crate) struct DeliveryOrder {
    target: Entity,
    instructions: Option<DeliveryInstructions>,
}

/// The delivery mode determines whether service requests are carried out one at
/// a time (serial) or in parallel.
#[derive(Component)]
pub(crate) enum Delivery {
    Serial(SerialDelivery),
    Parallel(ParallelDelivery),
}

impl Delivery {
    fn serial() -> Self {
        Delivery::Serial(SerialDelivery::default())
    }

    fn parallel() -> Self {
        Delivery::Parallel(ParallelDelivery::default())
    }
}

#[derive(Default)]
pub(crate) struct SerialDelivery {
    delivering: Option<DeliveryOrder>,
    queue: VecDeque<DeliveryOrder>,
}

#[derive(Default)]
pub(crate) struct ParallelDelivery {
    labeled: HashMap<RequestLabelId, SerialDelivery>,
}

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
                AsyncServiceBuilder::new(sys_async_service)
                .then(|app: &mut App, provider| {
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
