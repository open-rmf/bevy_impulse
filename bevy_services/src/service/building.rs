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
    Provider, Assistant, Service, GenericAssistant, Delivery, stream::*,
};

use bevy::{
    prelude::{App, In},
    ecs::{
        world::EntityMut,
        system::{
            IntoSystem, Commands, EntityCommands,
        }
    }
};

pub mod traits {
    use super::*;

    /// This trait is used to implement adding a service to an App at startup.
    pub trait AsyncServiceAdd<Marker> {
        type Request;
        type Response;
        type Streams;
        fn add_service(self, app: &mut App);
    }

    /// This trait is used to implement spawning a service through Commands
    pub trait AsyncServiceSpawn<Marker> {
        type Request;
        type Response;
        type Streams;
        fn spawn_service(self, commands: &mut Commands) -> Provider<Self::Request, Self::Response, Self::Streams>;
    }

    /// This trait allows service systems to be converted into a builder that
    /// allows users to customize how the service is configured.
    pub trait IntoAsyncServiceBuilder<Marker> {
        type Request;
        type Response;
        type Streams;
        fn serial(self) -> AsyncServiceBuilder<Self::Request, Self::Response, Self::Streams, SerialChosen, (), ()>;
        fn parallel(self) -> AsyncServiceBuilder<Self::Request, Self::Response, Self::Streams, ParallelChosen, (), ()>;
        fn with<With>(self, with: With) -> AsyncServiceBuilder<Self::Request, Self::Response, Self::Streams, (), With, ()>;
        fn also<Also>(self, also: Also) -> AsyncServiceBuilder<Self::Request, Self::Response, Self::Streams, (), (), Also>;
    }

    /// This trait is used to set the delivery mode of an async service.
    pub trait DeliveryChoice {
        fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>);
        fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
    }

    /// This trait is used to accept anything that can be executed on an EntityMut,
    /// used when adding a service with the App interface.
    pub trait WithEntityMut {
        fn apply<'w>(self, entity_mut: EntityMut<'w>);
    }

    /// This trait is used to accept anything that can be executed on an
    /// EntityCommands
    pub trait WithEntityCommands {
        fn apply<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
    }

    /// This trait allows users to perform more operations with a service while
    /// adding it to an App.
    pub trait AlsoAdd<Request, Response, Streams> {
        fn apply<'w>(self, app: &mut App, provider: Provider<Request, Response, Streams>);
    }
}

use traits::*;

pub struct AsyncServiceBuilder<Request, Response, Streams, Deliver, With, Also> {
    service: Service<Request, Response>,
    streams: std::marker::PhantomData<Streams>,
    deliver: Deliver,
    with: With,
    also: Also,
}

pub struct BuilderMarker;

impl<Request, Response, Streams, Deliver, With, Also>
AsyncServiceAdd<BuilderMarker>
for AsyncServiceBuilder<Request, Response, Streams, Deliver, With, Also>
where
    Streams: IntoStreamOutComponents,
    Deliver: DeliveryChoice,
    With: WithEntityMut,
    Also: AlsoAdd<Request, Response, Streams>,
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
        self.also.apply(app, provider);
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
            also: (),
        }
    }
}

impl<Request, Response, Streams, With, Also> AsyncServiceBuilder<Request, Response, Streams, (), With, Also> {
    /// Make this service always fulfill requests in serial. The system that
    /// provides the service will not be executed until any prior run of this
    /// service is finished (delivered or cancelled).
    pub fn serial(self) -> AsyncServiceBuilder<Request, Response, Streams, SerialChosen, With, Also> {
        AsyncServiceBuilder {
            service: self.service,
            streams: Default::default(),
            deliver: SerialChosen,
            with: self.with,
            also: self.also,
        }
    }

    /// Allow the service to run in parallel. Requests that shared the same
    /// RequestLabel will still be run in serial or interrupt each other
    /// depending on settings.
    pub fn parallel(self) -> AsyncServiceBuilder<Request, Response, Streams, ParallelChosen, With, Also> {
        AsyncServiceBuilder {
            service: self.service,
            streams: Default::default(),
            deliver: ParallelChosen,
            with: self.with,
            also: self.also,
        }
    }
}

impl<Request, Response, Streams, Deliver, Also> AsyncServiceBuilder<Request, Response, Streams, Deliver, (), Also> {
    pub fn with<With>(self, with: With) -> AsyncServiceBuilder<Request, Response, Streams, Deliver, With, Also> {
        AsyncServiceBuilder {
            service: self.service,
            streams: Default::default(),
            deliver: self.deliver,
            with,
            also: self.also,
        }
    }
}

impl<Request, Response, Streams, Deliver, With> AsyncServiceBuilder<Request, Response, Streams, Deliver, With, ()> {
    pub fn also<Also>(self, also: Also) -> AsyncServiceBuilder<Request, Response, Streams, Deliver, With, Also> {
        AsyncServiceBuilder {
            service: self.service,
            streams: Default::default(),
            deliver: self.deliver,
            with: self.with,
            also,
        }
    }
}

impl<Request, Response, Streams, Task, M, Sys>
IntoAsyncServiceBuilder<(Request, Response, Streams, Task, M)> for Sys
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
    fn serial(self) -> AsyncServiceBuilder<Self::Request, Self::Response, Self::Streams, SerialChosen, (), ()> {
        AsyncServiceBuilder::new(self).serial()
    }
    fn parallel(self) -> AsyncServiceBuilder<Self::Request, Self::Response, Self::Streams, ParallelChosen, (), ()> {
        AsyncServiceBuilder::new(self).parallel()
    }
    fn with<With>(self, with: With) -> AsyncServiceBuilder<Self::Request, Self::Response, Self::Streams, (), With, ()> {
        AsyncServiceBuilder::new(self).with(with)
    }
    fn also<Also>(self, also: Also) -> AsyncServiceBuilder<Self::Request, Self::Response, Self::Streams, (), (), Also> {
        AsyncServiceBuilder::new(self).also(also)
    }
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

impl<Request, Response, Streams, T> AlsoAdd<Request, Response, Streams> for T
where
    T: FnOnce(&mut App, Provider<Request, Response, Streams>)
{
    fn apply<'w>(self, app: &mut App, provider: Provider<Request, Response, Streams>) {
        self(app, provider)
    }
}

impl<Request, Response, Streams> AlsoAdd<Request, Response, Streams> for () {
    fn apply<'w>(self, _: &mut App, _: Provider<Request, Response, Streams>) {
        // Do nothing
    }
}
