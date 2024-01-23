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
    ServiceRef, IntoService, Delivery,
    stream::*,
    private,
};

use bevy::{
    prelude::{App, In},
    ecs::{
        world::EntityMut,
        system::{IntoSystem, Commands, EntityCommands}
    }
};

use super::traits::*;

pub struct ServiceBuilder<Srv, Deliver, With, Also> {
    service: Srv,
    deliver: Deliver,
    with: With,
    also: Also,
}

impl<Srv, Deliver: Default> ServiceBuilder<Srv, Deliver, (), ()> {
    pub(crate) fn new(service: Srv) -> Self {
        Self {
            service,
            deliver: Deliver::default(),
            with: (),
            also: (),
        }
    }
}

impl<Srv, With, Also> ServiceBuilder<Srv, (), With, Also> {
    /// Make this service always fulfill requests in serial. The system that
    /// provides the service will not be executed until any prior run of this
    /// service is finished (delivered or cancelled).
    pub fn serial(self) -> ServiceBuilder<Srv, SerialChosen, With, Also> {
        ServiceBuilder {
            service: self.service,
            deliver: SerialChosen,
            with: self.with,
            also: self.also,
        }
    }

    /// Allow the service to run in parallel. Requests that shared the same
    /// RequestLabel will still be run in serial or interrupt each other
    /// depending on settings.
    pub fn parallel(self) -> ServiceBuilder<Srv, ParallelChosen, With, Also> {
        ServiceBuilder {
            service: self.service,
            deliver: ParallelChosen,
            with: self.with,
            also: self.also,
        }
    }
}

impl<Srv, Deliver> ServiceBuilder<Srv, Deliver, (), ()> {
    pub fn with<With>(self, with: With) -> ServiceBuilder<Srv, Deliver, With, ()> {
        ServiceBuilder {
            service: self.service,
            deliver: self.deliver,
            with,
            also: self.also,
        }
    }
}

impl<Srv, Deliver, With> ServiceBuilder<Srv, Deliver, With, ()> {
    pub fn also<Also>(self, also: Also) -> ServiceBuilder<Srv, Deliver, With, Also> {
        ServiceBuilder {
            service: self.service,
            deliver: self.deliver,
            with: self.with,
            also,
        }
    }
}

impl<Srv, Deliver, With, Also> ServiceBuilder<Srv, Deliver, With, Also>
where
    Deliver: DeliveryChoice,
{
    pub(crate) fn add_service<M>(self, app: &mut App)
    where
        Srv: IntoService<M>,
        With: WithEntityMut,
        Also: AlsoAdd<Srv::Request, Srv::Response, Srv::Streams>,
        Srv::Request: 'static + Send + Sync,
        Srv::Response: 'static + Send + Sync,
        Srv::Streams: Stream,
    {
        let mut entity_mut = app.world.spawn(());
        self.service.insert_service_mut(&mut entity_mut);
        let provider = ServiceRef::<Srv::Request, Srv::Response, Srv::Streams>::new(entity_mut.id());
        // entity_mut.insert(<<Srv as IntoService<M>>::Streams as IntoStreamBundle>::StreamOutBundle::default());
        entity_mut.insert(<Srv::Streams as Stream>::StreamOutBundle::default());
        self.deliver.apply_entity_mut(&mut entity_mut);
        self.with.apply(entity_mut);
        self.also.apply(app, provider);
    }
}

impl<Srv, Deliver, With> ServiceBuilder<Srv, Deliver, With, ()>
where
    Deliver: DeliveryChoice,
{
    pub(crate) fn spawn_service<M>(self, commands: &mut Commands) -> ServiceRef<Srv::Request, Srv::Response, Srv::Streams>
    where
        Srv: IntoService<M>,
        With: WithEntityCommands,
        Srv::Request: 'static + Send + Sync,
        Srv::Response: 'static + Send + Sync,
        Srv::Streams: Stream,
    {
        let mut entity_cmds = commands.spawn(());
        self.service.insert_service_commands(&mut entity_cmds);
        let provider = ServiceRef::<Srv::Request, Srv::Response, Srv::Streams>::new(entity_cmds.id());
        entity_cmds.insert(<Srv::Streams as Stream>::StreamOutBundle::default());
        self.deliver.apply_entity_commands(&mut entity_cmds);
        self.with.apply(&mut entity_cmds);
        provider
    }
}

pub struct BuilderMarker<M>(std::marker::PhantomData<M>);

impl<M, Srv: IntoService<M>, Deliver, With, Also> IntoServiceBuilder<BuilderMarker<M>> for ServiceBuilder<Srv, Deliver, With, Also> {
    type Service = Srv;
    type Deliver = Deliver;
    type With = With;
    type Also = Also;

    fn into_service_builder(self) -> ServiceBuilder<Self::Service, Self::Deliver, Self::With, Self::Also> {
        self
    }
}

impl<M, Srv, Deliver, With, Also> private::Sealed<BuilderMarker<M>> for ServiceBuilder<Srv, Deliver, With, Also>
where
    Srv: IntoService<M>,
{

}

pub struct IntoBuilderMarker<M>(std::marker::PhantomData<M>);

impl<M, Sys> private::Sealed<IntoBuilderMarker<M>> for Sys { }

impl<M, Srv: IntoService<M>> IntoServiceBuilder<IntoBuilderMarker<M>> for Srv {
    type Service = Srv;
    type Deliver = Srv::DefaultDeliver;
    type With = ();
    type Also = ();

    fn into_service_builder(self) -> ServiceBuilder<Srv, Srv::DefaultDeliver, (), ()> {
        ServiceBuilder::new(self)
    }
}

impl<M, Srv> QuickServiceBuild<IntoBuilderMarker<M>> for Srv
where
    Srv: IntoService<M>,
{
    type Service = Srv;
    type Deliver = Srv::DefaultDeliver;
    fn with<With>(self, with: With) -> ServiceBuilder<Self::Service, Self::Deliver, With, ()> {
        self.into_service_builder().with(with)
    }

    fn also<Also>(self, also: Also) -> ServiceBuilder<Self::Service, Self::Deliver, (), Also> {
        self.into_service_builder().also(also)
    }
}

// impl<M, Srv: IntoService<M>> private::Sealed<M> for Srv { }

/// When this is used in the Deliver type parameter of AsyncServiceBuilder, the
/// user has indicated that the service should be executed in serial
#[derive(Default)]
pub struct SerialChosen;

impl DeliveryChoice for SerialChosen {
    fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        entity_mut.insert(Delivery::serial());
    }

    fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        entity_commands.insert(Delivery::serial());
    }
}

impl private::Sealed<()> for SerialChosen { }

/// When this is used in the Deliver type parameter of AsyncServiceBuilder, the
/// user has indicated that the service should be executed in parallel.
#[derive(Default)]
pub struct ParallelChosen;

impl DeliveryChoice for ParallelChosen {
    fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        entity_mut.insert(Delivery::parallel());
    }

    fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        entity_commands.insert(Delivery::parallel());
    }
}

impl private::Sealed<()> for ParallelChosen { }

/// When this is used in the Deliver type parameter of ServiceBuilder, the user
/// has indicated that the service is blocking and therefore does not have a
/// delivery type.
#[derive(Default)]
pub struct BlockingChosen;

impl DeliveryChoice for BlockingChosen {
    fn apply_entity_commands<'w, 's, 'a>(self, _: &mut EntityCommands<'w, 's, 'a>) {
        // Do nothing
    }
    fn apply_entity_mut<'w>(self, _: &mut EntityMut<'w>) {
        // Do nothing
    }
}

impl private::Sealed<()> for BlockingChosen { }

impl DeliveryChoice for () {
    fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        ParallelChosen.apply_entity_commands(entity_commands)
    }
    fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        ParallelChosen.apply_entity_mut(entity_mut)
    }
}

impl private::Sealed<()> for () { }

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
    T: FnOnce(&mut App, ServiceRef<Request, Response, Streams>)
{
    fn apply<'w>(self, app: &mut App, provider: ServiceRef<Request, Response, Streams>) {
        self(app, provider)
    }
}

impl<Request, Response, Streams> AlsoAdd<Request, Response, Streams> for () {
    fn apply<'w>(self, _: &mut App, _: ServiceRef<Request, Response, Streams>) {
        // Do nothing
    }
}
