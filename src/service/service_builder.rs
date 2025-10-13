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

use crate::{stream::*, Delivery, IntoContinuousService, IntoService, Service};

use bevy_app::prelude::App;
use bevy_ecs::{
    schedule::{ScheduleConfigs, ScheduleLabel},
    system::{Commands, EntityCommands, ScheduleSystem},
    world::EntityWorldMut,
};

use super::traits::*;

pub struct ServiceBuilder<Srv, Deliver, With, Also, Configure> {
    service: Srv,
    deliver: Deliver,
    with: With,
    also: Also,
    configure: Configure,
}

impl<Srv, Deliver: Default> ServiceBuilder<Srv, Deliver, (), (), ()> {
    pub(crate) fn new(service: Srv) -> Self {
        Self {
            service,
            deliver: Deliver::default(),
            with: (),
            also: (),
            configure: (),
        }
    }
}

impl<Srv, With, Also, Configure> ServiceBuilder<Srv, (), With, Also, Configure> {
    /// Make this service always fulfill requests in serial. The system that
    /// provides the service will not be executed until any prior run of this
    /// service is finished (delivered or cancelled).
    pub fn serial(self) -> ServiceBuilder<Srv, SerialChosen, With, Also, Configure> {
        ServiceBuilder {
            service: self.service,
            deliver: SerialChosen,
            with: self.with,
            also: self.also,
            configure: self.configure,
        }
    }

    /// Allow the service to run in parallel. Requests that shared the same
    /// [`DeliveryLabel`](crate::DeliveryLabel) will still be run in serial or
    /// interrupt each other depending on settings.
    pub fn parallel(self) -> ServiceBuilder<Srv, ParallelChosen, With, Also, Configure> {
        ServiceBuilder {
            service: self.service,
            deliver: ParallelChosen,
            with: self.with,
            also: self.also,
            configure: self.configure,
        }
    }
}

impl<Srv, Deliver, Configure> ServiceBuilder<Srv, Deliver, (), (), Configure> {
    pub fn with<With>(self, with: With) -> ServiceBuilder<Srv, Deliver, With, (), Configure> {
        ServiceBuilder {
            service: self.service,
            deliver: self.deliver,
            with,
            also: self.also,
            configure: self.configure,
        }
    }
}

impl<Srv, Deliver, With, Configure> ServiceBuilder<Srv, Deliver, With, (), Configure> {
    pub fn also<Also>(self, also: Also) -> ServiceBuilder<Srv, Deliver, With, Also, Configure> {
        ServiceBuilder {
            service: self.service,
            deliver: self.deliver,
            with: self.with,
            also,
            configure: self.configure,
        }
    }
}

impl<Srv, Deliver, With, Also> ServiceBuilder<Srv, Deliver, With, Also, ()> {
    pub fn configure<M, Configure>(
        self,
        configure: Configure,
    ) -> ServiceBuilder<Srv, Deliver, With, Also, Configure>
    where
        Srv: IntoContinuousService<M>,
    {
        ServiceBuilder {
            service: self.service,
            deliver: self.deliver,
            with: self.with,
            also: self.also,
            configure,
        }
    }
}

impl<Srv, Deliver, With, Also> ServiceBuilder<Srv, Deliver, With, Also, ()> {
    pub(crate) fn spawn_app_service<M>(
        self,
        app: &mut App,
    ) -> Service<Srv::Request, Srv::Response, Srv::Streams>
    where
        Srv: IntoService<M>,
        Deliver: DeliveryChoice,
        With: WithEntityWorldMut,
        Also: AlsoAdd<Srv::Request, Srv::Response, Srv::Streams>,
        Srv::Request: 'static + Send + Sync,
        Srv::Response: 'static + Send + Sync,
        Srv::Streams: StreamPack,
    {
        let mut entity_mut = app.world_mut().spawn(());
        self.service.insert_service_mut(&mut entity_mut);
        let service = Service::<Srv::Request, Srv::Response, Srv::Streams>::new(entity_mut.id());

        let mut stream_availability = StreamAvailability::default();
        <Srv::Streams as StreamPack>::set_stream_availability(&mut stream_availability);
        entity_mut.insert(stream_availability);

        self.deliver
            .apply_entity_mut::<Srv::Request>(&mut entity_mut);
        self.with.apply(entity_mut);
        self.also.apply(app, service);
        service
    }
}

impl<Srv, Deliver, With, Also, Configure> ServiceBuilder<Srv, Deliver, With, Also, Configure>
where
    Deliver: DeliveryChoice,
{
    pub(crate) fn spawn_continuous_service<M>(
        self,
        schedule: impl ScheduleLabel,
        app: &mut App,
    ) -> Service<Srv::Request, Srv::Response, Srv::Streams>
    where
        Srv: IntoContinuousService<M>,
        Deliver: DeliveryChoice,
        With: WithEntityWorldMut,
        Also: AlsoAdd<Srv::Request, Srv::Response, Srv::Streams>,
        Configure: ConfigureContinuousService,
        Srv::Request: 'static + Send + Sync,
        Srv::Response: 'static + Send + Sync,
        Srv::Streams: StreamPack,
    {
        let mut entity_mut = app.world_mut().spawn(());
        let provider = entity_mut.id();
        let config = self.service.into_system_config(&mut entity_mut);
        let config = self.configure.apply(config);
        self.deliver
            .apply_entity_mut::<Srv::Request>(&mut entity_mut);
        self.with.apply(entity_mut);
        let service = Service::<Srv::Request, Srv::Response, Srv::Streams>::new(provider);
        app.add_systems(schedule, config);
        self.also.apply(app, service);
        service
    }
}

impl<Srv, Deliver, With> ServiceBuilder<Srv, Deliver, With, (), ()>
where
    Deliver: DeliveryChoice,
{
    pub(crate) fn spawn_service<M>(
        self,
        commands: &mut Commands,
    ) -> Service<Srv::Request, Srv::Response, Srv::Streams>
    where
        Srv: IntoService<M>,
        With: WithEntityCommands,
        Srv::Request: 'static + Send + Sync,
        Srv::Response: 'static + Send + Sync,
        Srv::Streams: StreamPack,
    {
        let mut entity_cmds = commands.spawn(());
        self.service.insert_service_commands(&mut entity_cmds);
        let provider = Service::<Srv::Request, Srv::Response, Srv::Streams>::new(entity_cmds.id());

        let mut stream_availability = StreamAvailability::default();
        <Srv::Streams as StreamPack>::set_stream_availability(&mut stream_availability);
        entity_cmds.insert(stream_availability);

        self.deliver
            .apply_entity_commands::<Srv::Request>(&mut entity_cmds);
        self.with.apply(&mut entity_cmds);
        provider
    }
}

pub struct BuilderMarker<M>(std::marker::PhantomData<fn(M)>);

impl<M, Srv: IntoService<M>, Deliver, With, Also, Configure> IntoServiceBuilder<BuilderMarker<M>>
    for ServiceBuilder<Srv, Deliver, With, Also, Configure>
{
    type Service = Srv;
    type Deliver = Deliver;
    type With = With;
    type Also = Also;
    type Configure = Configure;

    fn into_service_builder(
        self,
    ) -> ServiceBuilder<Self::Service, Self::Deliver, Self::With, Self::Also, Self::Configure> {
        self
    }
}

pub struct ContinuousBuilderMarker<M>(std::marker::PhantomData<fn(M)>);

impl<M, Srv: IntoContinuousService<M>, Deliver, With, Also, Configure>
    IntoServiceBuilder<ContinuousBuilderMarker<M>>
    for ServiceBuilder<Srv, Deliver, With, Also, Configure>
{
    type Service = Srv;
    type Deliver = Deliver;
    type With = With;
    type Also = Also;
    type Configure = Configure;

    fn into_service_builder(
        self,
    ) -> ServiceBuilder<Self::Service, Self::Deliver, Self::With, Self::Also, Self::Configure> {
        self
    }
}

pub struct IntoBuilderMarker<M>(std::marker::PhantomData<fn(M)>);

impl<M, Srv: IntoService<M>> IntoServiceBuilder<IntoBuilderMarker<M>> for Srv {
    type Service = Srv;
    type Deliver = Srv::DefaultDeliver;
    type With = ();
    type Also = ();
    type Configure = ();

    fn into_service_builder(self) -> ServiceBuilder<Srv, Srv::DefaultDeliver, (), (), ()> {
        ServiceBuilder::new(self)
    }
}

impl<M, Srv> QuickServiceBuild<IntoBuilderMarker<M>> for Srv
where
    Srv: IntoService<M>,
{
    type Service = Srv;
    type Deliver = Srv::DefaultDeliver;
    fn with<With>(self, with: With) -> ServiceBuilder<Self::Service, Self::Deliver, With, (), ()> {
        self.into_service_builder().with(with)
    }

    fn also<Also>(self, also: Also) -> ServiceBuilder<Self::Service, Self::Deliver, (), Also, ()> {
        self.into_service_builder().also(also)
    }
}

impl<M, Srv> QuickContinuousServiceBuild<IntoBuilderMarker<M>> for Srv
where
    Srv: IntoContinuousService<M>,
{
    type Service = Srv;
    fn with<With>(self, with: With) -> ServiceBuilder<Self::Service, (), With, (), ()> {
        self.into_service_builder().with(with)
    }

    fn also<Also>(self, also: Also) -> ServiceBuilder<Self::Service, (), (), Also, ()> {
        self.into_service_builder().also(also)
    }

    fn configure<Configure>(
        self,
        configure: Configure,
    ) -> ServiceBuilder<Self::Service, (), (), (), Configure> {
        self.into_service_builder().configure(configure)
    }
}

/// When this is used in the Deliver type parameter of AsyncServiceBuilder, the
/// user has indicated that the service should be executed in serial
#[derive(Default)]
pub struct SerialChosen;

impl DeliveryChoice for SerialChosen {
    fn apply_entity_mut<Request: 'static + Send + Sync>(self, entity_mut: &mut EntityWorldMut) {
        entity_mut.insert(Delivery::<Request>::serial());
    }

    fn apply_entity_commands<Request: 'static + Send + Sync>(
        self,
        entity_commands: &mut EntityCommands,
    ) {
        entity_commands.insert(Delivery::<Request>::serial());
    }
}

/// When this is used in the Deliver type parameter of AsyncServiceBuilder, the
/// user has indicated that the service should be executed in parallel.
#[derive(Default)]
pub struct ParallelChosen;

impl DeliveryChoice for ParallelChosen {
    fn apply_entity_mut<Request: 'static + Send + Sync>(self, entity_mut: &mut EntityWorldMut) {
        entity_mut.insert(Delivery::<Request>::parallel());
    }

    fn apply_entity_commands<Request: 'static + Send + Sync>(
        self,
        entity_commands: &mut EntityCommands,
    ) {
        entity_commands.insert(Delivery::<Request>::parallel());
    }
}

/// When this is used in the Deliver type parameter of ServiceBuilder, the user
/// has indicated that the service is blocking and therefore does not have a
/// delivery type.
#[derive(Default)]
pub struct BlockingChosen;

impl DeliveryChoice for BlockingChosen {
    fn apply_entity_commands<Request: 'static + Send + Sync>(self, _: &mut EntityCommands) {
        // Do nothing
    }

    fn apply_entity_mut<Request: 'static + Send + Sync>(self, _: &mut EntityWorldMut) {
        // Do nothing
    }
}

impl DeliveryChoice for () {
    fn apply_entity_commands<Request: 'static + Send + Sync>(
        self,
        entity_commands: &mut EntityCommands,
    ) {
        ParallelChosen.apply_entity_commands::<Request>(entity_commands)
    }
    fn apply_entity_mut<Request: 'static + Send + Sync>(self, entity_mut: &mut EntityWorldMut) {
        ParallelChosen.apply_entity_mut::<Request>(entity_mut)
    }
}

impl<T: FnOnce(EntityWorldMut)> WithEntityWorldMut for T {
    fn apply(self, entity_mut: EntityWorldMut) {
        self(entity_mut);
    }
}

impl WithEntityWorldMut for () {
    fn apply(self, _: EntityWorldMut) {
        // Do nothing
    }
}

impl<T: FnOnce(&mut EntityCommands)> WithEntityCommands for T {
    fn apply(self, entity_commands: &mut EntityCommands) {
        self(entity_commands);
    }
}

impl WithEntityCommands for () {
    fn apply(self, _: &mut EntityCommands) {
        // Do nothing
    }
}

impl<Request, Response, Streams, T> AlsoAdd<Request, Response, Streams> for T
where
    T: FnOnce(&mut App, Service<Request, Response, Streams>),
{
    fn apply<'w>(self, app: &mut App, provider: Service<Request, Response, Streams>) {
        self(app, provider)
    }
}

impl<Request, Response, Streams> AlsoAdd<Request, Response, Streams> for () {
    fn apply<'w>(self, _: &mut App, _: Service<Request, Response, Streams>) {
        // Do nothing
    }
}

impl<T> ConfigureContinuousService for T
where
    T: FnOnce(ScheduleConfigs<ScheduleSystem>) -> ScheduleConfigs<ScheduleSystem>,
{
    fn apply(self, config: ScheduleConfigs<ScheduleSystem>) -> ScheduleConfigs<ScheduleSystem> {
        (self)(config)
    }
}

impl ConfigureContinuousService for () {
    fn apply(self, config: ScheduleConfigs<ScheduleSystem>) -> ScheduleConfigs<ScheduleSystem> {
        config
    }
}
