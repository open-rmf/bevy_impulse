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
    AddOperation, OperateService, ProvideOnce, Provider, RunCommandsOnWorldExt, StreamOf,
    StreamPack,
};

use bevy_app::prelude::App;
use bevy_derive::{Deref, DerefMut};
use bevy_ecs::{
    prelude::{Commands, Component, Entity, Event, World},
    schedule::ScheduleLabel,
};
pub use bevy_impulse_derive::DeliveryLabel;
use bevy_utils::{define_label, intern::Interned};
use std::{any::TypeId, collections::HashSet};
use thiserror::Error as ThisError;

mod async_srv;
pub use async_srv::*;

mod blocking;
pub use blocking::*;

mod continuous;
pub use continuous::*;

mod service_builder;
pub use service_builder::ServiceBuilder;

pub(crate) mod delivery;
pub(crate) use delivery::*;

pub mod discovery;
pub use discovery::*;

pub(crate) mod internal;
pub(crate) use internal::*;

pub mod traits;
pub use traits::*;

mod workflow;
pub(crate) use workflow::*;

/// [`Service`] is the public API handle for referring to an existing service
/// provider. You can obtain a service using:
/// - [`App`]`.`[`add_service(~)`][1]: Add a service to an `App` as part of a chain.
/// - [`App`]`.`[`spawn_service(~)`][2]: Spawn a service using an `App`.
/// - [`App`]`.`[`spawn_continuous_service(~)`][3]: Spawn a service that runs continuously in the regular App schedule.
/// - [`Commands`]`.`[`spawn_service(~)`][4]: Spawn a service using `Commands`. This can be done while the application is running. This cannot spawn continuous services.
/// - [`ServiceDiscovery`]`.iter()`: Search for compatible services that already exist within the [`World`].
///
/// To use a service, call [`Commands`]`.`[`request(input, service)`][5].
///
/// [1]: crate::AddServicesExt::add_service
/// [2]: crate::AddServicesExt::spawn_service
/// [3]: crate::AddContinuousServicesExt::spawn_continuous_service
/// [4]: SpawnServicesExt::spawn_service
/// [5]: crate::RequestExt::request
/// [App]: bevy_app::prelude::App
/// [Commands]: bevy_ecs::prelude::Commands
/// [World]: bevy_ecs::prelude::World
#[derive(Debug, PartialEq, Eq)]
pub struct Service<Request, Response, Streams = ()> {
    provider: Entity,
    instructions: Option<DeliveryInstructions>,
    _ignore: std::marker::PhantomData<fn(Request, Response, Streams)>,
}

impl<Req, Res, S> Clone for Service<Req, Res, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Req, Res, S> Copy for Service<Req, Res, S> {}

#[derive(ThisError, Debug, Clone)]
#[error("The original service is missing streams that are needed by the target service")]
pub struct MissingStreamsError {
    /// These stream types were missing from the original service
    types: HashSet<TypeId>,
}

impl<Request, Response, Streams> Service<Request, Response, Streams> {
    /// Get the underlying entity that the service provider is associated with.
    pub fn provider(&self) -> Entity {
        self.provider
    }

    /// Get the delivery instructions for this service.
    pub fn instructions(&self) -> Option<&DeliveryInstructions> {
        self.instructions.as_ref()
    }

    /// Create a new reference to the same service provider, but with new [`DeliveryInstructions`].
    /// This has no effect on the original [`Service`] instance.
    pub fn instruct(mut self, instructions: impl Into<DeliveryInstructions>) -> Self {
        self.instructions = Some(instructions.into());
        self
    }

    /// Create a new reference to the same service provider, but cast the streams
    /// into a different stream pack. This will fail if the target stream pack
    /// contains stream types that were not present in the original [`Service`]
    /// instance, regardless of whether or not the underlying service provider
    /// is able to provide the target stream types.
    ///
    /// If you are okay with misrepresenting the streams of the service, use
    /// [`Self::optional_stream_cast`]. Note that misrepresenting the service's
    /// streams means that users of the service will never receive anything from
    /// streams that the service does not actually provide.
    pub fn stream_cast<TargetStreams>(
        self,
    ) -> Result<Service<Request, Response, TargetStreams>, MissingStreamsError>
    where
        Streams: StreamPack,
        TargetStreams: StreamPack,
    {
        let mut original = HashSet::new();
        Streams::insert_types(&mut original);
        let mut target = HashSet::new();
        Streams::insert_types(&mut target);
        for t in original {
            target.remove(&t);
        }

        if !target.is_empty() {
            // Some of the target streams were not available in the original,
            // so this is not a valid cast
            return Err(MissingStreamsError { types: target });
        }

        Ok(Service {
            provider: self.provider,
            instructions: self.instructions,
            _ignore: Default::default(),
        })
    }

    /// Create a new reference to the same service provider, but cast the streams
    /// of this service into a different stream pack. This will succeed even if
    /// the original streams do not match the target streams.
    ///
    /// Be careful when using this since the service will not output anything to
    /// streams that the service provider was not originally equipped with. This
    /// could lead to confusing results for anyone trying to use the resulting service.
    ///
    /// There is never a risk of undefined behavior from performing this cast,
    /// only the unexpected absence of advertised streams, but stream data is
    /// always treated as optional by workflows anyway.
    pub fn optional_stream_cast<TargetStreams>(self) -> Service<Request, Response, TargetStreams> {
        Service {
            provider: self.provider,
            instructions: self.instructions,
            _ignore: Default::default(),
        }
    }

    // TODO(@mxgrey): Offer a stream casting method that uses StreamFilter.

    /// This can only be used internally. To obtain a Service, use one of the
    /// following:
    /// - App::add_*_service
    /// - Commands::spawn_*_service
    /// - Commands::spawn_workflow
    /// - ServiceDiscovery::iter()
    fn new(entity: Entity) -> Self {
        Self {
            provider: entity,
            instructions: None,
            _ignore: Default::default(),
        }
    }
}

define_label!(
    /// A strongly-typed class of labels used to tag delivery instructions that
    /// are related to each other.
    DeliveryLabel,
    DELIVERY_LABEL_INTERNER
);

pub mod utils {
    /// Used by the procedural macro for DeliveryLabel
    pub use bevy_utils::label::DynEq;
}

/// When using a service, you can bundle in delivery instructions that affect
/// how multiple requests to the same service may interact with each other.
///
/// `DeliveryInstructions` include a [`DeliveryLabelId`]. A `DeliveryLabelId`
/// value associates different service requests with each other, and the
/// remaining instructions determine how those related requests interact.
///
/// By default when a service provider receives a new request with the same
/// [`DeliveryLabelId`] as an earlier request, the earlier request will be
/// queued until the previous requests with the same label have all finished.
///
/// To change the default behavior there are two modifiers you can apply to
/// this label:
/// - `.preempt()` asks for the request to be preempt all other requests with
///   this same label, effectively cancelling any that have not been finished yet.
/// - `.ensure()` asks for this request to not be cancelled even if a preemptive
///   request comes in with the same label. The preemptive request will instead
///   be queued after this one.
///
/// You can choose to use either, both, or neither of these modifiers in
/// whatever way fits your use case. No matter what modifiers you choose
/// (or don't choose) the same service provider will never simultaneously
/// execute its service for two requests with the same label value. To that
/// extent, applying a label always guarantees mutual exclusivity between
/// requests.
///
/// This mutual exclusivity can be useful if the service involves making
/// modifications to the world which would conflict with each other when two
/// related requests are being delivered at the same time.
#[derive(Component, Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryInstructions {
    pub(crate) label: DeliveryLabelId,
    pub(crate) preempt: bool,
    pub(crate) ensure: bool,
}

/// Newtype to store types that implement `DeliveryLabel`
#[derive(Clone, Copy, Debug, Deref, DerefMut, Hash, PartialEq, Eq)]
pub struct DeliveryLabelId(Interned<dyn DeliveryLabel>);

impl DeliveryInstructions {
    /// Begin building a label for a request. You do not need to call this
    /// function explicitly. You can instead use `.preempt()` or `.ensure()`
    /// directly on a [`DeliveryLabel`] instance.
    pub fn new(label: impl DeliveryLabel) -> Self {
        Self {
            label: DeliveryLabelId(label.intern()),
            preempt: false,
            ensure: false,
        }
    }

    /// See the label for these delivery instructions.
    pub fn label(&self) -> &DeliveryLabelId {
        &self.label
    }

    /// New requests will preempt earlier requests.
    ///
    /// Ordinarily when multiple requests have the same delivery label, they
    /// will queue up with each other, running one at a time in order of which
    /// request arrived first. Use this method to change the instructions so
    /// that new requests will preempt earlier requests with the same delivery
    /// label, cancelling those earlier requests if they have not finished yet.
    ///
    /// To prevent requests from being preempted you can apply [`Self::ensure`]
    /// to the delivery instructions.
    pub fn preempt(mut self) -> Self {
        self.preempt = true;
        self
    }

    /// Check whether the requests will be preemptive.
    pub fn is_preemptive(&self) -> bool {
        self.preempt
    }

    /// Decide at runtime whether the [`Self::preempt`] field will be true or false.
    pub fn with_preemptive(mut self, preempt: bool) -> Self {
        self.preempt = preempt;
        self
    }

    /// Ensure that this request is resolved even if a preemptive request with
    /// the same label arrives.
    ///
    /// The [`Self::preempt`] setting will typically cause any earlier requests
    /// with the same delivery label to be cancelled when a new request comes
    /// in. If you apply `ensure` to the instructions, then later preemptive
    /// requests will not be able to cancel, and they will get queued instead.
    pub fn ensure(mut self) -> Self {
        self.ensure = true;
        self
    }

    /// Check whether the delivery instructions are ensuring that this will be
    /// delivered.
    pub fn is_ensured(&self) -> bool {
        self.ensure
    }

    /// Decide at runtime whether the [`Self::ensure`] field will be true or
    /// false.
    pub fn with_ensured(mut self, ensure: bool) -> Self {
        self.ensure = ensure;
        self
    }
}

impl<L: DeliveryLabel> From<L> for DeliveryInstructions {
    fn from(label: L) -> Self {
        DeliveryInstructions::new(label)
    }
}

/// Allow anything that can be converted into [`DeliveryInstructions`] to have
/// access to the [`Self::preempt`] and [`Self::ensure`] methods.
pub trait AsDeliveryInstructions {
    /// Instruct the delivery to have [preemptive behavior][1].
    ///
    /// [1]: DeliveryInstructions::preempt
    fn preempt(self) -> DeliveryInstructions;

    /// Decide at runtime whether to be preemptive
    fn with_preemptive(self, preempt: bool) -> DeliveryInstructions;

    /// Instruct the delivery to [be ensured][1].
    ///
    /// [1]: DeliveryInstructions::ensure
    fn ensure(self) -> DeliveryInstructions;

    /// Decide at runtime whether to be ensured.
    fn with_ensured(self, ensure: bool) -> DeliveryInstructions;
}

impl<T: Into<DeliveryInstructions>> AsDeliveryInstructions for T {
    fn preempt(self) -> DeliveryInstructions {
        self.into().preempt()
    }

    fn with_preemptive(self, preempt: bool) -> DeliveryInstructions {
        self.into().with_preemptive(preempt)
    }

    fn ensure(self) -> DeliveryInstructions {
        self.into().ensure()
    }

    fn with_ensured(self, ensure: bool) -> DeliveryInstructions {
        self.into().with_ensured(ensure)
    }
}

/// This trait extends the Commands interface so that services can spawned from
/// any system.
pub trait SpawnServicesExt<'w, 's> {
    /// Call this with Commands to create a new async service from any system.
    fn spawn_service<M1, M2, B: IntoServiceBuilder<M1, Also = (), Configure = ()>>(
        &mut self,
        builder: B,
    ) -> ServiceOf<M1, M2, B>
    where
        B::Service: IntoService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityCommands,
        RequestOf<M1, M2, B>: 'static + Send + Sync,
        ResponseOf<M1, M2, B>: 'static + Send + Sync,
        StreamsOf<M1, M2, B>: StreamPack;
}

impl<'w, 's> SpawnServicesExt<'w, 's> for Commands<'w, 's> {
    fn spawn_service<M1, M2, B: IntoServiceBuilder<M1, Also = (), Configure = ()>>(
        &mut self,
        builder: B,
    ) -> ServiceOf<M1, M2, B>
    where
        B::Service: IntoService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityCommands,
        <B::Service as IntoService<M2>>::Request: 'static + Send + Sync,
        <B::Service as IntoService<M2>>::Response: 'static + Send + Sync,
        <B::Service as IntoService<M2>>::Streams: StreamPack,
    {
        builder.into_service_builder().spawn_service(self)
    }
}

impl<'w, 's> SpawnServicesExt<'w, 's> for World {
    fn spawn_service<M1, M2, B: IntoServiceBuilder<M1, Also = (), Configure = ()>>(
        &mut self,
        builder: B,
    ) -> ServiceOf<M1, M2, B>
    where
        B::Service: IntoService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityCommands,
        <B::Service as IntoService<M2>>::Request: 'static + Send + Sync,
        <B::Service as IntoService<M2>>::Response: 'static + Send + Sync,
        <B::Service as IntoService<M2>>::Streams: StreamPack,
    {
        self.command(move |commands| commands.spawn_service(builder))
    }
}

/// This trait extends the App interface so that services can be added while
/// configuring an App.
pub trait AddServicesExt {
    /// Call this on an App to create a service that is available immediately.
    fn add_service<M1, M2, B: IntoServiceBuilder<M1, Configure = ()>>(
        &mut self,
        builder: B,
    ) -> &mut Self
    where
        B::Service: IntoService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityWorldMut,
        B::Also: AlsoAdd<RequestOf<M1, M2, B>, ResponseOf<M1, M2, B>, StreamsOf<M1, M2, B>>,
        RequestOf<M1, M2, B>: 'static + Send + Sync,
        ResponseOf<M1, M2, B>: 'static + Send + Sync,
        StreamsOf<M1, M2, B>: StreamPack,
    {
        self.spawn_service(builder);
        self
    }

    /// Call this on an App to create a service that is available immediately.
    fn spawn_service<M1, M2, B: IntoServiceBuilder<M1, Configure = ()>>(
        &mut self,
        builder: B,
    ) -> ServiceOf<M1, M2, B>
    where
        B::Service: IntoService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityWorldMut,
        B::Also: AlsoAdd<RequestOf<M1, M2, B>, ResponseOf<M1, M2, B>, StreamsOf<M1, M2, B>>,
        RequestOf<M1, M2, B>: 'static + Send + Sync,
        ResponseOf<M1, M2, B>: 'static + Send + Sync,
        StreamsOf<M1, M2, B>: StreamPack;
}

impl AddServicesExt for App {
    fn spawn_service<M1, M2, B: IntoServiceBuilder<M1, Configure = ()>>(
        &mut self,
        builder: B,
    ) -> ServiceOf<M1, M2, B>
    where
        B::Service: IntoService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityWorldMut,
        B::Also: AlsoAdd<RequestOf<M1, M2, B>, ResponseOf<M1, M2, B>, StreamsOf<M1, M2, B>>,
        RequestOf<M1, M2, B>: 'static + Send + Sync,
        ResponseOf<M1, M2, B>: 'static + Send + Sync,
        StreamsOf<M1, M2, B>: StreamPack,
    {
        builder.into_service_builder().spawn_app_service(self)
    }
}

type RequestOf<M1, M2, B> = <<B as IntoServiceBuilder<M1>>::Service as IntoService<M2>>::Request;
type ResponseOf<M1, M2, B> = <<B as IntoServiceBuilder<M1>>::Service as IntoService<M2>>::Response;
type StreamsOf<M1, M2, B> = <<B as IntoServiceBuilder<M1>>::Service as IntoService<M2>>::Streams;
type ServiceOf<M1, M2, B> =
    Service<RequestOf<M1, M2, B>, ResponseOf<M1, M2, B>, StreamsOf<M1, M2, B>>;

pub trait AddContinuousServicesExt {
    /// Spawn a continuous service. This needs to be used from [`App`] because
    /// continuous services are added to the Bevy schedule, which only the `App`
    /// can access.
    fn spawn_continuous_service<M1, M2, B: IntoServiceBuilder<M1>>(
        &mut self,
        schedule: impl ScheduleLabel,
        builder: B,
    ) -> ServiceOfC<M1, M2, B>
    where
        B::Service: IntoContinuousService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityWorldMut,
        B::Also: AlsoAdd<RequestOfC<M1, M2, B>, ResponseOfC<M1, M2, B>, StreamsOfC<M1, M2, B>>,
        B::Configure: ConfigureContinuousService,
        RequestOfC<M1, M2, B>: 'static + Send + Sync,
        ResponseOfC<M1, M2, B>: 'static + Send + Sync,
        StreamsOfC<M1, M2, B>: StreamPack;

    /// Add a continuous service to an [`App`].
    fn add_continuous_service<M1, M2, B: IntoServiceBuilder<M1>>(
        &mut self,
        schedule: impl ScheduleLabel,
        builder: B,
    ) -> &mut Self
    where
        B::Service: IntoContinuousService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityWorldMut,
        B::Also: AlsoAdd<RequestOfC<M1, M2, B>, ResponseOfC<M1, M2, B>, StreamsOfC<M1, M2, B>>,
        B::Configure: ConfigureContinuousService,
        RequestOfC<M1, M2, B>: 'static + Send + Sync,
        ResponseOfC<M1, M2, B>: 'static + Send + Sync,
        StreamsOfC<M1, M2, B>: StreamPack,
    {
        self.spawn_continuous_service(schedule, builder);
        self
    }

    /// Spawn a service that reads events from the world and streams them out
    /// after being activated. This service will never terminate so you'll need
    /// to use the [trim] operation if you want it to stop streaming events.
    ///
    /// [trim]: crate::Builder::create_trim
    fn spawn_event_streaming_service<E>(
        &mut self,
        schedule: impl ScheduleLabel,
    ) -> Service<(), (), StreamOf<E>>
    where
        E: 'static + Event + Send + Sync + Unpin + Clone,
    {
        self.spawn_continuous_service(schedule, event_streaming_service::<E>)
    }
}

impl AddContinuousServicesExt for App {
    fn spawn_continuous_service<M1, M2, B: IntoServiceBuilder<M1>>(
        &mut self,
        schedule: impl ScheduleLabel,
        builder: B,
    ) -> ServiceOfC<M1, M2, B>
    where
        B::Service: IntoContinuousService<M2>,
        B::Deliver: DeliveryChoice,
        B::With: WithEntityWorldMut,
        B::Also: AlsoAdd<RequestOfC<M1, M2, B>, ResponseOfC<M1, M2, B>, StreamsOfC<M1, M2, B>>,
        B::Configure: ConfigureContinuousService,
        RequestOfC<M1, M2, B>: 'static + Send + Sync,
        ResponseOfC<M1, M2, B>: 'static + Send + Sync,
        StreamsOfC<M1, M2, B>: StreamPack,
    {
        builder
            .into_service_builder()
            .spawn_continuous_service(schedule, self)
    }
}

type RequestOfC<M1, M2, B> =
    <<B as IntoServiceBuilder<M1>>::Service as IntoContinuousService<M2>>::Request;
type ResponseOfC<M1, M2, B> =
    <<B as IntoServiceBuilder<M1>>::Service as IntoContinuousService<M2>>::Response;
type StreamsOfC<M1, M2, B> =
    <<B as IntoServiceBuilder<M1>>::Service as IntoContinuousService<M2>>::Streams;
type ServiceOfC<M1, M2, B> =
    Service<RequestOfC<M1, M2, B>, ResponseOfC<M1, M2, B>, StreamsOfC<M1, M2, B>>;

impl<Request, Response, Streams> ProvideOnce for Service<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

    fn connect(
        self,
        scope: Option<Entity>,
        source: Entity,
        target: Entity,
        commands: &mut Commands,
    ) {
        commands.add(AddOperation::new(
            scope,
            source,
            OperateService::new(self, target),
        ));
    }
}

impl<Request, Response, Streams> Provider for Service<Request, Response, Streams> where
    Request: 'static + Send + Sync
{
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*, ServiceMarker};
    use bevy_app::{PostUpdate, PreUpdate, Startup};
    use bevy_ecs::{
        prelude::*,
        system::{StaticSystemParam, SystemParam},
        world::EntityWorldMut,
    };
    use smallvec::SmallVec;
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
        provider: Service<String, u64>,
    }

    #[test]
    fn test_spawn_async_service() {
        let mut app = App::new();
        app.insert_resource(TestSystemRan(false))
            .add_systems(Startup, sys_spawn_async_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_async_service() {
        let mut app = App::new();
        app.insert_resource(TestSystemRan(false))
            .add_service(sys_async_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_async_service_serial() {
        let mut app = App::new();
        app.insert_resource(TestSystemRan(false))
            .add_service(sys_async_service.serial())
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_built_async_service() {
        let mut app = App::new();
        app.insert_resource(TestSystemRan(false))
            .add_service(sys_async_service.also(|app: &mut App, provider| {
                app.insert_resource(MyServiceProvider { provider });
            }))
            .add_systems(Update, sys_use_my_service_provider);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_spawn_blocking_service() {
        let mut app = App::new();
        app.insert_resource(TestSystemRan(false))
            .add_systems(Startup, sys_spawn_blocking_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_simple_blocking_service() {
        let mut app = App::new();
        app.insert_resource(TestSystemRan(false))
            .add_service(sys_blocking_system.into_blocking_service())
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_self_aware_blocking_service() {
        let mut app = App::new();
        app.insert_resource(TestSystemRan(false))
            .add_service(sys_blocking_service.with(|mut entity_mut: EntityWorldMut| {
                entity_mut.insert(Multiplier(2));
            }))
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    fn sys_async_service(
        In(AsyncService { request, .. }): AsyncServiceInput<String>,
        people: Query<&TestPeople>,
    ) -> impl Future<Output = u64> {
        let mut matching_people = Vec::new();
        for person in &people {
            if person.name == request {
                matching_people.push(person.age);
            }
        }

        async move { matching_people.into_iter().fold(0, |sum, age| sum + age) }
    }

    fn sys_spawn_async_service(mut commands: Commands) {
        commands.spawn_service(sys_async_service);
    }

    fn sys_blocking_service(
        In(BlockingService {
            request, provider, ..
        }): BlockingServiceInput<String>,
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

    fn sys_blocking_system(In(name): In<String>, people: Query<&TestPeople>) -> u64 {
        let mut sum = 0;
        for person in &people {
            if person.name == name {
                sum += person.age;
            }
        }
        sum
    }

    fn sys_spawn_blocking_service(mut commands: Commands) {
        commands.spawn_service(sys_blocking_service);
    }

    fn sys_find_service(query: Query<&ServiceMarker<String, u64>>, mut ran: ResMut<TestSystemRan>) {
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
    #[derive(SystemParam)]
    struct CustomParamA<'w, 's> {
        _commands: Commands<'w, 's>,
    }

    fn service_with_generic<P: SystemParam>(
        In(BlockingService { .. }): BlockingServiceInput<()>,
        _: StaticSystemParam<P>,
    ) {
    }

    #[test]
    fn test_generic_service() {
        // Test that we can add services with generics
        let mut context = TestingContext::minimal_plugins();
        context
            .app
            .add_service(service_with_generic::<CustomParamA>);
    }

    #[test]
    fn test_event_streaming_service() {
        let mut context = TestingContext::minimal_plugins();

        // Add impulse flushes before and after the Update schedule so that the
        // request and streams can all be processed within one cycle.
        context.app.add_systems(PreUpdate, flush_impulses());
        context.app.add_systems(PostUpdate, flush_impulses());

        context.app.add_event::<CustomEvent>();
        let event_streamer = context
            .app
            .spawn_event_streaming_service::<CustomEvent>(Update);

        let mut recipient = context.command(|commands| commands.request((), event_streamer).take());

        context.app.world.send_event(CustomEvent(0));
        context.app.world.send_event(CustomEvent(1));
        context.app.world.send_event(CustomEvent(2));

        context.run_with_conditions(&mut recipient.response, 1);

        // We do not expect the response to be available because event streamers
        // never end.
        let mut result: SmallVec<[_; 3]> = SmallVec::new();
        while let Ok(r) = recipient.streams.try_recv() {
            result.push(r.0 .0);
        }
        assert_eq!(&result[..], &[0, 1, 2]);
    }

    #[derive(Event, Clone, Copy)]
    struct CustomEvent(i64);
}
