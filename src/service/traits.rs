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

use crate::{
    ServiceBuilder, Service, ServiceRequest, OperationResult,
    service::service_builder::{SerialChosen, ParallelChosen},
};

use bevy::{
    prelude::App,
    ecs::{
        world::EntityWorldMut,
        system::EntityCommands,
        schedule::SystemConfigs,
    },
};

pub trait ServiceTrait {
    // TODO(@mxgrey): Are we using these associated types anymore?
    type Request: 'static + Send + Sync;
    type Response: 'static + Send + Sync;
    fn serve(request: ServiceRequest) -> OperationResult;
}

pub trait IntoService<M> {
    type Request;
    type Response;
    type Streams;
    type DefaultDeliver: Default;

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityWorldMut<'w>);
    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
}

pub trait IntoContinuousService<M> {
    type Request;
    type Response;
    type Streams;

    fn into_system_config<'w>(self, entity_mut: &mut EntityWorldMut<'w>) -> SystemConfigs;
}

/// This trait allows service systems to be converted into a builder that
/// can be used to customize how the service is configured.
pub trait IntoServiceBuilder<M> {
    type Service;
    type Deliver;
    type With;
    type Also;
    type Configure;
    fn into_service_builder(self) -> ServiceBuilder<Self::Service, Self::Deliver, Self::With, Self::Also, Self::Configure>;
}

/// This trait allows users to immediately begin building a service off of a suitable system
/// without needing to explicitly create a builder.
pub trait QuickServiceBuild<M> {
    type Service;
    type Deliver;
    fn with<With>(self, with: With) -> ServiceBuilder<Self::Service, Self::Deliver, With, (), ()>;
    fn also<Also>(self, also: Also) -> ServiceBuilder<Self::Service, Self::Deliver, (), Also, ()>;
}

pub trait QuickContinuousServiceBuild<M> {
    type Service;
    fn with<With>(self, with: With) -> ServiceBuilder<Self::Service, (), With, (), ()>;
    fn also<Also>(self, also: Also) -> ServiceBuilder<Self::Service, (), (), Also, ()>;
    fn configure<Configure>(self, configure: Configure) -> ServiceBuilder<Self::Service, (), (), (), Configure>;
}

/// This trait allows async service systems to be converted into a builder
/// by specifying whether it should have serial or parallel service delivery.
pub trait ChooseAsyncServiceDelivery<M> {
    type Service;
    fn serial(self) -> ServiceBuilder<Self::Service, SerialChosen, (), (), ()>;
    fn parallel(self) -> ServiceBuilder<Self::Service, ParallelChosen, (), (), ()>;
}

/// This trait is used to set the delivery mode of a service.
pub trait DeliveryChoice {
    fn apply_entity_mut<'w, Request: 'static + Send + Sync>(
        self, entity_mut: &mut EntityWorldMut<'w>,
    );
    fn apply_entity_commands<'w, 's, 'a, Request: 'static + Send + Sync>(
        self, entity_commands: &mut EntityCommands<'w, 's, 'a>,
    );
}

/// This trait is used to accept anything that can be executed on an EntityWorldMut,
/// used when adding a service with the App interface.
pub trait WithEntityWorldMut {
    fn apply<'w>(self, entity_mut: EntityWorldMut<'w>);
}

/// This trait is used to accept anything that can be executed on an
/// EntityCommands.
pub trait WithEntityCommands {
    fn apply<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
}

/// This trait allows users to perform more operations with a service
/// provider while adding it to an App.
pub trait AlsoAdd<Request, Response, Streams> {
    fn apply<'w>(self, app: &mut App, provider: Service<Request, Response, Streams>);
}

pub trait ConfigureContinuousService {
    fn apply(self, config: SystemConfigs) -> SystemConfigs;
}
