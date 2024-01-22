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
    ServiceBuilder, ServiceRef, ServiceRequest,
    service::builder::{SerialChosen, ParallelChosen},
    private
};

use bevy::{
    prelude::App,
    ecs::{
        world::EntityMut,
        system::{Commands, EntityCommands},
    },
};

pub trait ServiceTrait {
    fn serve(request: ServiceRequest);
}

pub trait IntoService<M> {
    type Request;
    type Response;
    type Streams;
    type DefaultDeliver: Default;

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>);
    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
}

/// This trait is used to implement adding an async service to an App at
/// startup.
pub trait ServiceAdd<Marker>: private::Sealed<Marker> {
    type Request;
    type Response;
    type Streams;
    fn add_service(self, app: &mut App);
}

/// This trait is used to implement spawning an async service through Commands
pub trait ServiceSpawn<Marker>: private::Sealed<Marker> {
    type Request;
    type Response;
    type Streams;
    fn spawn_service(self, commands: &mut Commands) -> ServiceRef<Self::Request, Self::Response, Self::Streams>;
}

/// This trait allows service systems to be converted into a builder that
/// can be used to customize how the service is configured.
pub trait IntoServiceBuilder<M>: private::Sealed<M> {
    type Service: IntoService<M>;
    type Request;
    type Response;
    type Streams;
    type DefaultDeliver;
    fn builder(self) -> ServiceBuilder<Self::Service, <Self::Service as IntoService<M>>::DefaultDeliver, (), ()>;
    fn with<With>(self, with: With) -> ServiceBuilder<Self::Service, <Self::Service as IntoService<M>>::DefaultDeliver, With, ()>;
    fn also<Also>(self, also: Also) -> ServiceBuilder<Self::Service, <Self::Service as IntoService<M>>::DefaultDeliver, (), Also>;
}

/// This trait is used to set the delivery mode of a service.
pub trait DeliveryChoice: private::Sealed<()> {
    fn apply_entity_mut<'w>(self, entity_mut: &mut EntityMut<'w>);
    fn apply_entity_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
}

/// This trait is used to accept anything that can be executed on an EntityMut,
/// used when adding a service with the App interface.
pub trait WithEntityMut {
    fn apply<'w>(self, entity_mut: EntityMut<'w>);
}

/// This trait is used to accept anything that can be executed on an
/// EntityCommands.
pub trait WithEntityCommands {
    fn apply<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>);
}

/// This trait allows users to perform more operations with a service
/// provider while adding it to an App.
pub trait AlsoAdd<Request, Response, Streams> {
    fn apply<'w>(self, app: &mut App, provider: ServiceRef<Request, Response, Streams>);
}
