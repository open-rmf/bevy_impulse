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
    ServiceBuilder, ServiceRef,
    service::builder::{BlockingChosen, SerialChosen, ParallelChosen},
    private
};

use bevy::{
    prelude::App,
    ecs::{
        world::EntityMut,
        system::{Commands, EntityCommands},
    },
};

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
pub trait IntoServiceBuilder<Marker>: private::Sealed<Marker> {
    type Request;
    type Response;
    type Streams;
    type DefaultDeliver;
    fn builder(self) -> ServiceBuilder<Self::Request, Self::Response, Self::Streams, Self::DefaultDeliver, (), ()>;
    fn with<With>(self, with: With) -> ServiceBuilder<Self::Request, Self::Response, Self::Streams, Self::DefaultDeliver, With, ()>;
    fn also<Also>(self, also: Also) -> ServiceBuilder<Self::Request, Self::Response, Self::Streams, Self::DefaultDeliver, (), Also>;
}

/// This trait allows async service systems to be converted into a builder
/// by specifying whether it should have serial or parallel service delivery.
pub trait ChooseAsyncServiceDelivery<Marker>: private::Sealed<Marker> {
    type Request;
    type Response;
    type Streams;
    fn serial(self) -> ServiceBuilder<Self::Request, Self::Response, Self::Streams, SerialChosen, (), ()>;
    fn parallel(self) -> ServiceBuilder<Self::Request, Self::Response, Self::Streams, ParallelChosen, (), ()>;
}

/// This trait allows any system to be converted into a blocking service
/// builder.
pub trait IntoBlockingServiceBuilder<Marker>: private::Sealed<Marker> {
    type Request;
    type Response;
    fn into_blocking_service(self) -> ServiceBuilder<Self::Request, Self::Response, (), BlockingChosen, (), ()>;
}

/// This trait allows any system that returns a future to be converted into
/// an async service builder.
pub trait IntoAsyncServiceBuilder<Marker>: private::Sealed<Marker> {
    type Request;
    type Response;
    fn into_async_service(self) -> ServiceBuilder<Self::Request, Self::Response, (), (), (), ()>;
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
    fn apply<'w>(self, app: &mut App, provider: Provider<Request, Response, Streams>);
}
