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
    BlockingService, InBlockingService, IntoService, ServiceTrait, ServiceRequest,
    Input, ManageInput, ServiceBundle, OperationRequest, OperationError,
    OrBroken, dispose_for_despawned_service,
    service::builder::BlockingChosen,
};

use bevy::{
    prelude::{Component, In},
    ecs::{
        world::EntityMut,
        system::{IntoSystem, BoxedSystem, EntityCommands},
    }
};

pub struct Blocking<M>(std::marker::PhantomData<M>);

#[derive(Component)]
struct BlockingServiceStorage<Request, Response>(Option<BoxedSystem<BlockingService<Request>, Response>>);

#[derive(Component)]
struct UninitBlockingServiceStorage<Request, Response>(BoxedSystem<BlockingService<Request>, Response>);

impl<Request, Response, M, Sys> IntoService<Blocking<(Request, Response, M)>> for Sys
where
    Sys: IntoSystem<BlockingService<Request>, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();
    type DefaultDeliver = BlockingChosen;

    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        entity_commands.insert((
            UninitBlockingServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceBundle::<BlockingServiceStorage<Request, Response>>::new(),
        ));
    }

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        entity_mut.insert((
            UninitBlockingServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceBundle::<BlockingServiceStorage<Request, Response>>::new(),
        ));
    }
}

impl<Request: 'static + Send + Sync, Response: 'static + Send + Sync> ServiceTrait for BlockingServiceStorage<Request, Response> {
    type Request = Request;
    type Response = Response;
    fn serve(
        ServiceRequest { provider, target, operation: OperationRequest { source, world, roster } }: ServiceRequest
    ) -> Result<(), OperationError> {
        let Input { session, data: request } = world
            .get_entity_mut(source).or_broken()?
            .take_input::<Request>()?;

        let mut service = if let Some(mut provider_mut) = world.get_entity_mut(provider) {
            if let Some(mut storage) = provider_mut.get_mut::<BlockingServiceStorage<Request, Response>>() {
                storage.0.take().expect("Service is missing while attempting to serve")
            } else {
                // Check if the system still needs to be initialized
                if let Some(uninit) = provider_mut.take::<UninitBlockingServiceStorage<Request, Response>>() {
                    // We need to initialize the service
                    let mut service = uninit.0;
                    service.initialize(world);

                    // Re-obtain the provider since we needed to mutably borrow the world a moment ago
                    let mut provider_mut = world.entity_mut(provider);
                    provider_mut.insert(BlockingServiceStorage::<Request, Response>(None));
                    service
                } else {
                    // The provider has had its service removed, so we treat this request as cancelled.
                    dispose_for_despawned_service(provider, world, roster);
                    return Ok(());
                }
            }
        } else {
            // If the provider has been despawned then we treat this request as cancelled.
            dispose_for_despawned_service(provider, world, roster);
            return Ok(());
        };

        let response = service.run(BlockingService { request, provider }, world);
        service.apply_deferred(world);

        if let Some(mut provider_mut) = world.get_entity_mut(provider) {
            if let Some(mut storage) = provider_mut.get_mut::<BlockingServiceStorage<Request, Response>>() {
                storage.0 = Some(service);
            } else {
                // The service storage has been removed for some reason. We
                // will treat this as the service itself being removed. But we can
                // still complete this service request.
            }
        } else {
            // Apparently the service was despawned by the service itself.
            // But we can still deliver the response to the target, so we will
            // not consider this to be cancelled.
        }

        world.get_entity_mut(target).or_broken()?
            .give_input(session, response, roster)?;
        Ok(())
    }
}

/// Take any system that was not declared as a service and transform it into a
/// blocking service that can be passed into a ServiceBuilder.
pub struct AsBlockingService<Srv>(pub Srv);

/// This trait allows any system to be converted into a blocking service.
pub trait IntoBlockingService<M> {
    type Service;
    fn into_blocking_service(self) -> Self::Service;
}

impl<Request, Response, M, Sys> IntoBlockingService<AsBlockingService<(Request, Response, M)>> for Sys
where
    Sys: IntoSystem<Request, Response, M>,
    Request: 'static,
    Response: 'static,
{
    type Service = AsBlockingService<Sys>;
    fn into_blocking_service(self) -> AsBlockingService<Sys> {
        AsBlockingService(self)
    }
}

impl<Request, Response, M, Sys> IntoService<AsBlockingService<(Request, Response, M)>> for AsBlockingService<Sys>
where
    Sys: IntoSystem<Request, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();
    type DefaultDeliver = BlockingChosen;

    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        peel_blocking.pipe(self.0).insert_service_commands(entity_commands)
    }

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        peel_blocking.pipe(self.0).insert_service_mut(entity_mut)
    }
}

fn peel_blocking<Request>(In(BlockingService { request, .. }): InBlockingService<Request>) -> Request {
    request
}
