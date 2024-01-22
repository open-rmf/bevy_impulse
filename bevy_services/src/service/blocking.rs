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
    BlockingReq, InBlockingReq, IntoService, ServiceTrait, ServiceMarker, ServiceRequest, InputStorage,
    InputBundle,
    service::builder::BlockingChosen,
    private,
};

use bevy::{
    prelude::{Component, In},
    ecs::{
        world::EntityMut,
        system::{IntoSystem, BoxedSystem, EntityCommands},
    }
};

struct Blocking;

#[derive(Component)]
struct BlockingServiceStorage<Request, Response>(Option<BoxedSystem<BlockingReq<Request>, Response>>);

#[derive(Component)]
struct UninitBlockingServiceStorage<Request, Response>(BoxedSystem<BlockingReq<Request>, Response>);

impl<Request, Response, M, Sys> IntoService<(Request, Response, M, Blocking)> for Sys
where
    Sys: IntoSystem<BlockingReq<Request>, Response, M>,
    Request: 'static,
    Response: 'static,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();
    type DefaultDeliver = BlockingChosen;

    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        entity_commands.insert((
            UninitBlockingServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceMarker::<Request, Response>::new::<BlockingServiceStorage<Request, Response>>(),
        ));
    }

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        entity_mut.insert((
            UninitBlockingServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceMarker::<Request, Response>::new::<BlockingServiceStorage<Request, Response>>(),
        ));
    }
}

impl<Request, Response> ServiceTrait for BlockingServiceStorage<Request, Response> {
    fn serve(mut cmd: ServiceRequest) {
        let Some(request) = cmd.from_source::<InputStorage<Request>>() else {
            return;
        };

        let ServiceRequest { provider, source, target, world, roster } = cmd;

        let mut service = if let Some(mut provider_mut) = world.get_entity_mut(provider) {
            if let Some(mut storage) = provider_mut.get_mut::<BlockingServiceStorage<Request, Response>>() {
                storage.0.take().expect("Service is missing while attempting to serve")
            } else {
                // Check if the system still needs to be initialized
                if let Some(uninit) = provider_mut.get_mut::<UninitBlockingServiceStorage<Request, Response>>() {
                    // We need to initialize the service
                    let mut service = uninit.0;
                    service.initialize(world);
                    provider_mut.insert(BlockingServiceStorage::<Request, Response>(None));
                    service
                } else {
                    // The provider has had its service removed, so we treat this request as canceled.
                    roster.cancel(source);
                    return;
                }
            }
        } else {
            // If the provider has been despawned then we treat this request as canceled.
            roster.cancel(source);
            return;
        };

        let response = service.run(BlockingReq { request, provider }, world);
        service.apply_deferred(world);

        if let Some(mut provider_mut) = world.get_entity_mut(provider) {
            let Some(mut storage) = provider_mut.get_mut::<BlockingServiceStorage<Request, Response>>() else {
                // The service storage has been removed for some reason. We will
                // treat this as the service itself being removed. But we can still
                // complete this service request.
                return;
            };
            storage.0 = Some(service);
        } else {
            // Apparently the service was despawned by the service itself.
            // But we can still deliver the response to the target, so we will
            // not consider this to be canceled.
        }

        if let Some(mut target_mut) = world.get_entity_mut(target) {
            target_mut.insert(InputBundle::new(response));
        } else {
            // The target is no longer available for a delivery
            roster.cancel(target);
        }
    }
}

/// Take any system that was not declared as a service and transform it into a
/// blocking service that can be passed into a ServiceBuilder.
pub struct AsBlockingService<Srv>(pub Srv);

/// This trait allows any system to be converted into a blocking service.
pub trait IntoBlockingService<M>: private::Sealed<M> {
    fn into_blocking_service(self) -> AsBlockingService<Self>;
}

impl<Request, Response, M, Sys> IntoBlockingService<(Request, Response, M)> for Sys
where
    Sys: IntoSystem<Request, Response, M>,
    Request: 'static,
    Response: 'static,
{
    fn into_blocking_service(self) -> AsBlockingService<Sys> {
        AsBlockingService(self)
    }
}

impl<Request, Response, M, Sys> IntoService<(Request, Response, M, Sys)> for AsBlockingService<M>
where
    Sys: IntoSystem<Request, Response, M>,
    Request: 'static,
    Response: 'static,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();
    type DefaultDeliver = BlockingChosen;

    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        peel_blocking.pipe(self).insert_service_commands(entity_commands)
    }

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        peel_blocking.pipe(self).insert_service_mut(entity_mut)
    }
}

fn peel_blocking<Request>(In(BlockingReq { request, .. }): InBlockingReq<Request>) -> Request {
    request
}
