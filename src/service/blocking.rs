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

use bevy_ecs::{
    prelude::{Component, In},
    system::{BoxedSystem, EntityCommands, IntoSystem},
    world::EntityWorldMut,
};

use crate::{
    dispose_for_despawned_service, make_stream_buffer_from_world,
    service::service_builder::BlockingChosen, BlockingService, BlockingServiceInput, Input,
    IntoService, ManageDisposal, ManageInput, OperationError, OperationRequest, OrBroken,
    ServiceBundle, ServiceRequest, ServiceTrait, StreamPack, UnusedStreams,
};

pub struct Blocking<M>(std::marker::PhantomData<fn(M)>);

#[derive(Component)]
struct BlockingServiceStorage<Request, Response, Streams: StreamPack>(
    Option<BoxedSystem<In<BlockingService<Request, Streams>>, Response>>,
);

#[derive(Component)]
struct UninitBlockingServiceStorage<Request, Response, Streams: StreamPack>(
    BoxedSystem<In<BlockingService<Request, Streams>>, Response>,
);

impl<Request, Response, Streams, M, Sys> IntoService<Blocking<(Request, Response, Streams, M)>>
    for Sys
where
    Sys: IntoSystem<In<BlockingService<Request, Streams>>, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    type DefaultDeliver = BlockingChosen;

    fn insert_service_commands(self, entity_commands: &mut EntityCommands) {
        entity_commands.insert((
            UninitBlockingServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceBundle::<BlockingServiceStorage<Request, Response, Streams>>::new(),
        ));
    }

    fn insert_service_mut(self, entity_mut: &mut EntityWorldMut) {
        entity_mut.insert((
            UninitBlockingServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceBundle::<BlockingServiceStorage<Request, Response, Streams>>::new(),
        ));
    }
}

impl<Request, Response, Streams> ServiceTrait for BlockingServiceStorage<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    fn serve(
        ServiceRequest {
            provider,
            target,
            instructions: _,
            operation:
                OperationRequest {
                    source,
                    world,
                    roster,
                },
        }: ServiceRequest,
    ) -> Result<(), OperationError> {
        let Input {
            session,
            data: request,
        } = world
            .get_entity_mut(source)
            .or_broken()?
            .take_input::<Request>()?;

        let mut service = if let Ok(mut provider_mut) = world.get_entity_mut(provider) {
            if let Some(mut storage) =
                provider_mut.get_mut::<BlockingServiceStorage<Request, Response, Streams>>()
            {
                storage
                    .0
                    .take()
                    .expect("Service is missing while attempting to serve")
            } else {
                // Check if the system still needs to be initialized
                if let Some(uninit) =
                    provider_mut.take::<UninitBlockingServiceStorage<Request, Response, Streams>>()
                {
                    // We need to initialize the service
                    let mut service = uninit.0;
                    service.initialize(world);

                    // Re-obtain the provider since we needed to mutably borrow the world a moment ago
                    let mut provider_mut = world.entity_mut(provider);
                    provider_mut.insert(BlockingServiceStorage::<Request, Response, Streams>(None));
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

        let streams = make_stream_buffer_from_world::<Streams>(source, world)?;
        let response = service.run(
            BlockingService {
                request,
                streams: streams.clone(),
                provider,
                source,
                session,
            },
            world,
        );
        service.apply_deferred(world);

        let mut unused_streams = UnusedStreams::new(source);
        Streams::process_buffer(streams, source, session, &mut unused_streams, world, roster)?;

        if let Ok(mut provider_mut) = world.get_entity_mut(provider) {
            if let Some(mut storage) =
                provider_mut.get_mut::<BlockingServiceStorage<Request, Response, Streams>>()
            {
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

        if !unused_streams.streams.is_empty() {
            world.get_entity_mut(source).or_broken()?.emit_disposal(
                session,
                unused_streams.into(),
                roster,
            );
        }

        world
            .get_entity_mut(target)
            .or_broken()?
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

impl<Request, Response, M, Sys> IntoBlockingService<AsBlockingService<(Request, Response, M)>>
    for Sys
where
    Sys: IntoSystem<In<Request>, Response, M>,
    Request: 'static,
    Response: 'static,
{
    type Service = AsBlockingService<Sys>;
    fn into_blocking_service(self) -> AsBlockingService<Sys> {
        AsBlockingService(self)
    }
}

impl<Request, Response, M, Sys> IntoService<AsBlockingService<(Request, Response, M)>>
    for AsBlockingService<Sys>
where
    Sys: IntoSystem<In<Request>, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();
    type DefaultDeliver = BlockingChosen;

    fn insert_service_commands(self, entity_commands: &mut EntityCommands) {
        peel_blocking
            .pipe(self.0)
            .insert_service_commands(entity_commands)
    }

    fn insert_service_mut(self, entity_mut: &mut EntityWorldMut) {
        peel_blocking.pipe(self.0).insert_service_mut(entity_mut)
    }
}

fn peel_blocking<Request>(
    In(BlockingService { request, .. }): BlockingServiceInput<Request>,
) -> Request {
    request
}
