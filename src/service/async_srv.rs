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
    AsyncService, InAsyncService, IntoService, ServiceTrait, ServiceBundle, ServiceRequest,
    InnerChannel, ChannelQueue, OperationRoster, Blocker,
    Stream, ServiceBuilder, ChooseAsyncServiceDelivery, OperationRequest,
    OperationError, OrBroken, ManageInput, Input, OperateTask, Operation,
    OperationSetup, SingleTargetStorage, dispose_for_despawned_service,
    service::builder::{SerialChosen, ParallelChosen}, Disposal, emit_disposal,
    StopTask, UnhandledErrors, StopTaskFailure, Delivery, DeliveryInstructions,
    Deliver, DeliveryOrder, DeliveryUpdate, insert_new_order, pop_next_delivery,
    OperationResult,
};

use bevy::{
    prelude::{Component, In, World, Entity},
    tasks::AsyncComputeTaskPool,
    ecs::{
        world::EntityMut,
        system::{IntoSystem, BoxedSystem, EntityCommands},
    }
};

use std::future::Future;

pub trait IsAsyncService<M> { }

#[derive(Component)]
struct AsyncServiceStorage<Request, Streams, Task>(Option<BoxedSystem<AsyncService<Request, Streams>, Task>>);

#[derive(Component)]
struct UninitAsyncServiceStorage<Request, Streams, Task>(BoxedSystem<AsyncService<Request, Streams>, Task>);

impl<Request, Streams, Task, M, Sys> IntoService<(Request, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<AsyncService<Request, Streams>, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;
    type DefaultDeliver = ();

    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        entity_commands.insert((
            UninitAsyncServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceBundle::<AsyncServiceStorage<Request, Streams, Task>>::new(),
        ));
    }

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        entity_mut.insert((
            UninitAsyncServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceBundle::<AsyncServiceStorage<Request, Streams, Task>>::new(),
        ));
    }
}

impl<Request, Streams, Task, M, Sys> IsAsyncService<(Request, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<AsyncService<Request, Streams>, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{

}

impl<Request, Streams, Task> ServiceTrait for AsyncServiceStorage<Request, Streams, Task>
where
    Request: 'static + Send + Sync,
    Task: Future + 'static + Send,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    type Request = Request;
    type Response = Task::Output;
    fn serve(
        ServiceRequest {
            provider,
            target,
            operation: OperationRequest { source, world, roster },
        }: ServiceRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: request } = source_mut.take_input::<Request>()?;
        let instructions = source_mut.get::<DeliveryInstructions>().cloned();
        let task_id = world.spawn(()).id();

        let Some(mut delivery) = world.get_mut::<Delivery<Request>>(provider) else {
            // The async service's Delivery component has been removed so we should treat the request as cancelled.
            dispose_for_despawned_service(provider, world, roster);
            return Err(OperationError::NotReady);
        };

        let update = insert_new_order::<Request>(
            delivery.as_mut(),
            DeliveryOrder { source, session, task_id, request, instructions }
        );

        let (request, blocker) = match update {
            DeliveryUpdate::Immediate { blocking, request } => {
                let serve_next = serve_next_async_request::<Request, Streams, Task>;
                let blocker = blocking.map(|label| Blocker { provider, source, session, label, serve_next });
                (request, blocker)
            }
            DeliveryUpdate::Queued { cancelled, stop, label } => {
                for cancelled in cancelled {
                    let disposal = Disposal::supplanted(cancelled.source, source, session);
                    emit_disposal(cancelled.source, cancelled.session, disposal, world, roster);
                    world.despawn(cancelled.task_id);
                }
                if let Some(stop) = stop {
                    // This task is already running and we need to stop it at the
                    // task source level
                    let result = world.get_entity(stop.task_id).or_broken()
                        .and_then(|task_ref| task_ref.get::<StopTask>().or_broken().copied())
                        .and_then(|stop_task| {
                            let disposal = Disposal::supplanted(stop.source, source, session);
                            (stop_task.0)(
                                OperationRequest { source: task_id, world, roster },
                                disposal,
                            )
                        });

                    if let Err(OperationError::Broken(backtrace)) = result {
                        world
                        .get_resource_or_insert_with(|| UnhandledErrors::default())
                        .stop_tasks
                        .push(StopTaskFailure { task: stop.task_id, backtrace });

                        // Immediately queue up an unblocking, otherwise the next
                        // task will never be able to run.
                        let serve_next = serve_next_async_request::<Request, Streams, Task>;
                        roster.unblock(Blocker {
                            provider, source: stop.source, session: stop.session, label, serve_next
                        });
                    }
                }

                // The request has been queued up and should be delivered later
                return Ok(());
            }
        };

        serve_async_request::<Request, Streams, Task>(
            request,
            blocker,
            session,
            task_id,
            ServiceRequest {
                provider,
                target,
                operation: OperationRequest { source, world, roster },
            }
        )
    }
}

fn serve_async_request<Request, Streams, Task>(
    request: Request,
    blocker: Option<Blocker>,
    session: Entity,
    task_id: Entity,
    cmd: ServiceRequest,
) -> OperationResult
where
    Request: 'static + Send + Sync,
    Task: Future + 'static + Send,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    let ServiceRequest { provider, target, operation: OperationRequest { source, world, roster } } = cmd;
    let mut service = if let Some(mut provider_mut) = world.get_entity_mut(provider) {
        if let Some(mut storage) = provider_mut.get_mut::<AsyncServiceStorage<Request, Streams, Task>>() {
            storage.0.take().expect("Async service is missing while attempting to serve")
        } else {
            if let Some(uninit) = provider_mut.take::<UninitAsyncServiceStorage<Request, Streams, Task>>() {
                // We need to initialize the service
                let mut service = uninit.0;
                service.initialize(world);

                // Re-obtain the provider since we needed to mutably borrow the world a moment ago
                let mut provider_mut = world.entity_mut(provider);
                provider_mut.insert(AsyncServiceStorage::<Request, Streams, Task>(None));
                service
            } else {
                // The provider has had its service removed, so we treat this request as cancelled.
                dispose_for_despawned_service(provider, world, roster);
                // We've already issued the disposal, but we need to return an
                // error so that serve_next_async_request continues iterating.
                return Err(OperationError::NotReady);
            }
        }
    } else {
        // If the provider has been despawned then we treat this request as cancelled.
        dispose_for_despawned_service(provider, world, roster);
        // We've already issued the disposal, but we need to return an
        // error so that serve_next_async_request continues iterating.
        return Err(OperationError::NotReady);
    };

    let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
    let channel = InnerChannel::new(source, sender);
    let job = service.run(AsyncService { request, channel: channel.into_specific(), provider }, world);
    service.apply_deferred(world);

    if let Some(mut service_storage) = world.get_mut::<AsyncServiceStorage<Request, Streams, Task>>(provider) {
        service_storage.0 = Some(service);
    } else {
        // We've already done everything we need to do with the service, but
        // apparently the service erased itself. We will allow the task to keep
        // running since there is nothing to prevent it from doing so.
        //
        // TODO(@mxgrey): Consider whether the removal of the service should
        // imply that all the service's active tasks should be dropped?
    }

    let task = AsyncComputeTaskPool::get().spawn(job);

    OperateTask::new(session, source, target, task, blocker)
        .setup(OperationSetup { source: task_id, world });
    roster.queue(task_id);
    Ok(())
}

pub fn serve_next_async_request<Request, Streams, Task>(
    unblock: Blocker,
    world: &mut World,
    roster: &mut OperationRoster,
)
where
    Request: 'static + Send + Sync,
    Task: Future + 'static + Send,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    let Blocker { provider, label, .. } = unblock;
    loop {
        let Some(Deliver { request, task_id, blocker }) = pop_next_delivery::<Request>(
            provider, label, serve_next_async_request::<Request, Streams, Task>, world,
        ) else {
            // No more deliveries to pop, so we should return
            return;
        };

        let session = blocker.session;
        let source = blocker.source;

        let Some(target) = world.get::<SingleTargetStorage>(source) else {
            // This will not be able to run, so we should move onto the next
            // item in the queue.
            continue;
        };
        let target = target.get();

        if serve_async_request::<Request, Streams, Task>(
            request,
            Some(blocker),
            session,
            task_id,
            ServiceRequest {
                provider,
                target,
                operation: OperationRequest { source, world, roster }
            },
        ).is_err() {
            // The service did not launch so we should move onto the next item
            // in the queue.
            continue;
        }

        // The next delivery has begun so we can return
        return;
    }
}

/// Take any system that was not decalred as a service and transform it into a
/// blocking service that can be passed into a ServiceBuilder.
pub struct AsAsyncService<Srv>(pub Srv);

pub trait IntoAsyncService<M> {
    type Service;
    fn into_async_service(self) -> Self::Service;
}

impl<Request, Response, M, Sys> IntoAsyncService<AsAsyncService<(Request, Response, M)>> for Sys
where
    Sys: IntoSystem<Request, Response, M>,
    Request: 'static + Send,
    Response: 'static + Send,
{
    type Service = AsAsyncService<Sys>;
    fn into_async_service(self) -> AsAsyncService<Sys> {
        AsAsyncService(self)
    }
}

impl<Request, Task, M, Sys> IntoService<(Request, Task, M)> for AsAsyncService<Sys>
where
    Sys: IntoSystem<Request, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = ();
    type DefaultDeliver = ();

    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        peel_async.pipe(self.0).insert_service_commands(entity_commands)
    }

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        peel_async.pipe(self.0).insert_service_mut(entity_mut)
    }
}

impl<Request, Task, M, Sys> IsAsyncService<(Request, Task, M)> for AsAsyncService<Sys>
where
    Sys: IntoSystem<Request, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
{

}

impl<M, Srv> ChooseAsyncServiceDelivery<M> for Srv
where
    Srv: IntoService<M> + IsAsyncService<M>,
{
    type Service = Srv;
    fn serial(self) -> ServiceBuilder<Srv, SerialChosen, (), ()> {
        ServiceBuilder::new(self)
    }
    fn parallel(self) -> ServiceBuilder<Srv, ParallelChosen, (), ()> {
        ServiceBuilder::new(self)
    }
}

fn peel_async<Request>(In(AsyncService { request, .. }): InAsyncService<Request>) -> Request {
    request
}
