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
    InnerChannel, ChannelQueue, RequestLabelId, OperationRoster, Blocker,
    Stream, ServiceBuilder, ChooseAsyncServiceDelivery, OperationRequest,
    OperationError, OrBroken, ManageInput, Input, OperateTask, Operation,
    OperationSetup, SingleTargetStorage, dispose_for_despawned_service,
    service::builder::{SerialChosen, ParallelChosen}, Disposal, emit_disposal,
    StopTask, UnhandledErrors, StopTaskFailure,
};

use bevy::{
    prelude::{Component, In, Entity, World},
    tasks::AsyncComputeTaskPool,
    ecs::{
        world::EntityMut,
        system::{IntoSystem, BoxedSystem, EntityCommands},
    }
};

use std::{
    future::Future,
    collections::{VecDeque, HashMap},
};

use smallvec::SmallVec;

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
    fn serve(cmd: ServiceRequest) -> Result<(), OperationError> {
        let ServiceRequest { provider, target, operation: OperationRequest { source, world, roster } } = cmd;

        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: request } = source_mut.take_input::<Request>()?;
        let instructions = source_mut.get::<DeliveryInstructions>().cloned();
        let task_id = world.spawn(()).id();

        let Some(mut provider_mut) = world.get_entity_mut(provider) else {
            // The async service has been despawned, so we should treat the request as cancelled.
            dispose_for_despawned_service(provider, world, roster);
            return Ok(());
        };

        let Some(mut delivery) = provider_mut.get_mut::<Delivery<Request>>() else {
            // The async service's Delivery component has been removed so we should treat the request as cancelled.
            dispose_for_despawned_service(provider, world, roster);
            return Ok(());
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
                    // This task is already running so we need to stop it at the
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

                return Ok(());
            }
        };

        let mut service = if let Some(mut provider_mut) = world.get_entity_mut(provider) {
            if let Some(mut storage) = provider_mut.get_mut::<AsyncServiceStorage<Request, Streams, Task>>() {
                storage.0.take().expect("Service is missing while attempting to serve")
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
                    return Ok(());
                }
            }
        } else {
            // If the provider has been despawned then we treat this request as cancelled.
            dispose_for_despawned_service(provider, world, roster);
            return Ok(());
        };

        let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let channel = InnerChannel::new(source, sender);
        let job = service.run(AsyncService { request, channel: channel.into_specific(), provider }, world);
        service.apply_deferred(world);

        if let Some(mut provider_mut) = world.get_entity_mut(provider) {
            // The AsyncServiceStorage component must already exist because it was
            // inserted earier within this function if it did not exist.
            provider_mut.get_mut::<AsyncServiceStorage<Request, Streams, Task>>().unwrap().0 = Some(service);
        }

        let task = AsyncComputeTaskPool::get().spawn(job);

        OperateTask::new(session, source, target, task, blocker)
            .setup(OperationSetup { source: task_id, world });
        roster.queue(task_id);
        Ok(())
    }
}

pub(crate) fn serve_next_async_request<Request, Streams, Task>(
    mut unblock: Blocker,
    world: &mut World,
    roster: &mut OperationRoster,
)
where
    Request: 'static + Send + Sync,
    Task: Future + 'static + Send,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    loop {
        let Blocker { provider, source: finished_source, label, .. } = unblock;
        let Some(mut provider_mut) = world.get_entity_mut(provider) else {
            return;
        };
        let Some(mut delivery) = provider_mut.get_mut::<Delivery<Request>>() else {
            return;
        };

        let next_delivery = match &mut *delivery {
            Delivery::Serial(serial) => {
                pop_next_delivery::<Request, Streams, Task>(provider, finished_source, serial)
            }
            Delivery::Parallel(parallel) => {
                let label = label.expect(
                    "A request in a parallel async service was blocking without a label. \
                    Please report this to the bevy_impulse maintainers; this should not be possible."
                );
                let serial = parallel.labeled.get_mut(&label).expect(
                    "A labeled request in a parallel async service finished but the queue \
                    for its label has been erased. Please report this to the bevy_impulse \
                    maintainers; this should not be possible."
                );
                pop_next_delivery::<Request, Streams, Task>(provider, finished_source, serial)
            }
        };

        let Some(next_delivery) = next_delivery else {
            // Nothing left to unblock
            return;
        };
        let Deliver { request, task_id, blocker } = next_delivery;
        let session = blocker.session;
        let source = blocker.source;

        let mut service = world.get_entity_mut(provider)
            .unwrap()
            .get_mut::<AsyncServiceStorage<Request, Streams, Task>>()
            .unwrap()
            .0
            .take()
            .unwrap();

        let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let channel = InnerChannel::new(source, sender);
        let job = service.run(AsyncService { request, channel: channel.into_specific(), provider }, world);
        service.apply_deferred(world);

        if let Some(mut provider_mut) = world.get_entity_mut(provider) {
            provider_mut.get_mut::<AsyncServiceStorage<Request, Streams, Task>>().unwrap().0 = Some(service);
        }

        let task = AsyncComputeTaskPool::get().spawn(job);

        if let Some(source_mut) = world.get_entity(source) {
            let Some(target) = source_mut.get::<SingleTargetStorage>() else {
                unblock = blocker;
                continue;
            };
            let operate_task = OperateTask::new(session, source, target.get(), task, Some(blocker));
            operate_task.setup(OperationSetup { source: task_id, world });
            roster.queue(task_id);
        } else {
            // The request cancelled itself while running the service so we should
            // move on to the next request.
            unblock = blocker;
            continue;
        }

        // The next delivery has begun so we can return
        return;
    }
}

fn pop_next_delivery<Request, Streams, Task>(
    provider: Entity,
    finished_source: Entity,
    serial: &mut SerialDelivery<Request>,
) -> Option<Deliver<Request>>
where
    Request: 'static + Send + Sync,
    Task: Future + 'static + Send,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    let current = serial.delivering.take().expect(
        "Unblocking has been requested for an async service that is not currently \
        executing a request. Please report this to the bevy_impulse maintainers; \
        this should not be possible."
    );
    assert_eq!(current.source, finished_source);
    let Some(DeliveryOrder { source, session, task_id, request, instructions }) = serial.queue.pop_front() else {
        return None;
    };
    let blocker = Blocker {
        provider,
        source,
        session,
        label: instructions.as_ref().map(|x| x.label.clone()),
        serve_next: serve_next_async_request::<Request, Streams, Task>,
    };

    serial.delivering = Some(ActiveDelivery { source, session, task_id, instructions });
    return Some(Deliver { request, task_id, blocker });
}

struct Deliver<Request> {
    request: Request,
    task_id: Entity,
    blocker: Blocker,
}

#[derive(Component, Clone, Copy)]
pub(crate) struct DeliveryInstructions {
    pub(crate) label: RequestLabelId,
    pub(crate) queue: bool,
    pub(crate) ensure: bool,
}

pub(crate) struct DeliveryOrder<Request> {
    pub(crate) source: Entity,
    pub(crate) session: Entity,
    pub(crate) task_id: Entity,
    pub(crate) request: Request,
    pub(crate) instructions: Option<DeliveryInstructions>,
}

struct ActiveDelivery {
    source: Entity,
    session: Entity,
    task_id: Entity,
    instructions: Option<DeliveryInstructions>,
}

/// The delivery mode determines whether service requests are carried out one at
/// a time (serial) or in parallel.
#[derive(Component)]
pub(crate) enum Delivery<Request> {
    Serial(SerialDelivery<Request>),
    Parallel(ParallelDelivery<Request>),
}

impl<Request> Delivery<Request> {
    pub(crate) fn serial() -> Self {
        Delivery::Serial(SerialDelivery::<Request>::default())
    }

    pub(crate) fn parallel() -> Self {
        Delivery::Parallel(ParallelDelivery::<Request>::default())
    }
}

pub(crate) struct SerialDelivery<Request> {
    delivering: Option<ActiveDelivery>,
    queue: VecDeque<DeliveryOrder<Request>>,
}

impl<Request> Default for SerialDelivery<Request> {
    fn default() -> Self {
        Self {
            delivering: Default::default(),
            queue: Default::default(),
        }
    }
}

pub(crate) struct ParallelDelivery<Request> {
    labeled: HashMap<RequestLabelId, SerialDelivery<Request>>,
}

impl<Request> Default for ParallelDelivery<Request> {
    fn default() -> Self {
        Self { labeled: Default::default() }
    }
}

enum DeliveryUpdate<Request> {
    /// The new request should be delivered immediately
    Immediate {
        blocking: Option<Option<RequestLabelId>>,
        request: Request,
    },
    /// The new request has been placed in the queue
    Queued {
        /// Queued requests that have been cancelled
        cancelled: SmallVec<[DeliveryStoppage; 8]>,
        /// An actively running task that has been cancelled
        stop: Option<DeliveryStoppage>,
        /// The label that the blocking is based on
        label: Option<RequestLabelId>,
    }
}

struct DeliveryStoppage {
    source: Entity,
    session: Entity,
    task_id: Entity,
}

fn insert_new_order<Request>(
    delivery: &mut Delivery<Request>,
    order: DeliveryOrder<Request>,
) -> DeliveryUpdate<Request> {
    match delivery {
        Delivery::Serial(serial) => {
            insert_serial_order(serial, order)
        }
        Delivery::Parallel(parallel) => {
            match &order.instructions {
                Some(instructions) => {
                    let label = instructions.label.clone();
                    insert_serial_order(
                        parallel.labeled.entry(label).or_default(),
                        order,
                    )
                }
                None => {
                    DeliveryUpdate::Immediate { request: order.request, blocking: None }
                }
            }
        }
    }
}

fn insert_serial_order<Request>(
    serial: &mut SerialDelivery<Request>,
    order: DeliveryOrder<Request>,
) -> DeliveryUpdate<Request> {
    let Some(delivering) = &serial.delivering else {
        // INVARIANT: If there is anything in the queue then it should have been
        // moved into delivering when the last delivery was finished. If
        // delivering is empty then the queue should be as well.
        assert!(serial.queue.is_empty());
        serial.delivering = Some(ActiveDelivery {
            source: order.source,
            session: order.session,
            task_id: order.task_id,
            instructions: order.instructions
        });
        let label = order.instructions.map(|i| i.label);
        return DeliveryUpdate::Immediate { blocking: Some(label), request: order.request };
    };

    let Some(incoming_instructions) = order.instructions else {
        serial.queue.push_back(order);
        return DeliveryUpdate::Queued {
            cancelled: SmallVec::new(),
            stop: None,
            label: None,
        };
    };

    let mut cancelled = SmallVec::new();
    let mut stop = None;

    let should_discard = |prior_instructions: &DeliveryInstructions| {
        prior_instructions.label == incoming_instructions.label
        && !prior_instructions.ensure
    };

    if !incoming_instructions.queue {
        serial.queue.retain(|e| {
            let discard = e.instructions.as_ref().is_some_and(should_discard);
            if discard {
                cancelled.push(DeliveryStoppage {
                    source: e.source,
                    session: e.session,
                    task_id: e.task_id,
                });
            }

            !discard
        });
    }

    if delivering.instructions.as_ref().is_some_and(should_discard) {
        stop = Some(DeliveryStoppage {
            source: delivering.source,
            session: delivering.session,
            task_id: delivering.task_id,
        });
    }

    serial.queue.push_back(order);
    let label = Some(incoming_instructions.label);

    DeliveryUpdate::Queued { cancelled, stop, label }
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
