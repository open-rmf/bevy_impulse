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
    AsyncService, InAsyncService, IntoService, ServiceTrait, ServiceBundle, ServiceRequest, InputStorage,
    InputBundle, InnerChannel, ChannelQueue, RequestLabelId, SingleTargetStorage, OperationRoster, BlockingQueue,
    Stream, ServiceBuilder, ChooseAsyncServiceDelivery, Cancel, OperationRequest,
    service::builder::{SerialChosen, ParallelChosen},
    private,
};

use bevy::{
    prelude::{Component, In, Entity, World, Resource, Bundle},
    tasks::{AsyncComputeTaskPool, Task as BevyTask},
    ecs::{
        world::EntityMut,
        system::{IntoSystem, BoxedSystem, EntityCommands},
    }
};

use backtrace::Backtrace;

use std::{
    task::Poll,
    future::Future,
    pin::Pin,
    task::Context,
    sync::Arc,
    collections::{VecDeque, HashMap},
};

use futures::task::{waker_ref, ArcWake};

use crossbeam::channel::{unbounded, Sender as CbSender, Receiver as CbReceiver};

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

impl<Request, Streams, Task, M, Srv> private::Sealed<(Request, Streams, Task, M)> for Srv
where
    Srv: IntoSystem<AsyncService<Request, Streams>, Task, M>,
    Task: Future + 'static + Send,
    Streams: Stream + 'static,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
{

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
    fn serve(cmd: ServiceRequest) {
        let ServiceRequest { provider, target, operation: OperationRequest { source, requester, world, roster } } = cmd;

        let instructions = if let Some(mut source_mut) = world.get_entity_mut(source) {
            source_mut.take::<DeliveryInstructions>()
        } else {
            // The source entity does not exist which implies the request has been cancelled.
            // We no longer need to deliver on it.
            roster.cancel(Cancel::broken_here(source));
            return;
        };

        let Some(mut provider_mut) = world.get_entity_mut(provider) else {
            // The async service has been despawned, so we should treat the request as cancelled.
            roster.cancel(Cancel::service_unavailable(source, provider));
            return;
        };

        let Some(mut delivery) = provider_mut.get_mut::<Delivery>() else {
            // The async service's Delivery component has been removed so we should treat the request as cancelled.
            roster.cancel(Cancel::service_unavailable(source, provider));
            return
        };

        let update = insert_new_order(delivery.as_mut(), DeliveryOrder { source, instructions });
        let blocking = match update {
            DeliveryUpdate::Immediate { blocking } => {
                let serve_next = serve_next_async_request::<Request, Streams, Task>;
                blocking.map(|label| BlockingQueue { provider, source, label, serve_next })
            }
            DeliveryUpdate::Queued { cancelled, stop } => {
                if let Some(cancelled_source) = stop {
                    roster.cancel(Cancel::supplanted(cancelled_source, source));
                }
                for cancelled_source in cancelled {
                    roster.cancel(Cancel::supplanted(cancelled_source, source));
                }
                return;
            }
        };

        let mut cmd = ServiceRequest {
            provider, target,
            operation: OperationRequest { source, requester, world, roster }
        };
        let Some(request) = cmd.from_source::<InputStorage<Request>>() else {
            return;
        };
        let request = request.take();

        let ServiceRequest {
            provider, target,
            operation: OperationRequest { source, requester, world, roster }
        } = cmd;

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
                    roster.cancel(Cancel::service_unavailable(source, provider));
                    return;
                }
            }
        } else {
            // If the provider has been despawned then we treat this request as cancelled.
            roster.cancel(Cancel::service_unavailable(source, provider));
            return;
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

        if let Some(mut source_mut) = world.get_entity_mut(source) {
            source_mut.insert(TaskBundle::new(task));
            roster.poll(source);
            if let Some(blocking) = blocking {
                source_mut.insert(blocking);
            }
        }
    }
}

pub(crate) fn serve_next_async_request<Request, Streams, Task>(
    mut unblock: BlockingQueue,
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
        let BlockingQueue { provider, source: finished_source, label, .. } = unblock;
        let Some(mut provider_mut) = world.get_entity_mut(provider) else {
            return;
        };
        let Some(mut delivery) = provider_mut.get_mut::<Delivery>() else {
            return;
        };

        let next_blocking = match &mut *delivery {
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

        let Some(next_blocking) = next_blocking else {
            // Nothing left to unblock
            return;
        };

        let source = next_blocking.source;
        let Some(mut source_mut) = world.get_entity_mut(source) else {
            roster.cancel(Cancel::broken_here(source));
            unblock = next_blocking;
            continue;
        };

        let Some(request) = source_mut.take::<InputStorage<Request>>() else {
            roster.cancel(Cancel::broken_here(source));
            unblock = next_blocking;
            continue;
        };
        let request = request.take();

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

        if let Some(mut source_mut) = world.get_entity_mut(source) {
            source_mut.insert((TaskBundle::new(task), next_blocking));
            roster.poll(source);
        } else {
            // The request cancelled itself while running the service so we should
            // move on to the next request.
            unblock = next_blocking;
            continue;
        }

        // The next delivery has begun so we can return
        return;
    }

}

fn pop_next_delivery<Request, Streams, Task>(
    provider: Entity,
    finished_source: Entity,
    serial: &mut SerialDelivery
) -> Option<BlockingQueue>
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
    let Some(next) = serial.queue.pop_front() else {
        return None;
    };
    let block = BlockingQueue {
        provider,
        source: next.source,
        label: next.instructions.as_ref().map(|x| x.label.clone()),
        serve_next: serve_next_async_request::<Request, Streams, Task>,
    };
    serial.delivering = Some(next);
    return Some(block);
}

struct JobWaker {
    sender: CbSender<Entity>,
    entity: Entity,
}

impl ArcWake for JobWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.sender.send(arc_self.entity).ok();
    }
}

#[derive(Component)]
struct JobWakerStorage(Arc<JobWaker>);

#[derive(Resource)]
pub(crate) struct WakeQueue {
    sender: CbSender<Entity>,
    pub(crate) receiver: CbReceiver<Entity>,
}

impl WakeQueue {
    pub(crate) fn new() -> WakeQueue {
        let (sender, receiver) = unbounded();
        WakeQueue { sender, receiver }
    }
}

#[derive(Component)]
struct TaskStorage<Response>(BevyTask<Response>);

#[derive(Component)]
pub(crate) struct PollTask(pub(crate) fn(Entity, &mut World, &mut OperationRoster));

#[derive(Bundle)]
pub(crate) struct TaskBundle<Response: 'static + Send + Sync> {
    task: TaskStorage<Response>,
    poll: PollTask,
}

impl<Response: 'static + Send + Sync> TaskBundle<Response> {
    pub(crate) fn new(
        task: BevyTask<Response>,
    ) -> TaskBundle<Response> {
        TaskBundle {
            task: TaskStorage(task),
            poll: PollTask(poll_task::<Response>),
        }
    }
}

pub(crate) fn poll_task<Response: 'static + Send + Sync>(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let Some(mut source_mut) = world.get_entity_mut(source) else {
        roster.cancel(Cancel::broken_here(source));
        return;
    };
    let Some(mut task) = source_mut.take::<TaskStorage<Response>>().map(|t| t.0) else {
        roster.cancel(Cancel::broken_here(source));
        return;
    };

    let waker = if let Some(waker) = source_mut.take::<JobWakerStorage>() {
        waker.0.clone()
    } else {
        let wake_queue = world.get_resource_or_insert_with(|| WakeQueue::new());
        let waker = Arc::new(JobWaker {
            sender: wake_queue.sender.clone(),
            entity: source,
        });
        waker
    };

    match Pin::new(&mut task).poll(
        &mut Context::from_waker(&waker_ref(&waker))
    ) {
        Poll::Ready(result) => {
            // Task has finished
            let mut source_mut = world.entity_mut(source);
            let Some(target) = source_mut.get::<SingleTargetStorage>() else {
                roster.cancel(Cancel::broken_here(source));
                return;
            };
            let target = target.0;

            if let Some(unblock) = source_mut.take::<BlockingQueue>() {
                roster.unblock(unblock);
            }

            world.entity_mut(target).insert(InputBundle::new(result));
            roster.queue(target);
            roster.dispose(source);
            return;
        }
        Poll::Pending => {
            // Task is still running
            world.entity_mut(source).insert((
                TaskStorage(task),
                JobWakerStorage(waker),
            ));
        }
    }
}

#[derive(Component, Clone, Copy)]
pub(crate) struct DeliveryInstructions {
    pub(crate) label: RequestLabelId,
    pub(crate) queue: bool,
    pub(crate) ensure: bool,
}

pub(crate) struct DeliveryOrder {
    pub(crate) source: Entity,
    pub(crate) instructions: Option<DeliveryInstructions>,
}

/// The delivery mode determines whether service requests are carried out one at
/// a time (serial) or in parallel.
#[derive(Component)]
pub(crate) enum Delivery {
    Serial(SerialDelivery),
    Parallel(ParallelDelivery),
}

impl Delivery {
    pub(crate) fn serial() -> Self {
        Delivery::Serial(SerialDelivery::default())
    }

    pub(crate) fn parallel() -> Self {
        Delivery::Parallel(ParallelDelivery::default())
    }
}

#[derive(Default)]
pub(crate) struct SerialDelivery {
    delivering: Option<DeliveryOrder>,
    queue: VecDeque<DeliveryOrder>,
}

#[derive(Default)]
pub(crate) struct ParallelDelivery {
    labeled: HashMap<RequestLabelId, SerialDelivery>,
}

enum DeliveryUpdate {
    /// The new request should be delivered immediately
    Immediate { blocking: Option<Option<RequestLabelId>> },
    /// The new request has been placed in the queue
    Queued {
        /// Queued requests that have been cancelled
        cancelled: SmallVec<[Entity; 8]>,
        /// An actively running task that has been cancelled
        stop: Option<Entity>,
    }
}

impl DeliveryUpdate {
    fn with_label(mut self, label: RequestLabelId) -> Self {
        if let Self::Immediate { blocking } = &mut self {
            if let Some(blocking) = blocking {
                *blocking = Some(label);
            }
        }
        self
    }
}

fn insert_new_order(
    delivery: &mut Delivery,
    order: DeliveryOrder,
) -> DeliveryUpdate {
    match delivery {
        Delivery::Serial(serial) => {
            insert_serial_order(serial, order)
        }
        Delivery::Parallel(parallel) => {
            match order.instructions {
                Some(instructions) => {
                    let update = insert_serial_order(
                        parallel
                            .labeled
                            .entry(instructions.label.clone())
                            .or_default(),
                        order,
                    );
                    update.with_label(instructions.label)
                }
                None => {
                    DeliveryUpdate::Immediate { blocking: None }
                }
            }
        }
    }
}

fn insert_serial_order(
    serial: &mut SerialDelivery,
    order: DeliveryOrder,
) -> DeliveryUpdate {
    let Some(delivering) = &serial.delivering else {
        // INVARIANT: If there is anything in the queue then it should have been
        // moved into delivering when the last delivery was finished. If
        // delivering is empty then the queue should be as well.
        assert!(serial.queue.is_empty());
        serial.delivering = Some(order);
        return DeliveryUpdate::Immediate { blocking: Some(None) };
    };

    let Some(incoming_instructions) = order.instructions else {
        serial.queue.push_back(order);
        return DeliveryUpdate::Queued {
            cancelled: SmallVec::new(),
            stop: None,
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
                cancelled.push(e.source);
            }

            !discard
        });
    }

    if delivering.instructions.as_ref().is_some_and(should_discard) {
        stop = Some(delivering.source);
    }

    serial.queue.push_back(order);

    DeliveryUpdate::Queued { cancelled, stop }
}

/// Take any system that was not decalred as a service and transform it into a
/// blocking service that can be passed into a ServiceBuilder.
pub struct AsAsyncService<Srv>(pub Srv);

pub trait IntoAsyncService<M>: private::Sealed<M> {
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

impl<Request, Response, M, Sys> private::Sealed<AsAsyncService<(Request, Response, M)>> for Sys { }

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

impl<Request, Task, M, Sys> private::Sealed<(Request, Task, M)> for AsAsyncService<Sys> { }

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
