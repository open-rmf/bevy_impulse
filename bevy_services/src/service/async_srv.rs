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
    AsyncReq, InAsyncReq, IntoService, ServiceTrait, ServiceMarker, ServiceRequest, InputStorage,
    InputBundle, InnerChannel, ChannelQueue, RequestLabelId, TargetStorage, OperationRoster, BlockingQueue,
    IntoStreamBundle, ServiceBuilder,
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

#[derive(Component)]
struct AsyncServiceStorage<Request, Streams, Task>(Option<BoxedSystem<AsyncReq<Request, Streams>, Task>>);

#[derive(Component)]
struct UninitAsyncServiceStorage<Request, Streams, Task>(BoxedSystem<AsyncReq<Request, Streams>, Task>);

impl<Request, Streams, Task, M, Sys> IntoService<(Request, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<AsyncReq<Request, Streams>, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send,
    Task::Output: 'static + Send,
    Streams: 'static + Send + IntoStreamBundle,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;
    type DefaultDeliver = ();

    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        entity_commands.insert((
            UninitAsyncServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceMarker::<Request, Task::Output>::new::<AsyncServiceStorage<Request, Streams, Task>>(),
        ));
    }

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        entity_mut.insert((
            UninitAsyncServiceStorage(Box::new(IntoSystem::into_system(self))),
            ServiceMarker::<Request, Task::Output>::new::<AsyncServiceStorage<Request, Streams, Task>>(),
        ));
    }
}

impl<Request, Streams, Task> ServiceTrait for AsyncServiceStorage<Request, Streams, Task>
where
    Request: 'static + Send,
    Task: Future + 'static + Send,
    Streams: IntoStreamBundle,
{
    fn serve(mut cmd: ServiceRequest) {
        let ServiceRequest { provider, source, target, world, roster } = cmd;

        let instructions = if let Some(mut source_mut) = world.get_entity_mut(source) {
            source_mut.take::<DeliveryInstructions>()
        } else {
            // The source entity does not exist which implies the request has been canceled.
            // We no longer need to deliver on it.
            roster.cancel(source);
            return;
        };

        let Some(mut provider_mut) = world.get_entity_mut(provider) else {
            // The async service has been despawned, so we should treat the request as canceled.
            roster.cancel(source);
            return;
        };

        let Some(mut delivery) = provider_mut.get_mut::<Delivery>() else {
            // The async service's Delivery component has been removed so we should treat the request as canceled.
            roster.cancel(source);
            return
        };

        let update = insert_new_order(delivery.as_mut(), DeliveryOrder { source, instructions });
        let blocking = match update {
            DeliveryUpdate::Immediate { blocking } => {
                blocking.map(|label| BlockingQueue { provider, source, label })
            }
            DeliveryUpdate::Queued { canceled, stop } => {
                if let Some(source) = stop {
                    roster.cancel(source);
                }
                for source in canceled {
                    roster.cancel(source);
                }
                return;
            }
        };

        let mut cmd = ServiceRequest { provider, source, target, world, roster };
        let Some(request) = cmd.from_source::<InputStorage<Request>>() else {
            return;
        };

        let ServiceRequest { provider, source, target, world, roster } = cmd;

        let mut service = if let Some(mut provider_mut) = world.get_entity_mut(provider) {
            if let Some(mut storage) = provider_mut.get_mut::<AsyncServiceStorage<Request, Streams, Task>>() {
                storage.0.take().expect("Service is missing while attempting to serve")
            } else {
                if let Some(uninit) = provider_mut.get_mut::<UninitAsyncServiceStorage<Request, Streams, Task>>() {
                    // We need to initialize the service
                    let mut service = uninit.0;
                    service.initialize(world);
                    provider_mut.insert(AsyncServiceStorage::<Request, Streams, Task>(None));
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

        let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let channel = InnerChannel::new(source, sender);
        let job = service.run(AsyncReq { request, channel: channel.into_specific(), provider }, world);
        service.apply_deferred(world);

        let task = AsyncComputeTaskPool::get().spawn(job);

        if let Some(mut source_mut) = world.get_entity_mut(source) {
            source_mut.insert(TaskBundle::new(task, blocking));
        }
    }
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
struct WakeQueue {
    sender: CbSender<Entity>,
    receiver: CbReceiver<Entity>,
}

impl WakeQueue {
    fn new() -> WakeQueue {
        let (sender, receiver) = unbounded();
        WakeQueue { sender, receiver }
    }
}

#[derive(Component)]
struct TaskStorage<Response>(BevyTask<Option<Response>>);

#[derive(Component)]
struct PollTask(fn(Entity, &mut World) -> Result<bool, ()>);

#[derive(Bundle)]
pub(crate) struct TaskBundle<Response: 'static + Send + Sync> {
    task: TaskStorage<Response>,
    poll: PollTask,
    blocking: Option<BlockingQueue>,
}

impl<Response: 'static + Send + Sync> TaskBundle<Response> {
    pub(crate) fn new(
        task: BevyTask<Option<Response>>,
        blocking: Option<BlockingQueue>,
    ) -> TaskBundle<Response> {
        TaskBundle {
            task: TaskStorage(task),
            poll: PollTask(poll_task::<Response>),
            blocking,
        }
    }
}

/// Ok(true): Task has finished
/// Ok(false): Task is still running
/// Err(()): Task is canceled
pub(crate) fn poll_task<Response: 'static + Send + Sync>(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) -> Result<bool, ()> {
    let mut source_mut = world.get_entity_mut(source).ok_or(())?;
    let mut task = source_mut.take::<TaskStorage<Response>>().ok_or(())?.0;
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
        Poll::Ready(Some(result)) => {
            // Task has finished
            let mut source_mut = world.entity_mut(source);
            let Some(target) = source_mut.take::<TargetStorage>() else {
                roster.cancel(source);
                return;
            };
            let target = target.0;

            if let Some(unblock) = source_mut.take::<BlockingQueue>() {
                roster.unblock(unblock);
            }

            world.entity_mut(target).insert(InputBundle::new(result));
            roster.queue(target);
            Ok(true)
        }
        Poll::Ready(None) => {
            // Task has canceled
            Err(())
        }
        Poll::Pending => {
            // Task is still running
            world.entity_mut(source).insert((
                TaskStorage(task),
                JobWakerStorage(waker),
            ));
            Ok(false)
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
        /// Queued requests that have been canceled
        canceled: SmallVec<[Entity; 3]>,
        /// An actively running task that has been canceled
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
        // moved into delivering when the last delivering was finished. If
        // delivering is empty then the queue should be as well.
        assert!(serial.queue.is_empty());
        return DeliveryUpdate::Immediate;
    };

    let Some(incoming_instructions) = order.instructions else {
        serial.queue.push_back(order);
        return DeliveryUpdate::Queued {
            canceled: SmallVec::new(),
            stop: None,
        };
    };

    let mut canceled = SmallVec::new();
    let mut stop = None;

    let should_discard = |prior_instructions: &DeliveryInstructions| {
        prior_instructions.label == incoming_instructions.label
        && !prior_instructions.ensure
    };

    if !incoming_instructions.queue {
        serial.queue.retain(|e| {
            let discard = e.instructions.as_ref().is_some_and(should_discard);
            if discard {
                canceled.push(e.source);
            }

            !discard
        });
    }

    if delivering.instructions.as_ref().is_some_and(should_discard) {
        stop = Some(delivering.source);
    }

    serial.queue.push_back(order);

    DeliveryUpdate::Queued { canceled, stop }
}

/// Take any system that was not decalred as a service and transform it into a
/// blocking service that can be passed into a ServiceBuilder.
pub struct AsAsyncService<Srv>(pub Srv);

pub trait IntoAsyncService<M>: private::Sealed<M> {
    fn into_async_service(self) -> AsAsyncService<Self>;
}

impl<Request, Response, M, Sys> IntoAsyncService<(Request, Response, M)> for Sys
where
    Sys: IntoSystem<Request, Response, M>,
    Request: 'static + Send,
    Response: 'static + Send,
{
    fn into_async_service(self) -> AsAsyncService<Self> {
        AsAsyncService(self)
    }
}

impl<Request, Task, M, Sys> IntoService<(Request, Task, M)> for AsAsyncService<Sys>
where
    Sys: IntoSystem<Request, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send,
    Task::Output: 'static + Send,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = ();
    type DefaultDeliver = BlockingQueue;

    fn insert_service_commands<'w, 's, 'a>(self, entity_commands: &mut EntityCommands<'w, 's, 'a>) {
        peel_async.pipe(self).insert_service_commands(entity_commands)
    }

    fn insert_service_mut<'w>(self, entity_mut: &mut EntityMut<'w>) {
        peel_async.pipe(self).insert_service_mut(entity_mut)
    }
}

/// This trait allows async service systems to be converted into a builder
/// by specifying whether it should have serial or parallel service delivery.
pub trait ChooseAsyncServiceDelivery<Marker>: private::Sealed<Marker> {
    type Service;
    fn serial(self) -> ServiceBuilder<Self::Service, SerialChosen, (), ()>;
    fn parallel(self) -> ServiceBuilder<Self::Service, ParallelChosen, (), ()>;
}

impl<Request, Streams, Task, M, Srv> ChooseAsyncServiceDelivery<(Request, Streams, Task, M)> for Srv
where
    Srv: IntoSystem<AsyncReq<Request>, Task, M>,
    Task: Future + 'static + Send,
    Streams: IntoStreamBundle + 'static,
    Request: 'static + Send,
    Task::Output: 'static + Send,
{
    type Service = Srv;
    fn serial(self) -> ServiceBuilder<Srv, SerialChosen, (), ()> {
        ServiceBuilder::new_async(self).serial()
    }
    fn parallel(self) -> ServiceBuilder<Srv, ParallelChosen, (), ()> {
        ServiceBuilder::new_async(self).parallel()
    }
}

fn peel_async<Request>(In(AsyncReq { request, .. }): InAsyncReq<Request>) -> Request {
    request
}
