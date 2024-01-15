/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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

use crate::{RequestLabelId, InnerChannel, Req, Resp, Job, ChannelQueue};

use bevy::{
    prelude::{Component, Entity, World, Bundle, Resource},
    ecs::system::BoxedSystem,
    tasks::{Task, AsyncComputeTaskPool},
};

use smallvec::SmallVec;

use std::{
    task::Poll,
    future::Future,
    pin::Pin,
    task::Context,
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use futures::task::{waker_ref, ArcWake};

use crossbeam::channel::{unbounded, Sender as CbSender, Receiver as CbReceiver};

mod operation;
pub(crate) use operation::*;

mod fork;
pub(crate) use fork::*;

mod map;
pub(crate) use map::*;

mod serve;
pub(crate) use serve::*;

mod serve_once;
pub(crate) use serve_once::*;
pub use serve_once::traits::*;

mod terminate;
pub(crate) use terminate::*;

#[derive(Resource)]
struct ServiceQueue {
    is_delivering: bool,
    queue: VecDeque<DispatchCommand>,
}

#[derive(Component)]
pub(crate) struct TargetStorage(pub(crate) Entity);

pub(crate) type BoxedJob<Response> = Job<Box<dyn FnOnce(InnerChannel) -> Option<Response> + Send>>;

/// A service is a type of system that takes in a request and produces a
/// response, optionally emitting events from its streams using the provided
/// assistant.
#[derive(Component)]
pub(crate) enum Service<Request, Response> {
    /// The service takes in the request and blocks execution until the response
    /// is produced.
    Blocking(Option<BoxedSystem<(Entity, Req<Request>), Resp<Response>>>),
    /// The service produces a task that runs asynchronously in the bevy thread
    /// pool.
    Async(Option<BoxedSystem<(Entity, Req<Request>), BoxedJob<Response>>>),
}

impl<Request, Response> Service<Request, Response> {
    fn is_blocking(&self) -> bool {
        matches!(self, Service::Blocking(_))
    }
}

/// This struct encapsulates the data for flushing a command
#[derive(Clone, Copy)]
pub(crate) struct DispatchCommand {
    pub(crate) provider: Entity,
    pub(crate) source: Entity,
    pub(crate) target: Entity,
}

impl DispatchCommand {
    pub(crate) fn new(provider: Entity, source: Entity, target: Entity) -> Self {
        Self { provider, source, target }
    }
}

#[derive(Default)]
pub struct OperationRoster {
    /// Operation sources that should be triggered
    queue: VecDeque<Entity>,
    /// Operation sources that should be canceled
    cancel: VecDeque<Entity>,
}

impl OperationRoster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn queue(&mut self, source: Entity) {
        self.queue.push_back(source);
    }

    pub fn cancel(&mut self, source: Entity) {
        self.cancel.push_back(source);
    }
}

pub(crate) fn dispatch_service(
    cmd: DispatchCommand,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let mut service_queue = world.resource_mut::<ServiceQueue>();
    service_queue.queue.push_back(cmd);
    if service_queue.is_delivering {
        // Services are already being delivered, so to keep things simple we
        // will add this dispatch command to the service queue and let the
        // services be processed one at a time. Otherwise service recursion gets
        // messy or even impossible.
        return;
    }

    service_queue.is_delivering = true;
    while let Some(cmd) = world.resource_mut::<ServiceQueue>().queue.pop_back() {
        let Some(provider_ref) = world.get_entity(cmd.provider) else {
            roster.cancel(cmd.source);
            continue;
        };
        let Some(dispatch) = provider_ref.get::<Dispatch>() else {
            roster.cancel(cmd.source);
            continue;
        };
        (dispatch.0)(cmd, world, roster);
    }
    world.resource_mut::<ServiceQueue>().is_delivering = false;
}

fn dispatch_blocking<Request: 'static + Send + Sync, Response: 'static + Send + Sync>(
    cmd: DispatchCommand,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let DispatchCommand {provider, source, target} = cmd;
    let request = if let Some(mut source_mut) = world.get_entity_mut(source) {
        // INVARIANT: If the target entity exists then it must have been created
        // for the purpose of this service. PromiseCommands must have
        // inserted a RequestStorage of the correct type and placed that command
        // before the RequestCommand that triggered this dispatch. No outside
        // code is allowed to remove the RequestStorage component from the
        // entity. No other code can take the request data out of the
        // RequestStorage, and this code will only ever do it once per target.
        // Therefore if the implementation is correct and the entity exists then
        // this component must exist and it must contain the data inside.
        let request = source_mut.take::<InputStorage<Request>>().unwrap().0;
        source_mut.despawn();
        request
    } else {
        // The target entity does not exist which implies the request has been
        // canceled. We no longer need to deliver on it.
        roster.cancel(target);
        return;
    };

    // Note: We need to fully remove the service callback from the component so
    // that we can call it on the world without upsetting the borrow rules (e.g.
    // the compiler doesn't know if the service callback might try to change the
    // very same component that is storing it).
    let mut service = if let Some(mut provider_mut) = world.get_entity_mut(provider) {
        match &mut *provider_mut.get_mut::<Service<Request, Response>>().unwrap().as_mut() {
            Service::Blocking(service) => service.take().unwrap(),
            _ => {
                // INVARIANT: dispatch_blocking will only be used if the service
                // was set to the Blocking type. We do not allow the Service
                // component to be changed after the initial creation of the
                // service provider. Therefore it would not make sense for this
                // to ever match any variant besides Blocking.
                panic!(
                    "dispatch_blocking was called on a service provider \
                    [{provider:?}] that does not provide a blocking service. \
                    This is an internal error within bevy_services, please \
                    report it to the maintainers."
                );
            }
        }
    } else {
        // If the provider has been despawned then we treat this request as
        // canceled.
        roster.cancel(target);
        return;
    };

    let Resp(response) = service.run((provider, Req(request)), world);
    service.apply_deferred(world);
    if let Some(mut provider_ref) = world.get_entity_mut(provider) {
        match &mut *provider_ref.get_mut::<Service<Request, Response>>().unwrap().as_mut() {
            Service::Blocking(blocking) => {
                *blocking = Some(service);
            }
            _ => {
                // INVARIANT: There should be no way for the service type to be
                // changed after it has initially been set.
                panic!(
                    "The service type of provider [{provider:?}] was changed \
                    while its service was running. This is an internal error \
                    within bevy_services, please report it to the maintainers."
                );
            }
        }
    } else {
        // Apparently the service was despawned by the service itself .. ?
        // But we can still deliver the response to the target, so we will not
        // consider this to be canceled.
    }

    if let Some(mut target_mut) = world.get_entity_mut(target) {
        target_mut.insert(InputBundle::new(response));
    } else {
        // The target is no longer available for a delivery
        roster.cancel(target);
    }
}

fn dispatch_async_request<Request: 'static + Send + Sync, Response: 'static + Send + Sync>(
    cmd: DispatchCommand,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let DispatchCommand {provider, source, target} = cmd;

    let (instructions, request) = if let Some(mut source_mut) = world.get_entity_mut(source) {
        (
            source_mut.take::<DeliveryInstructions>(),
            source_mut.take::<InputStorage<Request>>().unwrap().0,
        )
    } else {
        // The target entity does not exist which implies the request has been
        // canceled. We no longer need to deliver on it.
        roster.cancel(target);
        return;
    };

    let Some(mut provider_mut) = world.get_entity_mut(provider) else {
        // The async service has been despawned, so we should treat the request
        // as canceled.
        roster.cancel(target);
        return;
    };

    // INVARIANT: Async services should always have a Delivery component
    let mut delivery = provider_mut.get_mut::<Delivery>().unwrap();

    let update = insert_new_order(
        delivery.as_mut(),
        DeliveryOrder { target, instructions }
    );

    match update {
        DeliveryUpdate::Immediate => { /* Continue */ }
        DeliveryUpdate::Queued { canceled, stop } => {
            if let Some(target) = stop {
                roster.cancel(target);
            }
            for target in canceled {
                roster.cancel(target);
            }
            return;
        }
    }

    // Note: We need to fully remove the service callback from the component so
    // that we can call it on the world without upsetting the borrow rules.
    let mut service = match &mut *provider_mut.get_mut::<Service<Request, Response>>().unwrap().as_mut() {
        Service::Async(service) => service.take().unwrap(),
        _ => {
            // INVARIANT: dispatch_async_request will only be used if the
            // service was set to Async type.
            panic!(
                "dispatch_async_request was called on a service provider \
                [{provider:?}] that does not provide an async service. \
                This is an internal error within bevy_services, please \
                report it to the maintainers."
            );
        }
    };

    let Job(job) = service.run((provider, Req(request)), world);
    service.apply_deferred(world);

    let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
    let task = AsyncComputeTaskPool::get().spawn(async move {
        job(InnerChannel::new(sender))
    });

    if let Some(mut target_mut) = world.get_entity_mut(target) {
        target_mut.insert((
            TaskStorage(task),
            PollTask(poll_task::<Response>),
        ));
        roster.queue(target);
    } else {
        // The target has been despawned, so we treat that as the request
        // being canceled.
        roster.cancel(target);

        // NOTE: Do NOT return here because the service still needs to be put
        // back into the provider.
    }

    let Some(mut provider_mut) = world.get_entity_mut(provider) else {
        // The async service was despawned by the service itself, so we should
        // treat the request as canceled.
        roster.cancel(target);
        return;
    };
    match &mut *provider_mut.get_mut::<Service<Request, Response>>().unwrap().as_mut() {
        Service::Async(async_service) => {
            *async_service = Some(service);
        }
        _ => {
            // INVARIANT: There should be no way for the service type to be
            // changed after it has initially been set.
            panic!(
                "The service type of provider [{provider:?}] was changed while \
                its service was running. This is an internal error within \
                bevy_services, please report it to the maintainers."
            );
        }
    }
}

enum DeliveryUpdate {
    /// The new request should be delivered immediately
    Immediate,
    /// The new request has been placed in the queue
    Queued {
        /// Queued requests that have been canceled
        canceled: SmallVec<[Entity; 3]>,
        /// An actively running task that has been canceled
        stop: Option<Entity>,
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
            match &order.instructions {
                Some(instructions) => {
                    insert_serial_order(
                        parallel
                            .labeled
                            .entry(instructions.label.clone())
                            .or_default(),
                        order,
                    )
                }
                None => {
                    DeliveryUpdate::Immediate
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
                canceled.push(e.target);
            }

            !discard
        });
    }

    if delivering.instructions.as_ref().is_some_and(should_discard) {
        stop = Some(delivering.target);
    }

    serial.queue.push_back(order);

    DeliveryUpdate::Queued { canceled, stop }
}

/// Marker trait to indicate when an input is ready without knowing the type of
/// input.
#[derive(Component)]
pub(crate) struct InputReady;

#[derive(Component)]
pub(crate) struct InputStorage<Input>(pub(crate) Input);

impl<Input> InputStorage<Input> {
    pub(crate) fn take(self) -> Input {
        self.0
    }
}

#[derive(Bundle)]
pub(crate) struct InputBundle<Input: 'static + Send + Sync> {
    value: InputStorage<Input>,
    ready: InputReady,
}

impl<Input: 'static + Send + Sync> InputBundle<Input> {
    pub(crate) fn new(value: Input) -> Self {
        Self { value: InputStorage(value), ready: InputReady }
    }
}

#[derive(Component)]
struct TaskStorage<Response>(Task<Option<Response>>);

#[derive(Component)]
pub(crate) enum OnCancel {
    OneTime(BoxedSystem),
    Service(Entity),
}

#[derive(Component)]
pub(crate) struct UnusedTarget;

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
struct PollTask(fn(Entity, &mut World) -> Result<bool, ()>);

#[derive(Bundle)]
pub(crate) struct TaskBundle<Response: 'static + Send + Sync> {
    task: TaskStorage<Response>,
    poll: PollTask,
}

impl<Response: 'static + Send + Sync> TaskBundle<Response> {
    pub(crate) fn new(task: Task<Option<Response>>) -> TaskBundle<Response> {
        TaskBundle {
            task: TaskStorage(task),
            poll: PollTask(poll_task::<Response>),
        }
    }
}

/// Ok(true): Task has finished
/// Ok(false): Task is still running
/// Err(()): Task is canceled
pub(crate) fn poll_task<Response: 'static + Send + Sync>(
    source: Entity,
    world: &mut World,
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
            world.entity_mut(source).insert(InputStorage(result));
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

/// This component determines how a provider responds to a dispatch request.
#[derive(Component)]
pub(crate) struct Dispatch(pub(crate) fn(DispatchCommand, &mut World, &mut OperationRoster));

/// This component indicates that a source entity has been queued for a service
/// so it should not be despawned yet.
#[derive(Component)]
pub(crate) struct Queued(pub(crate) Entity);

#[derive(Component)]
struct PollResponse(fn(&mut World, Entity));

impl Dispatch {
    fn new_blocking<Request: 'static + Send + Sync, Response: 'static + Send + Sync>() -> Self {
        Dispatch(dispatch_blocking::<Request, Response>)
    }

    fn new_async<Request: 'static + Send + Sync, Response: 'static + Send + Sync>() -> Self {
        Dispatch(dispatch_async_request::<Request, Response>)
    }
}

#[derive(Bundle)]
pub(crate) struct ServiceBundle<Request: 'static, Response: 'static> {
    service: Service<Request, Response>,
    dispatch: Dispatch,
}

impl<Request: 'static + Send + Sync, Response: 'static + Send + Sync> ServiceBundle<Request, Response> {
    pub(crate) fn new(service: Service<Request, Response>) -> Self {
        let dispatch = if service.is_blocking() {
            Dispatch::new_blocking::<Request, Response>()
        } else {
            Dispatch::new_async::<Request, Response>()
        };

        Self { service, dispatch }
    }
}

#[derive(Component, Clone, Copy)]
pub(crate) struct DeliveryInstructions {
    pub(crate) label: RequestLabelId,
    pub(crate) queue: bool,
    pub(crate) ensure: bool,
}

pub(crate) struct DeliveryOrder {
    pub(crate) target: Entity,
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
