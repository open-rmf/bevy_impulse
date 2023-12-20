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

use crate::{RequestLabelId, GenericAssistant, Req, Resp, Job};

use bevy::{
    prelude::{Component, Entity, World, Bundle, Resource},
    ecs::system::{Command, BoxedSystem},
    tasks::{Task, AsyncComputeTaskPool},
};

use smallvec::SmallVec;

use std::collections::{HashMap, VecDeque};

mod fork;
pub(crate) use fork::*;

mod map;
pub(crate) use map::*;

mod then;
pub(crate) use then::*;

mod servable;
pub(crate) use servable::*;
pub use servable::traits::*;

mod terminate;
pub(crate) use terminate::*;

#[derive(Resource)]
struct BlockingDeliveryQueue {
    is_delivering: bool,
    queue: VecDeque<(DispatchCommand, fn(&mut World, DispatchCommand))>,
}

#[derive(Component)]
pub(crate) struct Target(pub(crate) Entity);

pub(crate) type BoxedJob<Response> = Job<Box<dyn FnOnce(GenericAssistant) -> Option<Response> + Send>>;

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
    pub(crate) target: Entity,
}

impl DispatchCommand {
    pub(crate) fn new(provider: Entity, target: Entity) -> Self {
        Self { provider, target }
    }
}

impl Command for DispatchCommand {
    fn apply(self, world: &mut World) {
        let Some(provider_ref) = world.get_entity(self.provider) else {
            cancel(world, self.target);
            return;
        };

        let Some(dispatcher) = provider_ref.get::<Dispatch>() else {
            cancel(world, self.target);
            return;
        };

        (dispatcher.0)(world, self);
    }
}
pub(crate) fn cancel(world: &mut World, target: Entity) {

}

fn abort(world: &mut World, target: Entity) {

}

fn dispatch_blocking<Request: 'static + Send + Sync, Response: 'static + Send + Sync>(
    world: &mut World,
    cmd: DispatchCommand,
) {
    let mut blocking_delivery = world.resource_mut::<BlockingDeliveryQueue>();
    blocking_delivery.queue.push_back((cmd, dispatch_single_blocking::<Request, Response>));
    if blocking_delivery.is_delivering {
        // Blocking services are already being delivered, so to keep things
        // simple we will add this dispatch command to the queue and let the
        // services be processed one at a time. Otherwise service recursion gets
        // messy or even impossible.
        return;
    }

    blocking_delivery.is_delivering = true;
    while let Some((cmd, dispatch)) = world.resource_mut::<BlockingDeliveryQueue>().queue.pop_back() {
        dispatch(world, cmd);
    }
    world.resource_mut::<BlockingDeliveryQueue>().is_delivering = false;
}

fn dispatch_single_blocking<Request: 'static + Send + Sync, Response: 'static + Send + Sync>(
    world: &mut World,
    cmd: DispatchCommand,
) {
    let DispatchCommand {target, provider} = cmd;
    let request = if let Some(mut target) = world.get_entity_mut(target) {
        // INVARIANT: If the target entity exists then it must have been created
        // for the purpose of this service. PromiseCommands must have
        // inserted a RequestStorage of the correct type and placed that command
        // before the RequestCommand that triggered this dispatch. No outside
        // code is allowed to remove the RequestStorage component from the
        // entity. No other code can take the request data out of the
        // RequestStorage, and this code will only ever do it once per target.
        // Therefore if the implementation is correct and the entity exists then
        // this component must exist and it must contain the data inside.
        target.get_mut::<RequestStorage<Request>>().unwrap().0.take().unwrap()
    } else {
        // The target entity does not exist which implies the request has been
        // canceled. We no longer need to deliver on it.
        cancel(world, target);
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
        cancel(world, target);
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
        target_mut.insert(ResponseStorage(Some(response)));
        handle_response(world, target);
    } else {
        // Apparently the task was canceled by the service itself .. ?
        cancel(world, target);
    }
}

fn dispatch_async_request<Request: 'static + Send + Sync, Response: 'static + Send + Sync>(
    world: &mut World,
    cmd: DispatchCommand,
) {
    let DispatchCommand {target, provider} = cmd;

    let instructions = if let Some(mut target_mut) = world.get_entity_mut(target) {
        target_mut.take::<DeliveryInstructions>()
    } else {
        // The target entity does not exist which implies the request has been
        // canceled. We no longer need to deliver on it.
        cancel(world, target);
        return;
    };

    let Some(mut provider_mut) = world.get_entity_mut(provider) else {
        // The async service has been despawned, so we should treat the request
        // as canceled.
        cancel(world, target);
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
                abort(world, target);
            }
            for target in canceled {
                cancel(world, target);
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

    if let Some(mut target_mut) = world.get_entity_mut(target) {
        // INVARIANT: See the rationale in dispatch_single_blocking
        let request = target_mut.get_mut::<RequestStorage<Request>>().unwrap().0.take().unwrap();
        let Job(job) = service.run((provider, Req(request)), world);
        service.apply_deferred(world);

        let task = AsyncComputeTaskPool::get().spawn(async move {
            job(GenericAssistant {  })
        });

        if let Some(mut target_mut) = world.get_entity_mut(target) {
            target_mut.insert((
                TaskStorage(task),
                PollTask {
                    provider: Some(provider),
                    poll: poll_task::<Response>,
                }
            ));
        } else {
            // The target has been despawned, so we treat that as the request
            // being canceled.
            cancel(world, target);
        }
    } else {
        cancel(world, target);
    };

    let Some(mut provider_mut) = world.get_entity_mut(provider) else {
        // The async service was despawned by the service itself, so we should
        // treat the request as canceled.
        abort(world, target);
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

pub(crate) fn handle_response(
    world: &mut World,
    target: Entity,
) {

}

#[derive(Component)]
pub(crate) struct RequestStorage<Request>(pub(crate) Option<Request>);

#[derive(Component)]
pub(crate) struct ResponseStorage<Response>(pub(crate) Option<Response>);

#[derive(Component)]
pub(crate) struct TaskStorage<Response>(pub(crate) Task<Option<Response>>);

#[derive(Component)]
pub(crate) enum OnCancel {
    OneTime(BoxedSystem),
    Service(Entity),
}

#[derive(Component)]
pub(crate) struct UnusedTarget;

#[derive(Component)]
pub(crate) struct PollTask {
    pub(crate) provider: Option<Entity>,
    pub(crate) poll: fn(&mut World, Entity, Entity),
}

pub(crate) fn poll_task<Response>(world: &mut World, provider: Entity, target: Entity) {

}

/// This component determines how a provider responds to a dispatch request.
#[derive(Component)]
pub(crate) struct Dispatch(pub(crate) fn(&mut World, DispatchCommand));

/// This component determines how a target responds to a pending delivery being
/// fulfilled. The arguments are
/// - The world
/// - A queue of entities whose pending behaviors should be triggered next. When
///   this callback is triggered, it should add to the queue any targets that it
///   is delivering responses to.
/// - The entity that owns this Pending component.
#[derive(Component)]
pub(crate) struct Pending(pub(crate) fn(&mut World, &mut VecDeque<Entity>, Entity));

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
