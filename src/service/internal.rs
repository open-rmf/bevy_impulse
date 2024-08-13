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

use bevy_ecs::prelude::{Entity, Component, Bundle, World, Resource};

use tokio::sync::mpsc::{
    unbounded_channel,
    UnboundedSender as TokioSender,
    UnboundedReceiver as TokioReceiver,
};

use anyhow::anyhow;

use std::{collections::VecDeque, sync::Arc};

use crate::{
    OperationRoster, OperationRequest, PendingOperationRequest, ServiceTrait,
    OperationError, UnhandledErrors, MiscellaneousFailure, DeliveryInstructions,
    dispose_for_despawned_service,
};

pub struct ServiceRequest<'a> {
    /// The entity that holds the service that is being used.
    pub(crate) provider: Entity,
    pub(crate) target: Entity,
    pub(crate) instructions: Option<DeliveryInstructions>,
    pub(crate) operation: OperationRequest<'a>,
}

#[derive(Clone, Copy)]
pub struct PendingServiceRequest {
    pub provider: Entity,
    pub target: Entity,
    pub instructions: Option<DeliveryInstructions>,
    pub operation: PendingOperationRequest,
}

impl PendingServiceRequest {
    fn activate<'a>(
        self,
        world: &'a mut World,
        roster: &'a mut OperationRoster
    ) -> ServiceRequest<'a> {
        ServiceRequest {
            provider: self.provider,
            target: self.target,
            instructions: self.instructions,
            operation: self.operation.activate(world, roster),
        }
    }
}

#[derive(Component)]
pub(crate) struct ServiceMarker<Request, Response> {
    _ignore: std::marker::PhantomData<(Request, Response)>,
}

impl<Request, Response> Default for ServiceMarker<Request, Response> {
    fn default() -> Self {
        Self { _ignore: Default::default() }
    }
}

#[derive(Component)]
pub(crate) struct ServiceHook {
    pub(crate) trigger: fn(ServiceRequest),
    pub(crate) lifecycle: Option<ServiceLifecycle>,
}

impl ServiceHook {
    pub(crate) fn new(callback: fn(ServiceRequest)) -> Self {
        Self { trigger: callback, lifecycle: None }
    }
}

/// Keeps track of when a service entity gets despawned so we know to cancel
/// any pending requests
pub(crate) struct ServiceLifecycle {
    /// The entity that this is attached to
    entity: Entity,
    /// Used to send the signal that the service has despawned
    sender: TokioSender<Entity>,
}

impl ServiceLifecycle {
    pub(crate) fn new(entity: Entity, sender: TokioSender<Entity>) -> Self {
        Self { entity, sender }
    }
}

impl Drop for ServiceLifecycle {
    fn drop(&mut self) {
        self.sender.send(self.entity).ok();
    }
}

#[derive(Resource)]
pub(crate) struct ServiceLifecycleChannel {
    pub(crate) sender: TokioSender<Entity>,
    pub(crate) receiver: TokioReceiver<Entity>,
}

impl ServiceLifecycleChannel {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = unbounded_channel();
        Self { sender, receiver }
    }
}

impl Default for ServiceLifecycleChannel {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Bundle)]
pub(crate) struct ServiceBundle<Srv: ServiceTrait + 'static + Send + Sync> {
    hook: ServiceHook,
    marker: ServiceMarker<Srv::Request, Srv::Response>,
}

impl<Srv: ServiceTrait + 'static + Send + Sync> ServiceBundle<Srv> {
    pub(crate) fn new() -> Self {
        Self {
            hook: ServiceHook::new(service_hook::<Srv>),
            marker: Default::default(),
        }
    }
}

fn service_hook<Srv: ServiceTrait>(
    ServiceRequest {
            provider,
            target,
            instructions,
            operation: OperationRequest { source, world, roster }
        }: ServiceRequest,
) {
    match Srv::serve(ServiceRequest {
        provider,
        target,
        instructions,
        operation: OperationRequest { source, world, roster }
    }) {
        Ok(()) | Err(OperationError::NotReady) => {
            // Do nothing
        }
        Err(OperationError::Broken(backtrace)) => {
            world.get_resource_or_insert_with(|| UnhandledErrors::default())
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: Arc::new(anyhow!(
                        "Failed to serve: provider {provider:?}, source {source:?}, target {target:?}",
                    )),
                    backtrace,
                });
        }
    }
}

#[derive(Resource)]
struct ServiceQueue {
    is_delivering: bool,
    queue: VecDeque<PendingServiceRequest>,
}

impl ServiceQueue {
    fn new() -> Self {
        Self { is_delivering: false, queue: VecDeque::new() }
    }
}

pub(crate) fn dispatch_service(
    ServiceRequest {
        provider,
        target,
        instructions,
        operation: OperationRequest { source, world, roster }
    }: ServiceRequest,
) {
    let pending = PendingServiceRequest {
        provider, target, instructions, operation: PendingOperationRequest { source }
    };
    let mut service_queue = world.get_resource_or_insert_with(|| ServiceQueue::new());
    service_queue.queue.push_back(pending);
    if service_queue.is_delivering {
        // Services are already being delivered, so to keep things simple we
        // will add this dispatch command to the service queue and let the
        // services be processed one at a time. Otherwise service recursion gets
        // messy or even impossible.
        return;
    }

    service_queue.is_delivering = true;

    while let Some(pending) = world.resource_mut::<ServiceQueue>().queue.pop_back() {
        let Some(hook) = world.get::<ServiceHook>(pending.provider) else {
            // The service has become unavailable, so we should drain the source
            // node of all its inputs, emitting disposals for all of them.
            dispose_for_despawned_service(provider, world, roster);
            continue;
        };

        (hook.trigger)(pending.activate(world, roster));
    }
    world.resource_mut::<ServiceQueue>().is_delivering = false;
}
