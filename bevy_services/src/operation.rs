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

use crate::RequestLabelId;

use bevy::{
    prelude::{Entity, World, Component},
    ecs::system::Command,
};

use std::collections::VecDeque;

mod fork;
pub(crate) use fork::*;

mod operate_handler;
pub(crate) use operate_handler::*;

mod operate_map;
pub(crate) use operate_map::*;

mod operate_service;
pub(crate) use operate_service::*;

mod terminate;
pub(crate) use terminate::*;

mod cancel;
pub(crate) use cancel::*;

#[derive(Component)]
pub(crate) struct TargetStorage(pub(crate) Entity);

#[derive(Component)]
pub(crate) struct UnusedTarget;

#[derive(Default)]
pub struct OperationRoster {
    /// Operation sources that should be triggered
    pub(crate) operate: VecDeque<Entity>,
    /// Operation sources that should be canceled
    pub(crate) cancel: VecDeque<Entity>,
    /// Async services that should pull their next item
    pub(crate) unblock: VecDeque<BlockingQueue>,
    /// Remove these entities as they are no longer needed
    pub(crate) dispose: Vec<Entity>,
}

impl OperationRoster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn queue(&mut self, source: Entity) {
        self.operate.push_back(source);
    }

    pub fn cancel(&mut self, source: Entity) {
        self.cancel.push_back(source);
    }

    pub(crate) fn unblock(&mut self, provider: BlockingQueue) {
        self.unblock.push_back(provider);
    }

    pub fn dispose(&mut self, entity: Entity) {
        self.dispose.push(entity);
    }

    pub fn is_empty(&self) -> bool {
        self.operate.is_empty() && self.cancel.is_empty()
        && self.unblock.is_empty() && self.dispose.is_empty()
    }
}

#[derive(Component)]
pub(crate) struct BlockingQueue {
    /// The provider that is being blocked
    pub(crate) provider: Entity,
    /// The source that is doing the blocking
    pub(crate) source: Entity,
    /// The label of the queue that is being blocked
    pub(crate) label: Option<RequestLabelId>,
    /// Function pointer to call when this is no longer blocking
    pub(crate) serve_next: fn(BlockingQueue, &mut World, &mut OperationRoster),
}


pub(crate) enum OperationStatus {
    /// The source entity is no longer needed so it should be despawned.
    Finished,
    /// Do not despawn the source entity of this operation yet because it will
    /// be needed for a service that has been queued. The service will be
    /// responsible for despawning the entity when it is no longer needed.
    Queued{ provider: Entity },
}

/// This component indicates that a source entity has been queued for a service
/// so it should not be despawned yet.
#[derive(Component)]
pub(crate) struct Queued(pub(crate) Entity);

pub(crate) trait Operation {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    );

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()>;
}

pub(crate) struct PerformOperation<Op: Operation> {
    source: Entity,
    operation: Op,
}

impl<Op: Operation> PerformOperation<Op> {
    pub(crate) fn new(source: Entity, operation: Op) -> Self {
        Self { source, operation }
    }
}

impl<Op: Operation + 'static + Sync + Send> Command for PerformOperation<Op> {
    fn apply(self, world: &mut World) {
        self.operation.set_parameters(self.source, world);
        let mut provider_mut = world.entity_mut(self.source);
        provider_mut
            .insert(Operate(perform_operation::<Op>))
            .remove::<UnusedTarget>();
    }
}

#[derive(Component)]
struct Operate(fn(Entity, &mut World, &mut OperationRoster));

pub(crate) fn operate(entity: Entity, world: &mut World, roster: &mut OperationRoster) {
    let Some(operator) = world.get::<Operate>(entity) else {
        roster.cancel(entity);
        return;
    };
    let operator = operator.0;
    operator(entity, world, roster);
}

fn perform_operation<Op: Operation>(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    match Op::execute(source, world, roster) {
        Ok(OperationStatus::Finished) => {
            world.despawn(source);
        }
        Ok(OperationStatus::Queued{ provider }) => {
            if let Some(mut source_mut) = world.get_entity_mut(source) {
                source_mut.insert(Queued(provider));
            } else {
                // The source is no longer available even though it was queued.
                // We should cancel the job right away.
                roster.cancel(source);
            }
        }
        Err(()) => {
            roster.cancel(source);
        }
    }
}
