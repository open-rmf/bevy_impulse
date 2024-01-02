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

use crate::{UnusedTarget, Queued, cancel};

use std::collections::VecDeque;

use bevy::{
    prelude::{Entity, World, Component},
    ecs::system::Command,
};

pub(crate) enum OperationStatus {
    /// The source entity is no longer needed so it should be despawned.
    Finished,
    /// Do not despawn the source entity of this operation yet because it will
    /// be needed for a service that has been queued. The service will be
    /// responsible for despawning the entity when it is no longer needed.
    Queued(Entity),
}

pub(crate) trait Operation {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    );

    fn execute(
        source: Entity,
        world: &mut World,
        queue: &mut VecDeque<Entity>,
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
struct Operate(fn(Entity, &mut World, &mut VecDeque<Entity>));

fn perform_operation<Op: Operation>(
    source: Entity,
    world: &mut World,
    queue: &mut VecDeque<Entity>,
) {
    match Op::execute(source, world, queue) {
        Ok(OperationStatus::Finished) => {
            world.despawn(source);
        }
        Ok(OperationStatus::Queued(provider)) => {
            if let Some(mut source_mut) = world.get_entity_mut(source) {
                source_mut.insert(Queued(provider));
            } else {
                // The source is no longer available even though it was queued.
                // We should cancel the job right away.
                cancel(world, source);
            }
        }
        Err(()) => {
            cancel(world, source);
        }
    }
}
