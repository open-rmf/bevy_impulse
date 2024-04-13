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

use bevy::{
    prelude::{Entity, World, Component, Query},
    ecs::system::SystemState,
};

use smallvec::SmallVec;

use crate::{
    TargetStorage, ForkStorage, Operation, InputBundle, OperationStatus,
    OperationRoster, NextServiceLink,
};

/// This component is held by request target entities which have an associated
/// cancel target (on_cancel was applied to its position in the service chain).
///
/// This means that if the entity with this component experiences a cancel event,
/// we should insert an InputStorage(()) component into its target.
#[derive(Component)]
struct CancelSource {
    /// The target that should be triggered with an InputStorage(()) when a
    /// cancellation event happens.
    target: Entity
}

/// This component is applied to
#[derive(Component)]
struct CancelTarget {
    source: Entity,
}

#[derive(Component)]
struct CancelSignalStorage<T>(T);

pub(crate) struct Cancel<Signal: 'static + Send + Sync> {
    cancel_source: Entity,
    signal_target: Entity,
    signal: Signal,
}

impl<Signal: 'static + Send + Sync> Cancel<Signal> {
    pub(crate) fn new(cancel_source: Entity, signal_target: Entity, signal: Signal) -> Self {
        Self { cancel_source, signal_target, signal }
    }
}

impl<Signal: 'static + Send + Sync> Operation for Cancel<Signal> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        world.entity_mut(entity).insert((
            CancelTarget{ source: self.cancel_source },
            CancelSignalStorage(self.signal),
            TargetStorage(self.signal_target),
        ));
        world.entity_mut(self.cancel_source)
            .insert(CancelSource{ target: entity });
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let CancelSignalStorage::<Signal>(signal) = source_mut.take().ok_or(())?;
        let TargetStorage(target) = source_mut.take().ok_or(())?;
        if let Some(mut target_mut) = world.get_entity_mut(target) {
            target_mut.insert(InputBundle::new(signal));
            roster.queue(target);
        } else {
            roster.cancel(target);
        }

        Ok(OperationStatus::Finished)
    }
}

/// Cancel a request from this link in a service chain downwards. This will
/// trigger any on_cancel reactions that are associated with the canceled link
/// in the chain and all other links in the chain that come after it.
pub(crate) fn cancel_link(source: Entity, world: &mut World, roster: &mut OperationRoster) {
    let mut source_queue: SmallVec<[Entity; 16]> = SmallVec::new();
    source_queue.push(source);

    while let Some(source) = source_queue.pop() {
        if let Some(cancel_target) = get_cancel_target(source, world) {
            roster.queue(cancel_target);
        }

        let mut next_link_state: SystemState<NextServiceLink> = SystemState::new(world);
        let next_link = next_link_state.get(world);
        for next in next_link.iter(source) {
            source_queue.push(next);
        }
        world.despawn(source);
    }
}

pub(crate) fn dispose_cancellation_chain(source: Entity, world: &mut World) {
    let mut source_queue: SmallVec<[Entity; 16]> = SmallVec::new();
    source_queue.push(source);

    while let Some(source) = source_queue.pop() {
        // We should find whether this entity has a cancel chain, and despawn that
        // whole chain if it exists. We do not want to trigger any cancellation
        // behavior, merely despawn so that we aren't leaking entities that will
        // never get used.
        let Some(cancel_target) = get_cancel_target(source, world) else {
            continue;
        };

        // Go down the cancel chain, adding its descendants to the queue of sources
        // to dispose of, in case any descendants along the chain may also have
        // cancellation branches. We will also dispose of the descendants while
        // we do this.
        let mut next_link_state: SystemState<NextServiceLink> = SystemState::new(world);
        let next_link = next_link_state.get(world);

        let mut cancellation_tree: SmallVec<[Entity; 16]> = SmallVec::new();
        let mut despawn_queue: SmallVec<[Entity; 16]> = SmallVec::new();
        cancellation_tree.push(cancel_target);
        while let Some(e) = cancellation_tree.pop() {
            for next in next_link.iter(e) {
                cancellation_tree.push(next);
                source_queue.push(next);
            }

            despawn_queue.push(e);
        }

        for e in despawn_queue {
            world.despawn(e);
        }
    }
}

fn get_cancel_target(source: Entity, world: &mut World) -> Option<Entity> {
    if let Some(source_ref) = world.get_entity(source) {
        let Some(cancel_source) = source_ref.get::<CancelSource>() else {
            // This source does not have a cancel component, so we do
            // not need to dispose of any cancellation chain for it.
            return None;
        };

        return Some(cancel_source.target);
    } else {
        // The entity has been despawned prematurely, so we should look
        // through all existing CancelTarget components and find any whose
        // source matches this one, and then dispose of them.
        let mut all_cancel_targets_state: SystemState<Query<(Entity, &CancelTarget)>> =
            SystemState::new(world);
        let all_cancel_targets = all_cancel_targets_state.get(world);
        for (e, cancel_target) in &all_cancel_targets {
            if cancel_target.source == source {
                return Some(e);
            }
        }

        // There is no cancel target associated with this source, so we
        // do not need to dispose of any cancellation chain for it.
        return None;
    }
}
