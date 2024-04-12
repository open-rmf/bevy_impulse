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

use crate::{TargetStorage, ForkStorage};

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

pub(crate) fn dispose_cancellation_chain(source: Entity, world: &mut World) {
    let mut source_queue: SmallVec<[Entity; 16]> = SmallVec::new();
    source_queue.push(source);

    'source_queue_loop: while let Some(source) = source_queue.pop() {
        // We should find whether this entity has a cancel chain, and despawn that
        // whole chain if it exists. We do not want to trigger any cancellation
        // behavior, merely despawn so that we aren't leaking entities that will
        // never get used.
        let cancel_target = 'cancel_target: {
            if let Some(source_ref) = world.get_entity(source) {
                let Some(cancel_source) = source_ref.get::<CancelSource>() else {
                    continue 'source_queue_loop;
                };

                // This source does not have a cancel target component, so we do
                // not need to dispose of any cancellation chain for it.
                break 'cancel_target cancel_source.target;
            } else {
                // The entity has been despawned prematurely, so we should look
                // through all existing CancelTarget components and find any whose
                // source matches this one, and then dispose of them.
                let mut all_cancel_targets_state: SystemState<Query<(Entity, &CancelTarget)>> =
                    SystemState::new(world);
                let all_cancel_targets = all_cancel_targets_state.get(world);
                for (e, cancel_target) in &all_cancel_targets {
                    if cancel_target.source == source {
                        break 'cancel_target e;
                    }
                }

                // There is no cancel target associated with this source, so we
                // do not need to dispose of any cancellation chain for it.
                continue 'source_queue_loop;
            }
        };

        // Go down the cancel chain, adding its children to the queue of sources
        // to dispose of, in case any descendants along the chain may also have
        // cancellation branches.
        let mut children_state: SystemState<(Query<&TargetStorage>, Query<&ForkStorage>)> =
            SystemState::new(world);
        let (single_target, fork_targets) = children_state.get(world);
        if let Ok(child) = single_target.get(cancel_target) {
            source_queue.push(child.0);
        } else if let Ok(fork) = fork_targets.get(cancel_target) {
            for c in &fork.0 {
                source_queue.push(*c);
            }
        }

        world.despawn(cancel_target);
    }
}
