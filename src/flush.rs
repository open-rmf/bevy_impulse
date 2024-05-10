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
    prelude::{Entity, World, Query, QueryState, Added, With},
    ecs::system::SystemState,
};

use smallvec::SmallVec;

use crate::{
    ChannelQueue, WakeQueue, OperationRoster, ServiceHook, InputReady,
    Cancel, DroppedPromiseQueue, UnusedTarget, ServiceLifecycle, ServiceLifecycleQueue,
    OperationRequest,
    execute_operation, cancel_service,
};

#[allow(private_interfaces)]
pub fn flush_impulses(
    world: &mut World,
    input_ready_query: &mut QueryState<Entity, Added<InputReady>>,
    new_service_query: &mut QueryState<(Entity, &mut ServiceHook), Added<ServiceHook>>,
) {
    let mut roster = OperationRoster::new();

    world.get_resource_or_insert_with(|| ServiceLifecycleQueue::new());
    world.resource_scope::<ServiceLifecycleQueue, ()>(|world, lifecycles| {
        // Clean up the dangling requests of any services that have been despawned.
        for removed_service in lifecycles.receiver.try_iter() {
            cancel_service(removed_service, world, &mut roster)
        }

        // Add a lifecycle tracker to any new services that might have shown up
        for (e, mut hook) in new_service_query.iter_mut(world) {
            hook.lifecycle = Some(ServiceLifecycle::new(e, lifecycles.sender.clone()));
        }
    });

    // Get the receiver for async task commands
    let async_receiver = world.get_resource_or_insert_with(|| ChannelQueue::new()).receiver.clone();

    // Apply all the commands that have been received
    while let Ok(mut item) = async_receiver.try_recv() {
        (item)(world, &mut roster);
    }

    // Queue any operations whose inputs are ready
    for e in input_ready_query.iter(world) {
        roster.queue(e);
    }

    // Collect any tasks that are ready to be woken
    for wakeable in world
        .get_resource_or_insert_with(|| WakeQueue::new())
        .receiver
        .try_iter()
    {
        roster.queue(wakeable);
    }

    let mut unused_targets_state: SystemState<Query<Entity, With<UnusedTarget>>> =
        SystemState::new(world);
    let mut unused_targets: SmallVec<[_; 8]> = unused_targets_state.get(world).iter().collect();
    for target in unused_targets.drain(..) {
        roster.drop_dependency(Cancel::unused_target(target));
    }

    unused_targets.extend(
        world
            .get_resource_or_insert_with(|| DroppedPromiseQueue::new())
            .receiver
            .try_iter()
    );
    for target in unused_targets.drain(..) {
        roster.drop_dependency(Cancel::dropped(target))
    }

    while !roster.is_empty() {
        while let Some(unblock) = roster.unblock.pop_front() {
            let serve_next = unblock.serve_next;
            serve_next(unblock, world, &mut roster);

            while let Some(cleanup) = roster.cleanup_finished.pop() {
                cleanup.trigger(world, &mut roster);
            }

            while let Some(cancel) = roster.cancel.pop_front() {
                cancel.trigger(world, &mut roster);
            }
        }

        while let Some(source) = roster.queue.pop_front() {
            execute_operation(OperationRequest { source, world, roster: &mut roster });

            while let Some(cleanup) = roster.cleanup_finished.pop() {
                cleanup.trigger(world, &mut roster);
            }

            while let Some(cancel) = roster.cancel.pop_front() {
                cancel.trigger(world, &mut roster);
            }
        }
    }
}
