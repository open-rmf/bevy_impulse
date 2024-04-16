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
    prelude::{Entity, World, Query, QueryState, Added, RemovedComponents, With},
    ecs::system::SystemState,
};

use smallvec::SmallVec;

use crate::{
    ChannelQueue, PollTask, WakeQueue, OperationRoster, ServiceHook, InputReady,
    Cancel, DroppedPromiseQueue, UnusedTarget,
    operate, dispose_cancellation_chain, cancel_service, cancel_from_link,
    cancel_chain_upwards,
};

#[allow(private_interfaces)]
pub fn flush_services(
    world: &mut World,
    mut poll_task_query: QueryState<(Entity, &PollTask), Added<PollTask>>,
    mut input_ready_query: QueryState<Entity, Added<InputReady>>,
    mut removed_services: RemovedComponents<ServiceHook>,
) {
    let mut roster = OperationRoster::new();

    // Get the receiver for async task commands
    let async_receiver = world.get_resource_or_insert_with(|| ChannelQueue::new()).receiver.clone();

    // Apply all the commands that have been received
    while let Ok(mut command_queue) = async_receiver.try_recv() {
        command_queue.apply(world);
    }

    // Clean up the dangling requests of any services that have been despawned.
    for removed_service in removed_services.iter() {
        cancel_service(removed_service, world, &mut roster);
    }

    // Collect pollable tasks. These need to be collected before we can use them
    // because we need to borrow the world while iterating but also need the world
    // to be mutable while polling.
    let pollables: SmallVec<[_; 8]> = poll_task_query
        .iter(world)
        .map(|(e, f)| (e, f.0))
        .collect();

    // Poll any new tasks to get them hooked into the async task channel
    for (e, f) in pollables {
        f(e, world, &mut roster);
    }

    // Queue any operations whose inputs are ready
    for e in input_ready_query.iter(world) {
        roster.queue(e);
    }

    // Collect any tasks that are ready to be woken
    let wakeables: SmallVec<[_; 8]> = world
        .get_resource_or_insert_with(|| WakeQueue::new())
        .receiver
        .try_iter()
        .collect();

    // Poll any tasks that have asked to be woken
    for e in wakeables {
        let Some(f) = world.get::<PollTask>(e).map(|x| x.0) else {
            roster.cancel(Cancel::broken(e));
            continue;
        };

        f(e, world, &mut roster);
    }

    let mut unused_targets_state: SystemState<Query<Entity, With<UnusedTarget>>> =
        SystemState::new(world);
    let mut unused_targets: SmallVec<[_; 8]> = unused_targets_state.get(world).iter().collect();
    for target in unused_targets.drain(..) {
        cancel_chain_upwards(Cancel::unused_target(target), world, &mut roster)
    }

    unused_targets.extend(
        world
            .get_resource_or_insert_with(|| DroppedPromiseQueue::new())
            .receiver
            .try_iter()
    );
    for target in unused_targets.drain(..) {
        cancel_chain_upwards(Cancel::dropped(target), world, &mut roster)
    }

    while !roster.is_empty() {
        while let Some(e) = roster.cancel.pop_front() {
            cancel_from_link(e, world, &mut roster);
        }

        for e in roster.dispose.drain(..) {
            dispose_link(e, world);
        }

        while let Some(unblock) = roster.unblock.pop_front() {
            let serve_next = unblock.serve_next;
            serve_next(unblock, world, &mut roster);
        }

        while let Some(e) = roster.operate.pop_front() {
            operate(e, world, &mut roster);
        }
    }
}

/// Dispose a link in the chain that is no longer needed.
fn dispose_link(source: Entity, world: &mut World) {
    dispose_cancellation_chain(source, world);
    world.despawn(source);
}
