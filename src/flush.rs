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
    Cancel, DroppedPromiseQueue, UnusedTarget, FunnelSourceStorage, FunnelInputStatus,
    SingleTargetStorage, NextOperationLink, ServiceLifecycle, ServiceLifecycleQueue,
    OperationRequest,
    execute_operation, dispose_cancellation_chain, cancel_service, cancel_from_link,
    propagate_dependency_loss_upwards,
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
        (item)(world);
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
        // The dependency and cancellation processing must happen together,
        // without any disposal happening in between, or else cancellation
        // cascades could get lost prematurely.
        {
            while let Some(source) = roster.drop_dependency.pop() {
                propagate_dependency_loss_upwards(source, world, &mut roster)
            }

            while let Some(e) = roster.cancel.pop_front() {
                cancel_from_link(e, world, &mut roster);
            }
        }

        // Dispose of anything that is no longer needed
        while let Some(e) = roster.dispose.pop() {
            dispose_link(e, world, &mut roster);
        }

        while let Some(e) = roster.dispose_chain_from.pop() {
            dispose_chain_from(e, world, &mut roster);
        }

        while let Some(unblock) = roster.unblock.pop_front() {
            let serve_next = unblock.serve_next;
            serve_next(unblock, world, &mut roster);
        }

        while let Some(source) = roster.queue.pop_front() {
            execute_operation(OperationRequest { source, world, roster: &mut roster });
        }
    }
}

/// Dispose a chain from the given link downwards
fn dispose_chain_from(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let mut source_queue: SmallVec<[Entity; 16]> = SmallVec::new();
    source_queue.push(source);

    while let Some(source) = source_queue.pop() {
        let Some(mut source_mut) = world.get_entity_mut(source) else {
            continue;
        };

        if let Some(mut input_status) = source_mut.get_mut::<FunnelInputStatus>() {
            // If this is a funnel input source then we should not keep propagating
            // the disposal directly. Instead we mark the input status as disposed
            // and trigger the funnel to wake up.
            input_status.dispose();
            if let Some(funnel) = world.get::<SingleTargetStorage>(source) {
                roster.queue(funnel.0);
            }
            continue;
        }

        roster.dispose(source);

        // Iterate through the targets of the link and add them to the queue
        let mut next_link_state: SystemState<NextOperationLink> = SystemState::new(world);
        let next_link = next_link_state.get(world);
        for next in next_link.iter(source) {
            source_queue.push(next);
        }
    }
}

/// Dispose a link in the chain that is no longer needed.
fn dispose_link(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    dispose_cancellation_chain(source, world, roster);

    if let Some(funnel_sources) = world.get::<FunnelSourceStorage>(source) {
        // If the link is a funnel (join or race) then we should also dispose of
        // its input entities when we dispose of it.
        for e in funnel_sources.0.clone() {
            world.despawn(e);
        }
    }
    world.despawn(source);
}
