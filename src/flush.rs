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

use bevy_derive::{Deref, DerefMut};
use bevy_ecs::{
    prelude::{Added, Entity, Query, QueryState, Resource, With, World},
    schedule::{IntoSystemConfigs, SystemConfigs},
    system::{Command, SystemState},
};
use bevy_hierarchy::{BuildWorldChildren, Children, DespawnRecursiveExt};

use smallvec::SmallVec;

use anyhow::anyhow;

use backtrace::Backtrace;

use std::sync::Arc;

use crate::{
    awaken_task, dispose_for_despawned_service, execute_operation, AddImpulse, ChannelQueue,
    Detached, DisposalNotice, Finished, ImpulseLifecycleChannel, MiscellaneousFailure,
    OperationError, OperationRequest, OperationRoster, ServiceHook, ServiceLifecycle,
    ServiceLifecycleChannel, UnhandledErrors, UnusedTarget, UnusedTargetDrop,
    ValidateScopeReachability, ValidationRequest, WakeQueue,
};

#[cfg(feature = "single_threaded_async")]
use crate::async_execution::SingleThreadedExecution;

#[derive(Resource, Default)]
pub struct FlushParameters {
    /// By default, a flush will loop until the whole [`OperationRoster`] is empty.
    /// If there are loops of blocking services then it is possible for the flush
    /// to loop indefinitely, creating the appearance of a blockup in the
    /// application, or delaying other systems from running for a prolonged amount
    /// of time.
    ///
    /// Use this limit to prevent the flush from blocking for too long in those
    /// scenarios. If the flush loops beyond this limit, anything remaining in
    /// the roster will be moved into a deferred roster which will be processed
    /// during the next flush.
    ///
    /// A value of `None` means the flush can loop indefinitely (this is the default).
    pub flush_loop_limit: Option<usize>,
    /// When using the single_threaded_async feature, async futures get polled
    /// during the impulse flush. If async futures repeatedly spawn more async
    /// tasks then the flush could get stuck in a loop. This parameter lets you
    /// put a limit on how many futures get polled so that the flush cannot get
    /// stuck.
    ///
    /// A value of `None` means the futures can be polled indefinitely (this is the default).
    pub single_threaded_poll_limit: Option<usize>,
}

pub fn flush_impulses() -> SystemConfigs {
    flush_impulses_impl.into_configs()
}

fn flush_impulses_impl(
    world: &mut World,
    new_service_query: &mut QueryState<(Entity, &mut ServiceHook), Added<ServiceHook>>,
) {
    let parameters = world.get_resource_or_insert_with(FlushParameters::default);
    let single_threaded_poll_limit = parameters.single_threaded_poll_limit;
    let mut roster = OperationRoster::new();
    collect_from_channels(
        single_threaded_poll_limit,
        new_service_query,
        world,
        &mut roster,
    );

    let mut loop_count = 0;
    while !roster.is_empty() {
        let parameters = world.get_resource_or_insert_with(FlushParameters::default);
        let flush_loop_limit = parameters.flush_loop_limit;
        let single_threaded_poll_limit = parameters.single_threaded_poll_limit;
        if flush_loop_limit.is_some_and(|limit| limit <= loop_count) {
            // We have looped beyoond the limit, so we will defer anything that
            // remains in the roster and stop looping from here.
            world
                .get_resource_or_insert_with(DeferredRoster::default)
                .append(&mut roster);
            break;
        }

        loop_count += 1;

        garbage_cleanup(world, &mut roster);

        while let Some(unblock) = roster.unblock.pop_front() {
            let serve_next = unblock.serve_next;
            serve_next(unblock, world, &mut roster);
            garbage_cleanup(world, &mut roster);
        }

        while let Some(source) = roster.queue.pop_front() {
            execute_operation(OperationRequest {
                source,
                world,
                roster: &mut roster,
            });
            garbage_cleanup(world, &mut roster);
        }

        while let Some(source) = roster.awake.pop_front() {
            awaken_task(OperationRequest {
                source,
                world,
                roster: &mut roster,
            });
            garbage_cleanup(world, &mut roster);
        }

        collect_from_channels(
            single_threaded_poll_limit,
            new_service_query,
            world,
            &mut roster,
        );
    }
}

fn garbage_cleanup(world: &mut World, roster: &mut OperationRoster) {
    while let Some(cleanup) = roster.cleanup_finished.pop() {
        cleanup.trigger(world, roster);
    }

    while let Some(cancel) = roster.cancel.pop_front() {
        cancel.trigger(world, roster);
    }
}

fn collect_from_channels(
    _single_threaded_poll_limit: Option<usize>,
    new_service_query: &mut QueryState<(Entity, &mut ServiceHook), Added<ServiceHook>>,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    // Get the receiver for async task commands
    while let Ok(item) = world
        .get_resource_or_insert_with(ChannelQueue::new)
        .receiver
        .try_recv()
    {
        (item)(world, roster);
    }

    roster.process_deferals();

    let mut removed_services: SmallVec<[Entity; 8]> = SmallVec::new();
    world.get_resource_or_insert_with(ServiceLifecycleChannel::new);
    world.resource_scope::<ServiceLifecycleChannel, ()>(|world, mut lifecycles| {
        // Clean up the dangling requests of any services that have been despawned.
        while let Ok(removed_service) = lifecycles.receiver.try_recv() {
            removed_services.push(removed_service);
        }

        // Add a lifecycle tracker to any new services that might have shown up
        // TODO(@mxgrey): Make sure this works for services which are spawned by
        // providers that are being flushed.
        for (e, mut hook) in new_service_query.iter_mut(world) {
            if hook.lifecycle.is_none() {
                // Check if the lifecycle is none, because collect_from_channels
                // can be run multiple times per flush, in which case we will
                // iterate over the query again, and end up dropping the lifecycle
                // managers that we just created. When that happens, the service
                // gets treated as despawned prematurely.
                hook.lifecycle = Some(ServiceLifecycle::new(e, lifecycles.sender.clone()));
            }
        }
    });

    for removed_service in removed_services {
        dispose_for_despawned_service(removed_service, world, roster);
    }

    // Queue any operations that needed to be deferred
    let mut deferred = world.get_resource_or_insert_with(DeferredRoster::default);
    roster.append(&mut deferred);

    // Collect any tasks that are ready to be woken
    let mut wake_queue = world.get_resource_or_insert_with(WakeQueue::new);
    while let Ok(wakeable) = wake_queue.receiver.try_recv() {
        roster.awake(wakeable);
    }

    let mut unused_targets_state: SystemState<Query<(Entity, &Detached), With<UnusedTarget>>> =
        SystemState::new(world);

    let mut add_finish: SmallVec<[_; 8]> = SmallVec::new();
    let mut drop_targets: SmallVec<[_; 8]> = SmallVec::new();
    for (e, detached) in unused_targets_state.get(world).iter() {
        if detached.is_detached() {
            add_finish.push(e);
        } else {
            drop_targets.push(e);
        }
    }

    for e in add_finish {
        // Add a Finished impulse to the unused target of a detached impulse
        // chain.
        AddImpulse::new(e, Finished).apply(world);
    }

    for target in drop_targets.drain(..) {
        drop_target(target, world, roster, true);
    }

    let mut lifecycles = world.get_resource_or_insert_with(ImpulseLifecycleChannel::default);
    let mut dropped_targets: SmallVec<[Entity; 8]> = SmallVec::new();
    while let Ok(dropped_target) = lifecycles.receiver.try_recv() {
        dropped_targets.push(dropped_target);
    }

    for target in drop_targets {
        drop_target(target, world, roster, false);
    }

    while let Some(DisposalNotice {
        source,
        origin,
        session,
    }) = roster.disposed.pop()
    {
        let Some(validate) = world.get::<ValidateScopeReachability>(source) else {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: Arc::new(anyhow!(
                        "Scope {source:?} for disposal notification does not \
                        have validation component",
                    )),
                    backtrace: Some(Backtrace::new()),
                });
            continue;
        };

        let validate = validate.0;
        let req = ValidationRequest {
            source,
            origin,
            session,
            world,
            roster,
        };
        if let Err(OperationError::Broken(backtrace)) = validate(req) {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: Arc::new(anyhow!(
                        "Scope {source:?} broken while validating a disposal"
                    )),
                    backtrace,
                });
        }
    }

    #[cfg(feature = "single_threaded_async")]
    SingleThreadedExecution::world_poll(world, _single_threaded_poll_limit);
}

fn drop_target(target: Entity, world: &mut World, roster: &mut OperationRoster, unused: bool) {
    roster.purge(target);
    let mut dropped_impulses = Vec::new();
    let mut detached_impulse = None;

    let mut impulse = target;
    let mut search_state: SystemState<(Query<&Children>, Query<&Detached>)> =
        SystemState::new(world);

    let (q_children, q_detached) = search_state.get(world);
    loop {
        let mut move_up_chain = false;
        if let Ok(children) = q_children.get(impulse) {
            for child in children {
                let Ok(detached) = q_detached.get(*child) else {
                    continue;
                };
                if detached.is_detached() {
                    // This child is detached so we will not include it in the
                    // dropped impulses. We need to de-parent it so that it does
                    // not get despawned with the rest of the impulses that we
                    // are dropping.
                    detached_impulse = Some(*child);
                    break;
                } else {
                    // This child is not detached, so we will include it in our
                    // dropped impulses, and crawl towards one of it children.
                    if unused {
                        dropped_impulses.push(impulse);
                    }
                    roster.purge(impulse);
                    move_up_chain = true;
                    impulse = *child;
                    continue;
                }
            }
        }

        if !move_up_chain {
            // There is nothing further to include in the drop
            break;
        }
    }

    if let Some(detached_impulse) = detached_impulse {
        if let Some(mut detached_impulse_mut) = world.get_entity_mut(detached_impulse) {
            detached_impulse_mut.remove_parent();
        }
    }

    if let Some(unused_target_mut) = world.get_entity_mut(target) {
        unused_target_mut.despawn_recursive();
    }

    if unused {
        world
            .get_resource_or_insert_with(UnhandledErrors::default)
            .unused_targets
            .push(UnusedTargetDrop {
                unused_target: target,
                dropped_impulses,
            });
    }
}

/// This resource is used to queue up operations in the roster in situations
/// where the regular roster is not available.
#[derive(Resource, Default, Deref, DerefMut)]
pub(crate) struct DeferredRoster(pub OperationRoster);
