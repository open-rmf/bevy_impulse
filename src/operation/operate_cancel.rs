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
    SingleTargetStorage, Cancelled, Operation, InputBundle,
    OperationStatus, OperationRoster, NextOperationLink, Cancel,
    Cancellation, FunnelInputStatus, ForkTargetStatus, SingleSourceStorage,
    CancellationBehavior, OperationResult, OrBroken, OperationSetup, OperationRequest,
};

/// This component is held by request target entities which have an associated
/// cancel target (on_cancel was applied to its position in the impulse chain).
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

/// Apply this to a source entity to indicate that it should keep operating (not
/// be cancelled) even if its target(s) drop.
#[derive(Component)]
pub(crate) struct DetachDependency;

/// Apply this to a source entity to indicate that cancellation cascades should
/// not propagate past it.
#[derive(Component)]
pub(crate) struct DisposeOnCancel;

#[derive(Component)]
struct CancelSignalStorage<T>(T);

pub(crate) struct OperateCancel<Signal: 'static + Send + Sync> {
    cancel_source: Entity,
    signal_target: Entity,
    signal: Signal,
}

impl<Signal: 'static + Send + Sync> OperateCancel<Signal> {
    pub(crate) fn new(cancel_source: Entity, signal_target: Entity, signal: Signal) -> Self {
        Self { cancel_source, signal_target, signal }
    }
}

impl<Signal: 'static + Send + Sync> Operation for OperateCancel<Signal> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        world.entity_mut(source).insert((
            CancelTarget{ source: self.cancel_source },
            CancelSignalStorage(self.signal),
            SingleTargetStorage(self.signal_target),
        ));
        world.entity_mut(self.cancel_source)
            .insert(CancelSource{ target: source });
    }

    fn execute(
        OperationRequest { source, requester, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let CancelSignalStorage::<Signal>(signal) = source_mut.take().or_broken()?;
        let CancellationStorage(cause) = source_mut.take().or_broken()?;
        let SingleTargetStorage(target) = source_mut.take().or_broken()?;
        if let Some(mut target_mut) = world.get_entity_mut(target) {
            target_mut.insert(InputBundle::new(
                Cancelled{ signal, cancellation: cause }
            ));
            roster.queue(target);
        } else {
            roster.cancel(Cancel::broken_here(target));
        }

        Ok(OperationStatus::Finished)
    }
}

/// Find the highest link on the chain that is not detached and trigger a cancel
/// starting from there.
///
/// Do not trigger any cancellation if the target belongs to an inert chain, i.e.
/// part of a cancellation branch or an unused branch. In that case, just dispose
/// of the branch.
pub(crate) fn propagate_dependency_loss_upwards(
    target: Cancel,
    world: &mut World,
    roster: &mut OperationRoster
) {
    let mut target_queue: SmallVec<[Cancel; 16]> = SmallVec::new();
    target_queue.push(target);

    let mut state: SystemState<(
        Query<(&SingleSourceStorage, Option<&mut ForkTargetStatus>)>,
        Query<Entity>,
    )> = SystemState::new(world);
    let (mut source_query, existence_query) = state.get_mut(world);

    while let Some(Cancel { apply_to, cause }) = target_queue.pop() {
        if let Ok((source, target_status)) = source_query.get_mut(apply_to) {
            // Check that the source exists. If the source has already been
            // disposed of, then we should begin the cancellation here instead
            // of crawling up any further.
            if existence_query.contains(source.0) {
                if let Some(mut target_status) = target_status {
                    target_status.drop_dependency(cause);
                    roster.queue(source.0);
                } else {
                    target_queue.push(Cancel { apply_to: source.0, cause });
                }
                continue;
            }
        }

        // There is nothing further to crawl up, so we begin triggering the
        // cancellation from here.
        roster.cancel(Cancel { apply_to, cause });
    }
}

/// Cancel a request from this link in a impulse chain downwards. This will
/// trigger any on_cancel reactions that are associated with the cancelled link
/// in the chain and all other links in the chain that come after it.
///
/// If the cascade reaches a funnel input source then the cascade will change
/// the input's status to Cancelled and trigger the funnel to process it instead
/// of continuing like normal.
pub(crate) fn cancel_from_link(
    initial_source: Cancel,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let mut downstream_queue: SmallVec<[Cancel; 16]> = SmallVec::new();
    downstream_queue.push(initial_source);

    while let Some(Cancel { apply_to: source, cause }) = downstream_queue.pop() {
        if let Some(cancel_target) = get_cancel_target(source, world) {
            if let Some(mut cancel_target_mut) = world.get_entity_mut(cancel_target) {
                cancel_target_mut.insert(CancellationStorage(cause.clone()));
                roster.queue(cancel_target);
            } else {
                roster.cancel(Cancel::broken_here(cancel_target));
            }
        }

        if let Some(behavior) = world.get::<CancellationBehavior>(source).copied() {
            (behavior.hook)(&cause, source, world, roster);
        }

        if world.get::<DisposeOnCancel>(source).is_some() {
            // Do not cascade the cancellation down from here. The user has
            // asked for this to be disposed instead of cancelled.
            roster.dispose_chain_from(source);
            continue;
        }

        let mut state: SystemState<(
            NextOperationLink,
            Query<(&mut FunnelInputStatus, &SingleTargetStorage)>,
        )> = SystemState::new(world);
        let (next_link, mut funnel_input) = state.get_mut(world);

        if let Ok((mut input_status, target)) = funnel_input.get_mut(source) {
            // This link is a funnel input source. We should stop propagating here
            // and instead mark its status as cancelled, and trigger its funnel
            // to respond to it.
            input_status.cancel(cause.clone());
            roster.queue(target.0);
        } else {
            for next in next_link.iter(source) {
                downstream_queue.push(Cancel { apply_to: next, cause: cause.clone() });
            }
            world.despawn(source);
        }
    }
}

pub(crate) fn dispose_cancellation_chain(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    if let Some(cancel_target) = get_cancel_target(source, world) {
        roster.dispose_chain_from(cancel_target);
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

#[derive(Component)]
struct CancellationStorage(Cancellation);
