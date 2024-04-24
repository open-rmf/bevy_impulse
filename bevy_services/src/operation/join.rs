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

use crate::{
    InputStorage, InputBundle, FunnelInputStatus, FunnelSourceStorage,
    SingleTargetStorage, Operation, OperationStatus, OperationRoster, Unzippable,
    SingleSourceStorage, Cancel, CancellationCause, JoinCancelled, JoinedBundle,
};

use std::sync::Arc;

pub(crate) struct JoinInput<T> {
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> JoinInput<T> {
    pub(crate) fn new(target: Entity) -> Self {
        Self { target, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Operation for JoinInput<T> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        world.entity_mut(entity).insert((
            SingleTargetStorage(self.target),
            FunnelInputStatus::Pending,
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        source_mut.get_mut::<FunnelInputStatus>().ok_or(())?.ready();

        let target = source_mut.get::<SingleTargetStorage>().ok_or(())?.0;
        roster.queue(target);

        // We can't let this link be cleaned up automatically. Its cleanup needs
        // to be handled by the join that it belongs to.
        Ok(OperationStatus::Disregard)
    }
}

pub(crate) struct ZipJoin<Values> {
    sources: FunnelSourceStorage,
    target: Entity,
    _ignore: std::marker::PhantomData<Values>,
}

impl<Values> ZipJoin<Values> {
    pub(crate) fn new(
        sources: FunnelSourceStorage,
        target: Entity,
    ) -> Self {
        Self { sources, target, _ignore: Default::default() }
    }
}

impl<Values: Unzippable> Operation for ZipJoin<Values> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleSourceStorage(entity));
        }

        world.entity_mut(entity).insert((
            self.sources,
            SingleTargetStorage(self.target),
            JoinStatus::Pending,
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        manage_join_delivery(source, world, roster, Values::join_values)
    }
}

pub(crate) struct BundleJoin<T> {
    sources: FunnelSourceStorage,
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> BundleJoin<T> {
    pub(crate) fn new(sources: FunnelSourceStorage, target: Entity) -> Self {
        Self { sources, target, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Operation for BundleJoin<T> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleSourceStorage(entity));
        }

        world.entity_mut(entity).insert((
            self.sources,
            SingleTargetStorage(self.target),
            JoinStatus::Pending,
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        manage_join_delivery(source, world, roster, deliver_bundle_join::<T>)
    }
}

fn manage_join_delivery(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
    deliver: fn(Entity, &mut World, &mut OperationRoster) -> Result<OperationStatus, ()>,
) -> Result<OperationStatus, ()> {
    match world.get::<JoinStatus>(source).ok_or(())? {
        JoinStatus::Pending => {
            manage_pending_join(source, world, roster, deliver)
        }
        JoinStatus::Cancelled(cause) => {
            manage_cancelled_join(source, world, roster, Arc::clone(cause))
        }
        JoinStatus::Closed => {
            // No action is needed if the join has already reached closed status.
            // This means it has already been asked to get cleaned up.
            Ok(OperationStatus::Disregard)
        }
    }
}

fn manage_pending_join(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
    deliver: fn(Entity, &mut World, &mut OperationRoster) -> Result<OperationStatus, ()>,
) -> Result<OperationStatus, ()> {
    let inputs = world.get::<FunnelSourceStorage>(source).ok_or(())?;

    let mut cancel_join = false;
    let mut deliver_join = true;
    for input in &inputs.0 {
        let input_status = world.get::<FunnelInputStatus>(*input).ok_or(())?;
        if !input_status.is_ready() {
            deliver_join = false;
        }
        if input_status.undeliverable() {
            cancel_join = true;
            break;
        }
    }

    if cancel_join {
        let mut input_statuses = Vec::new();
        let mut any_pending = false;
        for input in &inputs.0 {
            let input_status = world.get::<FunnelInputStatus>(*input).ok_or(())?;
            if input_status.is_pending() {
                any_pending = true;
            }

            input_statuses.push((*input, input_status.clone()));
        }

        let cause = Arc::new(CancellationCause::JoinCancelled(
            JoinCancelled { join: source, input_statuses }
        ));

        if any_pending {
            // Some of the input sources are still pending, so we should wait
            // until those have caught up before propagating the cancel to the
            // target. Cancellation behaviors are meant to always be triggered
            // in order of their location along the chain.
            let inputs = world.get::<FunnelSourceStorage>(source).ok_or(())?;
            for input in &inputs.0 {
                let input_status = world.get::<FunnelInputStatus>(*input).ok_or(())?;
                if input_status.is_pending() {
                    roster.drop_dependency(
                        Cancel { apply_to: *input, cause: cause.clone() }
                    );
                }
            }

            world.get_mut::<JoinStatus>(source).ok_or(())?.cancel(cause);
        } else {
            // None of the input sources are pending, so we are ready to cancel
            // the target and dispose of the join.
            let apply_to = world.get::<SingleTargetStorage>(source).ok_or(())?.0;
            roster.cancel(Cancel { apply_to, cause });

            world.get_mut::<JoinStatus>(source).ok_or(())?.close();
            return Ok(OperationStatus::Finished);
        }
    } else if deliver_join {
        deliver(source, world, roster)?;
        world.get_mut::<JoinStatus>(source).ok_or(())?.close();
        return Ok(OperationStatus::Finished);
    }

    Ok(OperationStatus::Disregard)
}

fn manage_cancelled_join(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
    cause: Arc<CancellationCause>,
) -> Result<OperationStatus, ()> {
    let inputs = world.get::<FunnelSourceStorage>(source).ok_or(())?;
    for input in &inputs.0 {
        let input_status = world.get::<FunnelInputStatus>(*input).ok_or(())?;
        if input_status.is_pending() {
            // One of the inputs is still pending, so we should not take any
            // action yet.
            return Ok(OperationStatus::Disregard);
        }
    }

    // None of the inputs are pending so we can close down this join
    let apply_to = world.get::<SingleTargetStorage>(source).ok_or(())?.0;
    roster.cancel(Cancel { apply_to, cause });
    world.get_mut::<JoinStatus>(source).ok_or(())?.close();
    return Ok(OperationStatus::Finished);
}

#[derive(Component)]
enum JoinStatus {
    Pending,
    Cancelled(Arc<CancellationCause>),
    Closed,
}

impl JoinStatus {
    fn cancel(&mut self, cause: Arc<CancellationCause>) {
        if matches!(self, Self::Pending) {
            *self = Self::Cancelled(cause);
        }
    }

    fn close(&mut self) {
        *self = Self::Closed;
    }
}

fn deliver_bundle_join<T: 'static + Send + Sync>(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) -> Result<OperationStatus, ()> {
    let target = world.get::<SingleTargetStorage>(source).ok_or(())?.0;
    // Consider ways to avoid cloning here. Maybe InputStorage should have an
    // Option inside to take from it via a Query<&mut InputStorage<T>>.
    let inputs = world.get::<FunnelSourceStorage>(source).ok_or(())?.0.clone();
    let mut response = JoinedBundle::new();
    for input in inputs {
        let value = world
            .get_entity_mut(input).ok_or(())?
            .take::<InputStorage<T>>().ok_or(())?;
        response.push(value);
    }

    world
        .get_entity_mut(target).ok_or(())?
        .insert(InputBundle::new(response));
    roster.queue(target);

    Ok(OperationStatus::Finished)
}
