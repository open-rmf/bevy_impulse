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

use bevy::prelude::{Entity, Component};

use crate::{
    Input, ManageInput, FunnelInputStatus, FunnelSourceStorage,
    SingleTargetStorage, Operation, Unzippable,
    SingleSourceStorage, Cancel, Cancellation, JoinCancelled, JoinedBundle,
    CancellationCause, OperationResult, OrBroken, OperationRequest, OperationSetup,
};

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
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        world.entity_mut(source).insert((
            SingleTargetStorage(self.target),
            FunnelInputStatus::Pending,
        ));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        source_mut.get_mut::<FunnelInputStatus>().or_broken()?.ready();

        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        roster.queue(target);

        // We can't let this link be cleaned up automatically. Its cleanup needs
        // to be handled by the join that it belongs to.
        Ok(())
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
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleSourceStorage(source));
        }

        world.entity_mut(source).insert((
            self.sources,
            SingleTargetStorage(self.target),
            JoinStatus::Pending,
        ));
    }

    fn execute(request: OperationRequest) -> OperationResult {
        manage_join_delivery(request, Values::join_values)
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
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleSourceStorage(source));
        }

        world.entity_mut(source).insert((
            self.sources,
            SingleTargetStorage(self.target),
            JoinStatus::Pending,
        ));
    }

    fn execute(request: OperationRequest) -> OperationResult {
        manage_join_delivery(request, deliver_bundle_join::<T>)
    }
}

fn manage_join_delivery(
    OperationRequest { source, world, roster }: OperationRequest,
    deliver: fn(Entity, OperationRequest) -> OperationResult,
) -> OperationResult {
    match world.get::<JoinStatus>(source).or_broken()?.clone() {
        JoinStatus::Pending => {
            manage_pending_join(
                OperationRequest { source, world, roster },
                deliver,
            )
        }
        JoinStatus::Cancelled(cause) => {
            manage_cancelled_join(
                OperationRequest { source, world, roster },
                cause.clone()
            )
        }
        JoinStatus::Closed => {
            // No action is needed if the join has already reached closed status.
            // This means it has already been asked to get cleaned up.
            Ok(())
        }
    }
}

fn manage_pending_join(
    OperationRequest { source, world, roster, .. }: OperationRequest,
    deliver: fn(Entity, OperationRequest) -> OperationResult,
) -> OperationResult {
    let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;

    let mut cancel_join = false;
    let mut deliver_join = true;
    for input in &inputs.0 {
        let input_status = world.get::<FunnelInputStatus>(*input).or_broken()?;
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
            let input_status = world.get::<FunnelInputStatus>(*input).or_broken()?;
            if input_status.is_pending() {
                any_pending = true;
            }

            input_statuses.push((*input, input_status.clone()));
        }

        let cause = Cancellation::from_cause(CancellationCause::JoinCancelled(
            JoinCancelled { join: source, input_statuses }
        ));

        if any_pending {
            // Some of the input sources are still pending, so we should wait
            // until those have caught up before propagating the cancel to the
            // target. Cancellation behaviors are meant to always be triggered
            // in order of their location along the chain.
            let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;
            for input in &inputs.0 {
                let input_status = world.get::<FunnelInputStatus>(*input).or_broken()?;
                if input_status.is_pending() {
                    roster.drop_dependency(
                        Cancel { apply_to: *input, cause: cause.clone() }
                    );
                }
            }

            world.get_mut::<JoinStatus>(source).or_broken()?.cancel(cause);
        } else {
            // None of the input sources are pending, so we are ready to cancel
            // the target and dispose of the join.
            let apply_to = world.get::<SingleTargetStorage>(source).or_broken()?.0;
            roster.cancel(Cancel { apply_to, cause });

            world.get_mut::<JoinStatus>(source).or_broken()?.close();
            return Ok(());
        }
    } else if deliver_join {
        deliver(source, world, roster)?;
        world.get_mut::<JoinStatus>(source).or_broken()?.close();
        return Ok(());
    }

    Ok(())
}

fn manage_cancelled_join(
    OperationRequest { source, world, roster, .. }: OperationRequest,
    cause: Cancellation,
) -> OperationResult {
    let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;
    for input in &inputs.0 {
        let input_status = world.get::<FunnelInputStatus>(*input).or_broken()?;
        if input_status.is_pending() {
            // One of the inputs is still pending, so we should not take any
            // action yet.
            return Ok(());
        }
    }

    // None of the inputs are pending so we can close down this join
    let apply_to = world.get::<SingleTargetStorage>(source).or_broken()?.0;
    roster.cancel(Cancel { apply_to, cause });
    world.get_mut::<JoinStatus>(source).or_broken()?.close();
    return Ok(());
}

#[derive(Component, Clone)]
enum JoinStatus {
    Pending,
    Cancelled(Cancellation),
    Closed,
}

impl JoinStatus {
    fn cancel(&mut self, cause: Cancellation) {
        if matches!(self, Self::Pending) {
            *self = Self::Cancelled(cause);
        }
    }

    fn close(&mut self) {
        *self = Self::Closed;
    }
}

fn deliver_bundle_join<T: 'static + Send + Sync>(
    requester: Entity,
    OperationRequest { source, world, roster }: OperationRequest,
) -> OperationResult {
    let target = world.get::<SingleTargetStorage>(source).or_broken()?.0;
    // Consider ways to avoid cloning here. Maybe InputStorage should have an
    // Option inside to take from it via a Query<&mut InputStorage<T>>.
    let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?.0.clone();
    let mut response = JoinedBundle::new();
    for input in inputs {
        let value = world
            .get_entity_mut(input).or_broken()?
            .from_buffer::<T>(requester)?;
        response.push(value);
    }

    world
        .get_entity_mut(target).or_broken()?
        .give_input(requester, response, roster);

    Ok(())
}
