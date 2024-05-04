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

use bevy::prelude::Entity;

use crate::{
    Input, ManageInput, InspectInput, InputBundle, FunnelInputStorage,
    SingleTargetStorage, Operation, Unzippable,
    SingleInputStorage, Cancel, JoinedBundle, JoinStatus, JoinStatusResult,
    OperationResult, OrBroken, OperationRequest, OperationSetup,
    OperationCleanup, OperationReachability, ReachabilityResult,
};

use std::collections::HashMap;

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
        world.entity_mut(source).insert(SingleTargetStorage(self.target));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world
            .get_entity_mut(source).or_broken()?
            .transfer_to_buffer::<T>(roster);
        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(reachability)
    }
}

pub(crate) struct ZipJoin<Values> {
    sources: FunnelInputStorage,
    target: Entity,
    _ignore: std::marker::PhantomData<Values>,
}

impl<Values> ZipJoin<Values> {
    pub(crate) fn new(
        sources: FunnelInputStorage,
        target: Entity,
    ) -> Self {
        Self { sources, target, _ignore: Default::default() }
    }
}

impl<Values: Unzippable> Operation for ZipJoin<Values> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleInputStorage::new(source));
        }

        world.entity_mut(source).insert((
            self.sources,
            InputBundle::<()>::new(),
            SingleTargetStorage(self.target),
        ));
    }

    fn execute(request: OperationRequest) -> OperationResult {
        manage_join_delivery(request, Values::join_status, Values::join_values)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<()>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        let inputs = r.world.get_entity(r.source).or_broken()?
            .get::<FunnelInputStorage>().or_broken()?;
        for input in &inputs.0 {
            if !r.check_upstream(*input)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

pub(crate) struct BundleJoin<T> {
    sources: FunnelInputStorage,
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> BundleJoin<T> {
    pub(crate) fn new(sources: FunnelInputStorage, target: Entity) -> Self {
        Self { sources, target, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Operation for BundleJoin<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleInputStorage::new(source));
        }

        world.entity_mut(source).insert((
            self.sources,
            InputBundle::<()>::new(),
            SingleTargetStorage(self.target),
        ));
    }

    fn execute(request: OperationRequest) -> OperationResult {
        manage_join_delivery(request, status_bundle_join::<T>, deliver_bundle_join::<T>)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<()>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        let inputs = r.world.get_entity(r.source).or_broken()?
            .get::<FunnelInputStorage>().or_broken()?;
        for input in &inputs.0 {
            if !r.check_upstream(*input)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

fn manage_join_delivery(
    mut request: OperationRequest,
    reachable: fn(Entity, OperationReachability) -> JoinStatusResult,
    deliver: fn(Entity, OperationRequest) -> OperationResult,
) -> OperationResult {
    let Input { requester, .. } = request.world
        .get_entity_mut(request.source).or_broken()?
        .take_input::<()>()?;

    let mut visited = HashMap::new();
    match reachable(requester, OperationReachability::new(
        requester, request.source, request.world, &mut visited
    ))? {
        JoinStatus::Pending => {
            // Simply return
        }
        JoinStatus::Ready => {
            deliver(requester, request)?
        }
        JoinStatus::Unreachable(unreachable) => {
            request.roster.cancel(Cancel::join(request.source, unreachable));
        }
    }

    Ok(())
}

fn status_bundle_join<T: 'static + Send + Sync>(
    requester: Entity,
    mut reachability: OperationReachability,
) -> JoinStatusResult {
    let source = reachability.source();
    let requester = reachability.requester();
    let world = reachability.world();
    let inputs = world.get::<FunnelInputStorage>(source).or_broken()?;
    let mut unreachable: Vec<Entity> = Vec::new();
    let mut status = JoinStatus::Ready;

    for input in &inputs.0 {
        if !world.get_entity(*input).or_broken()?.buffer_ready::<T>(requester)? {
            status = JoinStatus::Pending;
            if !reachability.check_upstream(*input)? {
                unreachable.push(*input);
            }
        }
    }

    if !unreachable.is_empty() {
        return Ok(JoinStatus::Unreachable(unreachable));
    }

    Ok(status)
}

fn deliver_bundle_join<T: 'static + Send + Sync>(
    requester: Entity,
    OperationRequest { source, world, roster }: OperationRequest,
) -> OperationResult {
    let target = world.get::<SingleTargetStorage>(source).or_broken()?.0;
    // Consider ways to avoid cloning here. Maybe InputStorage should have an
    // Option inside to take from it via a Query<&mut InputStorage<T>>.
    let inputs = world.get::<FunnelInputStorage>(source).or_broken()?.0.clone();
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
