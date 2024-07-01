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
    prelude::{Entity, Component, World, DespawnRecursiveExt},
    ecs::system::Command,
};

use crossbeam::channel::Sender;

use anyhow::anyhow;

use backtrace::Backtrace;

use crate::{
    Operation, OperationSetup, OperationRequest, OperationReachability, OperationCleanup,
    MiscellaneousFailure, InputBundle, ReachabilityResult, UnusedTarget, Storage,
    UnhandledErrors, OperationCancel, OperationResult, OrBroken, Input, ManageInput,
    Collection,
    promise::private::Sender as PromiseSender,
};

#[derive(Component)]
pub(crate) struct ImpulseProperties {
    unused: bool,
    detached: bool,
}

impl ImpulseProperties {
    pub(crate) fn new() -> Self {
        Self { unused: true, detached: false }
    }
}

pub(crate) struct Detach {
    pub(crate) session: Entity,
}

impl Command for Detach {
    fn apply(self, world: &mut World) {
        let backtrace;
        if let Some(mut session_mut) = world.get_entity_mut(self.session) {
            if let Some(mut properties) = session_mut.get_mut::<ImpulseProperties>() {
                properties.detached = true;
                session_mut.remove::<UnusedTarget>();
                return;
            } else {
                // The session is missing the target properties that it's
                // supposed to have
                backtrace = Backtrace::new();
            }
        } else {
            // The session has despawned before we could manage to use it, or it
            // never existed in the first place.
            backtrace = Backtrace::new();
        }

        let failure = MiscellaneousFailure {
            error: anyhow!("Unable to detach target {:?}", self.session),
            backtrace: Some(backtrace),
        };
        world.get_resource_or_insert_with(|| UnhandledErrors::default())
            .miscellaneous
            .push(failure);
    }
}

#[derive(Component)]
pub(crate) struct TakenResponse<T> {
    sender: PromiseSender<T>,
}

impl<T> TakenResponse<T> {
    pub(crate) fn new(sender: PromiseSender<T>) -> Self {
        Self { sender }
    }
}

impl<T: 'static + Send + Sync> Operation for TakenResponse<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            self,
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { data, .. } = source_mut.take_input::<T>()?;
        let sender = source_mut.take::<TakenResponse<T>>().or_broken()?.sender;
        sender.send(data);
        source_mut.despawn_recursive();

        Ok(())
    }

    fn is_reachable(_: OperationReachability) -> ReachabilityResult {
        unreachable!("Unexpected query for reachability on a TakenResponse");
    }

    fn cleanup(OperationCleanup { source, world, .. }: OperationCleanup) -> OperationResult {
        let source_mut = world.get_entity_mut(source).or_not_ready()?;
        source_mut.despawn_recursive();
        Ok(())
    }
}

#[derive(Component)]
pub(crate) struct TakenStream<T> {
    sender: Sender<T>,
}

impl<T> TakenStream<T> {
    pub fn new(sender: Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T: 'static + Send + Sync> Operation for TakenStream<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            self,
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { data, .. } = source_mut.take_input::<T>()?;
        let stream = source_mut.get::<TakenStream<T>>().or_broken()?;
        stream.sender.send(data).ok();
        Ok(())
    }

    fn cleanup(_: OperationCleanup) -> OperationResult {
        unreachable!("Unexpected request to cleanup a TakenStream");
    }

    fn is_reachable(_: OperationReachability) -> ReachabilityResult {
        unreachable!("Unexpected query for reachability on a TakenStream")
    }
}

#[derive(Component)]
pub(crate) struct StoreResponse<Response> {
    target: Entity,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response> StoreResponse<Response> {
    pub(crate) fn new(target: Entity) -> Self {
        Self { target, _ignore: Default::default() }
    }
}

impl<Response: 'static + Send + Sync> Operation for StoreResponse<Response> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        add something to track the life of the target
        world.entity_mut(source).insert(self);
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<Response>()?;
        let target = source_mut.get::<StoreResponse<Response>>().or_broken()?.target;
        world.get_entity_mut(target).or_broken()?.insert(Storage { data, session });

        Ok(())
    }

    fn cleanup(_: OperationCleanup) -> OperationResult {
        unreachable!("Unexpected request to cleanup a StoreResponse");
    }

    fn is_reachable(_: OperationReachability) -> ReachabilityResult {
        unreachable!("Unexpected query for reachability on a StoreResponse");
    }
}

#[derive(Component)]
pub(crate) struct PushResponse<Response> {
    target: Entity,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response> PushResponse<Response> {
    pub(crate) fn new(target: Entity) -> Self {
        Self { target, _ignore: Default::default() }
    }
}

impl<Response: 'static + Send + Sync> Operation for PushResponse<Response> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        add something to track the life of the target
        world.entity_mut(source).insert(self);
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<Response>()?;
        let target = source_mut.get::<PushResponse<Response>>().or_broken()?.target;
        let mut target_mut = world.get_entity_mut(target).or_broken()?;
        if let Some(mut collection) = target_mut.get_mut::<Collection<Response>>() {
            collection.items.push(Storage { session, data });
        } else {
            let mut collection = Collection::default();
            collection.items.push(Storage { session, data });
            target_mut.insert(collection);
        }
        Ok(())
    }

    fn cleanup(_: OperationCleanup) -> OperationResult {
        unreachable!("Unexpected request to cleanup a PushResponse");
    }

    fn is_reachable(_: OperationReachability) -> ReachabilityResult {
        unreachable!("Unexpected query for reachability on a PushResponse");
    }
}

fn cancel_taken_target<T>(
    OperationCancel { cancel, world, .. }: OperationCancel,
) -> OperationResult
where
    T: 'static + Send + Sync,
{
    let mut target_mut = world.get_entity_mut(cancel.target).or_broken()?;
    let taken = target_mut.take::<TakenResponse<T>>().or_broken()?;
    taken.sender.cancel(cancel.cancellation);
    target_mut.despawn_recursive();

    Ok(())
}
