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
    prelude::{Bundle, Entity, World, Component},
    ecs::system::Command,
};

use std::sync::Arc;

use anyhow::anyhow;

use backtrace::Backtrace;

use smallvec::SmallVec;

use crate::{
    BufferStorage, Operation, OperationSetup, OperationRequest, OperationResult,
    OperationCleanup, OperationReachability, ReachabilityResult, OrBroken,
    ManageInput, ForkTargetStorage, SingleInputStorage, BufferSettings,
    UnhandledErrors, MiscellaneousFailure, InputBundle, OperationError,
    InspectInput,
};

#[derive(Bundle)]
pub(crate) struct OperateBuffer<T: 'static + Send + Sync> {
    storage: BufferStorage<T>,
}

impl<T: 'static + Send + Sync> OperateBuffer<T> {
    pub(crate) fn new(settings: BufferSettings) -> Self {
        Self { storage: BufferStorage::new(settings) }
    }
}

// TODO(@mxgrey): Implement an operation for removing / clearing items from buffers,
// and a way to subscribe to that operation.
impl<T> Operation for OperateBuffer<T>
where
    T: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            self,
            ForkTargetStorage::new(),
            SingleInputStorage::empty(),
            InputBundle::<T>::new(),
            BufferBundle::new::<T>(),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        world.get_entity_mut(source).or_broken()?
            .transfer_to_buffer::<T>(roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

#[derive(Debug)]
pub(crate) struct OnNewBufferValue {
    buffer: Entity,
    target: Entity,
}

impl OnNewBufferValue {
    pub(crate) fn new(buffer: Entity, target: Entity) -> Self {
        OnNewBufferValue { buffer, target }
    }
}

impl Command for OnNewBufferValue {
    fn apply(self, world: &mut World) {
        let Some(mut buffer_targets) = world.get_mut::<ForkTargetStorage>(self.buffer) else {
            self.on_failure(world);
            return;
        };

        buffer_targets.0.push(self.buffer);

        let Some(mut target_mut) = world.get_entity_mut(self.target) else {
            self.on_failure(world);
            return;
        };

        target_mut.insert(SingleInputStorage::new(self.buffer));
    }
}

impl OnNewBufferValue {
    fn on_failure(self, world: &mut World) {
        world.get_resource_or_insert_with(|| UnhandledErrors::default())
            .miscellaneous
            .push(MiscellaneousFailure {
                error: Arc::new(anyhow!(
                    "Unable to add target with OnNewBufferValue: {self:?}"
                )),
                backtrace: Some(Backtrace::new()),
            });
    }
}

#[derive(Bundle)]
struct BufferBundle {
    clear: ClearBufferFn,
    size: CheckBufferSizeFn,
    sessions: GetBufferedSessionsFn,
}

impl BufferBundle {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self {
            clear: ClearBufferFn::new::<T>(),
            size: CheckBufferSizeFn::new::<T>(),
            sessions: GetBufferedSessionsFn::new::<T>(),
        }
    }
}

#[derive(Component)]
pub struct ClearBufferFn(pub fn(Entity, Entity, &mut World) -> OperationResult);

impl ClearBufferFn {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self(clear_buffer::<T>)
    }
}

fn clear_buffer<T: 'static + Send + Sync>(
    source: Entity,
    session: Entity,
    world: &mut World,
) -> OperationResult {
    world.get_entity_mut(source).or_broken()?.clear_buffer::<T>(session)
}

#[derive(Component)]
pub struct CheckBufferSizeFn(pub fn(Entity, Entity, &World) -> Result<usize, OperationError>);

impl CheckBufferSizeFn {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self(check_buffer_size::<T>)
    }
}

fn check_buffer_size<T: 'static + Send + Sync>(
    source: Entity,
    session: Entity,
    world: &World,
) -> Result<usize, OperationError> {
    world.get_entity(source).or_broken()?.buffered_count::<T>(session)
}

#[derive(Component)]
pub struct GetBufferedSessionsFn(pub fn(Entity, &World) -> Result<SmallVec<[Entity; 16]>, OperationError>);

impl GetBufferedSessionsFn {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self(get_buffered_sessions::<T>)
    }
}

fn get_buffered_sessions<T: 'static + Send + Sync>(
    source: Entity,
    world: &World,
) -> Result<SmallVec<[Entity; 16]>, OperationError> {
    world.get_entity(source).or_broken()?.buffered_sessions::<T>()
}
