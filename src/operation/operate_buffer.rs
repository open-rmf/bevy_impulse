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

use bevy::prelude::Bundle;

use crate::{
    BufferStorage, Operation, OperationSetup, OperationRequest, OperationResult,
    OperationCleanup, OperationReachability, ReachabilityResult, OrBroken,
    ManageInput, ForkTargetStorage, SingleInputStorage, BufferSettings,
};

#[derive(Bundle)]
pub(crate) struct OperateBuffer<T: 'static + Send + Sync> {
    storage: BufferStorage<T>,
}

impl<T: 'static + Send + Sync> OperateBuffer<T> {
    pub(crate) fn new(settings: BufferSettings) -> Self {
        Self { storage: BufferStorage::new(settings) }
    }

    pub(crate) fn new_cloning(settings: BufferSettings) -> Self
    where
        T: Clone,
    {
        Self { storage: BufferStorage::new_cloning(settings) }
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
