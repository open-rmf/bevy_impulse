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

use bevy_ecs::prelude::Entity;

use crate::{
    Accessed, buffer_key_usage, get_access_keys, BufferAccessStorage, BufferKeyUsage,
    FunnelInputStorage, Input, InputBundle, ManageInput, Operation, OperationCleanup,
    OperationReachability, OperationRequest, OperationResult, OperationSetup, OrBroken,
    ReachabilityResult, SingleInputStorage, SingleTargetStorage,
};

pub(crate) struct Listen<B> {
    buffers: B,
    target: Entity,
}

impl<B> Listen<B> {
    pub(crate) fn new(buffers: B, target: Entity) -> Self {
        Self { buffers, target }
    }
}

impl<B> Operation for Listen<B>
where
    B: Accessed + 'static + Send + Sync,
    B::Key: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        self.buffers.add_accessor(source, world)?;
        self.buffers.add_listener(source, world)?;

        world.entity_mut(source).insert((
            InputBundle::<()>::new(),
            FunnelInputStorage::from(self.buffers.as_input()),
            BufferAccessStorage::new(self.buffers),
            SingleTargetStorage::new(self.target),
            BufferKeyUsage(buffer_key_usage::<B>),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let Input { session, .. } = world
            .get_entity_mut(source)
            .or_broken()?
            .take_input::<()>()?;

        let keys = get_access_keys::<B>(source, session, world)?;

        let target = world.get::<SingleTargetStorage>(source).or_broken()?.get();
        world
            .get_entity_mut(target)
            .or_broken()?
            .give_input(session, keys, roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<()>()?;
        clean.cleanup_buffer_access::<B>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<()>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut r)
    }
}
