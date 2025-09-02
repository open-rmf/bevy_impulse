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

use bevy_ecs::prelude::{Component, Entity};

use crate::{
    FunnelInputStorage, Input, InputBundle, Joining, ManageInput, Operation, OperationCleanup,
    OperationError, OperationReachability, OperationRequest, OperationResult, OperationSetup,
    OrBroken, ReachabilityResult, SingleInputStorage, SingleTargetStorage,
};

pub(crate) struct Join<Buffers> {
    buffers: Buffers,
    target: Entity,
}

impl<Buffers> Join<Buffers> {
    pub(crate) fn new(buffers: Buffers, target: Entity) -> Self {
        Self { buffers, target }
    }
}

#[derive(Component)]
struct BufferStorage<Buffers>(Buffers);

impl<Buffers: Joining + 'static + Send + Sync> Operation for Join<Buffers>
where
    Buffers::Item: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        self.buffers.add_listener(source, world)?;

        world.entity_mut(source).insert((
            FunnelInputStorage::from(self.buffers.as_input()),
            BufferStorage(self.buffers),
            InputBundle::<()>::new(),
            SingleTargetStorage::new(self.target),
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
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, .. } = source_mut.take_input::<()>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let buffers = source_mut
            .get::<BufferStorage<Buffers>>()
            .or_broken()?
            .0
            .clone();
        if dbg!(buffers.buffered_count(session, world))? < 1 {
            return Err(OperationError::NotReady);
        }

        let output = buffers.pull(session, world)?;
        world
            .get_entity_mut(target)
            .or_broken()?
            .give_input(session, output, roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<()>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        let buffers = r.world.get::<BufferStorage<Buffers>>(r.source)
            .or_broken()?;

        let inputs = r
            .world
            .get_entity(r.source)
            .or_broken()?
            .get::<FunnelInputStorage>()
            .or_broken()?;
        for input in &inputs.0 {
            if !r.check_upstream(*input)? {
                // This input buffer is no longer reachable, so if it is also
                // empty then there will be no way to ever perform a join.
                if buffers.0.buffered_count_for(*input, r.session, r.world)? == 0 {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}
