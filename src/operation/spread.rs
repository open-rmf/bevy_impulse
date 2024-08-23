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

use crate::{
    emit_disposal, Disposal, Input, InputBundle, ManageInput, Operation, OperationCleanup,
    OperationReachability, OperationRequest, OperationResult, OperationSetup, OrBroken,
    ReachabilityResult, SingleInputStorage, SingleTargetStorage,
};

use bevy_ecs::prelude::Entity;

pub(crate) struct Spread<I> {
    target: Entity,
    _ignore: std::marker::PhantomData<fn(I)>,
}

impl<I> Spread<I> {
    pub(crate) fn new(target: Entity) -> Self {
        Self {
            target,
            _ignore: Default::default(),
        }
    }
}

impl<I> Operation for Spread<I>
where
    I: IntoIterator + 'static + Send + Sync,
    I::Item: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<I>::new(),
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
        let Input { session, data } = source_mut.take_input::<I>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();

        let mut at_least_one = false;
        let mut target_mut = world.get_entity_mut(target).or_broken()?;
        for datum in data {
            at_least_one = true;
            target_mut.give_input(session, datum, roster)?;
        }

        if !at_least_one {
            // There was nothing to be sent, so we notify that a disposal has
            // happened.
            let disposal = Disposal::empty_spread(source);
            emit_disposal(source, session, disposal, world, roster);
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<I>()?;
        clean.cleanup_disposals()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<I>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}
