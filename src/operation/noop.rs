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
    Operation, SingleInputStorage, SingleTargetStorage, Input, ManageInput,
    OperationResult, OrBroken, OperationSetup, OperationRequest, OperationCleanup,
    OperationReachability, ReachabilityResult, InputBundle,
};

pub(crate) struct Noop<T> {
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Noop<T> {
    pub(crate) fn new(target: Entity) -> Self {
        Noop { target, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Operation for Noop<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            SingleTargetStorage(self.target),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let Input { session, data: value } = source_mut.take_input::<T>()?;
        let mut target_mut = world.get_entity_mut(target).or_broken()?;
        target_mut.give_input(session, value, roster);
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
