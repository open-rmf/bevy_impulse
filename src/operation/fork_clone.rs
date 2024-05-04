/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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
    Input, ManageInput, Operation, InputBundle,
    ForkTargetStorage, SingleInputStorage,
    OperationResult, OrBroken, OperationRequest, OperationSetup, OperationError,
    OperationCleanup, OperationReachability, ReachabilityResult,
};

pub(crate) struct ForkClone<Response: 'static + Send + Sync + Clone> {
    targets: ForkTargetStorage,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response: 'static + Send + Sync + Clone> ForkClone<Response> {
    pub(crate) fn new(targets: ForkTargetStorage) -> Self {
        Self { targets, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync + Clone> Operation for ForkClone<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        for target in &self.targets.0 {
            if let Some(mut target_mut) = world.get_entity_mut(*target) {
                target_mut.insert(SingleInputStorage::new(source));
            }
        }
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            self.targets,
        ));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { requester, data: input } = source_mut.take_input::<T>()?;
        let ForkTargetStorage(targets) = source_mut.take().or_broken()?;

        let mut send_value = |value: T, target: Entity| -> Result<(), OperationError> {
            world
            .get_entity_mut(target).or_broken()?
            .give_input(requester, value, roster)
        };

        // Distributing the values like this is a bit convoluted, but it ensures
        // that we are not producing any more clones of the input than what is
        // strictly necessary. This may be valuable if cloning the value is
        // expensive.
        for target in targets[..targets.len()-1].iter() {
            send_value(input.clone(), *target)?;
        }

        if let Some(last_target) = targets.last() {
            send_value(input, *last_target)?;
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>();
        clean.notify_cleaned()
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(reachability)
    }
}
