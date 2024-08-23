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
    ForkTargetStorage, InputBundle, Operation, OperationCleanup, OperationReachability,
    OperationRequest, OperationResult, OperationSetup, OrBroken, ReachabilityResult,
    SingleInputStorage, Unzippable,
};

pub(crate) struct ForkUnzip<T> {
    targets: ForkTargetStorage,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> ForkUnzip<T> {
    pub(crate) fn new(targets: ForkTargetStorage) -> Self {
        Self {
            targets,
            _ignore: Default::default(),
        }
    }
}

impl<T: Unzippable + 'static + Send + Sync> Operation for ForkUnzip<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        for target in &self.targets.0 {
            world
                .get_entity_mut(*target)
                .or_broken()?
                .insert(SingleInputStorage::new(source));
        }
        world
            .entity_mut(source)
            .insert((InputBundle::<T>::new(), self.targets));
        Ok(())
    }

    fn execute(request: OperationRequest) -> OperationResult {
        T::distribute_values(request)
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
