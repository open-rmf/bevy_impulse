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
    ScopeStorage, Operation, OperationRequest, OperationCleanup, OperationSetup,
    OperationReachability, OperationResult, ReachabilityResult, InputBundle,
    OrBroken, Input, ManageInput, FinishedStagingStorage, SingleInputStorage,
    TerminalStorage,
};

pub struct Terminate<T> {
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Operation for Terminate<T>
where
    T: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        let mut source_mut = world.entity_mut(source);
        source_mut.insert(InputBundle::<T>::new());
        let scope = source_mut.get::<ScopeStorage>().or_broken()?;
        let mut terminals = world.get_mut::<TerminalStorage>(scope.get()).or_broken()?;
        terminals.0.push(source);
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let scope = source_mut.get::<ScopeStorage>().or_broken()?.get();
        let Input { session, data } = source_mut.take_input::<T>()?;
        let staging = world.get_entity(scope).or_broken()?.get::<FinishedStagingStorage>()
            .or_broken()?.get();

        world.get_entity_mut(staging).or_broken()?.give_input(session, data, roster)
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
