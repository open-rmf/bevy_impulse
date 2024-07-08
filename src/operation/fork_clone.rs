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

use bevy::{
    prelude::{Entity, World},
    ecs::system::Command,
};

use anyhow::anyhow;

use backtrace::Backtrace;

use std::sync::Arc;

use crate::{
    Input, ManageInput, Operation, InputBundle, MiscellaneousFailure,
    ForkTargetStorage, SingleInputStorage, UnhandledErrors,
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
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        for target in &self.targets.0 {
            world.get_entity_mut(*target).or_broken()?
                .insert(SingleInputStorage::new(source));
        }
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            self.targets,
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: input } = source_mut.take_input::<T>()?;
        let ForkTargetStorage(targets) = source_mut.take().or_broken()?;

        let mut send_value = |value: T, target: Entity| -> Result<(), OperationError> {
            world
            .get_entity_mut(target).or_broken()?
            .give_input(session, value, roster)
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

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AddBranchToForkClone {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
}

impl Command for AddBranchToForkClone {
    fn apply(self, world: &mut World) {
        if let Err(OperationError::Broken(backtrace)) = try_add_branch_to_fork_clone(self, world) {
            world.get_resource_or_insert_with(|| UnhandledErrors::default())
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: Arc::new(anyhow!(
                        "Unable to create a new branch for a fork clone, source: {:?}, target: {:?}",
                        self.source, self.target,
                    )),
                    backtrace: Some(backtrace.unwrap_or_else(|| Backtrace::new()))
                })
        }
    }
}

fn try_add_branch_to_fork_clone(
    branch: AddBranchToForkClone,
    world: &mut World,
) -> OperationResult {
    let mut targets = world.get_mut::<ForkTargetStorage>(branch.source).or_broken()?;
    targets.0.push(branch.target);
    Ok(())
}
