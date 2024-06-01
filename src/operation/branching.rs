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

use bevy::prelude::{Component, World, Entity};

use crate::{
    Operation, OperationRoster, ForkTargetStorage, SingleInputStorage,
    OperationResult, OrBroken, OperationRequest, OperationSetup, OperationCleanup,
    ManageInput, Input, OperationReachability, ReachabilityResult, InputBundle,
    Disposal, ManageDisposal,
};

use thiserror::Error as ThisError;

pub struct Branching<Input, Outputs, F> {
    activator: F,
    targets: ForkTargetStorage,
    _ignore: std::marker::PhantomData<(Input, Outputs)>,
}

pub(crate) fn make_result_branching<T, E>(
    targets: ForkTargetStorage
) -> Branching<Result<T, E>, (T, E), fn(Result<T, E>) -> (BranchResult<T>, BranchResult<E>)> {
    Branching {
        activator: branch_result,
        targets,
        _ignore: Default::default(),
    }
}

pub(crate) fn make_option_branching<T>(
    targets: ForkTargetStorage
) -> Branching<Option<T>, (T, ()), fn(Option<T>) -> (BranchResult<T>, BranchResult<()>)> {
    Branching {
        activator: branch_option,
        targets,
        _ignore: Default::default(),
    }
}

#[derive(Component)]
struct BranchingActivatorStorage<F: 'static + Send + Sync>(F);

impl<InputT, Outputs, F> Operation for Branching<InputT, Outputs, F>
where
    InputT: 'static + Send + Sync,
    Outputs: Branchable,
    F: FnOnce(InputT) -> Outputs::Activation + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        for target in &self.targets.0 {
            world.get_entity_mut(*target).or_broken()?
                .insert(SingleInputStorage::new(source));
        }
        world.entity_mut(source).insert((
            self.targets,
            InputBundle::<InputT>::new(),
            BranchingActivatorStorage(self.activator),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: input } = source_mut.take_input::<InputT>()?;
        let BranchingActivatorStorage::<F>(activator) = source_mut.take().or_broken()?;

        let activation = activator(input);
        Outputs::activate(session, activation, source, world, roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<InputT>()?;
        clean.cleanup_disposals()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<InputT>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

pub trait Branchable {
    type Activation;

    fn activate<'a>(
        session: Entity,
        activation: Self::Activation,
        source: Entity,
        world: &'a mut World,
        roster: &'a mut OperationRoster,
    ) -> OperationResult;
}

pub type BranchResult<T> = Result<T, Option<anyhow::Error>>;

impl<A, B> Branchable for (A, B)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
{
    type Activation = (BranchResult<A>, BranchResult<B>);

    fn activate<'a>(
        session: Entity,
        (a, b): Self::Activation,
        source: Entity,
        world: &'a mut World,
        roster: &'a mut OperationRoster,
    ) -> OperationResult {
        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;
        let target_a = *targets.0.get(0).or_broken()?;
        let target_b = *targets.0.get(1).or_broken()?;

        let mut target_a_mut = world.get_entity_mut(target_a).or_broken()?;
        match a {
            Ok(a) => target_a_mut.give_input(session, a, roster)?,
            Err(reason) => {
                let disposal = Disposal::branching(source, target_a, reason);
                target_a_mut.emit_disposal(session, disposal, roster);
            }
        }

        let mut target_b_mut = world.get_entity_mut(target_b).or_broken()?;
        match b {
            Ok(b) => target_b_mut.give_input(session, b, roster)?,
            Err(reason) => {
                let disposal = Disposal::branching(source, target_b, reason);
                target_b_mut.emit_disposal(session, disposal, roster);
            }
        }

        Ok(())
    }
}

#[derive(ThisError, Debug)]
#[error("An Ok value was received, so the Err branch is being disposed")]
pub struct OkInput;

#[derive(ThisError, Debug)]
#[error("An Err value was received, so the Ok branch is being disposed")]
pub struct ErrInput;

fn branch_result<T, E>(input: Result<T, E>) -> (BranchResult<T>, BranchResult<E>) {
    match input {
        Ok(value) => (Ok(value), Err(Some(OkInput.into()))),
        Err(err) => (Err(Some(ErrInput.into())), Ok(err)),
    }
}

#[derive(ThisError, Debug)]
#[error("A Some value was received, so the None branch is being disposed")]
pub struct SomeInput;

#[derive(ThisError, Debug)]
#[error("A None value was received, so the Some branch is being disposed")]
pub struct NoneInput;

fn branch_option<T>(input: Option<T>) -> (BranchResult<T>, BranchResult<()>) {
    match input {
        Some(value) => (Ok(value), Err(Some(SomeInput.into()))),
        None => (Err(Some(NoneInput.into())), Ok(())),
    }
}
