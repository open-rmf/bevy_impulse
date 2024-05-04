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
};

pub struct Branching<Input, Outputs, F> {
    activator: F,
    targets: ForkTargetStorage,
    _ignore: std::marker::PhantomData<(Input, Outputs)>,
}

pub(crate) fn make_result_branching<T, E>(
    targets: ForkTargetStorage
) -> Branching<Result<T, E>, (T, E), fn(Result<T, E>, &mut (Option<T>, Option<E>))> {
    Branching {
        activator: branch_result,
        targets,
        _ignore: Default::default(),
    }
}

pub(crate) fn make_option_branching<T>(
    targets: ForkTargetStorage
) -> Branching<Option<T>, (T, ()), fn(Option<T>, &mut (Option<T>, Option<()>))> {
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
    F: FnOnce(InputT, &mut Outputs::Activation) + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        for target in &self.targets.0 {
            if let Some(mut target_mut) = world.get_entity_mut(*target) {
                target_mut.insert(SingleInputStorage::new(source));
            }
        }
        world.entity_mut(source).insert((
            self.targets,
            InputBundle::<InputT>::new(),
            BranchingActivatorStorage(self.activator),
        ));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { requester, data: input } = source_mut.take_input::<InputT>()?;
        let BranchingActivatorStorage::<F>(activator) = source_mut.take().or_broken()?;

        let mut activation = Outputs::new_activation();
        activator(input, &mut activation);

        Outputs::activate(requester, activation, source, world, roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<InputT>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<InputT>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(reachability)
    }
}

pub trait Branchable {
    type Activation;

    fn new_activation() -> Self::Activation;

    fn activate<'a>(
        requester: Entity,
        activation: Self::Activation,
        source: Entity,
        world: &'a mut World,
        roster: &'a mut OperationRoster,
    ) -> OperationResult;
}

impl<A, B> Branchable for (A, B)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
{
    type Activation = (Option<A>, Option<B>);

    fn new_activation() -> Self::Activation {
        (None, None)
    }

    fn activate<'a>(
        requester: Entity,
        (a, b): Self::Activation,
        source: Entity,
        world: &'a mut World,
        roster: &'a mut OperationRoster,
    ) -> OperationResult {
        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;
        let target_a = *targets.0.get(0).or_broken()?;
        let target_b = *targets.0.get(1).or_broken()?;

        if let Some(a) = a {
            let mut target_a_mut = world.get_entity_mut(target_a).or_broken()?;
            target_a_mut.give_input(requester, a, roster)?;
        } else {
            roster.dispose_chain_from(target_a);
        }

        if let Some(b) = b {
            let mut target_b_mut = world.get_entity_mut(target_b).or_broken()?;
            target_b_mut.give_input(requester, b, roster)?;
        } else {
            roster.dispose_chain_from(target_b);
        }

        Ok(())
    }
}

fn branch_result<T, E>(
    input: Result<T, E>,
    activation: &mut (Option<T>, Option<E>),
) {
    match input {
        Ok(value) => activation.0 = Some(value),
        Err(err) => activation.1 = Some(err),
    }
}

fn branch_option<T>(
    input: Option<T>,
    activation: &mut (Option<T>, Option<()>),
) {
    match input {
        Some(value) => activation.0 = Some(value),
        None => activation.1 = Some(()),
    }
}
