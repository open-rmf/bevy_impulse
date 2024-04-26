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
    Operation, OperationStatus, OperationRoster, ForkTargetStorage, InputStorage,
    SingleSourceStorage, InputBundle, ForkTargetStatus, inspect_fork_targets,
    OperationResult, OrBroken,
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

impl<Input, Outputs, F> Operation for Branching<Input, Outputs, F>
where
    Input: 'static + Send + Sync,
    Outputs: Branchable,
    F: FnOnce(Input, &mut Outputs::Activation) + 'static + Send + Sync,
{
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        for target in &self.targets.0 {
            if let Some(mut target_mut) = world.get_entity_mut(*target) {
                target_mut.insert((
                    SingleSourceStorage(entity),
                    ForkTargetStatus::Active,
                ));
            }
        }
        world.entity_mut(entity).insert((
            self.targets,
            BranchingActivatorStorage(self.activator),
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Some(input) = source_mut.take::<InputStorage<Input>>() else {
            return inspect_fork_targets(source, world, roster);
        };
        let input = input.take();
        let BranchingActivatorStorage::<F>(activator) = source_mut.take().or_broken()?;

        let mut activation = Outputs::new_activation();
        activator(input, &mut activation);

        Outputs::activate(activation, source, world, roster)
    }
}

pub trait Branchable {
    type Activation;

    fn new_activation() -> Self::Activation;

    fn activate<'a>(
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
            target_a_mut.insert(InputBundle::new(a));
            roster.queue(target_a);
        } else {
            roster.dispose_chain_from(target_a);
        }

        if let Some(b) = b {
            let mut target_b_mut = world.get_entity_mut(target_b).or_broken()?;
            target_b_mut.insert(InputBundle::new(b));
            roster.queue(target_b);
        } else {
            roster.dispose_chain_from(target_b);
        }

        Ok(OperationStatus::Finished)
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
