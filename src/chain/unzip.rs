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

use bevy::prelude::{Entity, Commands};

use smallvec::SmallVec;

use crate::{
    UnusedTarget, ForkTargetStorage, OperationRequest, Input, ManageInput,
    ForkUnzip, AddOperation, OperationResult, OrBroken, Output, Chain, Builder,
};

/// A trait for response types that can be unzipped
pub trait Unzippable: Sized {
    type Unzipped;
    fn unzip_output(output: Output<Self>, builder: &mut Builder) -> Self::Unzipped;

    fn make_targets(commands: &mut Commands) -> SmallVec<[Entity; 8]>;

    fn distribute_values(request: OperationRequest) -> OperationResult;

    type Prepended<T>;
    fn prepend<T>(self, value: T) -> Self::Prepended<T>;
}

impl<A: 'static + Send + Sync> Unzippable for (A,) {
    type Unzipped = Output<A>;
    fn unzip_output(output: Output<Self>, builder: &mut Builder) -> Self::Unzipped {
        assert_eq!(output.scope(), builder.scope());
        let targets = Self::make_targets(builder.commands);

        let result = Output::new(builder.scope, targets[0]);

        builder.commands.add(AddOperation::new(
            Some(output.scope()),
            output.id(),
            ForkUnzip::<Self>::new(ForkTargetStorage(targets)),
        ));
        result
    }

    fn make_targets(commands: &mut Commands) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([commands.spawn(UnusedTarget).id()])
    }

    fn distribute_values(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let Input { session, data: inputs } = world
            .get_entity_mut(source).or_broken()?
            .take_input::<Self>()?;

        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;
        let target = (targets.0)[0];
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.give_input(session, inputs.0, roster)?;
        }
        Ok(())
    }

    type Prepended<T> = (T, A);
    fn prepend<T>(self, value: T) -> Self::Prepended<T> {
        (value, self.0)
    }
}

impl<A: 'static + Send + Sync, B: 'static + Send + Sync> Unzippable for (A, B) {
    type Unzipped = (Output<A>, Output<B>);
    fn unzip_output(output: Output<Self>, builder: &mut Builder) -> Self::Unzipped {
        assert_eq!(output.scope(), builder.scope());
        let targets = Self::make_targets(builder.commands);

        let result = (
            Output::new(builder.scope, targets[0]),
            Output::new(builder.scope, targets[1]),
        );

        builder.commands.add(AddOperation::new(
            Some(output.scope()),
            output.id(),
            ForkUnzip::<Self>::new(ForkTargetStorage(targets)),
        ));
        result
    }

    fn make_targets(commands: &mut Commands) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([
            commands.spawn(UnusedTarget).id(),
            commands.spawn(UnusedTarget).id(),
        ])
    }

    fn distribute_values(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let Input { session, data: inputs } = world
            .get_entity_mut(source).or_broken()?
            .take_input::<Self>()?;

        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;
        let target_0 = *targets.0.get(0).or_broken()?;
        let target_1 = *targets.0.get(1).or_broken()?;

        if let Some(mut t_mut) = world.get_entity_mut(target_0) {
            t_mut.give_input(session, inputs.0, roster)?;
        }

        if let Some(mut t_mut) = world.get_entity_mut(target_1) {
            t_mut.give_input(session, inputs.1, roster)?;
        }

        Ok(())
    }

    type Prepended<T> = (T, A, B);
    fn prepend<T>(self, value: T) -> Self::Prepended<T> {
        (value, self.0, self.1)
    }
}

impl<A, B, C> Unzippable for (A, B, C)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    C: 'static + Send + Sync,
{
    type Unzipped = (Output<A>, Output<B>, Output<C>);
    fn unzip_output(output: Output<Self>, builder: &mut Builder) -> Self::Unzipped {
        assert_eq!(output.scope(), builder.scope());
        let targets = Self::make_targets(builder.commands);

        let result = (
            Output::new(builder.scope, targets[0]),
            Output::new(builder.scope, targets[1]),
            Output::new(builder.scope, targets[2]),
        );

        builder.commands.add(AddOperation::new(
            Some(output.scope()),
            output.id(),
            ForkUnzip::<Self>::new(ForkTargetStorage(targets)),
        ));
        result
    }

    fn make_targets(commands: &mut Commands) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([
            commands.spawn(UnusedTarget).id(),
            commands.spawn(UnusedTarget).id(),
            commands.spawn(UnusedTarget).id(),
        ])
    }

    fn distribute_values(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let Input { session, data: inputs } = world
            .get_entity_mut(source).or_broken()?
            .take_input::<Self>()?;

        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;
        let target_0 = *targets.0.get(0).or_broken()?;
        let target_1 = *targets.0.get(1).or_broken()?;
        let target_2 = *targets.0.get(2).or_broken()?;

        if let Some(mut t_mut) = world.get_entity_mut(target_0) {
            t_mut.give_input(session, inputs.0, roster)?;
        }

        if let Some(mut t_mut) = world.get_entity_mut(target_1) {
            t_mut.give_input(session, inputs.1, roster)?;
        }

        if let Some(mut t_mut) = world.get_entity_mut(target_2) {
            t_mut.give_input(session, inputs.2, roster)?;
        }

        Ok(())
    }

    type Prepended<T> = (T, A, B, C);
    fn prepend<T>(self, value: T) -> Self::Prepended<T> {
        (value, self.0, self.1, self.2)
    }
}

/// A trait for constructs that are able to perform a forking unzip of an
/// unzippable chain. An unzippable chain is one whose response type contains a
/// tuple.
pub trait UnzipBuilder<Z> {
    type ReturnType;
    fn unzip_build(self, output: Output<Z>, builder: &mut Builder) -> Self::ReturnType;
}

impl<A, Fa, Ua, B, Fb, Ub> UnzipBuilder<(A, B)> for (Fa, Fb)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    Fa: FnOnce(Chain<A>) -> Ua,
    Fb: FnOnce(Chain<B>) -> Ub,
{
    type ReturnType = (Ua, Ub);
    fn unzip_build(self, output: Output<(A, B)>, builder: &mut Builder) -> Self::ReturnType {
        let outputs = <(A, B)>::unzip_output(output, builder);
        let u_a = (self.0)(outputs.0.chain(builder));
        let u_b = (self.1)(outputs.1.chain(builder));
        (u_a, u_b)
    }
}

impl<A, Fa, Ua, B, Fb, Ub, C, Fc, Uc> UnzipBuilder<(A, B, C)> for (Fa, Fb, Fc)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    C: 'static + Send + Sync,
    Fa: FnOnce(Chain<A>) -> Ua,
    Fb: FnOnce(Chain<B>) -> Ub,
    Fc: FnOnce(Chain<C>) -> Uc,
{
    type ReturnType = (Ua, Ub, Uc);
    fn unzip_build(self, output: Output<(A, B, C)>, builder: &mut Builder) -> Self::ReturnType {
        let outputs = <(A, B, C)>::unzip_output(output, builder);
        let u_a = (self.0)(outputs.0.chain(builder));
        let u_b = (self.1)(outputs.1.chain(builder));
        let u_c = (self.2)(outputs.2.chain(builder));
        (u_a, u_b, u_c)
    }
}
