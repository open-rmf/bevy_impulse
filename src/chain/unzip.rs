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

use bevy::prelude::{Entity, Commands, World};

use smallvec::SmallVec;

use crate::{
    Dangling, UnusedTarget, ForkTargetStorage, OperationRoster, InputBundle,
    ForkUnzip, PerformOperation, OutputChain, FunnelSourceStorage, OperationResult,
    InputStorage, SingleTargetStorage, OperationStatus, OrBroken, OperationError,
};

/// A trait for response types that can be unzipped
pub trait Unzippable {
    type Unzipped;
    fn unzip_chain(source: Entity, commands: &mut Commands) -> Self::Unzipped;

    fn make_targets(commands: &mut Commands) -> SmallVec<[Entity; 8]>;

    fn distribute_values(
        self,
        targets: &ForkTargetStorage,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;

    type Prepended<T>;
    fn prepend<T>(self, value: T) -> Self::Prepended<T>;

    fn join_values(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;

    fn race_values(
        source: Entity,
        winner: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;
}

impl<A: 'static + Send + Sync> Unzippable for (A,) {
    type Unzipped = Dangling<A>;
    fn unzip_chain(source: Entity, commands: &mut Commands) -> Self::Unzipped {
        let targets = Self::make_targets(commands);

        let result = Dangling::new(source, targets[0]);

        commands.add(PerformOperation::new(
            source,
            ForkUnzip::<Self>::new(ForkTargetStorage(targets)),
        ));
        result
    }

    fn make_targets(commands: &mut Commands) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([commands.spawn(UnusedTarget).id()])
    }

    fn distribute_values(
        self,
        targets: &ForkTargetStorage,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let target = (targets.0)[0];
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.insert(InputBundle::new(self.0));
            roster.queue(target);
        }
        Ok(OperationStatus::Finished)
    }

    type Prepended<T> = (T, A);
    fn prepend<T>(self, value: T) -> Self::Prepended<T> {
        (value, self.0)
    }

    fn join_values(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;
        let target = world.get::<SingleTargetStorage>(source).or_broken()?.0;

        let input_0 = *inputs.0.get(0).or_broken()?;

        let v_0 = world
            .get_entity_mut(input_0).or_broken()?
            .take::<InputStorage<A>>().or_broken()?.take();

        world
            .get_entity_mut(target).or_broken()?
            .insert(InputBundle::new(v_0));
        roster.queue(target);
        Ok(OperationStatus::Finished)
    }

    fn race_values(
        source: Entity,
        winner: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;
        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;

        let target = *targets.0.get(0).or_broken()?;
        if target == winner {
            let input = world
                .get_entity_mut(*inputs.0.get(0).or_broken()?).or_broken()?
                .take::<InputStorage<A>>().or_broken()?.take();
            world.get_entity_mut(target).or_broken()?.insert(InputBundle::new(input));
            roster.queue(target);
        }

        Ok(OperationStatus::Finished)
    }
}

impl<A: 'static + Send + Sync, B: 'static + Send + Sync> Unzippable for (A, B) {
    type Unzipped = (Dangling<A>, Dangling<B>);
    fn unzip_chain(source: Entity, commands: &mut Commands) -> Self::Unzipped {
        let targets = Self::make_targets(commands);

        let result = (
            Dangling::new(source, targets[0]),
            Dangling::new(source, targets[1]),
        );

        commands.add(PerformOperation::new(
            source,
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
        self,
        targets: &ForkTargetStorage,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let target = *targets.0.get(0).or_broken()?;
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.insert(InputBundle::new(self.0));
            roster.queue(target);
        }

        let target = *targets.0.get(1).or_broken()?;
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.insert(InputBundle::new(self.1));
            roster.queue(target);
        }

        Ok(OperationStatus::Finished)
    }

    type Prepended<T> = (T, A, B);
    fn prepend<T>(self, value: T) -> Self::Prepended<T> {
        (value, self.0, self.1)
    }

    fn join_values(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;
        let target = world.get::<SingleTargetStorage>(source).or_broken()?.0;

        let input_0 = *inputs.0.get(0).or_broken()?;
        let input_1 = *inputs.0.get(1).or_broken()?;

        let v_0 = world
            .get_entity_mut(input_0).or_broken()?
            .take::<InputStorage<A>>().or_broken()?.take();

        let v_1 = world
            .get_entity_mut(input_1).or_broken()?
            .take::<InputStorage<B>>().or_broken()?.take();

        world
            .get_entity_mut(target).or_broken()?
            .insert(InputBundle::new((v_0, v_1)));
        roster.queue(target);
        Ok(OperationStatus::Finished)
    }

    fn race_values(
        source: Entity,
        winner: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;
        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;

        if *inputs.0.get(0).or_broken()? == winner {
            let target = *targets.0.get(0).or_broken()?;
            let input = world
                .get_entity_mut(winner).or_broken()?
                .take::<InputStorage<A>>().or_broken()?.take();
            world.get_entity_mut(target).or_broken()?.insert(InputBundle::new(input));
            roster.queue(target);
            return Ok(OperationStatus::Finished);
        }

        if *inputs.0.get(1).or_broken()? == winner {
            let target = *targets.0.get(1).or_broken()?;
            let input = world
                .get_entity_mut(winner).or_broken()?
                .take::<InputStorage<A>>().or_broken()?.take();
            world.get_entity_mut(target).or_broken()?.insert(InputBundle::new(input));
            roster.queue(target);
            return Ok(OperationStatus::Finished);
        }

        Err(OperationError::here())
    }
}

impl<A, B, C> Unzippable for (A, B, C)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    C: 'static + Send + Sync,
{
    type Unzipped = (Dangling<A>, Dangling<B>, Dangling<C>);
    fn unzip_chain(source: Entity, commands: &mut Commands) -> Self::Unzipped {
        let targets = Self::make_targets(commands);

        let result = (
            Dangling::new(source, targets[0]),
            Dangling::new(source, targets[1]),
            Dangling::new(source, targets[2]),
        );

        commands.add(PerformOperation::new(
            source,
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
        self,
        targets: &ForkTargetStorage,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let target = *targets.0.get(0).or_broken()?;
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.insert(InputBundle::new(self.0));
            roster.queue(target);
        }

        let target = *targets.0.get(1).or_broken()?;
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.insert(InputBundle::new(self.1));
            roster.queue(target);
        }

        let target = *targets.0.get(2).or_broken()?;
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.insert(InputBundle::new(self.2));
            roster.queue(target);
        }

        Ok(OperationStatus::Finished)
    }

    type Prepended<T> = (T, A, B, C);
    fn prepend<T>(self, value: T) -> Self::Prepended<T> {
        (value, self.0, self.1, self.2)
    }

    fn join_values(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;
        let target = world.get::<SingleTargetStorage>(source).or_broken()?.0;

        let input_0 = *inputs.0.get(0).or_broken()?;
        let input_1 = *inputs.0.get(1).or_broken()?;
        let input_2 = *inputs.0.get(2).or_broken()?;

        let v_0 = world
            .get_entity_mut(input_0).or_broken()?
            .take::<InputStorage<A>>().or_broken()?.take();

        let v_1 = world
            .get_entity_mut(input_1).or_broken()?
            .take::<InputStorage<B>>().or_broken()?.take();

        let v_2 = world
            .get_entity_mut(input_2).or_broken()?
            .take::<InputStorage<C>>().or_broken()?.take();

        world
            .get_entity_mut(target).or_broken()?
            .insert(InputBundle::new((v_0, v_1, v_2)));
        roster.queue(target);
        Ok(OperationStatus::Finished)
    }

    fn race_values(
        source: Entity,
        winner: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let inputs = world.get::<FunnelSourceStorage>(source).or_broken()?;
        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;

        if *inputs.0.get(0).or_broken()? == winner {
            let target = *targets.0.get(0).or_broken()?;
            let input = world
                .get_entity_mut(winner).or_broken()?
                .take::<InputStorage<A>>().or_broken()?.take();
            world.get_entity_mut(target).or_broken()?.insert(InputBundle::new(input));
            roster.queue(target);
            return Ok(OperationStatus::Finished);
        }

        if *inputs.0.get(1).or_broken()? == winner {
            let target = *targets.0.get(1).or_broken()?;
            let input = world
                .get_entity_mut(winner).or_broken()?
                .take::<InputStorage<A>>().or_broken()?.take();
            world.get_entity_mut(target).or_broken()?.insert(InputBundle::new(input));
            roster.queue(target);
            return Ok(OperationStatus::Finished);
        }

        if *inputs.0.get(2).or_broken()? == winner {
            let target = *targets.0.get(2).or_broken()?;
            let input = world
                .get_entity_mut(winner).or_broken()?
                .take::<InputStorage<A>>().or_broken()?.take();
            world.get_entity_mut(target).or_broken()?.insert(InputBundle::new(input));
            roster.queue(target);
            return Ok(OperationStatus::Finished);
        }

        Err(OperationError::here())
    }
}

/// A trait for constructs that are able to perform a forking unzip of an
/// unzippable chain. An unzippable chain is one whose response type contains a
/// tuple.
pub trait UnzipBuilder<Z> {
    type Output;
    fn unzip_build(self, source: Entity, commands: &mut Commands) -> Self::Output;
}

impl<A, Fa, Ua, B, Fb, Ub> UnzipBuilder<(A, B)> for (Fa, Fb)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    Fa: FnOnce(OutputChain<A>) -> Ua,
    Fb: FnOnce(OutputChain<B>) -> Ub,
{
    type Output = (Ua, Ub);
    fn unzip_build(self, source: Entity, commands: &mut Commands) -> Self::Output {
        let dangling = <(A, B)>::unzip_chain(source, commands);
        let u_a = (self.0)(dangling.0.resume(commands));
        let u_b = (self.1)(dangling.1.resume(commands));
        (u_a, u_b)
    }
}

impl<A, Fa, Ua, B, Fb, Ub, C, Fc, Uc> UnzipBuilder<(A, B, C)> for (Fa, Fb, Fc)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    C: 'static + Send + Sync,
    Fa: FnOnce(OutputChain<A>) -> Ua,
    Fb: FnOnce(OutputChain<B>) -> Ub,
    Fc: FnOnce(OutputChain<C>) -> Uc,
{
    type Output = (Ua, Ub, Uc);
    fn unzip_build(self, source: Entity, commands: &mut Commands) -> Self::Output {
        let dangling = <(A, B, C)>::unzip_chain(source, commands);
        let u_a = (self.0)(dangling.0.resume(commands));
        let u_b = (self.1)(dangling.1.resume(commands));
        let u_c = (self.2)(dangling.2.resume(commands));
        (u_a, u_b, u_c)
    }
}
