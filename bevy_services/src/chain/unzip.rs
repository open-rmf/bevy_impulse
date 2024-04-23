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
    ForkUnzip, PerformOperation, OutputChain, FunnelSourceStorage, OperationStatus,
    InputStorage,
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
    ) -> Result<OperationStatus, ()>;

    type Prepended<T>;
    fn prepend<T>(self, value: T) -> Self::Prepended<T>;

    fn join_values(
        sources: &FunnelSourceStorage,
        target: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()>;

    fn race_values(
        source: Entity,
        winner: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()>;
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
    ) -> Result<OperationStatus, ()> {
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
        sources: &FunnelSourceStorage,
        target: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let source_0 = *sources.0.get(0).ok_or(())?;
        let v_0 = world
            .get_entity_mut(source_0).ok_or(())?
            .take::<InputStorage<A>>().ok_or(())?.take();

        world
            .get_entity_mut(target).ok_or(())?
            .insert(InputBundle::new(v_0));
        roster.queue(target);
        Ok(OperationStatus::Finished)
    }

    fn race_values(
        source: Entity,
        winner: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let inputs = world.get::<FunnelSourceStorage>(source).ok_or(())?;
        let targets = world.get::<ForkTargetStorage>(source).ok_or(())?;

        let target = *targets.0.get(0).ok_or(())?;
        if target == winner {
            let input = world
                .get_entity_mut(*inputs.0.get(0).ok_or(())?).ok_or(())?
                .take::<InputStorage<A>>().ok_or(())?.take();
            world.get_entity_mut(target).ok_or(())?.insert(InputBundle::new(input));
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
    ) -> Result<OperationStatus, ()> {
        let target = *targets.0.get(0).ok_or(())?;
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.insert(InputBundle::new(self.0));
            roster.queue(target);
        }

        let target = *targets.0.get(1).ok_or(())?;
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
        sources: &FunnelSourceStorage,
        target: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let source_0 = *sources.0.get(0).ok_or(())?;
        let v_0 = world
            .get_entity_mut(source_0).ok_or(())?
            .take::<InputStorage<A>>().ok_or(())?.take();

        let source_1 = *sources.0.get(1).ok_or(())?;
        let v_1 = world
            .get_entity_mut(source_1).ok_or(())?
            .take::<InputStorage<B>>().ok_or(())?.take();

        world
            .get_entity_mut(target).ok_or(())?
            .insert(InputBundle::new((v_0, v_1)));
        roster.queue(target);
        Ok(OperationStatus::Finished)
    }

    fn race_values(
        source: Entity,
        winner: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let inputs = world.get::<FunnelSourceStorage>(source).ok_or(())?;
        let targets = world.get::<ForkTargetStorage>(source).ok_or(())?;

        let target = *targets.0.get(0).ok_or(())?;
        if target == winner {
            let input = world
                .get_entity_mut(*inputs.0.get(0).ok_or(())?).ok_or(())?
                .take::<InputStorage<A>>().ok_or(())?.take();
            world.get_entity_mut(target).ok_or(())?.insert(InputBundle::new(input));
            roster.queue(target);
            return Ok(OperationStatus::Finished);
        }

        let target = *targets.0.get(1).ok_or(())?;
        if target == winner {
            let input = world
                .get_entity_mut(*inputs.0.get(1).ok_or(())?).ok_or(())?
                .take::<InputStorage<B>>().ok_or(())?.take();
            world.get_entity_mut(target).ok_or(())?.insert(InputBundle::new(input));
            roster.queue(target);
            return Ok(OperationStatus::Finished);
        }

        Err(())
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
