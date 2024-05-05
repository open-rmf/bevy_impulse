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
    Dangling, UnusedTarget, ForkTargetStorage, OperationRequest, Input, ManageInput,
    ForkUnzip, PerformOperation, OutputChain, FunnelInputStorage, OperationResult,
    SingleTargetStorage, OrBroken, OperationReachability,
    OperationError, InspectInput,
};

/// A trait for response types that can be unzipped
pub trait Unzippable {
    type Unzipped;
    fn unzip_chain(source: Entity, commands: &mut Commands) -> Self::Unzipped;

    fn make_targets(commands: &mut Commands) -> SmallVec<[Entity; 8]>;

    fn distribute_values(request: OperationRequest) -> OperationResult;

    type Prepended<T>;
    fn prepend<T>(self, value: T) -> Self::Prepended<T>;

    fn join_status(
        reachability: OperationReachability,
    ) -> JoinStatusResult;

    fn join_values(
        session: Entity,
        request: OperationRequest,
    ) -> OperationResult;
}

/// What is the current status of a Join node
pub enum JoinStatus {
    /// The join is pending on some more inputs that are able to arrive
    Pending,
    /// The join is ready to be triggered
    Ready,
    /// The join can never be triggered because one or more of its inputs will
    /// never make it.
    Unreachable(Vec<Entity>),
}

pub type JoinStatusResult = Result<JoinStatus, OperationError>;

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
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let Input { session, data: inputs } = world
            .get_entity_mut(source).or_broken()?
            .take_input::<Self>()?;

        let targets = world.get::<ForkTargetStorage>(source).or_broken()?;
        let target = (targets.0)[0];
        if let Some(mut t_mut) = world.get_entity_mut(target) {
            t_mut.give_input(session, inputs.0, roster);
        }
        Ok(())
    }

    type Prepended<T> = (T, A);
    fn prepend<T>(self, value: T) -> Self::Prepended<T> {
        (value, self.0)
    }

    fn join_status(
        mut reachability: OperationReachability,
    ) -> JoinStatusResult {
        let source = reachability.source();
        let session = reachability.session();
        let world = reachability.world();
        let inputs = world.get::<FunnelInputStorage>(source).or_broken()?;
        let mut unreachable: Vec<Entity> = Vec::new();
        let mut status = JoinStatus::Ready;

        let input_0 = *inputs.0.get(0).or_broken()?;

        if !world.get_entity(input_0).or_broken()?.buffer_ready::<A>(session)? {
            status = JoinStatus::Pending;
            if !reachability.check_upstream(input_0)? {
                unreachable.push(input_0);
            }
        }

        if !unreachable.is_empty() {
            return Ok(JoinStatus::Unreachable(unreachable));
        }

        Ok(status)
    }

    fn join_values(
        session: Entity,
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let inputs = world.get::<FunnelInputStorage>(source).or_broken()?;
        let target = world.get::<SingleTargetStorage>(source).or_broken()?.0;

        let input_0 = *inputs.0.get(0).or_broken()?;

        let v_0  = world
            .get_entity_mut(input_0).or_broken()?
            .from_buffer::<A>(session)?;

        world
            .get_entity_mut(target).or_broken()?
            .give_input(session, v_0, roster)?;
        Ok(())
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
            t_mut.give_input(session, inputs.1, roster);
        }

        Ok(())
    }

    type Prepended<T> = (T, A, B);
    fn prepend<T>(self, value: T) -> Self::Prepended<T> {
        (value, self.0, self.1)
    }

    fn join_status(
        mut reachability: OperationReachability,
    ) -> JoinStatusResult {
        let source = reachability.source();
        let session = reachability.session();
        let world = reachability.world();
        let inputs = world.get::<FunnelInputStorage>(source).or_broken()?;
        let mut unreachable: Vec<Entity> = Vec::new();
        let mut status = JoinStatus::Ready;

        let input_0 = *inputs.0.get(0).or_broken()?;
        let input_1 = *inputs.0.get(1).or_broken()?;

        if !world.get_entity(input_0).or_broken()?.buffer_ready::<A>(session)? {
            status = JoinStatus::Pending;
            if !reachability.check_upstream(input_0)? {
                unreachable.push(input_0);
            }
        }

        if !world.get_entity(input_1).or_broken()?.buffer_ready::<B>(session)? {
            status = JoinStatus::Pending;
            if !reachability.check_upstream(input_1)? {
                unreachable.push(input_1);
            }
        }

        if !unreachable.is_empty() {
            return Ok(JoinStatus::Unreachable(unreachable));
        }

        Ok(status)
    }

    fn join_values(
        session: Entity,
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let inputs = world.get::<FunnelInputStorage>(source).or_broken()?;
        let target = world.get::<SingleTargetStorage>(source).or_broken()?.0;

        let input_0 = *inputs.0.get(0).or_broken()?;
        let input_1 = *inputs.0.get(1).or_broken()?;

        let v_0 = world
            .get_entity_mut(input_0).or_broken()?
            .from_buffer::<A>(session)?;

        let v_1 = world
            .get_entity_mut(input_1).or_broken()?
            .from_buffer::<B>(session)?;

        world
            .get_entity_mut(target).or_broken()?
            .give_input(session, (v_0, v_1), roster);
        roster.queue(target);
        Ok(())
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

    fn join_status(
        mut reachability: OperationReachability,
    ) -> JoinStatusResult {
        let source = reachability.source();
        let session = reachability.session();
        let world = reachability.world();
        let inputs = world.get::<FunnelInputStorage>(source).or_broken()?;
        let mut unreachable: Vec<Entity> = Vec::new();
        let mut status = JoinStatus::Ready;

        let input_0 = *inputs.0.get(0).or_broken()?;
        let input_1 = *inputs.0.get(1).or_broken()?;
        let input_2 = *inputs.0.get(2).or_broken()?;

        if !world.get_entity(input_0).or_broken()?.buffer_ready::<A>(session)? {
            status = JoinStatus::Pending;
            if !reachability.check_upstream(input_0)? {
                unreachable.push(input_0);
            }
        }

        if !world.get_entity(input_1).or_broken()?.buffer_ready::<B>(session)? {
            status = JoinStatus::Pending;
            if !reachability.check_upstream(input_1)? {
                unreachable.push(input_1);
            }
        }

        if !world.get_entity(input_2).or_broken()?.buffer_ready::<C>(session)? {
            status = JoinStatus::Pending;
            if !reachability.check_upstream(input_2)? {
                unreachable.push(input_2);
            }
        }

        if !unreachable.is_empty() {
            return Ok(JoinStatus::Unreachable(unreachable));
        }

        Ok(status)
    }

    fn join_values(
        session: Entity,
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let inputs = world.get::<FunnelInputStorage>(source).or_broken()?;
        let target = world.get::<SingleTargetStorage>(source).or_broken()?.0;

        let input_0 = *inputs.0.get(0).or_broken()?;
        let input_1 = *inputs.0.get(1).or_broken()?;
        let input_2 = *inputs.0.get(2).or_broken()?;

        let v_0 = world
            .get_entity_mut(input_0).or_broken()?
            .from_buffer::<A>(session)?;

        let v_1 = world
            .get_entity_mut(input_1).or_broken()?
            .from_buffer::<B>(session)?;

        let v_2 = world
            .get_entity_mut(input_2).or_broken()?
            .from_buffer::<C>(session)?;

        world
            .get_entity_mut(target).or_broken()?
            .give_input(session, (v_0, v_1, v_2), roster);
        roster.queue(target);
        Ok(())
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
