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
    ForkUnzip, PerformOperation, OutputChain,
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
    );
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
            ForkUnzip::<Self>::new(targets),
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
    ) {
        if let Some(mut t_mut) = world.get_entity_mut((targets.0)[0]) {
            t_mut.insert(InputBundle::new(self.0));
            roster.queue((targets.0)[0]);
        }

        if let Some(mut t_mut) = world.get_entity_mut((targets.0)[1]) {
            t_mut.insert(InputBundle::new(self.1));
            roster.queue((targets.0)[1]);
        }
    }
}

/// A trait for constructs that are able to perform a forking unzip of an
/// unzippable chain. An unzippable chain is one whose response type contains a
/// tuple.
pub trait Unzipper<Z> {
    type Output;
    fn fork_unzip(self, source: Entity, commands: &mut Commands) -> Self::Output;
}

impl<A, Fa, Ua, B, Fb, Ub> Unzipper<(A, B)> for (Fa, Fb)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    Fa: FnOnce(OutputChain<A>) -> Ua,
    Fb: FnOnce(OutputChain<B>) -> Ub,
{
    type Output = (Ua, Ub);
    fn fork_unzip(self, source: Entity, commands: &mut Commands) -> Self::Output {
        let dangling = <(A, B)>::unzip_chain(source, commands);
        let u_a = (self.0)(dangling.0.resume(commands));
        let u_b = (self.1)(dangling.1.resume(commands));
        (u_a, u_b)
    }
}
