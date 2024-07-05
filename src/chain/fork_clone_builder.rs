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

use bevy::prelude::Entity;

use crate::{
    Chain, UnusedTarget, AddOperation, ForkClone, ForkTargetStorage, Builder,
};

pub trait ForkCloneBuilder<Response> {
    type Outputs;

    fn build_fork_clone(
        self,
        source: Entity,
        builder: &mut Builder,
    ) -> Self::Outputs;
}

impl<R, F0, U0, F1, U1> ForkCloneBuilder<R> for (F0, F1)
where
    R: 'static + Send + Sync + Clone,
    F0: FnOnce(Chain<R>) -> U0,
    F1: FnOnce(Chain<R>) -> U1,
{
    type Outputs = (U0, U1);

    fn build_fork_clone(
        self,
        source: Entity,
        builder: &mut Builder,
    ) -> Self::Outputs {
        let target_0 = builder.commands.spawn(UnusedTarget).id();
        let target_1 = builder.commands.spawn(UnusedTarget).id();

        builder.commands.add(AddOperation::new(
            source,
            ForkClone::<R>::new(
                ForkTargetStorage::from_iter([target_0, target_1])
            )
        ));

        let u_0 = (self.0)(Chain::new(target_0, builder));
        let u_1 = (self.1)(Chain::new(target_1, builder));
        (u_0, u_1)
    }
}

impl<R, F0, U0, F1, U1, F2, U2> ForkCloneBuilder<R> for (F0, F1, F2)
where
    R: 'static + Send + Sync + Clone,
    F0: FnOnce(Chain<R>) -> U0,
    F1: FnOnce(Chain<R>) -> U1,
    F2: FnOnce(Chain<R>) -> U2,
{
    type Outputs = (U0, U1, U2);

    fn build_fork_clone(
        self,
        source: Entity,
        builder: &mut Builder,
    ) -> Self::Outputs {
        let target_0 = builder.commands.spawn(UnusedTarget).id();
        let target_1 = builder.commands.spawn(UnusedTarget).id();
        let target_2 = builder.commands.spawn(UnusedTarget).id();

        builder.commands.add(AddOperation::new(
            source,
            ForkClone::<R>::new(
                ForkTargetStorage::from_iter([target_0, target_1, target_2])
            )
        ));

        let u_0 = (self.0)(Chain::new(target_0, builder));
        let u_1 = (self.1)(Chain::new(target_1, builder));
        let u_2 = (self.2)(Chain::new(target_2, builder));
        (u_0, u_1, u_2)
    }
}
