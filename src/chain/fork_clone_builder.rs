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
use bevy::utils::all_tuples;
use smallvec::SmallVec;

use crate::{
    Chain, UnusedTarget, AddOperation, ForkClone, ForkTargetStorage, Builder,
    Output,
};

pub trait ForkCloneBuilder<Response> {
    type Outputs;

    fn build_fork_clone(
        self,
        source: Output<Response>,
        builder: &mut Builder,
    ) -> Self::Outputs;
}

macro_rules! impl_forkclonebuilder_for_tuple {
    ($(($F:ident, $U:ident)),*) => {
        #[allow(non_snake_case)]
        impl<R: 'static + Send + Sync + Clone, $($F: FnOnce(Chain<R>) -> $U),*, $($U),*> ForkCloneBuilder<R> for ($($F,)*)
        {
            type Outputs = ($($U,)*);
            fn build_fork_clone(
                self,
                source: Output<R>,
                builder: &mut Builder,
            ) -> Self::Outputs {
                let mut targets = SmallVec::<[Entity; 8]>::new();
                let ($($F,)*) = self;
                let u =
                (
                    $(
                        {
                            let target = builder.commands.spawn(UnusedTarget).id();
                            targets.push(target);
                            ($F)(Chain::new(target, builder))
                        },
                    )*
                );

                builder.commands.add(AddOperation::new(
                    Some(source.scope()),
                    source.id(),
                    ForkClone::<R>::new(
                        ForkTargetStorage::from_iter(targets)
                    )
                ));
                u
            }
        }
    }
}

// Implements the `ForkCloneBUilder` trait for all tuples between size 2 and 15
// (inclusive)
all_tuples!(impl_forkclonebuilder_for_tuple, 2, 15, F, U);
