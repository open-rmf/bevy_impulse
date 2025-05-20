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

use variadics_please::all_tuples;

use crate::{AddOperation, Builder, Chain, ForkClone, ForkTargetStorage, Output, UnusedTarget};

pub trait ForkCloneBuilder<Response> {
    type Outputs;

    fn build_fork_clone(self, source: Output<Response>, builder: &mut Builder) -> Self::Outputs;
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
                let targets =
                [
                    $(
                        {
                            // Variable is only used to make sure this cycle is repeated once
                            // for each instance of the $T type, but the type itself is not
                            // used.
                            #[allow(unused)]
                            let $F = std::marker::PhantomData::<$F>;
                            builder.commands.spawn(UnusedTarget).id()
                        },
                    )*
                ];

                builder.commands.queue(AddOperation::new(
                    Some(source.scope()),
                    source.id(),
                    ForkClone::<R>::new(
                        ForkTargetStorage::from_iter(targets)
                    )
                ));
                let ($($F,)*) = self;
                // The compiler throws a warning when implementing this for
                // tuple sizes that wouldn't use the result of the first _idx = _idx + 1
                // so we add a leading underscore to suppress the warning
                let mut _idx = 0;
                (
                    $(
                        {
                            let res = ($F)(Chain::new(targets[_idx], builder));
                            _idx += 1;
                            res
                        },
                    )*
                )
            }
        }
    }
}

// Implements the `ForkCloneBUilder` trait for all tuples between size 2 and 12
// (inclusive)
all_tuples!(impl_forkclonebuilder_for_tuple, 2, 12, F, U);
