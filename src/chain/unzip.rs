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

use bevy_utils::all_tuples;

use itertools::Itertools;
use smallvec::SmallVec;

use crate::{
    AddOperation, Builder, Chain, ForkTargetStorage, ForkUnzip, Input, ManageInput,
    OperationRequest, OperationResult, OrBroken, Output, UnusedTarget,
};

/// A trait for response types that can be unzipped
pub trait Unzippable: Sized {
    type Unzipped;
    fn unzip_output(output: Output<Self>, builder: &mut Builder) -> Self::Unzipped;

    fn distribute_values(request: OperationRequest) -> OperationResult;

    type Prepended<T>;
    fn prepend<T>(self, value: T) -> Self::Prepended<T>;
}

macro_rules! impl_unzippable_for_tuple {
    ($(($T:ident, $D:ident)),*) => {
        #[allow(non_snake_case)]
        impl<$($T: 'static + Send + Sync),*> Unzippable for ($($T,)*)
        {
            type Unzipped = ($(Output<$T>,)*);
            fn unzip_output(output: Output<Self>, builder: &mut Builder) -> Self::Unzipped {
                assert_eq!(output.scope(), builder.scope());
                let mut targets = SmallVec::new();
                let result =
                    (
                        $(
                            {
                                // Variable is only used to make sure this cycle is repeated once
                                // for each instance of the $T type, but the type itself is not
                                // used.
                                #[allow(unused)]
                                let $T = std::marker::PhantomData::<$T>;
                                let target = builder.commands.spawn(UnusedTarget).id();
                                targets.push(target);
                                Output::new(builder.scope(), target)
                            },
                        )*
                    );

                builder.commands.add(AddOperation::new(
                    Some(output.scope()),
                    output.id(),
                    ForkUnzip::<Self>::new(ForkTargetStorage(targets)),
                ));
                result
            }

            fn distribute_values(
                OperationRequest { source, world, roster }: OperationRequest,
            ) -> OperationResult {
                let Input { session, data: inputs } = world
                    .get_entity_mut(source).or_broken()?
                    .take_input::<Self>()?;

                let ($($D,)*) = world.get::<ForkTargetStorage>(source).or_broken()?.0.iter().copied().next_tuple().or_broken()?;
                let ($($T,)*) = inputs;
                $(
                    if let Some(mut t_mut) = world.get_entity_mut($D) {
                        t_mut.give_input(session, $T, roster)?;
                    }
                )*
                Ok(())
            }

            type Prepended<T> = (T, $($T,)*);
            fn prepend<T>(self, value: T) -> Self::Prepended<T> {
                let ($($T,)*) = self;
                (value, $($T,)*)
            }
        }
    }
}

// Implements the `Unzippable` trait for all tuples between size 1 and 12
// (inclusive) made of 'static lifetime types that are `Send` and `Sync`
// D is a dummy type
all_tuples!(impl_unzippable_for_tuple, 1, 12, T, D);

/// A trait for constructs that are able to perform a forking unzip of an
/// unzippable chain. An unzippable chain is one whose response type contains a
/// tuple.
pub trait UnzipBuilder<Z> {
    type ReturnType;
    fn fork_unzip(self, output: Output<Z>, builder: &mut Builder) -> Self::ReturnType;
}

macro_rules! impl_unzipbuilder_for_tuple {
    ($(($A:ident, $F:ident, $U:ident)),*) => {
        #[allow(non_snake_case)]
        impl<$($A: 'static + Send + Sync),*, $($F: FnOnce(Chain<$A>) -> $U),*, $($U),*> UnzipBuilder<($($A,)*)> for ($($F,)*)
        {
            type ReturnType = ($($U),*);
            fn fork_unzip(self, output: Output<($($A,)*)>, builder: &mut Builder) -> Self::ReturnType {
                let outputs = <($($A),*)>::unzip_output(output, builder);
                let ($($A,)*) = outputs;
                let ($($F,)*) = self;
                (
                    $(
                        ($F)($A.chain(builder)),
                    )*
                )
            }
        }
    }
}

// Implements the `UnzipBuilder` trait for all tuples between size 1 and 12
all_tuples!(impl_unzipbuilder_for_tuple, 2, 12, A, F, U);
