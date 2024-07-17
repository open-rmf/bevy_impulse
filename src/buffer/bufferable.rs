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

use bevy::utils::all_tuples;
use smallvec::SmallVec;

use crate::{
    Buffer, CloneFromBuffer, Output, Builder, BufferSettings, Buffered, Join,
    UnusedTarget, AddOperation, Chain,
};

pub trait Bufferable {
    type BufferType: Buffered;

    /// Convert these bufferable workflow elements into buffers if they are not
    /// buffers already.
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType;

    /// Join these bufferable workflow elements.
    fn join<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, <Self::BufferType as Buffered>::Item>
    where
        Self: Sized,
        Self::BufferType: 'static + Send + Sync,
        <Self::BufferType as Buffered>::Item: 'static + Send + Sync,
    {
        let buffers = self.as_buffer(builder);
        let join = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.add(AddOperation::new(
            Some(builder.scope()), join, Join::new(buffers, target)
        ));

        Output::new(builder.scope, target).chain(builder)
    }
}

impl<T: 'static + Send + Sync> Bufferable for Buffer<T> {
    type BufferType = Self;
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope, builder.scope());
        self
    }
}

impl<T: 'static + Send + Sync + Clone> Bufferable for CloneFromBuffer<T> {
    type BufferType = Self;
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope, builder.scope());
        self
    }
}

impl<T: 'static + Send + Sync> Bufferable for Output<T> {
    type BufferType = Buffer<T>;
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope(), builder.scope());
        let buffer = builder.create_buffer::<T>(BufferSettings::default());
        builder.connect(self, buffer.input_slot());
        buffer
    }
}

macro_rules! impl_bufferable_for_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: Bufferable),*> Bufferable for ($($T,)*)
        {
            type BufferType = ($($T::BufferType,)*);
            fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
                let ($($T,)*) = self;
                ($(
                    $T.as_buffer(builder),
                )*)
            }

        }
    }
}

// Implements the `Bufferable` trait for all tuples between size 2 and 12
// (inclusive) made of types that implement `Bufferable`
all_tuples!(impl_bufferable_for_tuple, 2, 12, T);

impl<T: Bufferable, const N: usize> Bufferable for [T; N] {
    type BufferType = [T::BufferType; N];
    fn as_buffer(self, builder: &mut Builder) -> Self::BufferType {
        self.map(|b| b.as_buffer(builder))
    }
}

pub trait IterBufferable {
    type BufferType: Buffered;

    /// Convert an iterable collection of bufferable workflow elements into
    /// buffers if they are not buffers already.
    fn as_buffer_vec<const N: usize>(
        self,
        builder: &mut Builder,
    ) -> SmallVec<[Self::BufferType; N]>;

    /// Join an iterable collection of bufferable workflow elements.
    ///
    /// Performance is best if you can choose an `N` which is equal to the
    /// number of buffers inside the iterable, but this will work even if `N`
    /// does not match the number.
    fn join_vec<const N: usize>(
        self,
        builder: &mut Builder,
    ) -> Output<SmallVec<[<Self::BufferType as Buffered>::Item; N]>>
    where
        Self: Sized,
        Self::BufferType: 'static + Send + Sync,
        <Self::BufferType as Buffered>::Item: 'static + Send + Sync,
    {
        let buffers = self.as_buffer_vec::<N>(builder);
        let join = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.add(AddOperation::new(
            Some(builder.scope()), join, Join::new(buffers, target),
        ));

        Output::new(builder.scope, target)
    }
}
