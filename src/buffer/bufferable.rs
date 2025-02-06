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
use smallvec::SmallVec;

use crate::{
    Accessed, AddOperation, Buffer, BufferSettings, Buffered, Builder, Chain,
    CloneFromBuffer, Join, Joined, Output, UnusedTarget,
};

pub type BufferKeys<B> = <<B as Bufferable>::BufferType as Accessed>::Key;
pub type JoinedItem<B> = <<B as Bufferable>::BufferType as Joined>::Item;

pub trait Bufferable {
    type BufferType: Buffered;

    /// Convert these bufferable workflow elements into buffers if they are not
    /// buffers already.
    fn into_buffer(self, builder: &mut Builder) -> Self::BufferType;
}

impl<T: 'static + Send + Sync> Bufferable for Buffer<T> {
    type BufferType = Self;
    fn into_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope(), builder.scope());
        self
    }
}

impl<T: 'static + Send + Sync + Clone> Bufferable for CloneFromBuffer<T> {
    type BufferType = Self;
    fn into_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope(), builder.scope());
        self
    }
}

impl<T: 'static + Send + Sync> Bufferable for Output<T> {
    type BufferType = Buffer<T>;
    fn into_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope(), builder.scope());
        let buffer = builder.create_buffer::<T>(BufferSettings::default());
        builder.connect(self, buffer.input_slot());
        buffer
    }
}

pub trait Joinable: Bufferable {
    type Item;

    fn join<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Self::Item>;
}

impl<B> Joinable for B
where
    B: Bufferable,
    B::BufferType: Joined,
{
    type Item = JoinedItem<B>;

    fn join<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Self::Item> {
        self.into_buffer(builder).join(builder)
    }
}

pub trait Accessible: Bufferable {
    type Keys;

    fn listen<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Self::Keys>;
}

impl<B> Accessible for B
where
    B: Bufferable,
    B::BufferType: Accessed,
{
    type Keys = BufferKeys<Self>;

    fn listen<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Self::Keys> {
        self.into_buffer(builder).listen(builder)
    }
}

macro_rules! impl_bufferable_for_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: Bufferable),*> Bufferable for ($($T,)*)
        {
            type BufferType = ($($T::BufferType,)*);
            fn into_buffer(self, builder: &mut Builder) -> Self::BufferType {
                let ($($T,)*) = self;
                ($(
                    $T.into_buffer(builder),
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
    fn into_buffer(self, builder: &mut Builder) -> Self::BufferType {
        self.map(|b| b.into_buffer(builder))
    }
}

pub trait IterBufferable {
    type BufferElement: Buffered + Joined;

    /// Convert an iterable collection of bufferable workflow elements into
    /// buffers if they are not buffers already.
    fn into_buffer_vec<const N: usize>(
        self,
        builder: &mut Builder,
    ) -> SmallVec<[Self::BufferElement; N]>;

    /// Join an iterable collection of bufferable workflow elements.
    ///
    /// Performance is best if you can choose an `N` which is equal to the
    /// number of buffers inside the iterable, but this will work even if `N`
    /// does not match the number.
    fn join_vec<'w, 's, 'a, 'b, const N: usize>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, SmallVec<[<Self::BufferElement as Joined>::Item; N]>>
    where
        Self: Sized,
        Self::BufferElement: 'static + Send + Sync,
        <Self::BufferElement as Joined>::Item: 'static + Send + Sync,
    {
        let buffers = self.into_buffer_vec::<N>(builder);
        let join = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.add(AddOperation::new(
            Some(builder.scope()),
            join,
            Join::new(buffers, target),
        ));

        Output::new(builder.scope, target).chain(builder)
    }
}

impl<T> IterBufferable for T
where
    T: IntoIterator,
    T::Item: Bufferable,
    <T::Item as Bufferable>::BufferType: Joined,
{
    type BufferElement = <T::Item as Bufferable>::BufferType;

    fn into_buffer_vec<const N: usize>(
        self,
        builder: &mut Builder,
    ) -> SmallVec<[Self::BufferElement; N]> {
        SmallVec::<[Self::BufferElement; N]>::from_iter(
            self.into_iter().map(|e| e.into_buffer(builder)),
        )
    }
}
