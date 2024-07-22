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

use bevy::prelude::{Entity, World};
use bevy::utils::all_tuples;

use smallvec::SmallVec;

use crate::{
    Buffer, CloneFromBuffer, OperationError, OrBroken, InspectInput, ManageInput,
    OperationResult, ForkTargetStorage,
};

pub trait Buffered: Clone {
    fn buffered_count(
        &self,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError>;

    type Item;
    fn pull(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError>;

    fn listen(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult;

    fn as_input(&self) -> SmallVec<[Entity; 8]>;
}

impl<T: 'static + Send + Sync> Buffered for Buffer<T> {
    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        world.get_entity(self.source).or_broken()?
            .buffered_count::<T>(session)
    }

    type Item = T;
    fn pull(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        world.get_entity_mut(self.source).or_broken()?
            .pull_from_buffer::<T>(session)
    }

    fn listen(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        let mut targets = world
            .get_mut::<ForkTargetStorage>(self.source)
            .or_broken()?;
        targets.0.push(listener);
        targets.0.sort();
        targets.0.dedup();
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.source])
    }
}

impl<T: 'static + Send + Sync + Clone> Buffered for CloneFromBuffer<T> {
    fn buffered_count(
        &self,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        world.get_entity(self.source).or_broken()?
            .buffered_count::<T>(session)
    }

    type Item = T;
    fn pull(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        world.get_entity(self.source).or_broken()?
            .clone_from_buffer(session)
    }

    fn listen(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        let mut targets = world
            .get_mut::<ForkTargetStorage>(self.source)
            .or_broken()?;
        targets.0.push(listener);
        targets.0.sort();
        targets.0.dedup();
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.source])
    }
}

macro_rules! impl_buffered_for_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: Buffered),*> Buffered for ($($T,)*)
        {
            fn buffered_count(
                &self,
                session: Entity,
                world: &World,
            ) -> Result<usize, OperationError> {
                let ($($T,)*) = self;
                Ok([
                    $(
                        $T.buffered_count(session, world)?,
                    )*
                ].iter().copied().min().unwrap_or(0))
            }

            type Item = ($($T::Item),*);
            fn pull(
                &self,
                session: Entity,
                world: &mut World,
            ) -> Result<Self::Item, OperationError> {
                let ($($T,)*) = self;
                Ok(($(
                    $T.pull(session, world)?,
                )*))
            }

            fn listen(
                &self,
                listener: Entity,
                world: &mut World,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.listen(listener, world)?;
                )*
                Ok(())
            }

            fn as_input(&self) -> SmallVec<[Entity; 8]> {
                let mut inputs = SmallVec::new();
                let ($($T,)*) = self;
                $(
                    inputs.extend($T.as_input());
                )*
                inputs
            }
        }
    }
}

// Implements the `Buffered` trait for all tuples between size 2 and 12
// (inclusive) made of types that implement `Buffered`
all_tuples!(impl_buffered_for_tuple, 2, 12, T);

impl<T: Buffered, const N: usize> Buffered for [T; N] {
    fn buffered_count(
        &self,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        let mut min_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count(session, world)?;
            if !min_count.is_some_and(|min| min < count) {
                min_count = Some(count);
            }
        }

        Ok(min_count.unwrap_or(0))
    }

    // TODO(@mxgrey) We may be able to use [T::Item; N] here instead of SmallVec
    // when try_map is stabilized: https://github.com/rust-lang/rust/issues/79711
    type Item = SmallVec<[T::Item; N]>;
    fn pull(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        self.iter().map(|buffer| {
            buffer.pull(session, world)
        }).collect()
    }

    fn listen(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in self {
            buffer.listen(listener, world)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }
}

impl<T: Buffered, const N: usize> Buffered for SmallVec<[T; N]> {
    fn buffered_count(
        &self,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        let mut min_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count(session, world)?;
            if !min_count.is_some_and(|min| min < count) {
                min_count = Some(count);
            }
        }

        Ok(min_count.unwrap_or(0))
    }

    type Item = SmallVec<[T::Item; N]>;
    fn pull(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        self.iter().map(|buffer| {
            buffer.pull(session, world)
        }).collect()
    }

    fn listen(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in self {
            buffer.listen(listener, world)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }
}
