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

use bevy_ecs::prelude::{Entity, World};
use bevy_utils::all_tuples;

use smallvec::SmallVec;

use crate::{
    Buffer, BufferAccessors, BufferKey, BufferKeyBuilder, BufferStorage, CloneFromBuffer,
    ForkTargetStorage, Gate, GateState, InspectBuffer, ManageBuffer, OperationError,
    OperationResult, OperationRoster, OrBroken, SingleInputStorage, DynBuffer,
};

pub trait Buffered: Clone {
    fn verify_scope(&self, scope: Entity);

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError>;

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult;

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;

    fn as_input(&self) -> SmallVec<[Entity; 8]>;

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult;
}

pub trait Joined: Buffered {
    type Item;
    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError>;
}

pub trait Accessed: Buffered {
    type Key: Clone;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult;
    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key;
    fn deep_clone_key(key: &Self::Key) -> Self::Key;
    fn is_key_in_use(key: &Self::Key) -> bool;
}

impl<T: 'static + Send + Sync> Buffered for Buffer<T> {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope);
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        world
            .get_entity(self.source)
            .or_broken()?
            .buffered_count::<T>(session)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        add_listener_to_source(self.source, listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        GateState::apply(self.source, session, action, world, roster)
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.source])
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        world
            .get_mut::<BufferStorage<T>>(self.source)
            .or_broken()?
            .ensure_session(session);
        Ok(())
    }
}

impl<T: 'static + Send + Sync> Joined for Buffer<T> {
    type Item = T;
    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        world
            .get_entity_mut(self.source)
            .or_broken()?
            .pull_from_buffer::<T>(session)
    }
}

impl<T: 'static + Send + Sync> Accessed for Buffer<T> {
    type Key = BufferKey<T>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        let mut accessors = world.get_mut::<BufferAccessors>(self.source).or_broken()?;

        accessors.0.push(accessor);
        accessors.0.sort();
        accessors.0.dedup();
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        builder.build(self.source)
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        key.deep_clone()
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        key.is_in_use()
    }
}

impl<T: 'static + Send + Sync + Clone> Buffered for CloneFromBuffer<T> {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope);
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        world
            .get_entity(self.source)
            .or_broken()?
            .buffered_count::<T>(session)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        add_listener_to_source(self.source, listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        GateState::apply(self.source, session, action, world, roster)
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.source])
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        world
            .get_entity_mut(self.source)
            .or_broken()?
            .ensure_session::<T>(session)
    }
}

impl<T: 'static + Send + Sync + Clone> Joined for CloneFromBuffer<T> {
    type Item = T;
    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        world
            .get_entity(self.source)
            .or_broken()?
            .try_clone_from_buffer(session)
            .and_then(|r| r.or_broken())
    }
}

impl<T: 'static + Send + Sync + Clone> Accessed for CloneFromBuffer<T> {
    type Key = BufferKey<T>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        let mut accessors = world.get_mut::<BufferAccessors>(self.source).or_broken()?;

        accessors.0.push(accessor);
        accessors.0.sort();
        accessors.0.dedup();
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        builder.build(self.source)
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        key.deep_clone()
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        key.is_in_use()
    }
}

impl Buffered for DynBuffer {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope);
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        world
            .get_entity(self.source)
            .or_broken()?
            .dyn_buffered_count(session)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        add_listener_to_source(self.source, listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        GateState::apply(self.source, session, action, world, roster)
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.source])
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        world
            .get_entity_mut(self.source)
            .or_broken()?
            .dyn_ensure_session(session)
    }
}

macro_rules! impl_buffered_for_tuple {
    ($(($T:ident, $K:ident)),*) => {
        #[allow(non_snake_case)]
        impl<$($T: Buffered),*> Buffered for ($($T,)*)
        {
            fn verify_scope(&self, scope: Entity) {
                let ($($T,)*) = self;
                $(
                    $T.verify_scope(scope);
                )*
            }

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

            fn add_listener(
                &self,
                listener: Entity,
                world: &mut World,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.add_listener(listener, world)?;
                )*
                Ok(())
            }

            fn gate_action(
                &self,
                session: Entity,
                action: Gate,
                world: &mut World,
                roster: &mut OperationRoster,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.gate_action(session, action, world, roster)?;
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

            fn ensure_active_session(
                &self,
                session: Entity,
                world: &mut World,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.ensure_active_session(session, world)?;
                )*
                Ok(())
            }
        }

        #[allow(non_snake_case)]
        impl<$($T: Joined),*> Joined for ($($T,)*)
        {
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
        }

        #[allow(non_snake_case)]
        impl<$($T: Accessed),*> Accessed for ($($T,)*)
        {
            type Key = ($($T::Key), *);
            fn add_accessor(
                &self,
                accessor: Entity,
                world: &mut World,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.add_accessor(accessor, world)?;
                )*
                Ok(())
            }

            fn create_key(
                &self,
                builder: &BufferKeyBuilder,
            ) -> Self::Key {
                let ($($T,)*) = self;
                ($(
                    $T.create_key(builder),
                )*)
            }

            fn deep_clone_key(key: &Self::Key) -> Self::Key {
                let ($($K,)*) = key;
                ($(
                    $T::deep_clone_key($K),
                )*)
            }

            fn is_key_in_use(key: &Self::Key) -> bool {
                let ($($K,)*) = key;
                false $(
                    || $T::is_key_in_use($K)
                )*
            }
        }
    }
}

// Implements the `Buffered` trait for all tuples between size 2 and 12
// (inclusive) made of types that implement `Buffered`
all_tuples!(impl_buffered_for_tuple, 2, 12, T, K);

impl<T: Buffered, const N: usize> Buffered for [T; N] {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self.iter() {
            buffer.verify_scope(scope);
        }
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        let mut min_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count(session, world)?;
            if !min_count.is_some_and(|min| min < count) {
                min_count = Some(count);
            }
        }

        Ok(min_count.unwrap_or(0))
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_listener(listener, world)?;
        }
        Ok(())
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        for buffer in self {
            buffer.gate_action(session, action, world, roster)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
    }
}

impl<T: Joined, const N: usize> Joined for [T; N] {
    // TODO(@mxgrey) We may be able to use [T::Item; N] here instead of SmallVec
    // when try_map is stabilized: https://github.com/rust-lang/rust/issues/79711
    type Item = SmallVec<[T::Item; N]>;
    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        self.iter()
            .map(|buffer| buffer.pull(session, world))
            .collect()
    }
}

impl<T: Accessed, const N: usize> Accessed for [T; N] {
    type Key = SmallVec<[T::Key; N]>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_accessor(accessor, world)?;
        }
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        let mut keys = SmallVec::new();
        for buffer in self {
            keys.push(buffer.create_key(builder));
        }
        keys
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        let mut keys = SmallVec::new();
        for k in key {
            keys.push(T::deep_clone_key(k));
        }
        keys
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        for k in key {
            if T::is_key_in_use(k) {
                return true;
            }
        }

        false
    }
}

impl<T: Buffered, const N: usize> Buffered for SmallVec<[T; N]> {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self.iter() {
            buffer.verify_scope(scope);
        }
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        let mut min_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count(session, world)?;
            if !min_count.is_some_and(|min| min < count) {
                min_count = Some(count);
            }
        }

        Ok(min_count.unwrap_or(0))
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_listener(listener, world)?;
        }
        Ok(())
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        for buffer in self {
            buffer.gate_action(session, action, world, roster)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
    }
}

impl<T: Joined, const N: usize> Joined for SmallVec<[T; N]> {
    type Item = SmallVec<[T::Item; N]>;
    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        self.iter()
            .map(|buffer| buffer.pull(session, world))
            .collect()
    }
}

impl<T: Accessed, const N: usize> Accessed for SmallVec<[T; N]> {
    type Key = SmallVec<[T::Key; N]>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_accessor(accessor, world)?;
        }
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        let mut keys = SmallVec::new();
        for buffer in self {
            keys.push(buffer.create_key(builder));
        }
        keys
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        let mut keys = SmallVec::new();
        for k in key {
            keys.push(T::deep_clone_key(k));
        }
        keys
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        for k in key {
            if T::is_key_in_use(k) {
                return true;
            }
        }

        false
    }
}

fn add_listener_to_source(
    source: Entity,
    listener: Entity,
    world: &mut World,
) -> OperationResult {
    let mut targets = world
        .get_mut::<ForkTargetStorage>(source)
        .or_broken()?;
    if !targets.0.contains(&listener) {
        targets.0.push(listener);
    }

    if let Some(mut input_storage) = world.get_mut::<SingleInputStorage>(listener) {
        input_storage.add(source);
    } else {
        world
            .get_entity_mut(listener)
            .or_broken()?
            .insert(SingleInputStorage::new(source));
    }

    Ok(())
}
