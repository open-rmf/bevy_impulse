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

use bevy::{
    prelude::{Entity, World},
    utils::all_tuples,
};

use smallvec::SmallVec;

use std::sync::Arc;

use crate::{
    Buffer, CloneFromBuffer, OperationError, OrBroken, InspectBuffer, ChannelSender,
    ManageBuffer, OperationResult, ForkTargetStorage, BufferKey, BufferAccessors,
    BufferStorage, SingleInputStorage,
};

pub trait Buffered: Clone {
    fn verify_scope(&self, scope: Entity);

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

    fn add_listener(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult;

    fn as_input(&self) -> SmallVec<[Entity; 8]>;

    type Key: Clone;
    fn add_accessor(
        &self,
        accessor: Entity,
        world: &mut World,
    ) -> OperationResult;

    fn create_key(
        &self,
        scope: Entity,
        session: Entity,
        accessor: Entity,
        sender: &ChannelSender,
        tracker: &Arc<()>,
    ) -> Result<Self::Key, OperationError>;

    fn ensure_active_session(
        &self,
        session: Entity,
        world: &mut World,
    ) -> OperationResult;

    fn deep_clone_key(key: &Self::Key) -> Self::Key;

    fn is_key_in_use(key: &Self::Key) -> bool;
}

impl<T: 'static + Send + Sync> Buffered for Buffer<T> {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope);
    }

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

    fn add_listener(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        let mut targets = world
            .get_mut::<ForkTargetStorage>(self.source)
            .or_broken()?;
        if !targets.0.contains(&listener) {
            targets.0.push(listener);
        }

        if let Some(mut input_storage) = world.get_mut::<SingleInputStorage>(listener) {
            input_storage.add(self.source);
        } else {
            world.get_entity_mut(listener).or_broken()?
                .insert(SingleInputStorage::new(self.source));
        }

        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.source])
    }

    type Key = BufferKey<T>;
    fn add_accessor(
        &self,
        accessor: Entity,
        world: &mut World,
    ) -> OperationResult {
        let mut accessors = world
            .get_mut::<BufferAccessors>(self.source)
            .or_broken()?;

        accessors.0.push(accessor);
        accessors.0.sort();
        accessors.0.dedup();
        Ok(())
    }

    fn create_key(
        &self,
        scope: Entity,
        session: Entity,
        accessor: Entity,
        sender: &ChannelSender,
        tracker: &Arc<()>,
    ) -> Result<Self::Key, OperationError> {
        Ok(BufferKey::new(scope, self.source, session, accessor, sender.clone(), tracker.clone()))
    }

    fn ensure_active_session(
        &self,
        session: Entity,
        world: &mut World,
    ) -> OperationResult {
        world.get_mut::<BufferStorage<Self::Item>>(self.source).or_broken()?
            .ensure_session(session);
        Ok(())
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
            .try_clone_from_buffer(session)
            .and_then(|r| r.or_broken())
    }

    fn add_listener(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        let mut targets = world
            .get_mut::<ForkTargetStorage>(self.source)
            .or_broken()?;
        if !targets.0.contains(&listener) {
            targets.0.push(listener);
        }

        if let Some(mut input_storage) = world.get_mut::<SingleInputStorage>(listener) {
            input_storage.add(self.source);
        } else {
            world.get_entity_mut(listener).or_broken()?
                .insert(SingleInputStorage::new(self.source));
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.source])
    }

    type Key = BufferKey<T>;
    fn add_accessor(
        &self,
        accessor: Entity,
        world: &mut World,
    ) -> OperationResult {
        let mut accessors = world
            .get_mut::<BufferAccessors>(self.source)
            .or_broken()?;

        accessors.0.push(accessor);
        accessors.0.sort();
        accessors.0.dedup();
        Ok(())
    }

    fn create_key(
        &self,
        scope: Entity,
        session: Entity,
        accessor: Entity,
        sender: &ChannelSender,
        tracker: &Arc<()>,
    ) -> Result<Self::Key, OperationError> {
        Ok(BufferKey::new(scope, self.source, session, accessor, sender.clone(), tracker.clone()))
    }

    fn ensure_active_session(
        &self,
        session: Entity,
        world: &mut World,
    ) -> OperationResult {
        world.get_mut::<BufferStorage<Self::Item>>(self.source).or_broken()?
            .ensure_session(session);
        Ok(())
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        key.deep_clone()
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        key.is_in_use()
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

            fn as_input(&self) -> SmallVec<[Entity; 8]> {
                let mut inputs = SmallVec::new();
                let ($($T,)*) = self;
                $(
                    inputs.extend($T.as_input());
                )*
                inputs
            }

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
                scope: Entity,
                session: Entity,
                accessor: Entity,
                sender: &ChannelSender,
                tracker: &Arc<()>,
            ) -> Result<Self::Key, OperationError> {
                let ($($T,)*) = self;
                Ok(($(
                    $T.create_key(scope, session, accessor, sender, tracker)?,
                )*))
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

    fn add_listener(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in self {
            buffer.add_listener(listener, world)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }

    type Key = SmallVec<[T::Key; N]>;
    fn add_accessor(
        &self,
        accessor: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in self {
            buffer.add_accessor(accessor, world)?;
        }
        Ok(())
    }

    fn create_key(
        &self,
        scope: Entity,
        session: Entity,
        accessor: Entity,
        sender: &ChannelSender,
        tracker: &Arc<()>,
    ) -> Result<Self::Key, OperationError> {
        let mut keys = SmallVec::new();
        for buffer in self {
            keys.push(buffer.create_key(scope, session, accessor, sender, tracker)?);
        }
        Ok(keys)
    }

    fn ensure_active_session(
        &self,
        session: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in self {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
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

        return false;
    }
}

impl<T: Buffered, const N: usize> Buffered for SmallVec<[T; N]> {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self.iter() {
            buffer.verify_scope(scope);
        }
    }

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

    fn add_listener(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in self {
            buffer.add_listener(listener, world)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }

    type Key = SmallVec<[T::Key; N]>;
    fn add_accessor(
        &self,
        accessor: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in self {
            buffer.add_accessor(accessor, world)?;
        }
        Ok(())
    }

    fn create_key(
        &self,
        scope: Entity,
        session: Entity,
        accessor: Entity,
        sender: &ChannelSender,
        tracker: &Arc<()>,
    ) -> Result<Self::Key, OperationError> {
        let mut keys = SmallVec::new();
        for buffer in self {
            keys.push(buffer.create_key(scope, session, accessor, sender, tracker)?);
        }
        Ok(keys)
    }

    fn ensure_active_session(
        &self,
        session: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in self {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
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

        return false;
    }
}
