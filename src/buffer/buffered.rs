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

use smallvec::SmallVec;

use crate::{
    Buffer, CloneFromBuffer, OperationError, OrBroken, InspectBuffer, ChannelSender,
    ManageBuffer, OperationResult, ForkTargetStorage, BufferKey, BufferAccessors,
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
        sender: &ChannelSender,
    ) -> Result<Self::Key, OperationError>;

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
        targets.0.push(listener);
        targets.0.sort();
        targets.0.dedup();
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
        sender: &ChannelSender,
    ) -> Result<Self::Key, OperationError> {
        Ok(BufferKey::new(scope, self.source, session, sender.clone()))
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
        targets.0.push(listener);
        targets.0.sort();
        targets.0.dedup();
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
        sender: &ChannelSender,
    ) -> Result<Self::Key, OperationError> {
        Ok(BufferKey::new(scope, self.source, session, sender.clone()))
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        key.is_in_use()
    }
}

impl<T0, T1> Buffered for (T0, T1)
where
    T0: Buffered,
    T1: Buffered,
{
    fn verify_scope(&self, scope: Entity) {
        self.0.verify_scope(scope);
        self.1.verify_scope(scope);
    }

    fn buffered_count(
        &self,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        Ok([
            self.0.buffered_count(session, world)?,
            self.1.buffered_count(session, world)?,
        ].iter().copied().min().unwrap_or(0))
    }

    type Item = (T0::Item, T1::Item);
    fn pull(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        let t0 = self.0.pull(session, world)?;
        let t1 = self.1.pull(session, world)?;
        Ok((t0, t1))
    }

    fn add_listener(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        self.0.add_listener(listener, world)?;
        self.1.add_listener(listener, world)?;
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        let mut inputs = SmallVec::new();
        inputs.extend(self.0.as_input());
        inputs.extend(self.1.as_input());
        inputs
    }

    type Key = (T0::Key, T1::Key);
    fn add_accessor(
        &self,
        accessor: Entity,
        world: &mut World,
    ) -> OperationResult {
        self.0.add_accessor(accessor, world)?;
        self.1.add_accessor(accessor, world)?;
        Ok(())
    }

    fn create_key(
        &self,
        scope: Entity,
        session: Entity,
        sender: &ChannelSender,
    ) -> Result<Self::Key, OperationError> {
        let t0 = self.0.create_key(scope, session, sender)?;
        let t1 = self.1.create_key(scope, session, sender)?;
        Ok((t0, t1))
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        T0::is_key_in_use(&key.0)
        || T1::is_key_in_use(&key.1)
    }
}

impl<T0, T1, T2> Buffered for (T0, T1, T2)
where
    T0: Buffered,
    T1: Buffered,
    T2: Buffered,
{
    fn verify_scope(&self, scope: Entity) {
        self.0.verify_scope(scope);
        self.1.verify_scope(scope);
        self.2.verify_scope(scope);
    }

    fn buffered_count(
        &self,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        Ok([
            self.0.buffered_count(session, world)?,
            self.1.buffered_count(session, world)?,
            self.2.buffered_count(session, world)?,
        ].iter().copied().min().unwrap_or(0))
    }

    type Item = (T0::Item, T1::Item, T2::Item);
    fn pull(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        let t0 = self.0.pull(session, world)?;
        let t1 = self.1.pull(session, world)?;
        let t2 = self.2.pull(session, world)?;
        Ok((t0, t1, t2))
    }

    fn add_listener(
        &self,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        self.0.add_listener(listener, world)?;
        self.1.add_listener(listener, world)?;
        self.2.add_listener(listener, world)?;
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        let mut inputs = SmallVec::new();
        inputs.extend(self.0.as_input());
        inputs.extend(self.1.as_input());
        inputs.extend(self.2.as_input());
        inputs
    }

    type Key = (T0::Key, T1::Key, T2::Key);
    fn add_accessor(
        &self,
        accessor: Entity,
        world: &mut World,
    ) -> OperationResult {
        self.0.add_accessor(accessor, world)?;
        self.1.add_accessor(accessor, world)?;
        self.2.add_accessor(accessor, world)?;
        Ok(())
    }

    fn create_key(
        &self,
        scope: Entity,
        session: Entity,
        sender: &ChannelSender,
    ) -> Result<Self::Key, OperationError> {
        let t0 = self.0.create_key(scope, session, sender)?;
        let t1 = self.1.create_key(scope, session, sender)?;
        let t2 = self.2.create_key(scope, session, sender)?;
        Ok((t0, t1, t2))
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        T0::is_key_in_use(&key.0)
        || T1::is_key_in_use(&key.1)
        || T2::is_key_in_use(&key.2)
    }
}

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
        sender: &ChannelSender,
    ) -> Result<Self::Key, OperationError> {
        let mut keys = SmallVec::new();
        for buffer in self {
            keys.push(buffer.create_key(scope, session, sender)?);
        }
        Ok(keys)
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
        sender: &ChannelSender,
    ) -> Result<Self::Key, OperationError> {
        let mut keys = SmallVec::new();
        for buffer in self {
            keys.push(buffer.create_key(scope, session, sender)?);
        }
        Ok(keys)
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
