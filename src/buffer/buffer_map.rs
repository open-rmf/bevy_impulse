/*
 * Copyright (C) 2025 Open Source Robotics Foundation
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

use std::{
    any::TypeId,
    borrow::Cow,
    collections::HashMap,
};

use thiserror::Error as ThisError;

use smallvec::SmallVec;

use bevy_ecs::prelude::{Entity, World};

use crate::{
    DynBuffer, OperationError, OperationResult, OperationRoster, Buffered, Gate,
    Joined, Accessed, BufferKeyBuilder, AnyBufferKey,
};

#[derive(Clone)]
pub struct BufferMap {
    inner: HashMap<Cow<'static, str>, DynBuffer>,
}

/// This error is used when the buffers provided for an input are not compatible
/// with the layout.
#[derive(ThisError, Debug, Clone)]
#[error("the incoming buffer map is incompatible with the layout")]
pub struct IncompatibleLayout {
    /// Names of buffers that were missing from the incoming buffer map.
    pub missing_buffers: Vec<Cow<'static, str>>,
    /// Buffers whose expected type did not match the received type.
    pub incompatible_buffers: Vec<BufferIncompatibility>,
}

/// Difference between the expected and received types of a named buffer.
#[derive(Debug, Clone)]
pub struct BufferIncompatibility {
    /// Name of the expected buffer
    pub name: Cow<'static, str>,
    /// The type that was expected for this buffer
    pub expected: TypeId,
    /// The type that was received for this buffer
    pub received: TypeId,
    // TODO(@mxgrey): Replace TypeId with TypeInfo
}

pub trait BufferMapLayout: Sized {
    fn is_compatible(buffers: &BufferMap) -> Result<(), IncompatibleLayout>;

    fn buffered_count(
        buffers: &BufferMap,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError>;

    fn add_listener(
        buffers: &BufferMap,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult;

    fn gate_action(
        buffers: &BufferMap,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;

    fn as_input(buffers: &BufferMap) -> SmallVec<[Entity; 8]>;

    fn ensure_active_session(
        buffers: &BufferMap,
        session: Entity,
        world: &mut World,
    ) -> OperationResult;
}

pub trait JoinedValue: BufferMapLayout {
    fn buffered_count(
        buffers: &BufferMap,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError>;

    fn pull(
        buffers: &BufferMap,
        session: Entity,
        world: &World,
    ) -> Result<Self, OperationError>;
}

/// Trait to describe a layout of buffer keys
pub trait BufferKeyMap: BufferMapLayout + Clone {
    fn add_accessor(
        buffers: &BufferMap,
        accessor: Entity,
        world: &mut World,
    ) -> OperationResult;

    fn create_key(buffers: &BufferMap, builder: &BufferKeyBuilder) -> Self;

    fn deep_clone_key(&self) -> Self;

    fn is_key_in_use(&self) -> bool;
}

struct BufferedMap<K> {
    map: BufferMap,
    _ignore: std::marker::PhantomData<fn(K)>,
}

impl<K> Clone for BufferedMap<K> {
    fn clone(&self) -> Self {
        Self { map: self.map.clone(), _ignore: Default::default() }
    }
}

impl<L: BufferMapLayout> Buffered for BufferedMap<L> {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self.map.inner.values() {
            assert_eq!(scope, buffer.scope);
        }
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        L::buffered_count(&self.map, session, world)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        L::add_listener(&self.map, listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        L::gate_action(&self.map, session, action, world, roster)
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        L::as_input(&self.map)
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        L::ensure_active_session(&self.map, session, world)
    }
}

impl<V: JoinedValue> Joined for BufferedMap<V> {
    type Item = V;

    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        V::pull(&self.map, session, world)
    }
}

impl<K: BufferKeyMap> Accessed for BufferedMap<K> {
    type Key = K;

    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        K::add_accessor(&self.map, accessor, world)
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        K::create_key(&self.map, builder)
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        K::deep_clone_key(key)
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        K::is_key_in_use(key)
    }
}

/// Container to represent any layout of buffer keys.
#[derive(Clone, Debug)]
pub struct AnyBufferKeyMap {
    pub keys: HashMap<Cow<'static, str>, AnyBufferKey>,
}

impl BufferMapLayout for AnyBufferKeyMap {
    fn is_compatible(_: &BufferMap) -> Result<(), IncompatibleLayout> {
        // AnyBufferKeyMap is always compatible with BufferMap
        Ok(())
    }

    fn buffered_count(
        buffers: &BufferMap,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        let mut min_count = None;
        for buffer in buffers.inner.values() {
            let count = buffer.buffered_count(session, world)?;

            min_count = if min_count.is_some_and(|m| m < count) {
                min_count
            } else {
                Some(count)
            };
        }

        Ok(min_count.unwrap_or(0))
    }

    fn add_listener(
        buffers: &BufferMap,
        listener: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in buffers.inner.values() {
            buffer.add_listener(listener, world)?;
        }

        Ok(())
    }

    fn gate_action(
        buffers: &BufferMap,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        for buffer in buffers.inner.values() {
            buffer.gate_action(session, action, world, roster)?;
        }

        Ok(())
    }

    fn as_input(buffers: &BufferMap) -> SmallVec<[Entity; 8]> {
        let mut input = SmallVec::new();
        input.reserve(buffers.inner.len());

        for buffer in buffers.inner.values() {
            input.extend(buffer.as_input());
        }

        input
    }

    fn ensure_active_session(
        buffers: &BufferMap,
        session: Entity,
        world: &mut World,
    ) -> OperationResult {
        for buffer in buffers.inner.values() {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
    }
}
