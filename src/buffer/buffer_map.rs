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
    collections::HashMap,
    borrow::Cow,
};

use smallvec::SmallVec;

use bevy_ecs::prelude::{Entity, World};

use crate::{
    DynBuffer, OperationError, OperationResult, OperationRoster, Buffered, Gate,
    Joined, Accessed,
};

#[derive(Clone)]
pub struct BufferMap {
    inner: HashMap<Cow<'static, str>, DynBuffer>,
}

pub trait BufferMapLayout: Sized {
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
}

pub trait BufferKeyMap: BufferMapLayout {
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

    }
}
