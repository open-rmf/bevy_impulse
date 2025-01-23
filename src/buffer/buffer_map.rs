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

use bevy_ecs::prelude::{Entity, World};

use crate::{DynBuffer, OperationError, OperationResult, OperationRoster, Buffered, Gate};

#[derive(Clone)]
pub struct BufferMap {
    inner: HashMap<Cow<'static, str>, DynBuffer>,
}

pub trait BufferKeyMap: Sized {

    fn buffered_count(
        buffers: &BufferMap,
        session: Entity,
        world: &World
    ) -> Result<usize, OperationError>;

    fn pull(
        buffers: &BufferMap,
        session: Entity,
        world: &mut World,
    ) -> Result<Self, OperationError>;

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
}

struct BufferedMap<K> {
    map: BufferMap,
    _ignore: std::marker::PhantomData<fn(K)>,
}

impl<K: BufferKeyMap> Buffered for BufferedMap<K> {

    type Item = K;

    fn verify_scope(&self, scope: Entity) {
        for buffer in self.map.inner.values() {
            assert_eq!(scope, buffer.scope);
        }
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        K::buffered_count(&self.map, session, world)
    }

    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        K::pull(&self.map, session, world)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {

    }
}
