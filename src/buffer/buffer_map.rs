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

use std::{any::TypeId, borrow::Cow, collections::HashMap};

use thiserror::Error as ThisError;

use smallvec::SmallVec;

use bevy_ecs::prelude::{Entity, World};

use crate::{
    add_listener_to_source, Accessed, AddOperation, AnyBuffer, AnyBufferKey, BufferKeyBuilder,
    Buffered, Builder, Chain, Gate, GateState, Join, Joined, OperationError, OperationResult,
    OperationRoster, Output, UnusedTarget,
};

#[derive(Clone, Default)]
pub struct BufferMap {
    inner: HashMap<Cow<'static, str>, AnyBuffer>,
}

impl BufferMap {
    /// Insert a named buffer into the map.
    pub fn insert(&mut self, name: Cow<'static, str>, buffer: impl Into<AnyBuffer>) {
        self.inner.insert(name, buffer.into());
    }

    /// Get one of the buffers from the map by its name.
    pub fn get(&self, name: &str) -> Option<&AnyBuffer> {
        self.inner.get(name)
    }
}

/// This error is used when the buffers provided for an input are not compatible
/// with the layout.
#[derive(ThisError, Debug, Clone, Default)]
#[error("the incoming buffer map is incompatible with the layout")]
pub struct IncompatibleLayout {
    /// Names of buffers that were missing from the incoming buffer map.
    pub missing_buffers: Vec<Cow<'static, str>>,
    /// Buffers whose expected type did not match the received type.
    pub incompatible_buffers: Vec<BufferIncompatibility>,
}

impl IncompatibleLayout {
    /// Check whether a named buffer is compatible with a specific type.
    pub fn require_buffer<T: 'static>(&mut self, expected_name: &str, buffers: &BufferMap) {
        if let Some((name, buffer)) = buffers.inner.get_key_value(expected_name) {
            if buffer.message_type_id() != TypeId::of::<T>() {
                self.incompatible_buffers.push(BufferIncompatibility {
                    name: name.clone(),
                    expected: TypeId::of::<T>(),
                    received: buffer.message_type_id(),
                });
            }
        } else {
            self.missing_buffers
                .push(Cow::Owned(expected_name.to_owned()));
        }
    }

    /// Convert the instance into a result. If any field has content in it, then
    /// this will produce an [`Err`]. Otherwise if no incompatibilities are
    /// present, this will produce an [`Ok`].
    pub fn into_result(self) -> Result<(), IncompatibleLayout> {
        if self.missing_buffers.is_empty() && self.incompatible_buffers.is_empty() {
            Ok(())
        } else {
            Err(self)
        }
    }
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

    fn ensure_active_session(
        buffers: &BufferMap,
        session: Entity,
        world: &mut World,
    ) -> OperationResult;

    fn add_listener(buffers: &BufferMap, listener: Entity, world: &mut World) -> OperationResult {
        for buffer in buffers.inner.values() {
            add_listener_to_source(buffer.id(), listener, world)?;
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
            GateState::apply(buffer.id(), session, action, world, roster)?;
        }
        Ok(())
    }

    fn as_input(buffers: &BufferMap) -> SmallVec<[Entity; 8]> {
        let mut inputs = SmallVec::new();
        for buffer in buffers.inner.values() {
            inputs.push(buffer.id());
        }
        inputs
    }
}

pub trait JoinedValue: 'static + BufferMapLayout + Send + Sync {
    /// This associated type must represent a buffer map layout that is
    /// guaranteed to be compatible for this JoinedValue. Failure to implement
    /// this trait accordingly will result in panics.
    type Buffers: Into<BufferMap>;

    fn pull(
        buffers: &BufferMap,
        session: Entity,
        world: &mut World,
    ) -> Result<Self, OperationError>;

    fn join_into<'w, 's, 'a, 'b>(
        buffers: Self::Buffers,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Self> {
        Self::try_join_into(buffers.into(), builder).unwrap()
    }

    fn try_join_into<'w, 's, 'a, 'b>(
        buffers: BufferMap,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Result<Chain<'w, 's, 'a, 'b, Self>, IncompatibleLayout> {
        let scope = builder.scope();
        let buffers = BufferedMap::<Self>::new(buffers)?;
        buffers.verify_scope(scope);

        let join = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.add(AddOperation::new(
            Some(scope),
            join,
            Join::new(buffers, target),
        ));

        Ok(Output::new(scope, target).chain(builder))
    }
}

/// Trait to describe a layout of buffer keys
pub trait BufferKeyMap: BufferMapLayout + Clone {
    fn add_accessor(buffers: &BufferMap, accessor: Entity, world: &mut World) -> OperationResult;

    fn create_key(buffers: &BufferMap, builder: &BufferKeyBuilder) -> Self;

    fn deep_clone_key(&self) -> Self;

    fn is_key_in_use(&self) -> bool;
}

struct BufferedMap<K> {
    map: BufferMap,
    _ignore: std::marker::PhantomData<fn(K)>,
}

impl<K: BufferMapLayout> BufferedMap<K> {
    fn new(map: BufferMap) -> Result<Self, IncompatibleLayout> {
        K::is_compatible(&map)?;
        Ok(Self {
            map,
            _ignore: Default::default(),
        })
    }
}

impl<K> Clone for BufferedMap<K> {
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            _ignore: Default::default(),
        }
    }
}

impl<L: BufferMapLayout> Buffered for BufferedMap<L> {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self.map.inner.values() {
            assert_eq!(scope, buffer.scope());
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

    fn add_listener(buffers: &BufferMap, listener: Entity, world: &mut World) -> OperationResult {
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use crate::{
        prelude::*, testing::*, BufferMap, InspectBuffer, ManageBuffer, OperationError,
        OperationResult, OrBroken,
    };

    use bevy_ecs::prelude::World;
    use bevy_impulse_derive::Joined;

    #[derive(Clone, Joined)]
    struct TestJoinedValue {
        integer: i64,
        float: f64,
        string: String,
    }

    #[test]
    fn test_joined_value() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer_i64 = builder.create_buffer(BufferSettings::default());
            let buffer_f64 = builder.create_buffer(BufferSettings::default());
            let buffer_string = builder.create_buffer(BufferSettings::default());

            let mut buffers = BufferMap::default();
            buffers.insert(Cow::Borrowed("integer"), buffer_i64);
            buffers.insert(Cow::Borrowed("float"), buffer_f64);
            buffers.insert(Cow::Borrowed("string"), buffer_string);

            scope.input.chain(builder).fork_unzip((
                |chain: Chain<_>| chain.connect(buffer_i64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_f64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_string.input_slot()),
            ));

            builder
                .try_join_into(buffers)
                .unwrap()
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
                .request((5_i64, 3.14_f64, "hello".to_string()), workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let value: TestJoinedValue = promise.take().available().unwrap();
        assert_eq!(value.integer, 5);
        assert_eq!(value.float, 3.14);
        assert_eq!(value.string, "hello");
        assert!(context.no_unhandled_errors());
    }
}
