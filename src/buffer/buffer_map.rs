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

use std::{borrow::Cow, collections::HashMap};

use thiserror::Error as ThisError;

use smallvec::SmallVec;

use bevy_ecs::prelude::{Entity, World};

use crate::{
    add_listener_to_source, Accessed, AddOperation, AnyBuffer, AnyBufferKey, AnyMessageBox, Buffer,
    BufferKeyBuilder, Bufferable, Buffered, Builder, Chain, Gate, GateState, Join, Joined,
    OperationError, OperationResult, OperationRoster, Output, UnusedTarget,
};

pub use bevy_impulse_derive::JoinedValue;

#[derive(Clone, Default)]
pub struct BufferMap {
    inner: HashMap<Cow<'static, str>, AnyBuffer>,
}

impl BufferMap {
    /// Insert a named buffer into the map.
    pub fn insert(&mut self, name: impl Into<Cow<'static, str>>, buffer: impl Into<AnyBuffer>) {
        self.inner.insert(name.into(), buffer.into());
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
    /// Check whether a named buffer is compatible with a specific concrete message type.
    pub fn require_message_type<Message: 'static>(
        &mut self,
        expected_name: &str,
        buffers: &BufferMap,
    ) -> Result<Buffer<Message>, ()> {
        if let Some((name, buffer)) = buffers.inner.get_key_value(expected_name) {
            if let Some(buffer) = buffer.downcast_for_message::<Message>() {
                return Ok(buffer);
            } else {
                self.incompatible_buffers.push(BufferIncompatibility {
                    name: name.clone(),
                    expected: std::any::type_name::<Buffer<Message>>(),
                    received: buffer.message_type_name(),
                });
            }
        } else {
            self.missing_buffers
                .push(Cow::Owned(expected_name.to_owned()));
        }

        Err(())
    }

    /// Check whether a named buffer is compatible with a specialized buffer type,
    /// such as `JsonBuffer`.
    pub fn require_buffer_type<BufferType: 'static>(
        &mut self,
        expected_name: &str,
        buffers: &BufferMap,
    ) -> Result<BufferType, ()> {
        if let Some((name, buffer)) = buffers.inner.get_key_value(expected_name) {
            if let Some(buffer) = buffer.downcast_buffer::<BufferType>() {
                return Ok(buffer);
            } else {
                self.incompatible_buffers.push(BufferIncompatibility {
                    name: name.clone(),
                    expected: std::any::type_name::<BufferType>(),
                    received: buffer.message_type_name(),
                });
            }
        } else {
            self.missing_buffers
                .push(Cow::Owned(expected_name.to_owned()));
        }

        Err(())
    }
}

/// Difference between the expected and received types of a named buffer.
#[derive(Debug, Clone)]
pub struct BufferIncompatibility {
    /// Name of the expected buffer
    pub name: Cow<'static, str>,
    /// The type that was expected for this buffer
    pub expected: &'static str,
    /// The type that was received for this buffer
    pub received: &'static str,
    // TODO(@mxgrey): Replace TypeId with TypeInfo
}

/// This trait can be implemented on structs that represent a layout of buffers.
/// You do not normally have to implement this yourself. Instead you should
/// `#[derive(JoinedValue)]` on a struct that you want a join operation to
/// produce.
pub trait BufferMapLayout: Sized + Clone + 'static + Send + Sync {
    /// Produce a list of the buffers that exist in this layout.
    fn buffer_list(&self) -> SmallVec<[AnyBuffer; 8]>;

    /// Try to convert a generic [`BufferMap`] into this specific layout.
    fn try_from_buffer_map(buffers: &BufferMap) -> Result<Self, IncompatibleLayout>;
}

impl<T: BufferMapLayout> Bufferable for T {
    type BufferType = Self;

    fn into_buffer(self, _: &mut Builder) -> Self::BufferType {
        self
    }
}

impl<T: BufferMapLayout> Buffered for T {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self.buffer_list() {
            assert_eq!(buffer.scope(), scope);
        }
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        let mut min_count = None;

        for buffer in self.buffer_list() {
            let count = buffer.buffered_count(session, world)?;
            min_count = if min_count.is_some_and(|m| m < count) {
                min_count
            } else {
                Some(count)
            };
        }

        Ok(min_count.unwrap_or(0))
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        for buffer in self.buffer_list() {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        for buffer in self.buffer_list() {
            add_listener_to_source(buffer.id(), listener, world)?;
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
        for buffer in self.buffer_list() {
            GateState::apply(buffer.id(), session, action, world, roster)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        let mut inputs = SmallVec::new();
        for buffer in self.buffer_list() {
            inputs.push(buffer.id());
        }
        inputs
    }
}

/// This trait can be implemented for structs that are created by joining together
/// values from a collection of buffers. Usually you do not need to implement this
/// yourself. Instead you can use `#[derive(JoinedValue)]`.
pub trait JoinedValue: 'static + Send + Sync + Sized {
    /// This associated type must represent a buffer map layout that implements
    /// the [`Joined`] trait. The message type yielded by [`Joined`] for this
    /// associated type must match the [`JoinedValue`] type.
    type Buffers: 'static + BufferMapLayout + Joined<Item = Self> + Send + Sync;

    /// Used by [`Self::try_join_from`]
    fn join_from<'w, 's, 'a, 'b>(
        buffers: Self::Buffers,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Self> {
        let scope = builder.scope();
        buffers.verify_scope(scope);

        let join = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.add(AddOperation::new(
            Some(scope),
            join,
            Join::new(buffers, target),
        ));

        Output::new(scope, target).chain(builder)
    }

    /// Used by [`Builder::try_join`]
    fn try_join_from<'w, 's, 'a, 'b>(
        buffers: &BufferMap,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Result<Chain<'w, 's, 'a, 'b, Self>, IncompatibleLayout> {
        let buffers: Self::Buffers = Self::Buffers::try_from_buffer_map(buffers)?;
        Ok(Self::join_from(buffers, builder))
    }
}

/// Trait to describe a set of buffer keys.
pub trait BufferKeyMap: 'static + Send + Sync + Sized + Clone {
    type Buffers: 'static + BufferMapLayout + Accessed<Key = Self> + Send + Sync;
}

impl BufferMapLayout for BufferMap {
    fn buffer_list(&self) -> SmallVec<[AnyBuffer; 8]> {
        self.inner.values().cloned().collect()
    }
    fn try_from_buffer_map(buffers: &BufferMap) -> Result<Self, IncompatibleLayout> {
        Ok(buffers.clone())
    }
}

impl Joined for BufferMap {
    type Item = HashMap<Cow<'static, str>, AnyMessageBox>;

    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        let mut value = HashMap::new();
        for (name, buffer) in &self.inner {
            value.insert(name.clone(), buffer.pull(session, world)?);
        }

        Ok(value)
    }
}

impl JoinedValue for HashMap<Cow<'static, str>, AnyMessageBox> {
    type Buffers = BufferMap;
}

impl Accessed for BufferMap {
    type Key = HashMap<Cow<'static, str>, AnyBufferKey>;

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        let mut keys = HashMap::new();
        for (name, buffer) in &self.inner {
            let key = AnyBufferKey {
                tag: builder.make_tag(buffer.id()),
                interface: buffer.interface,
            };
            keys.insert(name.clone(), key);
        }
        keys
    }

    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        for buffer in self.inner.values() {
            buffer.add_accessor(accessor, world)?;
        }
        Ok(())
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        let mut cloned_key = HashMap::new();
        for (name, key) in key.iter() {
            cloned_key.insert(name.clone(), key.deep_clone());
        }
        cloned_key
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        for k in key.values() {
            if k.is_in_use() {
                return true;
            }
        }

        return false;
    }
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*, BufferMap};

    #[derive(Clone, JoinedValue)]
    struct TestJoinedValue<T: Send + Sync + 'static + Clone> {
        integer: i64,
        float: f64,
        string: String,
        generic: T,
    }

    #[test]
    fn test_try_join() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer_i64 = builder.create_buffer(BufferSettings::default());
            let buffer_f64 = builder.create_buffer(BufferSettings::default());
            let buffer_string = builder.create_buffer(BufferSettings::default());
            let buffer_generic = builder.create_buffer(BufferSettings::default());

            let mut buffers = BufferMap::default();
            buffers.insert("integer", buffer_i64);
            buffers.insert("float", buffer_f64);
            buffers.insert("string", buffer_string);
            buffers.insert("generic", buffer_generic);

            scope.input.chain(builder).fork_unzip((
                |chain: Chain<_>| chain.connect(buffer_i64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_f64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_string.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_generic.input_slot()),
            ));

            builder.try_join(&buffers).unwrap().connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
                .request((5_i64, 3.14_f64, "hello".to_string(), "world"), workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let value: TestJoinedValue<&'static str> = promise.take().available().unwrap();
        assert_eq!(value.integer, 5);
        assert_eq!(value.float, 3.14);
        assert_eq!(value.string, "hello");
        assert_eq!(value.generic, "world");
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_joined_value() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer_i64 = builder.create_buffer(BufferSettings::default());
            let buffer_f64 = builder.create_buffer(BufferSettings::default());
            let buffer_string = builder.create_buffer(BufferSettings::default());
            let buffer_generic = builder.create_buffer(BufferSettings::default());

            let buffers = TestJoinedValue::select_buffers(
                builder,
                buffer_i64,
                buffer_f64,
                buffer_string,
                buffer_generic,
            );

            scope.input.chain(builder).fork_unzip((
                |chain: Chain<_>| chain.connect(buffers.integer.input_slot()),
                |chain: Chain<_>| chain.connect(buffers.float.input_slot()),
                |chain: Chain<_>| chain.connect(buffers.string.input_slot()),
                |chain: Chain<_>| chain.connect(buffers.generic.input_slot()),
            ));

            builder.join(buffers).connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
                .request((5_i64, 3.14_f64, "hello".to_string(), "world"), workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let value: TestJoinedValue<&'static str> = promise.take().available().unwrap();
        assert_eq!(value.integer, 5);
        assert_eq!(value.float, 3.14);
        assert_eq!(value.string, "hello");
        assert_eq!(value.generic, "world");
        assert!(context.no_unhandled_errors());
    }

    #[derive(Clone, JoinedValue)]
    #[buffers(struct_name = FooBuffers)]
    struct TestDeriveWithConfig {}

    #[test]
    fn test_derive_with_config() {
        // a compile test to check that the name of the generated struct is correct
        fn _check_buffer_struct_name(_: FooBuffers) {}
    }
}
