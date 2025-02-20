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
    add_listener_to_source, Accessed, AnyBuffer, AnyBufferKey, AnyMessageBox, AsAnyBuffer, Buffer,
    BufferKeyBuilder, BufferKeyLifecycle, Bufferable, Buffered, Builder, Chain, Gate, GateState,
    Joined, Node, OperationError, OperationResult, OperationRoster,
};

pub use bevy_impulse_derive::{BufferKeyMap, JoinedValue};

/// Uniquely identify a buffer within a buffer map, either by name or by an
/// index value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BufferIdentifier<'a> {
    /// Identify a buffer by name
    Name(Cow<'a, str>),
    /// Identify a buffer by an index value
    Index(usize),
}

impl BufferIdentifier<'static> {
    /// Clone a name to use as an identifier.
    pub fn clone_name(name: &str) -> Self {
        BufferIdentifier::Name(Cow::Owned(name.to_owned()))
    }

    /// Borrow a string literal name to use as an identifier.
    pub fn literal_name(name: &'static str) -> Self {
        BufferIdentifier::Name(Cow::Borrowed(name))
    }

    /// Use an index as an identifier.
    pub fn index(index: usize) -> Self {
        BufferIdentifier::Index(index)
    }
}

impl From<&'static str> for BufferIdentifier<'static> {
    fn from(value: &'static str) -> Self {
        BufferIdentifier::Name(Cow::Borrowed(value))
    }
}

pub type BufferMap = HashMap<BufferIdentifier<'static>, AnyBuffer>;

/// Extension trait that makes it more convenient to insert buffers into a [`BufferMap`].
pub trait AddBufferToMap {
    /// Convenience function for inserting items into a [`BufferMap`]. This
    /// automatically takes care of converting the types.
    fn insert_buffer<I: Into<BufferIdentifier<'static>>, B: AsAnyBuffer>(
        &mut self,
        identifier: I,
        buffer: B,
    );
}

impl AddBufferToMap for BufferMap {
    fn insert_buffer<I: Into<BufferIdentifier<'static>>, B: AsAnyBuffer>(
        &mut self,
        identifier: I,
        buffer: B,
    ) {
        self.insert(identifier.into(), buffer.as_any_buffer());
    }
}

/// This error is used when the buffers provided for an input are not compatible
/// with the layout.
#[derive(ThisError, Debug, Clone, Default)]
#[error("the incoming buffer map is incompatible with the layout")]
pub struct IncompatibleLayout {
    /// Identities of buffers that were missing from the incoming buffer map.
    pub missing_buffers: Vec<BufferIdentifier<'static>>,
    /// Identities of buffers in the incoming buffer map which cannot exist in
    /// the target layout.
    pub forbidden_buffers: Vec<BufferIdentifier<'static>>,
    /// Buffers whose expected type did not match the received type.
    pub incompatible_buffers: Vec<BufferIncompatibility>,
}

impl IncompatibleLayout {
    /// Convert this into an error if it has any contents inside.
    pub fn as_result(self) -> Result<(), Self> {
        if !self.missing_buffers.is_empty() {
            return Err(self);
        }

        if !self.incompatible_buffers.is_empty() {
            return Err(self);
        }

        Ok(())
    }

    /// Same as [`Self::require_buffer_by_literal`], but can be used with
    /// temporary borrows of a string slice. The string slice will be cloned if
    /// an error message needs to be produced.
    pub fn require_buffer_by_name<BufferType: 'static>(
        &mut self,
        expected_name: &str,
        buffers: &BufferMap,
    ) -> Result<BufferType, ()> {
        let identifier = BufferIdentifier::Name(Cow::Borrowed(expected_name));
        if let Some(buffer) = buffers.get(&identifier) {
            if let Some(buffer) = buffer.downcast_buffer::<BufferType>() {
                return Ok(buffer);
            } else {
                self.incompatible_buffers.push(BufferIncompatibility {
                    identifier: BufferIdentifier::Name(Cow::Owned(expected_name.to_owned())),
                    expected: std::any::type_name::<BufferType>(),
                    received: buffer.message_type_name(),
                });
            }
        } else {
            self.missing_buffers
                .push(BufferIdentifier::Name(Cow::Owned(expected_name.to_owned())));
        }

        Err(())
    }

    /// Check whether a named buffer is compatible with the required buffer type.
    pub fn require_buffer_by_literal<BufferType: 'static>(
        &mut self,
        expected_name: &'static str,
        buffers: &BufferMap,
    ) -> Result<BufferType, ()> {
        self.require_buffer::<BufferType>(BufferIdentifier::literal_name(expected_name), buffers)
    }

    /// Check whether an indexed buffer is compatible with the required buffer type.
    pub fn require_buffer_by_index<BufferType: 'static>(
        &mut self,
        expected_index: usize,
        buffers: &BufferMap,
    ) -> Result<BufferType, ()> {
        self.require_buffer::<BufferType>(BufferIdentifier::Index(expected_index), buffers)
    }

    /// Check whether the buffer associated with the identifier is compatible with
    /// the required buffer type.
    pub fn require_buffer<BufferType: 'static>(
        &mut self,
        identifier: BufferIdentifier<'static>,
        buffers: &BufferMap,
    ) -> Result<BufferType, ()> {
        if let Some(buffer) = buffers.get(&identifier) {
            if let Some(buffer) = buffer.downcast_buffer::<BufferType>() {
                return Ok(buffer);
            } else {
                self.incompatible_buffers.push(BufferIncompatibility {
                    identifier,
                    expected: std::any::type_name::<BufferType>(),
                    received: buffer.message_type_name(),
                });
            }
        } else {
            self.missing_buffers.push(identifier);
        }

        Err(())
    }
}

/// Difference between the expected and received types of a named buffer.
#[derive(Debug, Clone)]
pub struct BufferIncompatibility {
    /// Name of the expected buffer
    pub identifier: BufferIdentifier<'static>,
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
    /// Try to convert a generic [`BufferMap`] into this specific layout.
    fn try_from_buffer_map(buffers: &BufferMap) -> Result<Self, IncompatibleLayout>;
}

/// This trait helps auto-generated buffer map structs to implement the Buffered
/// trait.
pub trait BufferMapStruct: Sized + Clone + 'static + Send + Sync {
    /// Produce a list of the buffers that exist in this layout. Implementing
    /// this function alone is sufficient to implement the entire [`Buffered`] trait.
    fn buffer_list(&self) -> SmallVec<[AnyBuffer; 8]>;
}

impl<T: BufferMapStruct> Bufferable for T {
    type BufferType = Self;

    fn into_buffer(self, _: &mut Builder) -> Self::BufferType {
        self
    }
}

impl<T: BufferMapStruct> Buffered for T {
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
/// values from a collection of buffers. This allows [`join`][1] to produce arbitrary
/// structs. Structs with this trait can be produced by [`try_join`][2].
///
/// Each field in this struct needs to have the trait bounds `'static + Send + Sync`.
///
/// This does not generally need to be implemented explicitly. Instead you should
/// use `#[derive(JoinedValue)]`:
///
/// ```
/// use bevy_impulse::prelude::*;
///
/// #[derive(JoinedValue)]
/// struct SomeValues {
///     integer: i64,
///     string: String,
/// }
/// ```
///
/// The above example would allow you to join a value from an `i64` buffer with
/// a value from a `String` buffer. You can have as many fields in the struct
/// as you'd like.
///
/// This macro will generate a struct of buffers to match the fields of the
/// struct that it's applied to. The name of that struct is anonymous by default
/// since you don't generally need to use it directly, but if you want to give
/// it a name you can use #[joined(buffers_struct_name = ...)]`:
///
/// ```
/// # use bevy_impulse::prelude::*;
///
/// #[derive(JoinedValue)]
/// #[joined(buffers_struct_name = SomeBuffers)]
/// struct SomeValues {
///     integer: i64,
///     string: String,
/// }
/// ```
///
/// By default each field of the generated buffers struct will have a type of
/// [`Buffer<T>`], but you can override this using `#[joined(buffer = ...)]`
/// to specify a special buffer type. For example if your `JoinedValue` struct
/// contains an [`AnyMessageBox`] then by default the macro will use `Buffer<AnyMessageBox>`,
/// but you probably really want it to have an [`AnyBuffer`]:
///
/// ```
/// # use bevy_impulse::prelude::*;
///
/// #[derive(JoinedValue)]
/// struct SomeValues {
///     integer: i64,
///     string: String,
///     #[joined(buffer = AnyBuffer)]
///     any: AnyMessageBox,
/// }
/// ```
///
/// The above method also works for joining a `JsonMessage` field from a `JsonBuffer`.
///
/// [1]: crate::Builder::join
/// [2]: crate::Builder::try_join
pub trait JoinedValue: 'static + Send + Sync + Sized {
    /// This associated type must represent a buffer map layout that implements
    /// the [`Joined`] trait. The message type yielded by [`Joined`] for this
    /// associated type must match the [`JoinedValue`] type.
    type Buffers: 'static + BufferMapLayout + Joined<Item = Self> + Send + Sync;

    /// Used by [`Builder::try_join`]
    fn try_join_from<'w, 's, 'a, 'b>(
        buffers: &BufferMap,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Result<Chain<'w, 's, 'a, 'b, Self>, IncompatibleLayout> {
        let buffers: Self::Buffers = Self::Buffers::try_from_buffer_map(buffers)?;
        Ok(buffers.join(builder))
    }
}

/// Trait to describe a set of buffer keys. This allows [listen][1] and [access][2]
/// to work for arbitrary structs of buffer keys. Structs with this trait can be
/// produced by [`try_listen`][3] and [`try_create_buffer_access`][4].
///
/// Each field in the struct must be some kind of buffer key.
///
/// This does not generally need to be implemented explicitly. Instead you should
/// define a struct where all fields are buffer keys and then apply
/// `#[derive(BufferKeyMap)]` to it, e.g.:
///
/// ```
/// use bevy_impulse::prelude::*;
///
/// #[derive(Clone, BufferKeyMap)]
/// struct SomeKeys {
///     integer: BufferKey<i64>,
///     string: BufferKey<String>,
///     any: AnyBufferKey,
/// }
/// ```
///
/// The macro will generate a struct of buffers to match the keys. The name of
/// that struct is anonymous by default since you don't generally need to use it
/// directly, but if you want to give it a name you can use `#[key(buffers_struct_name = ...)]`:
///
/// ```
/// # use bevy_impulse::prelude::*;
///
/// #[derive(Clone, BufferKeyMap)]
/// #[key(buffers_struct_name = SomeBuffers)]
/// struct SomeKeys {
///     integer: BufferKey<i64>,
///     string: BufferKey<String>,
///     any: AnyBufferKey,
/// }
/// ```
///
/// [1]: crate::Builder::listen
/// [2]: crate::Builder::create_buffer_access
/// [3]: crate::Builder::try_listen
/// [4]: crate::Builder::try_create_buffer_access
pub trait BufferKeyMap: 'static + Send + Sync + Sized + Clone {
    type Buffers: 'static + BufferMapLayout + Accessed<Key = Self> + Send + Sync;

    fn try_listen_from<'w, 's, 'a, 'b>(
        buffers: &BufferMap,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Result<Chain<'w, 's, 'a, 'b, Self>, IncompatibleLayout> {
        let buffers: Self::Buffers = Self::Buffers::try_from_buffer_map(buffers)?;
        Ok(buffers.listen(builder))
    }

    fn try_buffer_access<T: 'static + Send + Sync>(
        buffers: &BufferMap,
        builder: &mut Builder,
    ) -> Result<Node<T, (T, Self)>, IncompatibleLayout> {
        let buffers: Self::Buffers = Self::Buffers::try_from_buffer_map(buffers)?;
        Ok(buffers.access(builder))
    }
}

impl BufferMapLayout for BufferMap {
    fn try_from_buffer_map(buffers: &BufferMap) -> Result<Self, IncompatibleLayout> {
        Ok(buffers.clone())
    }
}

impl BufferMapStruct for BufferMap {
    fn buffer_list(&self) -> SmallVec<[AnyBuffer; 8]> {
        self.values().cloned().collect()
    }
}

impl Joined for BufferMap {
    type Item = HashMap<BufferIdentifier<'static>, AnyMessageBox>;

    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        let mut value = HashMap::new();
        for (name, buffer) in self.iter() {
            value.insert(name.clone(), buffer.pull(session, world)?);
        }

        Ok(value)
    }
}

impl JoinedValue for HashMap<BufferIdentifier<'static>, AnyMessageBox> {
    type Buffers = BufferMap;
}

impl Accessed for BufferMap {
    type Key = HashMap<BufferIdentifier<'static>, AnyBufferKey>;

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        let mut keys = HashMap::new();
        for (name, buffer) in self.iter() {
            let key = AnyBufferKey {
                tag: builder.make_tag(buffer.id()),
                interface: buffer.interface,
            };
            keys.insert(name.clone(), key);
        }
        keys
    }

    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        for buffer in self.values() {
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

impl<T: 'static + Send + Sync> JoinedValue for Vec<T> {
    type Buffers = Vec<Buffer<T>>;
}

impl<B: 'static + Send + Sync + AsAnyBuffer + Clone> BufferMapLayout for Vec<B> {
    fn try_from_buffer_map(buffers: &BufferMap) -> Result<Self, IncompatibleLayout> {
        let mut downcast_buffers = Vec::new();
        let mut compatibility = IncompatibleLayout::default();
        for i in 0..buffers.len() {
            if let Ok(downcast) = compatibility.require_buffer_by_index::<B>(i, buffers) {
                downcast_buffers.push(downcast);
            }
        }

        compatibility.as_result()?;
        Ok(downcast_buffers)
    }
}

impl<T: 'static + Send + Sync, const N: usize> JoinedValue for SmallVec<[T; N]> {
    type Buffers = SmallVec<[Buffer<T>; N]>;
}

impl<B: 'static + Send + Sync + AsAnyBuffer + Clone, const N: usize> BufferMapLayout
    for SmallVec<[B; N]>
{
    fn try_from_buffer_map(buffers: &BufferMap) -> Result<Self, IncompatibleLayout> {
        let mut downcast_buffers = SmallVec::new();
        let mut compatibility = IncompatibleLayout::default();
        for i in 0..buffers.len() {
            if let Ok(downcast) = compatibility.require_buffer_by_index::<B>(i, buffers) {
                downcast_buffers.push(downcast);
            }
        }

        compatibility.as_result()?;
        Ok(downcast_buffers)
    }
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*, AddBufferToMap, BufferMap};

    #[derive(JoinedValue)]
    struct TestJoinedValue<T: Send + Sync + 'static + Clone> {
        integer: i64,
        float: f64,
        string: String,
        generic: T,
        #[joined(buffer = AnyBuffer)]
        any: AnyMessageBox,
    }

    #[test]
    fn test_try_join() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer_i64 = builder.create_buffer(BufferSettings::default());
            let buffer_f64 = builder.create_buffer(BufferSettings::default());
            let buffer_string = builder.create_buffer(BufferSettings::default());
            let buffer_generic = builder.create_buffer(BufferSettings::default());
            let buffer_any = builder.create_buffer(BufferSettings::default());

            let mut buffers = BufferMap::default();
            buffers.insert_buffer("integer", buffer_i64);
            buffers.insert_buffer("float", buffer_f64);
            buffers.insert_buffer("string", buffer_string);
            buffers.insert_buffer("generic", buffer_generic);
            buffers.insert_buffer("any", buffer_any);

            scope.input.chain(builder).fork_unzip((
                |chain: Chain<_>| chain.connect(buffer_i64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_f64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_string.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_generic.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_any.input_slot()),
            ));

            builder.try_join(&buffers).unwrap().connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
                .request(
                    (5_i64, 3.14_f64, "hello".to_string(), "world", 42_i64),
                    workflow,
                )
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let value: TestJoinedValue<&'static str> = promise.take().available().unwrap();
        assert_eq!(value.integer, 5);
        assert_eq!(value.float, 3.14);
        assert_eq!(value.string, "hello");
        assert_eq!(value.generic, "world");
        assert_eq!(*value.any.downcast::<i64>().unwrap(), 42);
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
            let buffer_any = builder.create_buffer::<i64>(BufferSettings::default());

            scope.input.chain(builder).fork_unzip((
                |chain: Chain<_>| chain.connect(buffer_i64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_f64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_string.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_generic.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_any.input_slot()),
            ));

            let buffers = TestJoinedValue::select_buffers(
                buffer_i64,
                buffer_f64,
                buffer_string,
                buffer_generic,
                buffer_any.into(),
            );

            builder.join(buffers).connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
                .request(
                    (5_i64, 3.14_f64, "hello".to_string(), "world", 42_i64),
                    workflow,
                )
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let value: TestJoinedValue<&'static str> = promise.take().available().unwrap();
        assert_eq!(value.integer, 5);
        assert_eq!(value.float, 3.14);
        assert_eq!(value.string, "hello");
        assert_eq!(value.generic, "world");
        assert_eq!(*value.any.downcast::<i64>().unwrap(), 42);
        assert!(context.no_unhandled_errors());
    }

    #[derive(Clone, JoinedValue)]
    #[joined(buffers_struct_name = FooBuffers)]
    struct TestDeriveWithConfig {}

    #[test]
    fn test_derive_with_config() {
        // a compile test to check that the name of the generated struct is correct
        fn _check_buffer_struct_name(_: FooBuffers) {}
    }

    struct MultiGenericValue<T: 'static + Send + Sync, U: 'static + Send + Sync> {
        t: T,
        u: U,
    }

    #[derive(JoinedValue)]
    #[joined(buffers_struct_name = MultiGenericBuffers)]
    struct JoinedMultiGenericValue<T: 'static + Send + Sync, U: 'static + Send + Sync> {
        #[joined(buffer = Buffer<MultiGenericValue<T, U>>)]
        a: MultiGenericValue<T, U>,
        b: String,
    }

    #[test]
    fn test_multi_generic_joined_value() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(
            |scope: Scope<(i32, String), JoinedMultiGenericValue<i32, String>>, builder| {
                let multi_generic_buffers = MultiGenericBuffers::<i32, String> {
                    a: builder.create_buffer(BufferSettings::default()),
                    b: builder.create_buffer(BufferSettings::default()),
                };

                let copy = multi_generic_buffers;

                scope
                    .input
                    .chain(builder)
                    .map_block(|(integer, string)| {
                        (
                            MultiGenericValue {
                                t: integer,
                                u: string.clone(),
                            },
                            string,
                        )
                    })
                    .fork_unzip((
                        |a: Chain<_>| a.connect(multi_generic_buffers.a.input_slot()),
                        |b: Chain<_>| b.connect(multi_generic_buffers.b.input_slot()),
                    ));

                multi_generic_buffers.join(builder).connect(scope.terminate);
                copy.join(builder).connect(scope.terminate);
            },
        );

        let mut promise = context.command(|commands| {
            commands
                .request((5, "hello".to_string()), workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let value = promise.take().available().unwrap();
        assert_eq!(value.a.t, 5);
        assert_eq!(value.a.u, "hello");
        assert_eq!(value.b, "hello");
        assert!(context.no_unhandled_errors());
    }

    /// We create this struct just to verify that it is able to compile despite
    /// NonCopyBuffer not being copyable.
    #[derive(JoinedValue)]
    #[allow(unused)]
    struct JoinedValueForNonCopyBuffer {
        #[joined(buffer = NonCopyBuffer<String>, noncopy_buffer)]
        _a: String,
        _b: u32,
    }

    #[derive(Clone, BufferKeyMap)]
    #[key(buffers_struct_name = TestKeysBuffers)]
    struct TestKeys<T: 'static + Send + Sync + Clone> {
        integer: BufferKey<i64>,
        float: BufferKey<f64>,
        string: BufferKey<String>,
        generic: BufferKey<T>,
        any: AnyBufferKey,
    }
    #[test]
    fn test_listen() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer_any = builder.create_buffer::<i64>(BufferSettings::default());

            let buffers = TestKeys::select_buffers(
                builder.create_buffer(BufferSettings::default()),
                builder.create_buffer(BufferSettings::default()),
                builder.create_buffer(BufferSettings::default()),
                builder.create_buffer(BufferSettings::default()),
                buffer_any.as_any_buffer(),
            );

            scope.input.chain(builder).fork_unzip((
                |chain: Chain<_>| chain.connect(buffers.integer.input_slot()),
                |chain: Chain<_>| chain.connect(buffers.float.input_slot()),
                |chain: Chain<_>| chain.connect(buffers.string.input_slot()),
                |chain: Chain<_>| chain.connect(buffers.generic.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_any.input_slot()),
            ));

            builder
                .listen(buffers)
                .then(join_via_listen.into_blocking_callback())
                .dispose_on_none()
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
                .request(
                    (5_i64, 3.14_f64, "hello".to_string(), "world", 42_i64),
                    workflow,
                )
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let value: TestJoinedValue<&'static str> = promise.take().available().unwrap();
        assert_eq!(value.integer, 5);
        assert_eq!(value.float, 3.14);
        assert_eq!(value.string, "hello");
        assert_eq!(value.generic, "world");
        assert_eq!(*value.any.downcast::<i64>().unwrap(), 42);
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_try_listen() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer_i64 = builder.create_buffer::<i64>(BufferSettings::default());
            let buffer_f64 = builder.create_buffer::<f64>(BufferSettings::default());
            let buffer_string = builder.create_buffer::<String>(BufferSettings::default());
            let buffer_generic = builder.create_buffer::<&'static str>(BufferSettings::default());
            let buffer_any = builder.create_buffer::<i64>(BufferSettings::default());

            scope.input.chain(builder).fork_unzip((
                |chain: Chain<_>| chain.connect(buffer_i64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_f64.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_string.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_generic.input_slot()),
                |chain: Chain<_>| chain.connect(buffer_any.input_slot()),
            ));

            let mut buffer_map = BufferMap::new();
            buffer_map.insert_buffer("integer", buffer_i64);
            buffer_map.insert_buffer("float", buffer_f64);
            buffer_map.insert_buffer("string", buffer_string);
            buffer_map.insert_buffer("generic", buffer_generic);
            buffer_map.insert_buffer("any", buffer_any);

            builder
                .try_listen(&buffer_map)
                .unwrap()
                .then(join_via_listen.into_blocking_callback())
                .dispose_on_none()
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
                .request(
                    (5_i64, 3.14_f64, "hello".to_string(), "world", 42_i64),
                    workflow,
                )
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let value: TestJoinedValue<&'static str> = promise.take().available().unwrap();
        assert_eq!(value.integer, 5);
        assert_eq!(value.float, 3.14);
        assert_eq!(value.string, "hello");
        assert_eq!(value.generic, "world");
        assert_eq!(*value.any.downcast::<i64>().unwrap(), 42);
        assert!(context.no_unhandled_errors());
    }

    /// This macro is a manual implementation of the join operation that uses
    /// the buffer listening mechanism. There isn't any reason to reimplement
    /// join here except so we can test that listening is working correctly for
    /// BufferKeyMap.
    fn join_via_listen(
        In(keys): In<TestKeys<&'static str>>,
        world: &mut World,
    ) -> Option<TestJoinedValue<&'static str>> {
        if world.buffer_view(&keys.integer).ok()?.is_empty() {
            return None;
        }
        if world.buffer_view(&keys.float).ok()?.is_empty() {
            return None;
        }
        if world.buffer_view(&keys.string).ok()?.is_empty() {
            return None;
        }
        if world.buffer_view(&keys.generic).ok()?.is_empty() {
            return None;
        }
        if world.any_buffer_view(&keys.any).ok()?.is_empty() {
            return None;
        }

        let integer = world
            .buffer_mut(&keys.integer, |mut buffer| buffer.pull())
            .unwrap()
            .unwrap();
        let float = world
            .buffer_mut(&keys.float, |mut buffer| buffer.pull())
            .unwrap()
            .unwrap();
        let string = world
            .buffer_mut(&keys.string, |mut buffer| buffer.pull())
            .unwrap()
            .unwrap();
        let generic = world
            .buffer_mut(&keys.generic, |mut buffer| buffer.pull())
            .unwrap()
            .unwrap();
        let any = world
            .any_buffer_mut(&keys.any, |mut buffer| buffer.pull())
            .unwrap()
            .unwrap();

        Some(TestJoinedValue {
            integer,
            float,
            string,
            generic,
            any,
        })
    }
}
