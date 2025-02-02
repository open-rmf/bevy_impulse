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

// TODO(@mxgrey): Add module-level documentation describing how to use JsonBuffer

use std::{
    any::TypeId,
    collections::HashMap,
    ops::RangeBounds,
    sync::{Arc, Mutex, OnceLock},
};

use bevy_ecs::{
    prelude::{Commands, Entity, EntityRef, EntityWorldMut, Mut, World},
    system::SystemState,
};

use serde::{de::DeserializeOwned, Serialize};

use serde_json::Value as JsonMessage;

use smallvec::SmallVec;

use crate::{
    AnyBufferAccessImpl, AnyBufferAccessInterface, AnyBuffer, AnyBufferKey,
    AnyRange, Buffer, BufferAccessors, BufferKey, BufferKeyBuilder, BufferAccessLifecycle,
    BufferAccessMut, BufferError, BufferStorage, Builder, DrainBuffer, OperationError,
    OperationResult, InspectBuffer, ManageBuffer, Gate, GateState,
    NotifyBufferUpdate, Bufferable, Buffered, OrBroken, Joined, Accessed,
    add_listener_to_source,
};

/// A [`Buffer`] whose message type has been anonymized, but which is known to
/// support serialization and deserialization. Joining this buffer type will
/// yield a [`JsonMessage`].
#[derive(Clone, Copy, Debug)]
pub struct JsonBuffer {
    scope: Entity,
    source: Entity,
    interface: &'static (dyn JsonBufferAccessInterface + Send + Sync),
}

impl JsonBuffer {
    /// Downcast this into a concerete [`Buffer`] type.
    pub fn downcast<T: 'static>(&self) -> Option<Buffer<T>> {
        if TypeId::of::<T>() == self.interface.any_access_interface().message_type_id() {
            Some(Buffer {
                scope: self.scope,
                source: self.source,
                _ignore: Default::default(),
            })
        } else {
            None
        }
    }
}

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned> From<Buffer<T>> for JsonBuffer {
    fn from(value: Buffer<T>) -> Self {
        Self {
            scope: value.scope,
            source: value.source,
            interface: JsonBufferAccessImpl::<T>::get_interface(),
        }
    }
}

impl From<JsonBuffer> for AnyBuffer {
    fn from(value: JsonBuffer) -> Self {
        Self {
            scope: value.scope,
            source: value.source,
            interface: value.interface.any_access_interface(),
        }
    }
}

/// Similar to a [`BufferKey`] except it can be used for any buffer that supports
/// serialization and deserialization without knowing the buffer's specific
/// message type at compile time.
///
/// Use this with [`JsonBufferAccess`] to directly view or manipulate the contents
/// of a buffer.
#[derive(Clone)]
pub struct JsonBufferKey {
    buffer: Entity,
    session: Entity,
    accessor: Entity,
    lifecycle: Option<Arc<BufferAccessLifecycle>>,
    interface: &'static (dyn JsonBufferAccessInterface + Send + Sync),
}

impl JsonBufferKey {
    pub fn downcast<T: 'static>(&self) -> Option<BufferKey<T>> {
        if TypeId::of::<T>() == self.interface.any_access_interface().message_type_id() {
            Some(BufferKey {
                buffer: self.buffer,
                session: self.session,
                accessor: self.accessor,
                lifecycle: self.lifecycle.clone(),
                _ignore: Default::default()
            })
        } else {
            None
        }
    }

    fn deep_clone(&self) -> Self {
        let mut deep = self.clone();
        deep.lifecycle = self
            .lifecycle
            .as_ref()
            .map(|l| Arc::new(l.as_ref().clone()));
        deep
    }

    fn is_in_use(&self) -> bool {
        self.lifecycle.as_ref().is_some_and(|l| l.is_in_use())
    }
}

impl std::fmt::Debug for JsonBufferKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("JsonBufferKey")
            .field("buffer", &self.buffer)
            .field("session", &self.session)
            .field("accessor", &self.accessor)
            .field("in_use", &self.lifecycle.as_ref().is_some_and(|l| l.is_in_use()))
            .field("message_type_name", &self.interface.any_access_interface().message_type_name())
            .finish()
    }
}

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned> From<BufferKey<T>> for JsonBufferKey {
    fn from(value: BufferKey<T>) -> Self {
        let interface = JsonBufferAccessImpl::<T>::get_interface();
        JsonBufferKey {
            buffer: value.buffer,
            session: value.session,
            accessor: value.accessor,
            lifecycle: value.lifecycle,
            interface
        }
    }
}

impl From<JsonBufferKey> for AnyBufferKey {
    fn from(value: JsonBufferKey) -> Self {
        AnyBufferKey {
            buffer: value.buffer,
            session: value.session,
            accessor: value.accessor,
            lifecycle: value.lifecycle,
            interface: value.interface.any_access_interface(),
        }
    }
}

/// Similar to [`BufferMut`][crate::BufferMut], but this can be unlocked with a
/// [`JsonBufferKey`], so it can work for any buffer whose message types support
/// serialization and deserialization.
pub struct JsonBufferMut<'w, 's, 'a> {
    storage: Box<dyn JsonBufferManagement + 'a>,
    gate: Mut<'a, GateState>,
    buffer: Entity,
    session: Entity,
    accessor: Option<Entity>,
    commands: &'a mut Commands<'w, 's>,
    modified: bool,
}

impl<'w, 's, 'a> JsonBufferMut<'w, 's,  'a> {
    /// Same as [BufferMut::allow_closed_loops][1].
    ///
    /// [1]: crate::BufferMut::allow_closed_loops
    pub fn allow_closed_loops(mut self) -> Self {
        self.accessor = None;
        self
    }

    /// Get a serialized copy of the oldest message in the buffer.
    pub fn oldest(&self) -> JsonMessageViewResult {
        self.storage.json_oldest(self.session)
    }

    /// Get a serialized copy of the newest message in the buffer.
    pub fn newest(&self) -> JsonMessageViewResult {
        self.storage.json_newest(self.session)
    }

    /// Get a serialized copy of a message in the buffer.
    pub fn get(&self, index: usize) -> JsonMessageViewResult {
        self.storage.json_get(self.session, index)
    }

    /// Get how many messages are in this buffer.
    pub fn len(&self) -> usize {
        self.storage.json_count(self.session)
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Check whether the gate of this buffer is open or closed.
    pub fn gate(&self) -> Gate {
        self.gate
            .map
            .get(&self.session)
            .copied()
            .unwrap_or(Gate::Open)
    }

    /// Modify the oldest message in the buffer.
    pub fn oldest_mut(&mut self) -> Option<JsonMut<'_>> {
        self.storage.json_oldest_mut(self.session, &mut self.modified)
    }

    /// Modify the newest message in the buffer.
    pub fn newest_mut(&mut self) -> Option<JsonMut<'_>> {
        self.storage.json_newest_mut(self.session, &mut self.modified)
    }

    /// Modify a message in the buffer.
    pub fn get_mut(&mut self, index: usize) -> Option<JsonMut<'_>> {
        self.storage.json_get_mut(self.session, index, &mut self.modified)
    }

    /// Drain a range of messages out of the buffer.
    pub fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> DrainJsonBuffer<'_> {
        self.modified = true;
        DrainJsonBuffer {
            interface: self.storage.json_drain(self.session, AnyRange::new(range))
        }
    }

    /// Pull the oldest message from the buffer as a JSON value. Unlike
    /// [`Self::oldest`] this will remove the message from the buffer.
    pub fn pull(&mut self) -> JsonMessageViewResult {
        self.modified = true;
        self.storage.json_pull(self.session)
    }

    /// Pull the oldest message from the buffer and attempt to deserialize it
    /// into the target type.
    pub fn pull_as<T: DeserializeOwned>(&mut self) -> Result<Option<T>, serde_json::Error> {
        self.pull()?.map(|m| serde_json::from_value(m)).transpose()
    }

    /// Pull the newest message from the buffer as a JSON value. Unlike
    /// [`Self::newest`] this will remove the message from the buffer.
    pub fn pull_newest(&mut self) -> JsonMessageViewResult {
        self.modified = true;
        self.storage.json_pull_newest(self.session)
    }

    /// Pull the newest message from the buffer and attempt to deserialize it
    /// into the target type.
    pub fn pull_newest_as<T: DeserializeOwned>(&mut self) -> Result<Option<T>, serde_json::Error> {
        self.pull_newest()?.map(|m| serde_json::from_value(m)).transpose()
    }

    /// Attempt to push a new value into the buffer.
    ///
    /// If the input value is compatible with the message type of the buffer,
    /// this will return [`Ok`]. If the buffer is at its limit before a successful
    /// push, this will return the value that needed to be removed.
    ///
    /// If the input value does not match the message type of the buffer, this
    /// will return [`Err`]. This may also return [`Err`] if the message coming
    /// out of the buffer failed to serialize.
    // TODO(@mxgrey): Consider having an error type that differentiates the
    // various possible error modes.
    pub fn push<T: 'static + Serialize>(&mut self, value: T) -> Result<Option<JsonMessage>, serde_json::Error> {
        let message = serde_json::to_value(&value)?;
        self.modified = true;
        self.storage.json_push(self.session, message)
    }

    /// Same as [`Self::push`] but no serialization step is needed for the incoming
    /// message.
    pub fn push_json(&mut self, message: JsonMessage) -> Result<Option<JsonMessage>, serde_json::Error> {
        self.modified = true;
        self.storage.json_push(self.session, message)
    }

    /// Same as [`Self::push`] but the message will be interpreted as the oldest
    /// message in the buffer.
    pub fn push_as_oldest<T: 'static + Serialize>(&mut self, value: T) -> Result<Option<JsonMessage>, serde_json::Error> {
        let message = serde_json::to_value(&value)?;
        self.modified = true;
        self.storage.json_push_as_oldest(self.session, message)
    }

    /// Same as [`Self:push_as_oldest`] but no serialization step is needed for
    /// the incoming message.
    pub fn push_json_as_oldest(&mut self, message: JsonMessage) -> Result<Option<JsonMessage>, serde_json::Error> {
        self.modified = true;
        self.storage.json_push_as_oldest(self.session, message)
    }

    /// Tell the buffer [`Gate`] to open.
    pub fn open_gate(&mut self) {
        if let Some(gate) = self.gate.map.get_mut(&self.session) {
            if *gate != Gate::Open {
                *gate = Gate::Open;
                self.modified = true;
            }
        }
    }

    /// Tell the buffer [`Gate`] to close.
    pub fn close_gate(&mut self) {
        if let Some(gate) = self.gate.map.get_mut(&self.session) {
            *gate = Gate::Closed;
            // There is no need to to indicate that a modification happened
            // because listeners do not get notified about gates closing.
        }
    }

    /// Perform an action on the gate of the buffer.
    pub fn gate_action(&mut self, action: Gate) {
        match action {
            Gate::Open => self.open_gate(),
            Gate::Closed => self.close_gate(),
        }
    }

    /// Trigger the listeners for this buffer to wake up even if nothing in the
    /// buffer has changed. This could be used for timers or timeout elements
    /// in a workflow.
    pub fn pulse(&mut self) {
        self.modified = true;
    }
}

impl<'w, 's, 'a> Drop for JsonBufferMut<'w, 's, 'a> {
    fn drop(&mut self) {
        if self.modified {
            self.commands.add(NotifyBufferUpdate::new(
                self.buffer,
                self.session,
                self.accessor,
            ));
        }
    }
}

pub trait JsonBufferWorldAccess {
    fn json_buffer_mut<U>(
        &mut self,
        key: &JsonBufferKey,
        f: impl FnOnce(JsonBufferMut) -> U,
    ) -> Result<U, BufferError>;
}

impl JsonBufferWorldAccess for World {
    fn json_buffer_mut<U>(
        &mut self,
        key: &JsonBufferKey,
        f: impl FnOnce(JsonBufferMut) -> U,
    ) -> Result<U, BufferError> {
        let interface = key.interface;
        let mut state = interface.create_json_buffer_access_mut_state(self);
        let mut access = state.get_json_buffer_access_mut(self);
        let buffer_mut = access.as_json_buffer_mut(key)?;
        Ok(f(buffer_mut))
    }
}

///  View or modify a buffer message in terms of JSON values.
pub struct JsonMut<'a> {
    interface: &'a mut dyn JsonMutInterface,
    modified: &'a mut bool,
}

impl<'a> JsonMut<'a> {
    /// Serialize the message within the buffer into JSON.
    ///
    /// This new [`JsonMessage`] will be a duplicate of the data of the message
    /// inside the buffer, effectively meaning this function clones the data.
    pub fn serialize(&self) -> Result<JsonMessage, serde_json::Error> {
        self.interface.serialize()
    }

    /// This will first serialize the message within the buffer into JSON and
    /// then attempt to deserialize it into the target type.
    ///
    /// The target type does not need to match the message type inside the buffer,
    /// as long as the target type can be deserialized from a serialized value
    /// of the buffer's message type.
    ///
    /// The returned value will duplicate the data of the message inside the
    /// buffer, effectively meaning this function clones the data.
    pub fn deserialize_into<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value::<T>(self.serialize()?)
    }

    /// Replace the underlying message with new data, and receive its original
    /// data as JSON.
    #[must_use = "if you are going to discard the returned message, use insert instead"]
    pub fn replace(&mut self, message: JsonMessage) -> JsonMessageReplaceResult {
        *self.modified = true;
        self.interface.replace(message)
    }

    /// Insert new data into the underyling message. This is the same as replace
    /// except it is more efficient if you don't care about the original data,
    /// because it will discard the original data instead of serializing it.
    pub fn insert(&mut self, message: JsonMessage) -> Result<(), serde_json::Error> {
        *self.modified = true;
        self.interface.insert(message)
    }
}

/// The return type for functions that give a JSON view of a message in a buffer.
/// If an error occurs while attempting to serialize the message, this will return
/// [`Err`].
///
/// If this returns [`Ok`] then [`None`] means there was no message available at
/// the requested location while [`Some`] will contain a serialized copy of the
/// message.
pub type JsonMessageViewResult = Result<Option<JsonMessage>, serde_json::Error>;

/// The return type for functions that push a new message into a buffer. If an
/// error occurs while deserializing the message into the buffer's message type
/// then this will return [`Err`].
///
/// If this returns [`Ok`] then [`None`] means the new message was added and all
/// prior messages have been retained in the buffer. [`Some`] will contain an
/// old message which has now been removed from the buffer.
pub type JsonMessagePushResult = Result<Option<JsonMessage>, serde_json::Error>;

/// The return type for functions that replace (swap out) one message with
/// another. If an error occurs while serializing or deserializing either
/// message to/from the buffer's message type then this will return [`Err`].
///
/// If this returns [`Ok`] then the message was successfully replaced, and the
/// value inside [`Ok`] is the message that was previously in the buffer.
pub type JsonMessageReplaceResult = Result<JsonMessage, serde_json::Error>;

trait JsonBufferViewing {
    fn json_count(&self, session: Entity) -> usize;
    fn json_oldest<'a>(&'a self, session: Entity) -> JsonMessageViewResult;
    fn json_newest<'a>(&'a self, session: Entity) -> JsonMessageViewResult;
    fn json_get<'a>(&'a self, session: Entity, index: usize) ->JsonMessageViewResult;
}

trait JsonBufferManagement: JsonBufferViewing {
    fn json_push(&mut self, session: Entity, value: JsonMessage) -> JsonMessagePushResult;
    fn json_push_as_oldest(&mut self, session: Entity, value: JsonMessage) -> JsonMessagePushResult;
    fn json_pull(&mut self, session: Entity) -> JsonMessageViewResult;
    fn json_pull_newest(&mut self, session: Entity) -> JsonMessageViewResult;
    fn json_oldest_mut<'a>(&'a mut self, session: Entity, modified: &'a mut bool) -> Option<JsonMut<'a>>;
    fn json_newest_mut<'a>(&'a mut self, session: Entity, modified: &'a mut bool) -> Option<JsonMut<'a>>;
    fn json_get_mut<'a>(&'a mut self, session: Entity, index: usize, modified: &'a mut bool) -> Option<JsonMut<'a>>;
    fn json_drain<'a>(&'a mut self, session: Entity, range: AnyRange) -> Box<dyn DrainJsonBufferInterface + 'a>;
}

impl<T> JsonBufferViewing for Mut<'_, BufferStorage<T>>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    fn json_count(&self, session: Entity) -> usize {
        self.count(session)
    }

    fn json_oldest<'a>(&'a self, session: Entity) -> JsonMessageViewResult {
        self.oldest(session).map(serde_json::to_value).transpose()
    }

    fn json_newest<'a>(&'a self, session: Entity) -> JsonMessageViewResult {
        self.newest(session).map(serde_json::to_value).transpose()
    }

    fn json_get<'a>(&'a self, session: Entity, index: usize) ->JsonMessageViewResult {
        self.get(session, index).map(serde_json::to_value).transpose()
    }
}

impl<T> JsonBufferManagement for Mut<'_, BufferStorage<T>>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    fn json_push(&mut self, session: Entity, value: JsonMessage) -> JsonMessagePushResult {
        let value: T = serde_json::from_value(value)?;
        self.push(session, value).map(serde_json::to_value).transpose()
    }

    fn json_push_as_oldest(&mut self, session: Entity, value: JsonMessage) -> JsonMessagePushResult {
        let value: T = serde_json::from_value(value)?;
        self.push(session, value).map(serde_json::to_value).transpose()
    }

    fn json_pull(&mut self, session: Entity) -> JsonMessageViewResult {
        self.pull(session).map(serde_json::to_value).transpose()
    }

    fn json_pull_newest(&mut self, session: Entity) -> JsonMessageViewResult {
        self.pull_newest(session).map(serde_json::to_value).transpose()
    }

    fn json_oldest_mut<'a>(&'a mut self, session: Entity, modified: &'a mut bool) -> Option<JsonMut<'a>> {
        self.oldest_mut(session).map(|interface| JsonMut { interface, modified })
    }

    fn json_newest_mut<'a>(&'a mut self, session: Entity, modified: &'a mut bool) -> Option<JsonMut<'a>> {
        self.newest_mut(session).map(|interface| JsonMut { interface, modified })
    }

    fn json_get_mut<'a>(&'a mut self, session: Entity, index: usize, modified: &'a mut bool) -> Option<JsonMut<'a>> {
        self.get_mut(session, index).map(|interface| JsonMut { interface, modified })
    }

    fn json_drain<'a>(&'a mut self, session: Entity, range: AnyRange) -> Box<dyn DrainJsonBufferInterface + 'a> {
        Box::new(self.drain(session, range))
    }
}

trait JsonMutInterface {
    /// Serialize the underlying message into JSON
    fn serialize(&self) -> Result<JsonMessage, serde_json::Error>;
    /// Replace the underlying message with new data, and receive its original
    /// data as JSON
    fn replace(&mut self, message: JsonMessage) -> JsonMessageReplaceResult;
    /// Insert new data into the underyling message. This is the same as replace
    /// except it is more efficient if you don't care about the original data,
    /// because it will discard the original data instead of serializing it.
    fn insert(&mut self, message: JsonMessage) -> Result<(), serde_json::Error>;
}

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned> JsonMutInterface for T {
    fn serialize(&self) -> Result<JsonMessage, serde_json::Error> {
        serde_json::to_value(self)
    }

    fn replace(&mut self, message: JsonMessage) -> JsonMessageReplaceResult {
        let new_message: T = serde_json::from_value(message)?;
        let old_message = serde_json::to_value(&self)?;
        *self = new_message;
        Ok(old_message)
    }

    fn insert(&mut self, message: JsonMessage) -> Result<(), serde_json::Error> {
        let new_message: T = serde_json::from_value(message)?;
        *self = new_message;
        Ok(())
    }
}

trait JsonBufferAccessInterface {
    fn any_access_interface(&self) -> &'static (dyn AnyBufferAccessInterface + Send + Sync);

    fn buffered_count(
        &self,
        buffer_ref: &EntityRef,
        session: Entity,
    ) -> Result<usize, OperationError>;

    fn ensure_session(
        &self,
        buffer_mut: &mut EntityWorldMut,
        session: Entity,
    ) -> OperationResult;

    fn pull(
        &self,
        buffer_mut: &mut EntityWorldMut,
        session: Entity,
    ) -> Result<JsonMessage, OperationError>;

    fn create_json_buffer_access_mut_state(
        &self,
        world: &mut World,
    ) -> Box<dyn JsonBufferAccessMutState>;
}

impl<'a> std::fmt::Debug for &'a (dyn JsonBufferAccessInterface + Send + Sync) {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("Message Properties")
            .field("type", &self.any_access_interface().message_type_name())
            .finish()
    }
}

struct JsonBufferAccessImpl<T>(std::marker::PhantomData<T>);

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned> JsonBufferAccessImpl<T> {
    pub(crate) fn get_interface() -> &'static (dyn JsonBufferAccessInterface + Send + Sync) {
        const INTERFACE_MAP: OnceLock<Mutex<HashMap<
            TypeId,
            &'static (dyn JsonBufferAccessInterface + Send + Sync)
        >>> = OnceLock::new();
        let binding = INTERFACE_MAP;

        let interfaces = binding.get_or_init(|| Mutex::default());

        let mut interfaces_mut = interfaces.lock().unwrap();
        *interfaces_mut.entry(TypeId::of::<T>()).or_insert_with(|| {
            Box::leak(Box::new(JsonBufferAccessImpl::<T>(Default::default())))
        })
    }
}

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned> JsonBufferAccessInterface for JsonBufferAccessImpl<T> {
    fn any_access_interface(&self) -> &'static (dyn AnyBufferAccessInterface + Send + Sync) {
        AnyBufferAccessImpl::<T>::get_interface()
    }

    fn buffered_count(
        &self,
        buffer_ref: &EntityRef,
        session: Entity,
    ) -> Result<usize, OperationError> {
        buffer_ref.buffered_count::<T>(session)
    }

    fn ensure_session(
        &self,
        buffer_mut: &mut EntityWorldMut,
        session: Entity,
    ) -> OperationResult {
        buffer_mut.ensure_session::<T>(session)
    }

    fn pull(
        &self,
        buffer_mut: &mut EntityWorldMut,
        session: Entity,
    ) -> Result<JsonMessage, OperationError> {
        let value = buffer_mut.pull_from_buffer::<T>(session)?;
        serde_json::to_value(value).or_broken()
    }

    fn create_json_buffer_access_mut_state(
        &self,
        world: &mut World,
    ) -> Box<dyn JsonBufferAccessMutState> {
        Box::new(SystemState::<BufferAccessMut<T>>::new(world))
    }
}

trait JsonBufferAccessMutState {
    fn get_json_buffer_access_mut<'s, 'w: 's>(&'s mut self, world: &'w mut World) -> Box<dyn JsonBufferAccessMut<'w, 's> + 's>;
}

impl<T> JsonBufferAccessMutState for SystemState<BufferAccessMut<'static, 'static, T>>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    fn get_json_buffer_access_mut<'s, 'w: 's>(&'s mut self, world: &'w mut World) -> Box<dyn JsonBufferAccessMut<'w, 's> + 's> {
        Box::new(self.get_mut(world))
    }
}

trait JsonBufferAccessMut<'w, 's> {
    fn as_json_buffer_mut<'a>(&'a mut self, key: &JsonBufferKey) -> Result<JsonBufferMut<'w, 's ,'a>, BufferError>;
}

impl<'w, 's, T> JsonBufferAccessMut<'w, 's> for BufferAccessMut<'w, 's, T>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    fn as_json_buffer_mut<'a>(&'a mut self, key: &JsonBufferKey) -> Result<JsonBufferMut<'w, 's ,'a>, BufferError> {
        let BufferAccessMut { query, commands } = self;
        let (storage, gate) = query.get_mut(key.buffer).map_err(|_| BufferError::BufferMissing)?;
        Ok(JsonBufferMut {
            storage: Box::new(storage),
            gate,
            buffer: key.buffer,
            session: key.session,
            accessor: Some(key.accessor),
            commands,
            modified: false,
        })
    }
}

pub struct DrainJsonBuffer<'a> {
    interface: Box<dyn DrainJsonBufferInterface + 'a>,
}

impl<'a> Iterator for DrainJsonBuffer<'a> {
    type Item = Result<JsonMessage, serde_json::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.interface.json_next()
    }
}

trait DrainJsonBufferInterface {
    fn json_next(&mut self) -> Option<Result<JsonMessage, serde_json::Error>>;
}

impl<T: 'static + Send + Sync + Serialize> DrainJsonBufferInterface for DrainBuffer<'_, T> {
    fn json_next(&mut self) -> Option<Result<JsonMessage, serde_json::Error>> {
        self.next().map(serde_json::to_value)
    }
}

impl Bufferable for JsonBuffer {
    type BufferType = Self;
    fn into_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope, builder.scope());
        self
    }
}

impl Buffered for JsonBuffer {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope);
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        let buffer_ref = world.get_entity(self.source).or_broken()?;
        self.interface.buffered_count(&buffer_ref, session)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        add_listener_to_source(self.source, listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut crate::OperationRoster,
    ) -> OperationResult {
        GateState::apply(self.source, session, action, world, roster)
    }

    fn as_input(&self) -> smallvec::SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.source])
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        let mut buffer_mut = world.get_entity_mut(self.source).or_broken()?;
        self.interface.ensure_session(&mut buffer_mut, session)
    }
}

impl Joined for JsonBuffer {
    type Item = JsonMessage;
    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        let mut buffer_mut = world.get_entity_mut(self.source).or_broken()?;
        self.interface.pull(&mut buffer_mut, session)
    }
}

impl Accessed for JsonBuffer {
    type Key = JsonBufferKey;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        world
            .get_mut::<BufferAccessors>(self.source)
            .or_broken()?
            .add_accessor(accessor);
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        let components = builder.as_components(self.source);
        JsonBufferKey {
            buffer: components.buffer,
            session: components.session,
            accessor: components.accessor,
            lifecycle: components.lifecycle,
            interface: self.interface,
        }
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        key.deep_clone()
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        key.is_in_use()
    }
}
