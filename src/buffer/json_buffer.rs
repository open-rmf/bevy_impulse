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
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use bevy_ecs::{
    prelude::{Commands, Entity, EntityRef, EntityWorldMut, Mut, World},
    system::SystemState,
};

use serde::{de::DeserializeOwned, Serialize};

use serde_json::Value as JsonMessage;

use thiserror::Error as ThisError;

use crate::{
    AnyRange, BufferKey, BufferAccessLifecycle, BufferAccessMut, BufferError, BufferStorage,
    DrainBuffer, OperationError, OperationResult, InspectBuffer, ManageBuffer, GateState,
};

#[derive(Clone, Copy, Debug)]
pub struct JsonBuffer {
    pub(crate) scope: Entity,
    pub(crate) source: Entity,
    pub(crate) interface: &'static (dyn JsonBufferAccessInterface + Send + Sync),
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

impl std::fmt::Debug for JsonBufferKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("JsonBufferKey")
            .field("buffer", &self.buffer)
            .field("session", &self.session)
            .field("accessor", &self.accessor)
            .field("in_use", &self.lifecycle.as_ref().is_some_and(|l| l.is_in_use()))
            .field("message_type_name", &self.interface.message_type_name())
            .finish()
    }
}

pub struct JsonBufferMut<'w, 's, 'a> {
    storage: Box<dyn JsonBufferManagement + 'a>,
    gate: Mut<'a, GateState>,
    buffer: Entity,
    session: Entity,
    accessor: Option<Entity>,
    commands: &'a mut Commands<'w, 's>,
    modified: bool,
}

///  View or modify a buffer message in terms of JSON values.
pub struct JsonMut<'a> {
    interface: &'a mut dyn JsonMutInterface,
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
        self.interface.replace(message)
    }

    /// Insert new data into the underyling message. This is the same as replace
    /// except it is more efficient if you don't care about the original data,
    /// because it will discard the original data instead of serializing it.
    pub fn insert(&mut self, message: JsonMessage) -> Result<(), serde_json::Error> {
        self.interface.insert(message)
    }
}

pub type JsonMessageFetchResult = Result<Option<JsonMessage>, serde_json::Error>;
pub type JsonMessagePushResult = Result<Option<JsonMessage>, serde_json::Error>;
pub type JsonMessageReplaceResult = Result<JsonMessage, serde_json::Error>;

trait JsonBufferViewing {
    fn json_count(&self, session: Entity) -> usize;
    fn json_oldest<'a>(&'a self, session: Entity) -> JsonMessageFetchResult;
    fn json_newest<'a>(&'a self, session: Entity) -> JsonMessageFetchResult;
    fn json_get<'a>(&'a self, session: Entity, index: usize) ->JsonMessageFetchResult;
}

trait JsonBufferManagement: JsonBufferViewing {
    fn json_push(&mut self, session: Entity, value: JsonMessage) -> JsonMessagePushResult;
    fn json_push_as_oldest(&mut self, session: Entity, value: JsonMessage) -> JsonMessagePushResult;
    fn json_pull(&mut self, session: Entity) -> JsonMessageFetchResult;
    fn json_pull_newest(&mut self, session: Entity) -> JsonMessageFetchResult;
    fn json_oldest_mut<'a>(&'a mut self, session: Entity) -> Option<JsonMut<'a>>;
    fn json_newest_mut<'a>(&'a mut self, session: Entity) -> Option<JsonMut<'a>>;
    fn json_get_mut<'a>(&'a mut self, session: Entity, index: usize) -> Option<JsonMut<'a>>;
    fn json_drain<'a>(&'a mut self, session: Entity, range: AnyRange) -> Box<dyn DrainJsonBufferInterface + 'a>;
}

impl<T> JsonBufferViewing for Mut<'_, BufferStorage<T>>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    fn json_count(&self, session: Entity) -> usize {
        self.count(session)
    }

    fn json_oldest<'a>(&'a self, session: Entity) -> JsonMessageFetchResult {
        self.oldest(session).map(serde_json::to_value).transpose()
    }

    fn json_newest<'a>(&'a self, session: Entity) -> JsonMessageFetchResult {
        self.newest(session).map(serde_json::to_value).transpose()
    }

    fn json_get<'a>(&'a self, session: Entity, index: usize) ->JsonMessageFetchResult {
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

    fn json_pull(&mut self, session: Entity) -> JsonMessageFetchResult {
        self.pull(session).map(serde_json::to_value).transpose()
    }

    fn json_pull_newest(&mut self, session: Entity) -> JsonMessageFetchResult {
        self.pull_newest(session).map(serde_json::to_value).transpose()
    }

    fn json_oldest_mut<'a>(&'a mut self, session: Entity) -> Option<JsonMut<'a>> {
        self.oldest_mut(session).map(|interface| JsonMut { interface })
    }

    fn json_newest_mut<'a>(&'a mut self, session: Entity) -> Option<JsonMut<'a>> {
        self.newest_mut(session).map(|interface| JsonMut { interface })
    }

    fn json_get_mut<'a>(&'a mut self, session: Entity, index: usize) -> Option<JsonMut<'a>> {
        self.get_mut(session, index).map(|interface| JsonMut { interface })
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

pub(crate) trait JsonBufferAccessInterface {
    fn message_type_id(&self) -> TypeId;

    fn message_type_name(&self) -> &'static str;

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

    fn create_json_buffer_access_mut_state(
        &self,
        world: &mut World,
    ) -> Box<dyn JsonBufferAccessMutState>;
}

impl<'a> std::fmt::Debug for &'a (dyn JsonBufferAccessInterface + Send + Sync) {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("Message Properties")
            .field("type", &self.message_type_name())
            .finish()
    }
}

struct JsonBufferAccessImpl<T>(std::marker::PhantomData<T>);

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned> JsonBufferAccessImpl<T> {

}

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned> JsonBufferAccessInterface for JsonBufferAccessImpl<T> {
    fn message_type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    fn message_type_id(&self) -> TypeId {
        TypeId::of::<T>()
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

trait DrainJsonBufferInterface {
    fn json_next(&mut self) -> Option<Result<JsonMessage, serde_json::Error>>;
}

impl<T: 'static + Send + Sync + Serialize> DrainJsonBufferInterface for DrainBuffer<'_, T> {
    fn json_next(&mut self) -> Option<Result<JsonMessage, serde_json::Error>> {
        self.next().map(serde_json::to_value)
    }
}
