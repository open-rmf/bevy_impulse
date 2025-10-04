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

// TODO(@mxgrey): Add module-level documentation describing how to use AnyBuffer

use std::{
    any::{Any, TypeId},
    collections::{hash_map::Entry, HashMap},
    ops::RangeBounds,
    sync::{Mutex, OnceLock},
};

use bevy_ecs::{
    prelude::{Commands, Entity, EntityRef, EntityWorldMut, Mut, World},
    system::SystemState,
};

use thiserror::Error as ThisError;

use smallvec::SmallVec;

use crate::{
    add_listener_to_source, Accessing, Accessor, Buffer, BufferAccessMut, BufferAccessors,
    BufferError, BufferKey, BufferKeyBuilder, BufferKeyLifecycle, BufferKeyTag, BufferLocation,
    BufferMap, BufferMapLayout, BufferStorage, Bufferable, Buffering, Builder, DrainBuffer, Gate,
    GateState, IncompatibleLayout, InspectBuffer, Joining, ManageBuffer, NotifyBufferUpdate,
    OperationError, OperationResult, OperationRoster, OrBroken,
};

/// A [`Buffer`] whose message type has been anonymized. Joining with this buffer
/// type will yield an [`AnyMessageBox`].
#[derive(Clone, Copy)]
pub struct AnyBuffer {
    pub(crate) location: BufferLocation,
    pub(crate) interface: &'static (dyn AnyBufferAccessInterface + Send + Sync),
}

impl AnyBuffer {
    /// The buffer ID for this key.
    pub fn id(&self) -> Entity {
        self.location.source
    }

    /// ID of the workflow that this buffer is associated with.
    pub fn scope(&self) -> Entity {
        self.location.scope
    }

    /// Get the type ID of the messages that this buffer supports.
    pub fn message_type_id(&self) -> TypeId {
        self.interface.message_type_id()
    }

    pub fn message_type_name(&self) -> &'static str {
        self.interface.message_type_name()
    }

    /// Get the [`AnyBufferAccessInterface`] for this specific instance of [`AnyBuffer`].
    pub fn get_interface(&self) -> &'static (dyn AnyBufferAccessInterface + Send + Sync) {
        self.interface
    }

    /// Get the [`AnyBufferAccessInterface`] for a concrete message type.
    pub fn interface_for<T: 'static + Send + Sync>(
    ) -> &'static (dyn AnyBufferAccessInterface + Send + Sync) {
        static INTERFACE_MAP: OnceLock<
            Mutex<HashMap<TypeId, &'static (dyn AnyBufferAccessInterface + Send + Sync)>>,
        > = OnceLock::new();
        let interfaces = INTERFACE_MAP.get_or_init(|| Mutex::default());

        // SAFETY: This will leak memory exactly once per type, so the leakage is bounded.
        // Leaking this allows the interface to be shared freely across all instances.
        let mut interfaces_mut = interfaces.lock().unwrap();
        *interfaces_mut
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::leak(Box::new(AnyBufferAccessImpl::<T>::new())))
    }
}

impl std::fmt::Debug for AnyBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnyBuffer")
            .field("scope", &self.location.scope)
            .field("source", &self.location.source)
            .field("message_type_name", &self.interface.message_type_name())
            .finish()
    }
}

impl AnyBuffer {
    /// Downcast this into a concrete [`Buffer`] for the specified message type.
    ///
    /// To downcast into a specialized kind of buffer, use [`Self::downcast_buffer`] instead.
    pub fn downcast_for_message<Message: 'static>(&self) -> Option<Buffer<Message>> {
        if TypeId::of::<Message>() == self.interface.message_type_id() {
            Some(Buffer {
                location: self.location,
                _ignore: Default::default(),
            })
        } else {
            None
        }
    }

    /// Downcast this into a different special buffer representation, such as a
    /// `JsonBuffer`.
    pub fn downcast_buffer<BufferType: 'static>(&self) -> Option<BufferType> {
        self.interface.buffer_downcast(TypeId::of::<BufferType>())?(self.location)
            .downcast::<BufferType>()
            .ok()
            .map(|x| *x)
    }
}

impl<T: 'static + Send + Sync + Any> From<Buffer<T>> for AnyBuffer {
    fn from(value: Buffer<T>) -> Self {
        let interface = AnyBuffer::interface_for::<T>();
        AnyBuffer {
            location: value.location,
            interface,
        }
    }
}

/// A trait for turning a buffer into an [`AnyBuffer`]. It is expected that all
/// buffer types implement this trait.
pub trait AsAnyBuffer {
    /// Convert this buffer into an [`AnyBuffer`].
    fn as_any_buffer(&self) -> AnyBuffer;
}

impl AsAnyBuffer for AnyBuffer {
    fn as_any_buffer(&self) -> AnyBuffer {
        *self
    }
}

impl<T: 'static + Send + Sync> AsAnyBuffer for Buffer<T> {
    fn as_any_buffer(&self) -> AnyBuffer {
        (*self).into()
    }
}

/// Similar to a [`BufferKey`] except it can be used for any buffer without
/// knowing the buffer's message type at compile time.
///
/// This can key be used with a [`World`][1] to directly view or manipulate the
/// contents of a buffer through the [`AnyBufferWorldAccess`] interface.
///
/// [1]: bevy_ecs::prelude::World
#[derive(Clone)]
pub struct AnyBufferKey {
    pub(crate) tag: BufferKeyTag,
    pub(crate) interface: &'static (dyn AnyBufferAccessInterface + Send + Sync),
}

impl AnyBufferKey {
    /// Downcast this into a concrete [`BufferKey`] for the specified message type.
    ///
    /// To downcast to a specialized kind of key, use [`Self::downcast_buffer_key`] instead.
    pub fn downcast_for_message<Message: 'static>(self) -> Option<BufferKey<Message>> {
        if TypeId::of::<Message>() == self.interface.message_type_id() {
            Some(BufferKey {
                tag: self.tag,
                _ignore: Default::default(),
            })
        } else {
            None
        }
    }

    /// Downcast this into a different special buffer key representation, such
    /// as a `JsonBufferKey`.
    pub fn downcast_buffer_key<KeyType: 'static>(self) -> Option<KeyType> {
        self.interface.key_downcast(TypeId::of::<KeyType>())?(self.tag)
            .downcast::<KeyType>()
            .ok()
            .map(|x| *x)
    }

    /// The buffer ID of this key.
    pub fn id(&self) -> Entity {
        self.tag.buffer
    }

    /// The session that this key belongs to.
    pub fn session(&self) -> Entity {
        self.tag.session
    }
}

impl BufferKeyLifecycle for AnyBufferKey {
    type TargetBuffer = AnyBuffer;

    fn create_key(buffer: &AnyBuffer, builder: &BufferKeyBuilder) -> Self {
        AnyBufferKey {
            tag: builder.make_tag(buffer.id()),
            interface: buffer.interface,
        }
    }

    fn is_in_use(&self) -> bool {
        self.tag.is_in_use()
    }

    fn deep_clone(&self) -> Self {
        Self {
            tag: self.tag.deep_clone(),
            interface: self.interface,
        }
    }
}

impl std::fmt::Debug for AnyBufferKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnyBufferKey")
            .field("message_type_name", &self.interface.message_type_name())
            .field("tag", &self.tag)
            .finish()
    }
}

impl<T: 'static + Send + Sync + Any> From<BufferKey<T>> for AnyBufferKey {
    fn from(value: BufferKey<T>) -> Self {
        let interface = AnyBuffer::interface_for::<T>();
        AnyBufferKey {
            tag: value.tag,
            interface,
        }
    }
}

impl Accessor for AnyBufferKey {
    type Buffers = AnyBuffer;
}

impl BufferMapLayout for AnyBuffer {
    fn try_from_buffer_map(buffers: &BufferMap) -> Result<Self, IncompatibleLayout> {
        let mut compatibility = IncompatibleLayout::default();

        if let Ok(any_buffer) = compatibility.require_buffer_for_identifier::<AnyBuffer>(0, buffers)
        {
            return Ok(any_buffer);
        }

        Err(compatibility)
    }
}

/// Similar to [`BufferView`][crate::BufferView], but this can be unlocked with
/// an [`AnyBufferKey`], so it can work for any buffer whose message types
/// support serialization and deserialization.
pub struct AnyBufferView<'a> {
    storage: Box<dyn AnyBufferViewing + 'a>,
    gate: &'a GateState,
    session: Entity,
}

impl<'a> AnyBufferView<'a> {
    /// Look at the oldest message in the buffer.
    pub fn oldest(&self) -> Option<AnyMessageRef<'_>> {
        self.storage.any_oldest(self.session)
    }

    /// Look at the newest message in the buffer.
    pub fn newest(&self) -> Option<AnyMessageRef<'_>> {
        self.storage.any_newest(self.session)
    }

    /// Borrow a message from the buffer. Index 0 is the oldest message in the buffer
    /// while the highest index is the newest message in the buffer.
    pub fn get(&self, index: usize) -> Option<AnyMessageRef<'_>> {
        self.storage.any_get(self.session, index)
    }

    /// Get how many messages are in this buffer.
    pub fn len(&self) -> usize {
        self.storage.any_count(self.session)
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
}

/// Similar to [`BufferMut`][crate::BufferMut], but this can be unlocked with an
/// [`AnyBufferKey`], so it can work for any buffer regardless of the data type
/// inside.
pub struct AnyBufferMut<'w, 's, 'a> {
    storage: Box<dyn AnyBufferManagement + 'a>,
    buffer: Entity,
    session: Entity,
    accessor: Option<Entity>,
    commands: &'a mut Commands<'w, 's>,
    modified: bool,
}

impl<'w, 's, 'a> AnyBufferMut<'w, 's, 'a> {
    /// Same as [BufferMut::allow_closed_loops][1].
    ///
    /// [1]: crate::BufferMut::allow_closed_loops
    pub fn allow_closed_loops(mut self) -> Self {
        self.accessor = None;
        self
    }

    /// Look at the oldest message in the buffer.
    pub fn oldest(&self) -> Option<AnyMessageRef<'_>> {
        self.storage.any_oldest(self.session)
    }

    /// Look at the newest message in the buffer.
    pub fn newest(&self) -> Option<AnyMessageRef<'_>> {
        self.storage.any_newest(self.session)
    }

    /// Borrow a message from the buffer. Index 0 is the oldest message in the buffer
    /// while the highest index is the newest message in the buffer.
    pub fn get(&self, index: usize) -> Option<AnyMessageRef<'_>> {
        self.storage.any_get(self.session, index)
    }

    /// Get how many messages are in this buffer.
    pub fn len(&self) -> usize {
        self.storage.any_count(self.session)
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Modify the oldest message in the buffer.
    pub fn oldest_mut(&mut self) -> Option<AnyMessageMut<'_>> {
        self.modified = true;
        self.storage.any_oldest_mut(self.session)
    }

    /// Modify the newest message in the buffer.
    pub fn newest_mut(&mut self) -> Option<AnyMessageMut<'_>> {
        self.modified = true;
        self.storage.any_newest_mut(self.session)
    }

    /// Modify a message in the buffer. Index 0 is the oldest message in the buffer
    /// with the highest index being the newest message in the buffer.
    pub fn get_mut(&mut self, index: usize) -> Option<AnyMessageMut<'_>> {
        self.modified = true;
        self.storage.any_get_mut(self.session, index)
    }

    /// Drain a range of messages out of the buffer.
    pub fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> DrainAnyBuffer<'_> {
        self.modified = true;
        DrainAnyBuffer {
            interface: self.storage.any_drain(self.session, AnyRange::new(range)),
        }
    }

    /// Pull the oldest message from the buffer.
    pub fn pull(&mut self) -> Option<AnyMessageBox> {
        self.modified = true;
        self.storage.any_pull(self.session)
    }

    /// Pull the message that was most recently put into the buffer (instead of the
    /// oldest, which is what [`Self::pull`] gives).
    pub fn pull_newest(&mut self) -> Option<AnyMessageBox> {
        self.modified = true;
        self.storage.any_pull_newest(self.session)
    }

    /// Attempt to push a new value into the buffer.
    ///
    /// If the input value matches the message type of the buffer, this will
    /// return [`Ok`]. If the buffer is at its limit before a successful push, this
    /// will return the value that needed to be removed.
    ///
    /// If the input value does not match the message type of the buffer, this
    /// will return [`Err`] and give back the message that you tried to push.
    pub fn push<T: 'static + Send + Sync + Any>(&mut self, value: T) -> Result<Option<T>, T> {
        if TypeId::of::<T>() != self.storage.any_message_type() {
            return Err(value);
        }

        self.modified = true;

        // SAFETY: We checked that T matches the message type for this buffer,
        // so pushing and downcasting should not exhibit any errors.
        let removed = self
            .storage
            .any_push(self.session, Box::new(value))
            .unwrap()
            .map(|value| *value.downcast::<T>().unwrap());

        Ok(removed)
    }

    /// Attempt to push a new value of any message type into the buffer.
    ///
    /// If the input value matches the message type of the buffer, this will
    /// return [`Ok`]. If the buffer is at its limit before a successful push, this
    /// will return the value that needed to be removed.
    ///
    /// If the input value does not match the message type of the buffer, this
    /// will return [`Err`] and give back an error with the message that you
    /// tried to push and the type information for the expected message type.
    pub fn push_any(
        &mut self,
        value: AnyMessageBox,
    ) -> Result<Option<AnyMessageBox>, AnyMessageError> {
        self.storage.any_push(self.session, value)
    }

    /// Attempt to push a value into the buffer as if it is the oldest value of
    /// the buffer.
    ///
    /// The result follows the same rules as [`Self::push`].
    pub fn push_as_oldest<T: 'static + Send + Sync + Any>(
        &mut self,
        value: T,
    ) -> Result<Option<T>, T> {
        if TypeId::of::<T>() != self.storage.any_message_type() {
            return Err(value);
        }

        self.modified = true;

        // SAFETY: We checked that T matches the message type for this buffer,
        // so pushing and downcasting should not exhibit any errors.
        let removed = self
            .storage
            .any_push_as_oldest(self.session, Box::new(value))
            .unwrap()
            .map(|value| *value.downcast::<T>().unwrap());

        Ok(removed)
    }

    /// Attempt to push a value into the buffer as if it is the oldest value of
    /// the buffer.
    ///
    /// The result follows the same rules as [`Self::push_any`].
    pub fn push_any_as_oldest(
        &mut self,
        value: AnyMessageBox,
    ) -> Result<Option<AnyMessageBox>, AnyMessageError> {
        self.storage.any_push_as_oldest(self.session, value)
    }

    /// Trigger the listeners for this buffer to wake up even if nothing in the
    /// buffer has changed. This could be used for timers or timeout elements
    /// in a workflow.
    pub fn pulse(&mut self) {
        self.modified = true;
    }
}

impl<'w, 's, 'a> Drop for AnyBufferMut<'w, 's, 'a> {
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

/// This trait allows [`World`] to give you access to any buffer using an
/// [`AnyBufferKey`].
pub trait AnyBufferWorldAccess {
    /// Call this to get read-only access to any buffer.
    ///
    /// For technical reasons this requires direct [`World`] access, but you can
    /// do other read-only queries on the world while holding onto the
    /// [`AnyBufferView`].
    fn any_buffer_view(&self, key: &AnyBufferKey) -> Result<AnyBufferView<'_>, BufferError>;

    /// Call this to get mutable access to any buffer.
    ///
    /// Pass in a callback that will receive a [`AnyBufferMut`], allowing it to
    /// view and modify the contents of the buffer.
    fn any_buffer_mut<U>(
        &mut self,
        key: &AnyBufferKey,
        f: impl FnOnce(AnyBufferMut) -> U,
    ) -> Result<U, BufferError>;
}

impl AnyBufferWorldAccess for World {
    fn any_buffer_view(&self, key: &AnyBufferKey) -> Result<AnyBufferView<'_>, BufferError> {
        key.interface.create_any_buffer_view(key, self)
    }

    fn any_buffer_mut<U>(
        &mut self,
        key: &AnyBufferKey,
        f: impl FnOnce(AnyBufferMut) -> U,
    ) -> Result<U, BufferError> {
        let interface = key.interface;
        let mut state = interface.create_any_buffer_access_mut_state(self);
        let mut access = state.get_any_buffer_access_mut(self);
        let buffer_mut = access.as_any_buffer_mut(key)?;
        Ok(f(buffer_mut))
    }
}

trait AnyBufferViewing {
    fn any_count(&self, session: Entity) -> usize;
    fn any_oldest<'a>(&'a self, session: Entity) -> Option<AnyMessageRef<'a>>;
    fn any_newest<'a>(&'a self, session: Entity) -> Option<AnyMessageRef<'a>>;
    fn any_get<'a>(&'a self, session: Entity, index: usize) -> Option<AnyMessageRef<'a>>;
    fn any_message_type(&self) -> TypeId;
}

trait AnyBufferManagement: AnyBufferViewing {
    fn any_push(&mut self, session: Entity, value: AnyMessageBox) -> AnyMessagePushResult;
    fn any_push_as_oldest(&mut self, session: Entity, value: AnyMessageBox)
        -> AnyMessagePushResult;
    fn any_pull(&mut self, session: Entity) -> Option<AnyMessageBox>;
    fn any_pull_newest(&mut self, session: Entity) -> Option<AnyMessageBox>;
    fn any_oldest_mut<'a>(&'a mut self, session: Entity) -> Option<AnyMessageMut<'a>>;
    fn any_newest_mut<'a>(&'a mut self, session: Entity) -> Option<AnyMessageMut<'a>>;
    fn any_get_mut<'a>(&'a mut self, session: Entity, index: usize) -> Option<AnyMessageMut<'a>>;
    fn any_drain<'a>(
        &'a mut self,
        session: Entity,
        range: AnyRange,
    ) -> Box<dyn DrainAnyBufferInterface + 'a>;
}

pub(crate) struct AnyRange {
    start_bound: std::ops::Bound<usize>,
    end_bound: std::ops::Bound<usize>,
}

impl AnyRange {
    pub(crate) fn new<T: std::ops::RangeBounds<usize>>(range: T) -> Self {
        AnyRange {
            start_bound: deref_bound(range.start_bound()),
            end_bound: deref_bound(range.end_bound()),
        }
    }
}

fn deref_bound(bound: std::ops::Bound<&usize>) -> std::ops::Bound<usize> {
    match bound {
        std::ops::Bound::Included(v) => std::ops::Bound::Included(*v),
        std::ops::Bound::Excluded(v) => std::ops::Bound::Excluded(*v),
        std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
    }
}

impl std::ops::RangeBounds<usize> for AnyRange {
    fn start_bound(&self) -> std::ops::Bound<&usize> {
        self.start_bound.as_ref()
    }

    fn end_bound(&self) -> std::ops::Bound<&usize> {
        self.end_bound.as_ref()
    }

    fn contains<U>(&self, item: &U) -> bool
    where
        usize: PartialOrd<U>,
        U: ?Sized + PartialOrd<usize>,
    {
        match self.start_bound {
            std::ops::Bound::Excluded(lower) => {
                if *item <= lower {
                    return false;
                }
            }
            std::ops::Bound::Included(lower) => {
                if *item < lower {
                    return false;
                }
            }
            _ => {}
        }

        match self.end_bound {
            std::ops::Bound::Excluded(upper) => {
                if upper <= *item {
                    return false;
                }
            }
            std::ops::Bound::Included(upper) => {
                if upper < *item {
                    return false;
                }
            }
            _ => {}
        }

        return true;
    }
}

pub type AnyMessageRef<'a> = &'a (dyn Any + 'static + Send + Sync);

impl<T: 'static + Send + Sync + Any> AnyBufferViewing for &'_ BufferStorage<T> {
    fn any_count(&self, session: Entity) -> usize {
        self.count(session)
    }

    fn any_oldest<'a>(&'a self, session: Entity) -> Option<AnyMessageRef<'a>> {
        self.oldest(session).map(to_any_ref)
    }

    fn any_newest<'a>(&'a self, session: Entity) -> Option<AnyMessageRef<'a>> {
        self.newest(session).map(to_any_ref)
    }

    fn any_get<'a>(&'a self, session: Entity, index: usize) -> Option<AnyMessageRef<'a>> {
        self.get(session, index).map(to_any_ref)
    }

    fn any_message_type(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

impl<T: 'static + Send + Sync + Any> AnyBufferViewing for Mut<'_, BufferStorage<T>> {
    fn any_count(&self, session: Entity) -> usize {
        self.count(session)
    }

    fn any_oldest<'a>(&'a self, session: Entity) -> Option<AnyMessageRef<'a>> {
        self.oldest(session).map(to_any_ref)
    }

    fn any_newest<'a>(&'a self, session: Entity) -> Option<AnyMessageRef<'a>> {
        self.newest(session).map(to_any_ref)
    }

    fn any_get<'a>(&'a self, session: Entity, index: usize) -> Option<AnyMessageRef<'a>> {
        self.get(session, index).map(to_any_ref)
    }

    fn any_message_type(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

pub type AnyMessageMut<'a> = &'a mut (dyn Any + 'static + Send + Sync);

pub type AnyMessageBox = Box<dyn Any + 'static + Send + Sync>;

#[derive(ThisError, Debug)]
#[error("failed to convert a message")]
pub struct AnyMessageError {
    /// The original value provided
    pub value: AnyMessageBox,
    /// The ID of the type expected by the buffer
    pub type_id: TypeId,
    /// The name of the type expected by the buffer
    pub type_name: &'static str,
}

pub type AnyMessagePushResult = Result<Option<AnyMessageBox>, AnyMessageError>;

impl<T: 'static + Send + Sync + Any> AnyBufferManagement for Mut<'_, BufferStorage<T>> {
    fn any_push(&mut self, session: Entity, value: AnyMessageBox) -> AnyMessagePushResult {
        let value = from_any_message::<T>(value)?;
        Ok(self.push(session, value).map(to_any_message))
    }

    fn any_push_as_oldest(
        &mut self,
        session: Entity,
        value: AnyMessageBox,
    ) -> AnyMessagePushResult {
        let value = from_any_message::<T>(value)?;
        Ok(self.push_as_oldest(session, value).map(to_any_message))
    }

    fn any_pull(&mut self, session: Entity) -> Option<AnyMessageBox> {
        self.pull(session).map(to_any_message)
    }

    fn any_pull_newest(&mut self, session: Entity) -> Option<AnyMessageBox> {
        self.pull_newest(session).map(to_any_message)
    }

    fn any_oldest_mut<'a>(&'a mut self, session: Entity) -> Option<AnyMessageMut<'a>> {
        self.oldest_mut(session).map(to_any_mut)
    }

    fn any_newest_mut<'a>(&'a mut self, session: Entity) -> Option<AnyMessageMut<'a>> {
        self.newest_mut(session).map(to_any_mut)
    }

    fn any_get_mut<'a>(&'a mut self, session: Entity, index: usize) -> Option<AnyMessageMut<'a>> {
        self.get_mut(session, index).map(to_any_mut)
    }

    fn any_drain<'a>(
        &'a mut self,
        session: Entity,
        range: AnyRange,
    ) -> Box<dyn DrainAnyBufferInterface + 'a> {
        Box::new(self.drain(session, range))
    }
}

fn to_any_ref<'a, T: 'static + Send + Sync + Any>(x: &'a T) -> AnyMessageRef<'a> {
    x
}

fn to_any_mut<'a, T: 'static + Send + Sync + Any>(x: &'a mut T) -> AnyMessageMut<'a> {
    x
}

fn to_any_message<T: 'static + Send + Sync + Any>(x: T) -> AnyMessageBox {
    Box::new(x)
}

fn from_any_message<T: 'static + Send + Sync + Any>(
    value: AnyMessageBox,
) -> Result<T, AnyMessageError>
where
    T: 'static,
{
    let value = value.downcast::<T>().map_err(|value| AnyMessageError {
        value,
        type_id: TypeId::of::<T>(),
        type_name: std::any::type_name::<T>(),
    })?;

    Ok(*value)
}

pub trait AnyBufferAccessMutState {
    fn get_any_buffer_access_mut<'s, 'w: 's>(
        &'s mut self,
        world: &'w mut World,
    ) -> Box<dyn AnyBufferAccessMut<'w, 's> + 's>;
}

impl<T: 'static + Send + Sync + Any> AnyBufferAccessMutState
    for SystemState<BufferAccessMut<'static, 'static, T>>
{
    fn get_any_buffer_access_mut<'s, 'w: 's>(
        &'s mut self,
        world: &'w mut World,
    ) -> Box<dyn AnyBufferAccessMut<'w, 's> + 's> {
        Box::new(self.get_mut(world))
    }
}

pub trait AnyBufferAccessMut<'w, 's> {
    fn as_any_buffer_mut<'a>(
        &'a mut self,
        key: &AnyBufferKey,
    ) -> Result<AnyBufferMut<'w, 's, 'a>, BufferError>;
}

impl<'w, 's, T: 'static + Send + Sync + Any> AnyBufferAccessMut<'w, 's>
    for BufferAccessMut<'w, 's, T>
{
    fn as_any_buffer_mut<'a>(
        &'a mut self,
        key: &AnyBufferKey,
    ) -> Result<AnyBufferMut<'w, 's, 'a>, BufferError> {
        let BufferAccessMut { query, commands } = self;
        let storage = query
            .get_mut(key.tag.buffer)
            .map_err(|_| BufferError::BufferMissing)?;

        Ok(AnyBufferMut {
            storage: Box::new(storage),
            buffer: key.tag.buffer,
            session: key.tag.session,
            accessor: Some(key.tag.accessor),
            commands,
            modified: false,
        })
    }
}

pub trait AnyBufferAccessInterface {
    fn message_type_id(&self) -> TypeId;

    fn message_type_name(&self) -> &'static str;

    fn buffered_count(&self, entity: &EntityRef, session: Entity) -> Result<usize, OperationError>;

    fn ensure_session(&self, entity_mut: &mut EntityWorldMut, session: Entity) -> OperationResult;

    fn register_buffer_downcast(&self, buffer_type: TypeId, f: BufferDowncastBox);

    fn buffer_downcast(&self, buffer_type: TypeId) -> Option<BufferDowncastRef>;

    fn register_key_downcast(&self, key_type: TypeId, f: KeyDowncastBox);

    fn key_downcast(&self, key_type: TypeId) -> Option<KeyDowncastRef>;

    fn pull(
        &self,
        entity_mut: &mut EntityWorldMut,
        session: Entity,
    ) -> Result<AnyMessageBox, OperationError>;

    fn create_any_buffer_view<'a>(
        &self,
        key: &AnyBufferKey,
        world: &'a World,
    ) -> Result<AnyBufferView<'a>, BufferError>;

    fn create_any_buffer_access_mut_state(
        &self,
        world: &mut World,
    ) -> Box<dyn AnyBufferAccessMutState>;
}

pub type BufferDowncastBox = Box<dyn Fn(BufferLocation) -> Box<dyn Any> + Send + Sync>;
pub type BufferDowncastRef = &'static (dyn Fn(BufferLocation) -> Box<dyn Any> + Send + Sync);
pub type KeyDowncastBox = Box<dyn Fn(BufferKeyTag) -> Box<dyn Any> + Send + Sync>;
pub type KeyDowncastRef = &'static (dyn Fn(BufferKeyTag) -> Box<dyn Any> + Send + Sync);

struct AnyBufferAccessImpl<T> {
    buffer_downcasts: Mutex<HashMap<TypeId, BufferDowncastRef>>,
    key_downcasts: Mutex<HashMap<TypeId, KeyDowncastRef>>,
    _ignore: std::marker::PhantomData<fn(T)>,
}

impl<T: 'static + Send + Sync> AnyBufferAccessImpl<T> {
    fn new() -> Self {
        let mut buffer_downcasts: HashMap<_, BufferDowncastRef> = HashMap::new();

        // SAFETY: These leaks are okay because we will only ever instantiate
        // AnyBufferAccessImpl once per generic argument T, which puts a firm
        // ceiling on how many of these callbacks will get leaked.

        // Automatically register a downcast into AnyBuffer
        buffer_downcasts.insert(
            TypeId::of::<AnyBuffer>(),
            Box::leak(Box::new(|location| -> Box<dyn Any> {
                Box::new(AnyBuffer {
                    location,
                    interface: AnyBuffer::interface_for::<T>(),
                })
            })),
        );

        // Allow downcasting back to the original Buffer<T>
        buffer_downcasts.insert(
            TypeId::of::<Buffer<T>>(),
            Box::leak(Box::new(|location| -> Box<dyn Any> {
                Box::new(Buffer::<T> {
                    location,
                    _ignore: Default::default(),
                })
            })),
        );

        let mut key_downcasts: HashMap<_, KeyDowncastRef> = HashMap::new();

        // Automatically register a downcast to AnyBufferKey
        key_downcasts.insert(
            TypeId::of::<AnyBufferKey>(),
            Box::leak(Box::new(|tag| -> Box<dyn Any> {
                Box::new(AnyBufferKey {
                    tag,
                    interface: AnyBuffer::interface_for::<T>(),
                })
            })),
        );

        Self {
            buffer_downcasts: Mutex::new(buffer_downcasts),
            key_downcasts: Mutex::new(key_downcasts),
            _ignore: Default::default(),
        }
    }
}

impl<T: 'static + Send + Sync + Any> AnyBufferAccessInterface for AnyBufferAccessImpl<T> {
    fn message_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    fn message_type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    fn buffered_count(&self, entity: &EntityRef, session: Entity) -> Result<usize, OperationError> {
        entity.buffered_count::<T>(session)
    }

    fn ensure_session(&self, entity_mut: &mut EntityWorldMut, session: Entity) -> OperationResult {
        entity_mut.ensure_session::<T>(session)
    }

    fn register_buffer_downcast(&self, buffer_type: TypeId, f: BufferDowncastBox) {
        let mut downcasts = self.buffer_downcasts.lock().unwrap();

        if let Entry::Vacant(entry) = downcasts.entry(buffer_type) {
            // SAFETY: We only leak this into the register once per type
            entry.insert(Box::leak(f));
        }
    }

    fn buffer_downcast(&self, buffer_type: TypeId) -> Option<BufferDowncastRef> {
        self.buffer_downcasts
            .lock()
            .unwrap()
            .get(&buffer_type)
            .copied()
    }

    fn register_key_downcast(&self, key_type: TypeId, f: KeyDowncastBox) {
        let mut downcasts = self.key_downcasts.lock().unwrap();

        if let Entry::Vacant(entry) = downcasts.entry(key_type) {
            // SAFTY: We only leak this in to the register once per type
            entry.insert(Box::leak(f));
        }
    }

    fn key_downcast(&self, key_type: TypeId) -> Option<KeyDowncastRef> {
        self.key_downcasts.lock().unwrap().get(&key_type).copied()
    }

    fn pull(
        &self,
        entity_mut: &mut EntityWorldMut,
        session: Entity,
    ) -> Result<AnyMessageBox, OperationError> {
        entity_mut
            .pull_from_buffer::<T>(session)
            .map(to_any_message)
    }

    fn create_any_buffer_view<'a>(
        &self,
        key: &AnyBufferKey,
        world: &'a World,
    ) -> Result<AnyBufferView<'a>, BufferError> {
        let buffer_ref = world
            .get_entity(key.tag.buffer)
            .ok_or(BufferError::BufferMissing)?;
        let storage = buffer_ref
            .get::<BufferStorage<T>>()
            .ok_or(BufferError::BufferMissing)?;
        let gate = buffer_ref
            .get::<GateState>()
            .ok_or(BufferError::BufferMissing)?;
        Ok(AnyBufferView {
            storage: Box::new(storage),
            gate,
            session: key.tag.session,
        })
    }

    fn create_any_buffer_access_mut_state(
        &self,
        world: &mut World,
    ) -> Box<dyn AnyBufferAccessMutState> {
        Box::new(SystemState::<BufferAccessMut<T>>::new(world))
    }
}

pub struct DrainAnyBuffer<'a> {
    interface: Box<dyn DrainAnyBufferInterface + 'a>,
}

impl<'a> Iterator for DrainAnyBuffer<'a> {
    type Item = AnyMessageBox;

    fn next(&mut self) -> Option<Self::Item> {
        self.interface.any_next()
    }
}

trait DrainAnyBufferInterface {
    fn any_next(&mut self) -> Option<AnyMessageBox>;
}

impl<T: 'static + Send + Sync + Any> DrainAnyBufferInterface for DrainBuffer<'_, T> {
    fn any_next(&mut self) -> Option<AnyMessageBox> {
        self.next().map(to_any_message)
    }
}

impl Bufferable for AnyBuffer {
    type BufferType = Self;
    fn into_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope(), builder.scope());
        self
    }
}

impl Buffering for AnyBuffer {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope());
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        let entity_ref = world.get_entity(self.id()).or_broken()?;
        self.interface.buffered_count(&entity_ref, session)
    }

    fn buffered_count_for(
        &self,
        buffer: Entity,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        if buffer != self.id() {
            return Ok(0);
        }

        self.buffered_count(session, world)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        add_listener_to_source(self.id(), listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        GateState::apply(self.id(), session, action, world, roster)
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.id()])
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        let mut entity_mut = world.get_entity_mut(self.id()).or_broken()?;
        self.interface.ensure_session(&mut entity_mut, session)
    }
}

impl Joining for AnyBuffer {
    type Item = AnyMessageBox;
    fn pull(&self, session: Entity, world: &mut World) -> Result<Self::Item, OperationError> {
        let mut buffer_mut = world.get_entity_mut(self.id()).or_broken()?;
        self.interface.pull(&mut buffer_mut, session)
    }
}

impl Accessing for AnyBuffer {
    type Key = AnyBufferKey;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        world
            .get_mut::<BufferAccessors>(self.id())
            .or_broken()?
            .add_accessor(accessor);
        Ok(())
    }

    fn create_key(&self, builder: &super::BufferKeyBuilder) -> Self::Key {
        AnyBufferKey {
            tag: builder.make_tag(self.id()),
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

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*};
    use bevy_ecs::prelude::World;

    #[test]
    fn test_any_count() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer = builder.create_buffer(BufferSettings::keep_all());
            let push_multiple_times = builder
                .commands()
                .spawn_service(push_multiple_times_into_buffer.into_blocking_service());
            let count = builder
                .commands()
                .spawn_service(get_buffer_count.into_blocking_service());

            scope
                .input
                .chain(builder)
                .with_access(buffer)
                .then(push_multiple_times)
                .then(count)
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| commands.request(1, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let count = promise.take().available().unwrap();
        assert_eq!(count, 5);
        assert!(context.no_unhandled_errors());
    }

    fn push_multiple_times_into_buffer(
        In((value, key)): In<(usize, BufferKey<usize>)>,
        mut access: BufferAccessMut<usize>,
    ) -> AnyBufferKey {
        let mut buffer = access.get_mut(&key).unwrap();
        for _ in 0..5 {
            buffer.push(value);
        }

        key.into()
    }

    fn get_buffer_count(In(key): In<AnyBufferKey>, world: &mut World) -> usize {
        world.any_buffer_view(&key).unwrap().len()
    }

    #[test]
    fn test_modify_any_message() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer = builder.create_buffer(BufferSettings::keep_all());
            let push_multiple_times = builder
                .commands()
                .spawn_service(push_multiple_times_into_buffer.into_blocking_service());
            let modify_content = builder
                .commands()
                .spawn_service(modify_buffer_content.into_blocking_service());
            let drain_content = builder
                .commands()
                .spawn_service(pull_each_buffer_item.into_blocking_service());

            scope
                .input
                .chain(builder)
                .with_access(buffer)
                .then(push_multiple_times)
                .then(modify_content)
                .then(drain_content)
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| commands.request(3, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let values = promise.take().available().unwrap();
        assert_eq!(values, vec![0, 3, 6, 9, 12]);
        assert!(context.no_unhandled_errors());
    }

    fn modify_buffer_content(In(key): In<AnyBufferKey>, world: &mut World) -> AnyBufferKey {
        world
            .any_buffer_mut(&key, |mut access| {
                for i in 0..access.len() {
                    access.get_mut(i).map(|value| {
                        *value.downcast_mut::<usize>().unwrap() *= i;
                    });
                }
            })
            .unwrap();

        key
    }

    fn pull_each_buffer_item(In(key): In<AnyBufferKey>, world: &mut World) -> Vec<usize> {
        world
            .any_buffer_mut(&key, |mut access| {
                let mut values = Vec::new();
                while let Some(value) = access.pull() {
                    values.push(*value.downcast::<usize>().unwrap());
                }
                values
            })
            .unwrap()
    }

    #[test]
    fn test_drain_any_message() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer = builder.create_buffer(BufferSettings::keep_all());
            let push_multiple_times = builder
                .commands()
                .spawn_service(push_multiple_times_into_buffer.into_blocking_service());
            let modify_content = builder
                .commands()
                .spawn_service(modify_buffer_content.into_blocking_service());
            let drain_content = builder
                .commands()
                .spawn_service(drain_buffer_contents.into_blocking_service());

            scope
                .input
                .chain(builder)
                .with_access(buffer)
                .then(push_multiple_times)
                .then(modify_content)
                .then(drain_content)
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| commands.request(3, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let values = promise.take().available().unwrap();
        assert_eq!(values, vec![0, 3, 6, 9, 12]);
        assert!(context.no_unhandled_errors());
    }

    fn drain_buffer_contents(In(key): In<AnyBufferKey>, world: &mut World) -> Vec<usize> {
        world
            .any_buffer_mut(&key, |mut access| {
                access
                    .drain(..)
                    .map(|value| *value.downcast::<usize>().unwrap())
                    .collect()
            })
            .unwrap()
    }

    #[test]
    fn double_any_messages() {
        let mut context = TestingContext::minimal_plugins();

        let workflow =
            context.spawn_io_workflow(|scope: Scope<(u32, i32, f32), (u32, i32, f32)>, builder| {
                let buffer_u32: AnyBuffer = builder
                    .create_buffer::<u32>(BufferSettings::default())
                    .into();
                let buffer_i32: AnyBuffer = builder
                    .create_buffer::<i32>(BufferSettings::default())
                    .into();
                let buffer_f32: AnyBuffer = builder
                    .create_buffer::<f32>(BufferSettings::default())
                    .into();

                let (input_u32, input_i32, input_f32) = scope.input.chain(builder).unzip();
                input_u32.chain(builder).map_block(|v| 2 * v).connect(
                    buffer_u32
                        .downcast_for_message::<u32>()
                        .unwrap()
                        .input_slot(),
                );

                input_i32.chain(builder).map_block(|v| 2 * v).connect(
                    buffer_i32
                        .downcast_for_message::<i32>()
                        .unwrap()
                        .input_slot(),
                );

                input_f32.chain(builder).map_block(|v| 2.0 * v).connect(
                    buffer_f32
                        .downcast_for_message::<f32>()
                        .unwrap()
                        .input_slot(),
                );

                (buffer_u32, buffer_i32, buffer_f32)
                    .join(builder)
                    .map_block(|(value_u32, value_i32, value_f32)| {
                        (
                            *value_u32.downcast::<u32>().unwrap(),
                            *value_i32.downcast::<i32>().unwrap(),
                            *value_f32.downcast::<f32>().unwrap(),
                        )
                    })
                    .connect(scope.terminate);
            });

        let mut promise = context.command(|commands| {
            commands
                .request((1u32, 2i32, 3f32), workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let (v_u32, v_i32, v_f32) = promise.take().available().unwrap();
        assert_eq!(v_u32, 2);
        assert_eq!(v_i32, 4);
        assert_eq!(v_f32, 6.0);
        assert!(context.no_unhandled_errors());
    }
}
