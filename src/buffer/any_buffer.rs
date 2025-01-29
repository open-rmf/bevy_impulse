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
    any::{Any, TypeId},
    ops::RangeBounds,
    sync::{Arc, OnceLock},
};

use bevy_ecs::{
    prelude::{Component, Entity, EntityRef, EntityWorldMut, Commands, Mut, World},
    system::SystemState,
};

use thiserror::Error as ThisError;

use crate::{
    Buffer, BufferAccessLifecycle, BufferAccessMut, BufferKey, BufferStorage,
    DrainBuffer, NotifyBufferUpdate, GateState, Gate, OperationResult, OperationError,
    InspectBuffer, ManageBuffer,
};

/// A [`Buffer`] whose type has been anonymized. Joining with this buffer type
/// will yield an [`AnyMessage`].
#[derive(Clone, Copy, Debug)]
pub struct AnyBuffer {
    pub(crate) scope: Entity,
    pub(crate) source: Entity,
    pub(crate) type_id: TypeId,
}

impl AnyBuffer {
    /// Downcast this into a concrete [`Buffer`] type.
    pub fn into_buffer<T: 'static>(&self) -> Option<Buffer<T>> {
        if TypeId::of::<T>() == self.type_id {
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

impl<T: 'static> From<Buffer<T>> for AnyBuffer {
    fn from(value: Buffer<T>) -> Self {
        let type_id = TypeId::of::<T>();
        AnyBuffer {
            scope: value.scope,
            source: value.source,
            type_id,
        }
    }
}

/// Similar to a [`BufferKey`][crate::BufferKey] except it can be used for any
/// buffer without knowing the buffer's type ahead of time.
///
/// Use this with [`AnyBufferAccess`] to directly view or manipulate the contents
/// of a buffer.
#[derive(Clone)]
pub struct AnyBufferKey {
    buffer: Entity,
    session: Entity,
    accessor: Entity,
    lifecycle: Option<Arc<BufferAccessLifecycle>>,
    type_id: TypeId,
}

impl std::fmt::Debug for AnyBufferKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("DynBufferKey")
            .field("buffer", &self.buffer)
            .field("session", &self.session)
            .field("accessor", &self.accessor)
            .field("in_use", &self.lifecycle.as_ref().is_some_and(|l| l.is_in_use()))
            .field("type_id", &self.type_id)
            .finish()
    }
}

impl AnyBufferKey {
    /// Downcast this into a concrete [`BufferKey`] type.
    pub fn into_buffer_key<T: 'static>(&self) -> Option<BufferKey<T>> {
        if TypeId::of::<T>() == self.type_id {
            Some(BufferKey {
                buffer: self.buffer,
                session: self.session,
                accessor: self.accessor,
                lifecycle: self.lifecycle.clone(),
                _ignore: Default::default(),
            })
        } else {
            None
        }
    }
}

impl<T: 'static> From<BufferKey<T>> for AnyBufferKey {
    fn from(value: BufferKey<T>) -> Self {
        let type_id = TypeId::of::<T>();
        AnyBufferKey {
            buffer: value.buffer,
            session: value.session,
            accessor: value.accessor,
            lifecycle: value.lifecycle.clone(),
            type_id,
        }
    }
}

/// Similar to [`BufferMut`][crate::BufferMut], but this can be unlocked with a
/// [`DynBufferKey`], so it can work for any buffer regardless of the data type
/// inside.
pub struct AnyBufferMut<'w, 's, 'a> {
    storage: Box<dyn AnyBufferManagement + 'a>,
    gate: Mut<'a, GateState>,
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

    /// Look at the oldest item in the buffer.
    pub fn oldest(&self) -> Option<AnyMessageRef<'_>> {
        self.storage.any_oldest(self.session)
    }

    /// Look at the newest item in the buffer.
    pub fn newest(&self) -> Option<AnyMessageRef<'_>> {
        self.storage.any_newest(self.session)
    }

    /// Borrow an item from the buffer. Index 0 is the oldest item in the buffer
    /// with the highest index being the newest item in the buffer.
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

    /// Modify the oldest item in the buffer.
    pub fn oldest_mut(&mut self) -> Option<AnyMessageMut<'_>> {
        self.modified = true;
        self.storage.any_oldest_mut(self.session)
    }

    /// Modify the newest item in the buffer.
    pub fn newest_mut(&mut self) -> Option<AnyMessageMut<'_>> {
        self.modified = true;
        self.storage.any_newest_mut(self.session)
    }

    /// Modify an item in the buffer. Index 0 is the oldest item in the buffer
    /// with the highest index being the newest item in the buffer.
    pub fn get_mut(&mut self, index: usize) -> Option<AnyMessageMut<'_>> {
        self.modified = true;
        self.storage.any_get_mut(self.session, index)
    }

    pub fn drain<R: RangeBounds<usize>>(&mut self, range: R) -> DrainAnyBuffer<'_> {
        self.modified = true;
        DrainAnyBuffer {
            inner: self.storage.any_drain(self.session, AnyRange::new(range))
        }
    }

    /// Pull the oldest item from the buffer.
    pub fn pull(&mut self) -> Option<AnyMessage> {
        self.modified = true;
        self.storage.any_pull(self.session)
    }

    /// Pull the item that was most recently put into the buffer (instead of the
    /// oldest, which is what [`Self::pull`] gives).
    pub fn pull_newest(&mut self) -> Option<AnyMessage> {
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
    pub fn push_any(&mut self, value: AnyMessage) -> Result<Option<AnyMessage>, AnyMessageError> {
        self.storage.any_push(self.session, value)
    }

    /// Attempt to push a value into the buffer as if it is the oldest value of
    /// the buffer.
    ///
    /// The result follows the same rules as [`Self::push`].
    pub fn push_as_oldest<T: 'static + Send + Sync + Any>(&mut self, value: T) -> Result<Option<T>, T> {
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
    pub fn push_any_as_oldest(&mut self, value: AnyMessage) -> Result<Option<AnyMessage>, AnyMessageError> {
        self.storage.any_push_as_oldest(self.session, value)
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

#[derive(ThisError, Debug, Clone)]
pub enum AnyBufferError {
    #[error("The key was unable to identify a buffer")]
    BufferMissing,
}

/// This trait allows [`World`] to give you access to any buffer using an
/// [`AnyBufferKey`].
pub trait AnyBufferWorldAccess {
    fn any_buffer_mut<U>(
        &mut self,
        key: &AnyBufferKey,
        f: impl FnOnce(AnyBufferMut) -> U,
    ) -> Result<U, AnyBufferError>;
}

impl AnyBufferWorldAccess for World {
    fn any_buffer_mut<U>(
        &mut self,
        key: &AnyBufferKey,
        f: impl FnOnce(AnyBufferMut) -> U,
    ) -> Result<U, AnyBufferError> {
        let interface = self.get::<AnyBufferStorageAccess>(key.buffer)
            .ok_or(AnyBufferError::BufferMissing)?
            .interface;

        let mut state = interface.create_any_buffer_access_mut_state(self);
        let mut access = state.get_buffer_access_mut(self);
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
    fn any_push(&mut self, session: Entity, value: AnyMessage) -> AnyMessagePushResult;
    fn any_push_as_oldest(&mut self, session: Entity, value: AnyMessage) -> AnyMessagePushResult;
    fn any_pull(&mut self, session: Entity) -> Option<AnyMessage>;
    fn any_pull_newest(&mut self, session: Entity) -> Option<AnyMessage>;
    fn any_oldest_mut<'a>(&'a mut self, session: Entity) -> Option<AnyMessageMut<'a>>;
    fn any_newest_mut<'a>(&'a mut self, session: Entity) -> Option<AnyMessageMut<'a>>;
    fn any_get_mut<'a>(&'a mut self, session: Entity, index: usize) -> Option<AnyMessageMut<'a>>;
    fn any_drain<'a>(&'a mut self, session: Entity, range: AnyRange) -> Box<dyn DrainAnyBufferImpl + 'a>;
}

struct AnyRange {
    start_bound: std::ops::Bound<usize>,
    end_bound: std::ops::Bound<usize>,
}

impl AnyRange {
    fn new<T: std::ops::RangeBounds<usize>>(range: T) -> Self {
        AnyRange {
            start_bound: range.start_bound().map(|x| *x),
            end_bound: range.end_bound().map(|x| *x),
        }
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

pub type AnyMessage = Box<dyn Any + 'static + Send + Sync>;

#[derive(ThisError, Debug)]
#[error("failed to convert a message")]
pub struct AnyMessageError {
    /// The original value provided
    pub value: AnyMessage,
    /// The type expected by the buffer
    pub type_id: TypeId,
}

pub type AnyMessagePushResult = Result<Option<AnyMessage>, AnyMessageError>;

impl<T: 'static + Send + Sync + Any> AnyBufferManagement for Mut<'_, BufferStorage<T>> {
    fn any_push(&mut self, session: Entity, value: AnyMessage) -> Result<Option<AnyMessage>, AnyMessageError> {
        let value = from_any_message::<T>(value)?;
        Ok(self.push(session, value).map(to_any_message))
    }

    fn any_push_as_oldest(&mut self, session: Entity, value: AnyMessage) -> Result<Option<AnyMessage>, AnyMessageError> {
        let value = from_any_message::<T>(value)?;
        Ok(self.push_as_oldest(session, value).map(to_any_message))
    }

    fn any_pull(&mut self, session: Entity) -> Option<AnyMessage> {
        self.pull(session).map(to_any_message)
    }

    fn any_pull_newest(&mut self, session: Entity) -> Option<AnyMessage> {
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

    fn any_drain<'a>(&'a mut self, session: Entity, range: AnyRange) -> Box<dyn DrainAnyBufferImpl + 'a> {
        Box::new(self.drain(session, range))
    }
}

fn to_any_ref<'a, T: 'static + Send + Sync + Any>(x: &'a T) -> AnyMessageRef<'a> {
    x
}

fn to_any_mut<'a, T: 'static + Send + Sync + Any>(x: &'a mut T) -> AnyMessageMut<'a> {
    x
}

fn to_any_message<T: 'static + Send + Sync + Any>(x: T) -> AnyMessage {
    Box::new(x)
}

fn from_any_message<T: 'static + Send + Sync + Any>(value: AnyMessage) -> Result<T, AnyMessageError>
where
    T: 'static,
{
    let value = value.downcast::<T>().map_err(|value| {
        AnyMessageError {
            value,
            type_id: TypeId::of::<T>(),
        }
    })?;

    Ok(*value)
}

pub(crate) trait AnyBufferAccessMutState {
    fn get_buffer_access_mut<'s, 'w: 's>(&'s mut self, world: &'w mut World) -> Box<dyn AnyBufferAccessMut<'w, 's> + 's>;
}

impl<T: 'static + Send + Sync + Any> AnyBufferAccessMutState for SystemState<BufferAccessMut<'static, 'static, T>> {
    fn get_buffer_access_mut<'s, 'w: 's>(&'s mut self, world: &'w mut World) -> Box<dyn AnyBufferAccessMut<'w, 's> + 's> {
        Box::new(self.get_mut(world))
    }
}

pub(crate) trait AnyBufferAccessMut<'w, 's> {
    fn as_any_buffer_mut<'a>(&'a mut self, key: &AnyBufferKey) -> Result<AnyBufferMut<'w, 's, 'a>, AnyBufferError>;
}

impl<'w, 's, T: 'static + Send + Sync + Any> AnyBufferAccessMut<'w, 's> for BufferAccessMut<'w, 's, T> {
    fn as_any_buffer_mut<'a>(&'a mut self, key: &AnyBufferKey) -> Result<AnyBufferMut<'w, 's, 'a>, AnyBufferError> {
        let BufferAccessMut { query, commands } = self;
        let (storage, gate) = query.get_mut(key.buffer).map_err(|_| AnyBufferError::BufferMissing)?;
        Ok(AnyBufferMut {
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

/// A component that lets us inspect buffer properties without knowing the
/// message type of the buffer.
#[derive(Component, Clone, Copy)]
pub(crate) struct AnyBufferStorageAccess {
    pub(crate) interface: &'static (dyn AnyBufferStorageAccessInterface + Send + Sync),
}

impl AnyBufferStorageAccess {
    pub(crate) fn new<T: 'static + Send + Sync + Any>() -> Self {
        let once: OnceLock<&'static AnyBufferStorageAccessImpl<T>> = OnceLock::new();
        let interface = *once.get_or_init(|| {
            Box::leak(Box::new(AnyBufferStorageAccessImpl(Default::default())))
        });

        Self { interface }
    }
}

pub(crate) trait AnyBufferStorageAccessInterface {
    fn buffered_count(
        &self,
        entity: &EntityRef,
        session: Entity,
    ) -> Result<usize, OperationError>;

    fn ensure_session(
        &self,
        entity_mut: &mut EntityWorldMut,
        session: Entity,
    ) -> OperationResult;

    fn create_any_buffer_access_mut_state(
        &self,
        world: &mut World,
    ) -> Box<dyn AnyBufferAccessMutState>;
}

struct AnyBufferStorageAccessImpl<T>(std::marker::PhantomData<T>);

impl<T: 'static + Send + Sync + Any> AnyBufferStorageAccessInterface for AnyBufferStorageAccessImpl<T> {
    fn buffered_count(
        &self,
        entity: &EntityRef,
        session: Entity,
    ) -> Result<usize, OperationError> {
        entity.buffered_count::<T>(session)
    }

    fn ensure_session(
        &self,
        entity_mut: &mut EntityWorldMut,
        session: Entity,
    ) -> OperationResult {
        entity_mut.ensure_session::<T>(session)
    }

    fn create_any_buffer_access_mut_state(
        &self,
        world: &mut World,
    ) -> Box<dyn AnyBufferAccessMutState> {
        Box::new(SystemState::<BufferAccessMut<T>>::new(world))
    }
}

pub struct DrainAnyBuffer<'a> {
    inner: Box<dyn DrainAnyBufferImpl + 'a>,
}

impl<'a> Iterator for DrainAnyBuffer<'a> {
    type Item = AnyMessage;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.any_next()
    }
}

trait DrainAnyBufferImpl {
    fn any_next(&mut self) -> Option<AnyMessage>;
}

impl<T: 'static + Send + Sync + Any> DrainAnyBufferImpl for DrainBuffer<'_, T> {
    fn any_next(&mut self) -> Option<AnyMessage> {
        self.next().map(to_any_message)
    }
}

#[cfg(test)]
mod tests {
    use bevy_ecs::prelude::World;
    use crate::{prelude::*, testing::*};

    #[test]
    fn test_any_count() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer = builder.create_buffer(BufferSettings::keep_all());
            let push_multiple_times = builder.commands().spawn_service(
                push_multiple_times_into_buffer.into_blocking_service()
            );
            let count = builder.commands().spawn_service(
                get_buffer_count.into_blocking_service()
            );

            scope
                .input
                .chain(builder)
                .with_access(buffer)
                .then(push_multiple_times)
                .then(count)
                .connect(scope.terminate);
        });

        let mut promise = context.command(
            |commands| commands.request(1, workflow).take_response()
        );

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

    fn get_buffer_count(
        In(key): In<AnyBufferKey>,
        world: &mut World,
    ) -> usize {
        world.any_buffer_mut(&key, |access| {
            access.len()
        }).unwrap()
    }

    #[test]
    fn test_modify_any_message() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer = builder.create_buffer(BufferSettings::keep_all());
            let push_multiple_times = builder.commands().spawn_service(
                push_multiple_times_into_buffer.into_blocking_service()
            );
            let modify_content = builder.commands().spawn_service(
                modify_buffer_content.into_blocking_service()
            );
            let drain_content = builder.commands().spawn_service(
                pull_each_buffer_item.into_blocking_service()
            );

            scope
                .input
                .chain(builder)
                .with_access(buffer)
                .then(push_multiple_times)
                .then(modify_content)
                .then(drain_content)
                .connect(scope.terminate);
        });

        let mut promise = context.command(
            |commands| commands.request(3, workflow).take_response()
        );

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let values = promise.take().available().unwrap();
        assert_eq!(values, vec![0, 3, 6, 9, 12]);
        assert!(context.no_unhandled_errors());
    }

    fn modify_buffer_content(
        In(key): In<AnyBufferKey>,
        world: &mut World,
    ) -> AnyBufferKey {
        world.any_buffer_mut(&key, |mut access| {
            for i in 0..access.len() {
                access.get_mut(i).map(|value| {
                    *value.downcast_mut::<usize>().unwrap() *= i;
                });
            }
        }).unwrap();

        key
    }

    fn pull_each_buffer_item(
        In(key): In<AnyBufferKey>,
        world: &mut World,
    ) -> Vec<usize> {
        world.any_buffer_mut(&key, |mut access| {
            let mut values = Vec::new();
            while let Some(value) = access.pull() {
                values.push(*value.downcast::<usize>().unwrap());
            }
            values
        }).unwrap()
    }

    #[test]
    fn test_drain_any_message() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer = builder.create_buffer(BufferSettings::keep_all());
            let push_multiple_times = builder.commands().spawn_service(
                push_multiple_times_into_buffer.into_blocking_service()
            );
            let modify_content = builder.commands().spawn_service(
                modify_buffer_content.into_blocking_service()
            );
            let drain_content = builder.commands().spawn_service(
                drain_buffer_contents.into_blocking_service()
            );

            scope
                .input
                .chain(builder)
                .with_access(buffer)
                .then(push_multiple_times)
                .then(modify_content)
                .then(drain_content)
                .connect(scope.terminate);
        });

        let mut promise = context.command(
            |commands| commands.request(3, workflow).take_response()
        );

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        let values = promise.take().available().unwrap();
        assert_eq!(values, vec![0, 3, 6, 9, 12]);
        assert!(context.no_unhandled_errors());
    }

    fn drain_buffer_contents(
        In(key): In<AnyBufferKey>,
        world: &mut World,
    ) -> Vec<usize> {
        world.any_buffer_mut(&key, |mut access| {
            access
            .drain(..)
            .map(|value| *value.downcast::<usize>().unwrap())
            .collect()
        }).unwrap()
    }
}
