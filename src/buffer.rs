/*
 * Copyright (C) 2024 Open Source Robotics Foundation
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

use bevy::{
    prelude::{Entity, Query, Commands},
    ecs::{
        system::SystemParam,
        change_detection::Mut,
        query::QueryEntityError,
    }
};

use std::{
    sync::Arc,
    ops::RangeBounds,
};

use crossbeam::channel::Sender as CbSender;

use crate::{
    Builder, Chain, UnusedTarget, OnNewBufferValue, InputSlot,
    NotifyBufferUpdate, ChannelItem,
};

mod buffer_access_lifecycle;
pub(crate) use buffer_access_lifecycle::*;

mod buffer_storage;
pub(crate) use buffer_storage::*;

mod buffered;
pub use buffered::*;

mod bufferable;
pub use bufferable::*;

mod manage_buffer;
pub use manage_buffer::*;

/// A buffer is a special type of node within a workflow that is able to store
/// and release data. When a session is finished, the buffered data from the
/// session will be automatically cleared.
pub struct Buffer<T> {
    pub(crate) scope: Entity,
    pub(crate) source: Entity,
    pub(crate) _ignore: std::marker::PhantomData<T>,
}

impl<T> Buffer<T> {
    /// Get a unit `()` trigger output each time a new value is added to the buffer.
    pub fn on_new_value<'w, 's, 'a, 'b>(
        &self,
        builder: &'b mut Builder<'w, 's, 'a>
    ) -> Chain<'w, 's, 'a, 'b, ()> {
        assert_eq!(self.scope, builder.scope);
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.add(OnNewBufferValue::new(self.source, target));
        Chain::new(target, builder)
    }

    /// Specify that you want to pull from this Buffer by cloning. This can be
    /// used by operations like join to tell them that they should clone from
    /// the buffer instead of consuming from it.
    pub fn by_cloning(self) -> CloneFromBuffer<T>
    where
        T: Clone,
    {
        CloneFromBuffer {
            scope: self.scope,
            source: self.source,
            _ignore: Default::default()
        }
    }

    /// Get an input slot for this buffer.
    pub fn input_slot(self) -> InputSlot<T> {
        InputSlot::new(self.scope, self.source)
    }
}

pub struct CloneFromBuffer<T: Clone> {
    pub(crate) scope: Entity,
    pub(crate) source: Entity,
    pub(crate) _ignore: std::marker::PhantomData<T>,
}

/// Settings to describe the behavior of a buffer.
#[derive(Default, Clone, Copy)]
pub struct BufferSettings {
    retention: RetentionPolicy,
}

impl BufferSettings {
    /// Get the retention policy for the buffer.
    pub fn retention(&self) -> RetentionPolicy {
        self.retention
    }
}

/// Describe how data within a buffer gets retained. Most mechanisms that pull
/// data from a buffer will remove the oldest item in the buffer, so this policy
/// is for dealing with situations where items are being stored faster than they
/// are being pulled.
///
/// The default value is KeepLast(1).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RetentionPolicy {
    /// Keep the last N items that were stored into the buffer. Once the limit
    /// is reached, the oldest item will be removed any time a new item arrives.
    KeepLast(usize),
    /// Keep the first N items that are stored into the buffer. Once the limit
    /// is reached, any new item that arrives will be discarded.
    KeepFirst(usize),
    /// Do not limit how many items can be stored in the buffer.
    KeepAll,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::KeepLast(1)
    }
}

impl<T> Clone for Buffer<T> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope,
            source: self.source,
            _ignore: Default::default(),
        }
    }
}

impl<T> Copy for Buffer<T> {}

impl<T: Clone> Clone for CloneFromBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope,
            source: self.source,
            _ignore: Default::default(),
        }
    }
}

impl<T: Clone> Copy for CloneFromBuffer<T> {}

/// This key can unlock access to the contents of a buffer by passing it into
/// [`BufferAccess`] or [`BufferAccessMut`].
///
/// To obtain a `BufferKey`, use [`Chain::with_access`][1], or [`listen`][2].
///
/// [1]: crate::Chain::with_access
/// [2]: crate::Bufferable::listen
pub struct BufferKey<T> {
    buffer: Entity,
    session: Entity,
    lifecycle: Arc<BufferAccessLifecycle>,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Clone for BufferKey<T> {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer,
            session: self.session,
            lifecycle: Arc::clone(&self.lifecycle),
            _ignore: Default::default(),
        }
    }
}

impl<T> BufferKey<T> {
    /// The buffer ID of this key.
    pub fn id(&self) -> Entity {
        self.buffer
    }

    /// The session that this key belongs to.
    pub fn session(&self) -> Entity {
        self.session
    }

    pub(crate) fn is_in_use(&self) -> bool {
        Arc::strong_count(&self.lifecycle) > 1
    }

    pub(crate) fn new(
        scope: Entity,
        buffer: Entity,
        session: Entity,
        sender: CbSender<ChannelItem>,
    ) -> BufferKey<T> {
        let lifecycle = Arc::new(BufferAccessLifecycle::new(scope, session, sender));
        BufferKey { buffer, session, lifecycle, _ignore: Default::default() }
    }
}

/// This system parameter lets you get read-only access to a buffer that exists
/// within a workflow. Use a [`BufferKey`] to unlock the access.
///
/// See [`BufferAccessMut`] for mutable access.
#[derive(SystemParam)]
pub struct BufferAccess<'w, 's, T>
where
    T: 'static + Send + Sync,
{
    query: Query<'w, 's, &'static BufferStorage<T>>,
}

impl<'w, 's, T: 'static + Send + Sync> BufferAccess<'w, 's, T> {
    pub fn get<'a>(
        &'a self,
        key: &BufferKey<T>,
    ) -> Result<BufferView<'a, T>, QueryEntityError> {
        let session = key.session;
        self.query.get(key.buffer).map(|storage| BufferView { storage, session })
    }
}

/// This system parameter lets you get mutable access to a buffer that exists
/// within a workflow. Use a [`BufferKey`] to unlock the access.
///
/// See [`BufferAccess`] for read-only access.
#[derive(SystemParam)]
pub struct BufferAccessMut<'w, 's, T>
where
    T: 'static + Send + Sync,
{
    query: Query<'w, 's, &'static mut BufferStorage<T>>,
    commands: Commands<'w, 's>,
}

impl<'w, 's, T> BufferAccessMut<'w, 's, T>
where
    T: 'static + Send + Sync,
{
    pub fn get<'a>(
        &'a self,
        key: &BufferKey<T>,
    ) -> Result<BufferView<'a, T>, QueryEntityError> {
        let session = key.session;
        self.query.get(key.buffer).map(|storage| BufferView { storage, session })
    }

    pub fn get_mut<'a>(
        &'a mut self,
        key: &BufferKey<T>,
        source: Option<Entity>,
    ) -> Result<BufferMut<'w, 's, 'a, T>, QueryEntityError> {
        let buffer = key.buffer;
        let session = key.session;
        self.query
            .get_mut(key.buffer)
            .map(|storage| BufferMut::new(
                storage, buffer, session, source, &mut self.commands,
            ))
    }
}

/// Access to view a buffer that exists inside a workflow.
pub struct BufferView<'a, T>
where
    T: 'static + Send + Sync,
{
    storage: &'a BufferStorage<T>,
    session: Entity,
}

impl<'a, T> BufferView<'a, T>
where
    T: 'static + Send + Sync,
{
    /// Iterate over the contents in the buffer
    pub fn iter<'b>(&'b self) -> IterBufferView<'b, T> {
        self.storage.iter(self.session)
    }

    /// Clone the oldest item in the buffer.
    pub fn clone_oldest(&self) -> Option<T>
    where
        T: Clone,
    {
        self.storage.clone_oldest(self.session)
    }

    /// Clone the newest item in the buffer.
    pub fn clone_newest(&self) -> Option<T>
    where
        T: Clone,
    {
        self.storage.clone_newest(self.session)
    }

    /// How many items are in the buffer?
    pub fn len(&self) -> usize {
        self.storage.count(self.session)
    }
}

/// Access to mutate a buffer that exists inside a workflow.
pub struct BufferMut<'w, 's, 'a, T>
where
    T: 'static + Send + Sync,
{
    storage: Mut<'a, BufferStorage<T>>,
    buffer: Entity,
    session: Entity,
    source: Option<Entity>,
    commands: &'a mut Commands<'w, 's>,
    modified: bool,
}

impl<'w, 's, 'a, T> BufferMut<'w, 's, 'a, T>
where
    T: 'static + Send + Sync,
{
    /// Iterate over the contents in the buffer.
    pub fn iter<'b>(&'b self) -> IterBufferView<'b, T> {
        self.storage.iter(self.session)
    }

    /// Clone the oldest item in the buffer.
    pub fn clone_oldest(&self) -> Option<T>
    where
        T: Clone,
    {
        self.storage.clone_oldest(self.session)
    }

    /// Clone the newest item in the buffer.
    pub fn clone_newest(&self) -> Option<T>
    where
        T: Clone,
    {
        self.storage.clone_newest(self.session)
    }

    /// How many items are in the buffer?
    pub fn len(&self) -> usize {
        self.storage.count(self.session)
    }

    /// Iterate over mutable borrows of the contents in the buffer.
    pub fn iter_mut<'b>(&'b mut self) -> IterBufferMut<'b, T> {
        self.modified = true;
        self.storage.iter_mut(self.session)
    }

    /// Drain items out of the buffer
    pub fn drain<'b, R>(&'b mut self, range: R) -> DrainBuffer<'b, T>
    where
        R: RangeBounds<usize>,
    {
        self.modified = true;
        self.storage.drain(self.session, range)
    }

    /// Pull the oldest item from the buffer
    pub fn pull(&mut self) -> Option<T> {
        self.modified = true;
        self.storage.pull(self.session)
    }

    /// Pull the item that was most recently put into the buffer (instead of
    /// the oldest, which is what [`Self::pull`] gives).
    pub fn pull_newest(&mut self) -> Option<T> {
        self.modified = true;
        self.storage.pull_newest(self.session)
    }

    /// Push a new value into the buffer. If the buffer is at its limit, this
    /// will return the value that needed to be removed.
    pub fn push(&mut self, value: T) -> Option<T> {
        self.modified = true;
        self.storage.push(self.session, value)
    }

    /// Push a value into the buffer as if it is the oldest value of the buffer.
    /// If the buffer is at its limit, this will return the value that needed to
    /// be removed.
    pub fn push_as_oldest(&mut self, value: T) -> Option<T> {
        self.modified = true;
        self.storage.push_as_oldest(self.session, value)
    }

    fn new(
        storage: Mut<'a, BufferStorage<T>>,
        buffer: Entity,
        session: Entity,
        source: Option<Entity>,
        commands: &'a mut Commands<'w, 's>,
    ) -> Self {
        Self { storage, buffer, session, source, commands, modified: false }
    }
}

impl<'w, 's, 'a, T> Drop for BufferMut<'w, 's, 'a, T>
where
    T: 'static + Send + Sync,
{
    fn drop(&mut self) {
        if self.modified {
            self.commands.add(NotifyBufferUpdate::new(
                self.buffer, self.session, self.source,
            ));
        }
    }
}
