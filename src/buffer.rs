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
    /// Define new buffer settings
    pub fn new(retention: RetentionPolicy) -> Self {
        Self { retention }
    }

    /// Create `BufferSettings` with a retention policy of [`RetentionPolicy::KeepLast`]`(n)`.
    pub fn keep_last(n: usize) -> Self {
        Self::new(RetentionPolicy::KeepLast(n))
    }

    /// Create `BufferSettings` with a retention policy of [`RetentionPolicy::KeepFirst`]`(n)`.
    pub fn keep_first(n: usize) -> Self {
        Self::new(RetentionPolicy::KeepFirst(n))
    }

    /// Create `BufferSettings` with a retention policy of [`RetentionPolicy::KeepAll`].
    pub fn keep_all() -> Self {
        Self::new(RetentionPolicy::KeepAll)
    }

    /// Get the retention policy for the buffer.
    pub fn retention(&self) -> RetentionPolicy {
        self.retention
    }

    /// Modify the retention policy for the buffer.
    pub fn retention_mut(&mut self) -> &mut RetentionPolicy {
        &mut self.retention
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
    accessor: Entity,
    lifecycle: Arc<BufferAccessLifecycle>,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Clone for BufferKey<T> {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer,
            session: self.session,
            accessor: self.accessor,
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
        accessor: Entity,
        sender: CbSender<ChannelItem>,
    ) -> BufferKey<T> {
        let lifecycle = Arc::new(BufferAccessLifecycle::new(scope, session, sender));
        BufferKey { buffer, session, accessor, lifecycle, _ignore: Default::default() }
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
    ) -> Result<BufferMut<'w, 's, 'a, T>, QueryEntityError> {
        let buffer = key.buffer;
        let session = key.session;
        let accessor = key.accessor;
        self.query
            .get_mut(key.buffer)
            .map(|storage| BufferMut::new(
                storage, buffer, session, accessor, &mut self.commands,
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

    /// Borrow the oldest item in the buffer.
    pub fn oldest(&self) -> Option<&T> {
        self.storage.oldest(self.session)
    }

    /// Borrow the newest item in the buffer.
    pub fn newest(&self) -> Option<&T> {
        self.storage.newest(self.session)
    }

    /// Borrow an item from the buffer. Index 0 is the oldest item in the buffer
    /// with the highest index being the newest item in the buffer.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.storage.get(self.session, index)
    }

    /// How many items are in the buffer?
    pub fn len(&self) -> usize {
        self.storage.count(self.session)
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    accessor: Option<Entity>,
    commands: &'a mut Commands<'w, 's>,
    modified: bool,
}

impl<'w, 's, 'a, T> BufferMut<'w, 's, 'a, T>
where
    T: 'static + Send + Sync,
{
    /// When you make a modification using this `BufferMut`, anything listening
    /// to the buffer will be notified about the update. This can create
    /// unintentional infinite loops where a node in the workflow wakes itself
    /// up every time it runs because of a modification it makes to a buffer.
    ///
    /// By default this closed loop is disabled by keeping track of which
    /// listener created the key that's being used to modify the buffer, and
    /// then skipping that listener when notifying about the modification.
    ///
    /// In some cases a key can be used far downstream of the listener. In that
    /// case, there may be nodes downstream of the listener that do want to be
    /// woken up by the modification. Use this function to allow that closed
    /// loop to happen. It will be up to you to prevent the closed loop from
    /// being a problem.
    pub fn allow_closed_loops(mut self) -> Self {
        self.accessor = None;
        self
    }

    /// Iterate over the contents in the buffer.
    pub fn iter<'b>(&'b self) -> IterBufferView<'b, T> {
        self.storage.iter(self.session)
    }

    /// Look at the oldest item in the buffer.
    pub fn oldest(&self) -> Option<&T> {
        self.storage.oldest(self.session)
    }

    /// Look at the newest item in the buffer.
    pub fn newest(&self) -> Option<&T> {
        self.storage.newest(self.session)
    }

    /// Borrow an item from the buffer. Index 0 is the oldest item in the buffer
    /// with the highest index being the newest item in the buffer.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.storage.get(self.session, index)
    }

    /// How many items are in the buffer?
    pub fn len(&self) -> usize {
        self.storage.count(self.session)
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over mutable borrows of the contents in the buffer.
    pub fn iter_mut<'b>(&'b mut self) -> IterBufferMut<'b, T> {
        self.modified = true;
        self.storage.iter_mut(self.session)
    }

    /// Modify the oldest item in the buffer.
    pub fn oldest_mut(&mut self) -> Option<&mut T> {
        self.modified = true;
        self.storage.oldest_mut(self.session)
    }

    /// Modify the newest item in the buffer.
    pub fn newest_mut(&mut self) -> Option<&mut T> {
        self.modified = true;
        self.storage.newest_mut(self.session)
    }

    /// Modify an item in the buffer. Index 0 is the oldest item in the buffer
    /// with the highest index being the newest item in the buffer.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.modified = true;
        self.storage.get_mut(self.session, index)
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
        accessor: Entity,
        commands: &'a mut Commands<'w, 's>,
    ) -> Self {
        Self { storage, buffer, session, accessor: Some(accessor), commands, modified: false }
    }
}

impl<'w, 's, 'a, T> Drop for BufferMut<'w, 's, 'a, T>
where
    T: 'static + Send + Sync,
{
    fn drop(&mut self) {
        if self.modified {
            self.commands.add(NotifyBufferUpdate::new(
                self.buffer, self.session, self.accessor,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{*, testing::*};

    #[test]
    fn test_buffer_key_access() {
        let mut context = TestingContext::minimal_plugins();

        let add_buffers_cb = add_buffers.into_blocking_callback();
        let add_from_buffer_srv = add_from_buffer.into_blocking_callback();
        let multiply_buffers_cb = multiply_buffers.into_blocking_callback();

        let workflow = context.spawn_io_workflow(
            |scope: Scope<(f64, f64), f64>, builder| {
                scope.input.chain(builder)
                    .unzip()
                    .listen(builder)
                    .then(multiply_buffers_cb)
                    .connect(scope.terminate);
            }
        );

        let mut promise = context.command(|commands|
            commands
            .request((2.0, 3.0), workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|value| value == 6.0));
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(
            |scope: Scope<(f64, f64), f64>, builder| {
                scope.input.chain(builder)
                    .unzip()
                    .listen(builder)
                    .then(add_buffers_cb)
                    .dispose_on_none()
                    .connect(scope.terminate);
            }
        );

        let mut promise = context.command(|commands|
            commands
            .request((4.0, 5.0), workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|value| value == 9.0));

        let workflow = context.spawn_io_workflow(
            |scope: Scope<(f64, f64), Result<f64, f64>>, builder| {
                let (branch_to_add, branch_to_buffer) = scope.input.chain(builder).unzip();
                let buffer = builder.create_buffer::<f64>(BufferSettings::keep_first(10));
                builder.connect(branch_to_buffer, buffer.input_slot());

                let add_node = branch_to_add.chain(builder)
                    .with_access(buffer)
                    .then_node(add_from_buffer_srv.clone());

                add_node.output.chain(builder)
                    .fork_result(
                        // If the buffer had an item in it, we send it to another
                        // node that tries to pull a second time (we expect the
                        // buffer to be empty this second time) and then
                        // terminates.
                        |chain| chain
                            .with_access(buffer)
                            .then(add_from_buffer_srv)
                            .connect(scope.terminate),
                        // If the buffer was empty, keep looping back until there
                        // is a value available.
                        |chain| chain
                            .with_access(buffer)
                            .connect(add_node.input),
                    );
            }
        );

        let mut promise = context.command(|commands|
            commands
            .request((2.0, 3.0), workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|value| value.is_err_and(|n| n == 5.0)));
        assert!(context.no_unhandled_errors());
    }

    fn add_from_buffer(
        In((lhs, key)): In<(f64, BufferKey<f64>)>,
        mut access: BufferAccessMut<f64>,
        // access: BufferAccess<f64>,
    ) -> Result<f64, f64> {
        let rhs = access.get_mut(&key).map_err(|_| lhs)?.pull().ok_or(lhs)?;
        Ok(lhs + rhs)
    }

    fn multiply_buffers(
        In((key_a, key_b)): In<(BufferKey<f64>, BufferKey<f64>)>,
        access: BufferAccess<f64>,
    ) -> f64 {
        *access.get(&key_a).unwrap().oldest().unwrap()
        * *access.get(&key_b).unwrap().oldest().unwrap()
    }

    fn add_buffers(
        In((key_a, key_b)): In<(BufferKey<f64>, BufferKey<f64>)>,
        mut access: BufferAccessMut<f64>,
    ) -> Option<f64> {
        if access.get(&key_a).unwrap().is_empty() {
            return None;
        }

        if access.get(&key_b).unwrap().is_empty() {
            return None;
        }

        let rhs = access.get_mut(&key_a).unwrap().pull().unwrap();
        let lhs = access.get_mut(&key_b).unwrap().pull().unwrap();
        Some(rhs + lhs)
    }
}
