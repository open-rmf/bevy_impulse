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
    prelude::{Entity, Component, Bundle},
    ecs::{
        world::{EntityMut, EntityRef, World},
        system::Command,
    },
};

use smallvec::SmallVec;

use std::collections::HashMap;

use backtrace::Backtrace;

use crate::{
    OperationRoster, OperationError, OrBroken,
    DeferredRoster, Cancel, Cancellation, CancellationCause, Broken,
    BufferSettings, RetentionPolicy, ForkTargetStorage,
};

/// Typical container for input data accompanied by its session information.
/// This defines the elements of [`InputStorage`].
pub struct Input<T> {
    pub session: Entity,
    pub data: T,
}

/// General purpose input storage used by most [operations](crate::Operation).
/// This component is inserted on the source entity of the operation and will
/// queue up inputs that have arrived for the source.
#[derive(Component)]
pub(crate) struct InputStorage<T> {
    // Items will be inserted into this queue from the front, so we pop off the
    // back to get the oldest items out.
    // TODO(@mxgrey): Consider if it's worth implementing a Deque on top of
    // the SmallVec data structure.
    reverse_queue: SmallVec<[Input<T>; 16]>,
}

impl<T> InputStorage<T> {
    pub fn new() -> Self {
        Self { reverse_queue: Default::default() }
    }

    pub fn contains_session(&self, session: Entity) -> bool {
        self.reverse_queue.iter()
        .find(|input| input.session == session)
        .is_some()
    }
}

impl<T> Default for InputStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Bundle)]
pub struct InputBundle<T: 'static + Send + Sync> {
    storage: InputStorage<T>,
}

impl<T: 'static + Send + Sync> InputBundle<T> {
    pub fn new() -> Self {
        Self { storage: Default::default() }
    }
}

pub trait ManageInput {
    fn give_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError>;

    /// Give an input to this node without flagging it in the roster. This
    /// should not generally be used. It's only for special cases where we know
    /// the node will be manually run after giving this input. It's marked
    /// unsafe to bring attention to this requirement.
    unsafe fn sneak_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
    ) -> Result<(), OperationError>;

    /// Get an input that is ready to be taken, or else produce an error.
    fn take_input<T: 'static + Send + Sync>(
        &mut self
    ) -> Result<Input<T>, OperationError>;

    /// Try to take an input if one is ready. If no input is ready this will
    /// return Ok(None). It only returns an error if the node is broken.
    fn try_take_input<T: 'static + Send + Sync>(
        &mut self
    ) -> Result<Option<Input<T>>, OperationError>;

    fn transfer_to_buffer<T: 'static + Send + Sync>(
        &mut self,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError>;

    fn pull_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<T, OperationError> {
        self.try_pull_from_buffer(session).and_then(|r| r.or_broken())
    }

    fn try_pull_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<Option<T>, OperationError>;

    fn consume_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<SmallVec<[T; 16]>, OperationError>;

    fn cleanup_inputs<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    );
}

pub trait InspectInput {
    fn buffered_count<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<usize, OperationError>;

    fn has_input<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError>;

    fn clone_from_buffer<T: 'static + Send + Sync + Clone>(
        &self,
        session: Entity,
    ) -> Result<T, OperationError>
    where
        T: Clone,
    {
        self.try_clone_from_buffer(session).and_then(|r| r.or_broken())
    }

    fn try_clone_from_buffer<T: 'static + Send + Sync + Clone>(
        &self,
        session: Entity,
    ) -> Result<Option<T>, OperationError>
    where
        T: Clone;
}

impl<'w> ManageInput for EntityMut<'w> {
    fn give_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError> {
        unsafe { self.sneak_input(session, data)?; }
        roster.queue(self.id());
        Ok(())
    }

    unsafe fn sneak_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
    ) -> Result<(), OperationError> {
        let mut storage = self.get_mut::<InputStorage<T>>().or_broken()?;
        storage.reverse_queue.insert(0, Input { session, data });
        Ok(())
    }

    fn take_input<T: 'static + Send + Sync>(&mut self) -> Result<Input<T>, OperationError> {
        self.try_take_input()?.or_not_ready()
    }

    fn try_take_input<T: 'static + Send + Sync>(
        &mut self
    ) -> Result<Option<Input<T>>, OperationError> {
        let mut storage = self.get_mut::<InputStorage<T>>().or_broken()?;
        Ok(storage.reverse_queue.pop())
    }

    fn transfer_to_buffer<T: 'static + Send + Sync>(
        &mut self,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError> {
        let Input { session, data } = self.take_input::<T>()?;
        let mut buffer = self.get_mut::<BufferStorage<T>>().or_broken()?;
        if !buffer.push(session, data) {
            // The data is not being stored because the buffer is at its limit,
            // so return here.
            return Ok(());
        }

        let targets = self.get::<ForkTargetStorage>().or_broken()?.0.clone();
        self.world_scope(|world| {
            for target in targets {
                    let mut target_mut = world.get_entity_mut(target).or_broken()?;
                    target_mut.give_input(session, (), roster)?;
            }
            Ok(())
        })?;

        Ok(())
    }

    fn try_pull_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<Option<T>, OperationError> {
        let mut buffer = self.get_mut::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.pull(session))
    }

    fn consume_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<SmallVec<[T; 16]>, OperationError> {
        let mut buffer = self.get_mut::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.consume(session))
    }

    fn cleanup_inputs<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) {
        if let Some(mut inputs) = self.get_mut::<InputStorage<T>>() {
            inputs.reverse_queue.retain(
                |Input { session: r, .. }| *r != session
            );
        }

        if let Some(mut buffer) = self.get_mut::<BufferStorage<T>>() {
            buffer.reverse_queues.remove(&session);
        };
    }
}

impl<'a> InspectInput for EntityMut<'a> {
    fn buffered_count<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<usize, OperationError> {
        let buffer = self.get::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.reverse_queues.get(&session).map(|q| q.len()).unwrap_or(0))
    }

    fn has_input<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError> {
        let inputs = self.get::<InputStorage<T>>().or_broken()?;
        Ok(inputs.contains_session(session))
    }

    fn try_clone_from_buffer<T: 'static + Send + Sync + Clone>(
        &self,
        session: Entity,
    ) -> Result<Option<T>, OperationError>
    where
        T: Clone,
    {
        let buffer = self.get::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.reverse_queues.get(&session).map(|q| q.last().cloned()).flatten())
    }
}

impl<'a> InspectInput for EntityRef<'a> {
    fn buffered_count<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<usize, OperationError> {
        let buffer = self.get::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.reverse_queues.get(&session).map(|q| q.len()).unwrap_or(0))
    }

    fn has_input<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError> {
        let inputs = self.get::<InputStorage<T>>().or_broken()?;
        Ok(inputs.contains_session(session))
    }

    fn try_clone_from_buffer<T: 'static + Send + Sync + Clone>(
        &self,
        session: Entity,
    ) -> Result<Option<T>, OperationError>
    where
        T: Clone,
    {
        let buffer = self.get::<BufferStorage<T>>().or_broken()?;
        Ok(buffer.reverse_queues.get(&session).map(|q| q.last().cloned()).flatten())
    }
}

pub(crate) struct InputCommand<T> {
    pub(crate) target: Entity,
    pub(crate) session: Entity,
    pub(crate) data: T,
}

impl<T: 'static + Send + Sync> Command for InputCommand<T> {
    fn apply(self, world: &mut World) {
        match world.get_mut::<InputStorage<T>>(self.target) {
            Some(mut storage) => {
                storage.reverse_queue
                    .insert(0, Input { session: self.session, data: self.data });

                world.get_resource_or_insert_with(|| DeferredRoster::default())
                    .queue(self.target);
            }
            None => {
                let cause = CancellationCause::Broken(Broken {
                    node: self.target,
                    backtrace: Some(Backtrace::new()),
                });
                let cancel = Cancel {
                    origin: self.target,
                    target: self.session,
                    session: Some(self.session),
                    cancellation: Cancellation::from_cause(cause)
                };

                world.get_resource_or_insert_with(|| DeferredRoster::default())
                    .cancel(cancel);
            }
        }

    }
}

#[derive(Component)]
pub(crate) struct BufferStorage<T> {
    /// Settings that determine how this buffer will behave.
    settings: BufferSettings,
    /// Map from session ID to a queue of data that has arrived for it. This
    /// is used by nodes that feed into joiner nodes to store input so that it's
    /// readily available when needed.
    ///
    /// The main reason we use this as a reverse queue instead of a forward queue
    /// is because SmallVec doesn't have a version of pop that we can use on the
    /// front. We should reconsider whether this is really a sensible choice.
    reverse_queues: HashMap<Entity, SmallVec<[T; 16]>>,
}

impl<T> BufferStorage<T> {
    fn push(&mut self, session: Entity, value: T) -> bool {
        let reverse_queue = self.reverse_queues.entry(session).or_default();
        match self.settings.retention() {
            RetentionPolicy::KeepFirst(n) => {
                while n > 0 && reverse_queue.len() > n {
                    // This shouldn't happen, but we'll handle it anyway.
                    reverse_queue.remove(0);
                }

                if reverse_queue.len() == n {
                    // We're at the limit for inputs in this queue so just skip
                    return false;
                }
            }
            RetentionPolicy::KeepLast(n) => {
                while n > 0 && reverse_queue.len() >= n {
                    reverse_queue.pop();
                }
            }
            RetentionPolicy::KeepAll => {
                // Do nothing
            }
        }

        reverse_queue.insert(0, value);
        return true;
    }

    fn pull(&mut self, session: Entity) -> Option<T> {
        let reverse_queue = self.reverse_queues.entry(session).or_default();
        reverse_queue.pop()
    }

    fn consume(&mut self, session: Entity) -> SmallVec<[T; 16]> {
        let reverse_queue = self.reverse_queues.entry(session).or_default();
        let mut result = SmallVec::new();
        std::mem::swap(reverse_queue, &mut result);
        result.reverse();
        result
    }

    pub(crate) fn new(settings: BufferSettings) -> Self {
        Self {
            settings,
            reverse_queues: Default::default(),
        }
    }
}
