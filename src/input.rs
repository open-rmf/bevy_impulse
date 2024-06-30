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
    prelude::{Entity, Component},
    ecs::{
        world::{EntityMut, EntityRef, World},
        system::Command,
    },
};

use smallvec::SmallVec;

use std::collections::HashMap;

use backtrace::Backtrace;

use crate::{
    OperationRoster, OperationError, OrBroken, SingleTargetStorage,
    DeferredRoster, Cancel, Cancellation, CancellationCause, Broken,
};


/// Marker trait to indicate when an input is ready.
#[derive(Component, Default)]
pub(crate) struct InputReady;

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

#[derive(Component)]
pub struct InputBundle<T> {
    ready: InputReady,
    storage: InputStorage<T>,
}

impl<T> InputBundle<T> {
    pub fn new() -> Self {
        Self {
            ready: Default::default(),
            storage: Default::default(),
        }
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

    fn from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity
    ) -> Result<T, OperationError> {
        self.try_from_buffer(session).and_then(|r| r.or_not_ready())
    }

    fn try_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<Option<T>, OperationError>;

    fn cleanup_inputs<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    );
}

pub trait InspectInput {
    fn buffer_ready<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError>;

    fn has_input<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError>;
}

impl<'w> ManageInput for EntityMut<'w> {
    fn give_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError> {
        unsafe { self.sneak_input(session, data); }
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
        let mut buffer = self.get_mut::<Buffer<T>>().or_broken()?;
        let policy = buffer.settings.policy;
        let reverse_queue = buffer.reverse_queues.entry(session).or_default();
        match policy {
            BufferPolicy::KeepFirst(n) => {
                while n > 0 && reverse_queue.len() > n {
                    // This shouldn't happen, but we'll handle it anyway.
                    reverse_queue.remove(0);
                }

                if reverse_queue.len() == n {
                    // We're at the limit for inputs in this queue so just skip
                    return Ok(());
                }
            }
            BufferPolicy::KeepLast(n) => {
                while n > 0 && reverse_queue.len() >= n {
                    reverse_queue.pop();
                }
            }
            BufferPolicy::KeepAll => {
                // Do nothing
            }
        }

        reverse_queue.insert(0, data);

        // CancelInputBuffer does not have a target, it is pull-only, so we
        // should not queue an operation for it.
        if let Some(target) = self.get::<SingleTargetStorage>() {
            roster.queue(target.get());
        }

        Ok(())
    }

    fn try_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
    ) -> Result<Option<T>, OperationError> {
        let mut buffer = self.get_mut::<Buffer<T>>().or_broken()?;
        let reverse_queue = buffer.reverse_queues.entry(session).or_default();
        Ok(reverse_queue.pop())
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

        if let Some(mut buffer) = self.get_mut::<Buffer<T>>() {
            buffer.reverse_queues.remove(&session);
        };
    }
}

impl<'a> InspectInput for EntityMut<'a> {
    fn buffer_ready<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError> {
        let buffer = self.get::<Buffer<T>>().or_broken()?;
        Ok(buffer.reverse_queues.get(&session).is_some_and(|q| !q.is_empty()))
    }

    fn has_input<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError> {
        let inputs = self.get::<InputStorage<T>>().or_broken()?;
        Ok(inputs.contains_session(session))
    }
}

impl<'a> InspectInput for EntityRef<'a> {
    fn buffer_ready<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError> {
        let buffer = self.get::<Buffer<T>>().or_broken()?;
        Ok(buffer.reverse_queues.get(&session).is_some_and(|q| !q.is_empty()))
    }

    fn has_input<T: 'static + Send + Sync>(
        &self,
        session: Entity,
    ) -> Result<bool, OperationError> {
        let inputs = self.get::<InputStorage<T>>().or_broken()?;
        Ok(inputs.contains_session(session))
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
                    source: self.target,
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
pub struct Buffer<T> {
    /// Settings that determine how this buffer will behave.
    settings: BufferSettings,
    /// Map from session ID to a queue of data that has arrived for it. This
    /// is used by nodes that feed into joiner nodes to store input so that it's
    /// readily available when needed.
    reverse_queues: HashMap<Entity, SmallVec<[T; 16]>>,
}

impl<T> Buffer<T> {
    pub fn new(settings: BufferSettings) -> Self {
        Self { settings, reverse_queues: Default::default() }
    }
}

#[derive(Default, Clone, Copy)]
pub struct BufferSettings {
    policy: BufferPolicy,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BufferPolicy {
    KeepLast(usize),
    KeepFirst(usize),
    KeepAll,
}

impl Default for BufferPolicy {
    fn default() -> Self {
        Self::KeepLast(1)
    }
}
