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
    prelude::{Entity, Component, DetectChangesMut},
    ecs::world::{EntityMut, EntityRef}
};

use smallvec::SmallVec;

use crate::{OperationRoster, OperationError, OrBroken, SingleTargetStorage};

use std::collections::HashMap;

/// Marker trait to indicate when an input is ready without knowing the type of
/// input.
#[derive(Component, Default)]
pub(crate) struct InputReady {
    requesters: SmallVec<[Entity; 16]>,
}

/// Typical container for input data accompanied by its requester information.
/// This defines the elements of [`InputStorage`].
pub struct Input<T> {
    pub requester: Entity,
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

    pub fn contains_requester(&self, requester: Entity) -> bool {
        self.reverse_queue.iter()
        .find(|input| input.requester == requester)
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
        requester: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError>;

    fn take_input<T: 'static + Send + Sync>(
        &mut self
    ) -> Result<Input<T>, OperationError>;

    fn transfer_to_buffer<T: 'static + Send + Sync>(
        &mut self,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError>;

    fn from_buffer<T: 'static + Send + Sync>(
        &mut self,
        requester: Entity
    ) -> Result<T, OperationError> {
        self.try_from_buffer(requester).and_then(|r| r.or_not_ready())
    }

    fn try_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        requester: Entity,
    ) -> Result<Option<T>, OperationError>;

    fn cleanup_inputs<T: 'static + Send + Sync>(
        &mut self,
        requester: Entity,
    );
}

pub trait InspectInput {
    fn buffer_ready<T: 'static + Send + Sync>(
        &self,
        requester: Entity,
    ) -> Result<bool, OperationError>;

    fn has_input<T: 'static + Send + Sync>(
        &self,
        requester: Entity,
    ) -> Result<bool, OperationError>;
}

impl<'w> ManageInput for EntityMut<'w> {
    fn give_input<T: 'static + Send + Sync>(
        &mut self,
        requester: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError> {
        let source = self.id();
        let mut storage = self.get_mut::<InputStorage<T>>().or_broken()?;
        storage.reverse_queue.push(Input { requester, data });
        self.get_mut::<InputReady>().or_broken()?.set_changed();
        roster.queue(source);
        Ok(())
    }

    fn take_input<T: 'static + Send + Sync>(&mut self) -> Result<Input<T>, OperationError> {
        let source = self.id();
        let mut storage = self.get_mut::<InputStorage<T>>().or_broken()?;
        storage.reverse_queue.pop().or_not_ready()
    }

    fn transfer_to_buffer<T: 'static + Send + Sync>(
        &mut self,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError> {
        let target = self.get::<SingleTargetStorage>().or_broken()?.0;
        let Input { requester, data } = self.take_input::<T>()?;
        let mut buffer = self.get_mut::<Buffer<T>>().or_broken()?;
        let reverse_queue = buffer.reverse_queues.entry(requester).or_default();
        match buffer.settings.policy {
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
        roster.queue(target);

        Ok(())
    }

    fn try_from_buffer<T: 'static + Send + Sync>(
        &mut self,
        requester: Entity,
    ) -> Result<Option<T>, OperationError> {
        let mut buffer = self.get_mut::<Buffer<T>>().or_broken()?;
        let reverse_queue = buffer.reverse_queues.entry(requester).or_default();
        Ok(reverse_queue.pop())
    }

    fn cleanup_inputs<T: 'static + Send + Sync>(
        &mut self,
        requester: Entity,
    ) {
        if let Some(mut inputs) = self.get_mut::<InputStorage<T>>() {
            inputs.reverse_queue.retain(
                |Input { requester: r, .. }| *r != requester
            );
        }

        if let Some(mut buffer) = self.get_mut::<Buffer<T>>() {
            buffer.reverse_queues.remove(&requester);
        };
    }
}

impl<'a> InspectInput for EntityMut<'a> {
    fn buffer_ready<T: 'static + Send + Sync>(
        &self,
        requester: Entity,
    ) -> Result<bool, OperationError> {
        let buffer = self.get::<Buffer<T>>().or_broken()?;
        Ok(buffer.reverse_queues.get(&requester).is_some_and(|q| !q.is_empty()))
    }

    fn has_input<T: 'static + Send + Sync>(
        &self,
        requester: Entity,
    ) -> Result<bool, OperationError> {
        let inputs = self.get::<InputStorage<T>>().or_broken()?;
        Ok(inputs.contains_requester(requester))
    }
}

impl<'a> InspectInput for EntityRef<'a> {
    fn buffer_ready<T: 'static + Send + Sync>(
        &self,
        requester: Entity,
    ) -> Result<bool, OperationError> {
        let buffer = self.get::<Buffer<T>>().or_broken()?;
        Ok(buffer.reverse_queues.get(&requester).is_some_and(|q| !q.is_empty()))
    }

    fn has_input<T: 'static + Send + Sync>(
        &self,
        requester: Entity,
    ) -> Result<bool, OperationError> {
        let inputs = self.get::<InputStorage<T>>().or_broken()?;
        Ok(inputs.contains_requester(requester))
    }
}

pub struct InputCommand<T> {
    target: Entity,
    requester: Entity,
    data: T,
}

#[derive(Component)]
pub(crate) struct Buffer<T> {
    /// Settings that determine how this buffer will behave.
    settings: BufferSettings,
    /// Map from requester ID to a queue of data that has arrived for it. This
    /// is used by nodes that feed into joiner nodes to store input so that it's
    /// readily available when needed.
    reverse_queues: HashMap<Entity, SmallVec<[T; 16]>>,
}

#[derive(Default, Clone)]
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
