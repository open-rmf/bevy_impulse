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

use bevy_ecs::prelude::{Entity, World};

use smallvec::SmallVec;

use crate::{
    Accessing, AnyBuffer, AsAnyBuffer, Buffer, BufferKey, BufferKeyBuilder, BufferKeyLifecycle,
    BufferLocation, Bufferable, Buffering, Builder, CloneFromBuffer, Gate, InputSlot,
    InspectBuffer, JoinBehavior, Joining, ManageBuffer, OperationError, OperationResult,
    OperationRoster, OrBroken,
};

/// This is an alternative to the [`Buffer`] and [`CloneFromBuffer`] structs
/// which erases the explicit join behavior (pull vs clone) from the data type.
/// Instead it stores a function pointer and an enum for the intended behavior.
///
/// This is used by the [`Joined`][crate::Joined] macro to allow an
/// [`AnyBuffer`][crate::AnyBuffer] to have its join behavior preserved when it
/// gets downcast into a specific buffer type.
pub struct FetchFromBuffer<T> {
    location: BufferLocation,
    fetch_for_join: FetchFromBufferFn<T>,
    join_behavior: JoinBehavior,
}

pub(super) type FetchFromBufferFn<T> =
    fn(&FetchFromBuffer<T>, Entity, &mut World) -> Result<T, OperationError>;

impl<T: 'static + Send + Sync> FetchFromBuffer<T> {
    /// Get an input slot for this buffer.
    pub fn input_slot(self) -> InputSlot<T> {
        InputSlot::new(self.scope(), self.id())
    }

    /// Get the entity ID of the buffer.
    pub fn id(&self) -> Entity {
        self.location.source
    }

    /// Get the ID of the workflow that the buffer is associated with.
    pub fn scope(&self) -> Entity {
        self.location.scope
    }

    /// Get general information about the buffer.
    pub fn location(&self) -> BufferLocation {
        self.location
    }
}

impl<T> Clone for FetchFromBuffer<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for FetchFromBuffer<T> {}

impl<T: 'static + Send + Sync> From<Buffer<T>> for FetchFromBuffer<T> {
    fn from(value: Buffer<T>) -> Self {
        FetchFromBuffer {
            location: value.location,
            fetch_for_join: pull_for_join::<T>,
            join_behavior: JoinBehavior::Pull,
        }
    }
}

impl<T: 'static + Send + Sync> From<FetchFromBuffer<T>> for Buffer<T> {
    fn from(value: FetchFromBuffer<T>) -> Self {
        Buffer {
            location: value.location,
            _ignore: Default::default(),
        }
    }
}

impl<T: 'static + Send + Sync + Clone> From<CloneFromBuffer<T>> for FetchFromBuffer<T> {
    fn from(value: CloneFromBuffer<T>) -> Self {
        FetchFromBuffer {
            location: value.location,
            fetch_for_join: clone_for_join::<T>,
            join_behavior: JoinBehavior::Clone,
        }
    }
}

impl<T: 'static + Send + Sync> From<FetchFromBuffer<T>> for AnyBuffer {
    fn from(value: FetchFromBuffer<T>) -> Self {
        let interface = AnyBuffer::interface_for::<T>();
        AnyBuffer {
            location: value.location,
            join_behavior: value.join_behavior,
            interface,
        }
    }
}

impl<T: 'static + Send + Sync> AsAnyBuffer for FetchFromBuffer<T> {
    fn as_any_buffer(&self) -> AnyBuffer {
        (*self).into()
    }
}

// NOTE: It is important that this trait is implemented even if T does not
// implement Clone.
impl<T: 'static + Send + Sync> TryFrom<AnyBuffer> for FetchFromBuffer<T> {
    type Error = OperationError;
    fn try_from(value: AnyBuffer) -> Result<Self, Self::Error> {
        let fetch_for_join = match value.join_behavior {
            JoinBehavior::Pull => pull_for_join::<T>,
            JoinBehavior::Clone => *value
                .interface
                .clone_for_join_fn()
                .or_broken()?
                .downcast_ref::<FetchFromBufferFn<T>>()
                .or_broken()?,
        };

        Ok(FetchFromBuffer {
            location: value.location,
            fetch_for_join,
            join_behavior: value.join_behavior,
        })
    }
}

fn pull_for_join<T: 'static + Send + Sync>(
    buffer: &FetchFromBuffer<T>,
    session: Entity,
    world: &mut World,
) -> Result<T, OperationError> {
    world
        .get_entity_mut(buffer.id())
        .or_broken()?
        .pull_from_buffer::<T>(session)
}

pub(super) fn clone_for_join<T: 'static + Send + Sync + Clone>(
    buffer: &FetchFromBuffer<T>,
    session: Entity,
    world: &mut World,
) -> Result<T, OperationError> {
    world
        .get_entity(buffer.id())
        .or_broken()?
        .clone_from_buffer::<T>(session)
}

impl<T: 'static + Send + Sync> Bufferable for FetchFromBuffer<T> {
    type BufferType = Self;
    fn into_buffer(self, builder: &mut Builder) -> Self::BufferType {
        assert_eq!(self.scope(), builder.scope());
        self
    }
}

impl<T: 'static + Send + Sync> Buffering for FetchFromBuffer<T> {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope());
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        Buffer::<T>::buffered_count(&(*self).into(), session, world)
    }

    fn buffered_count_for(
        &self,
        buffer: Entity,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        Buffer::<T>::buffered_count_for(&(*self).into(), buffer, session, world)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        Buffer::<T>::add_listener(&(*self).into(), listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        Buffer::<T>::gate_action(&(*self).into(), session, action, world, roster)
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        Buffer::<T>::as_input(&(*self).into())
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        Buffer::<T>::ensure_active_session(&(*self).into(), session, world)
    }
}

impl<T: 'static + Send + Sync> Joining for FetchFromBuffer<T> {
    type Item = T;
    fn fetch_for_join(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        let f = self.fetch_for_join;
        f(self, session, world)
    }
}

impl<T: 'static + Send + Sync> Accessing for FetchFromBuffer<T> {
    type Key = BufferKey<T>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        Buffer::<T>::add_accessor(&(*self).into(), accessor, world)
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        Buffer::<T>::create_key(&(*self).into(), builder)
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        key.deep_clone()
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        key.is_in_use()
    }
}
