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

use std::any::Any;

use bevy_ecs::{
    prelude::{Entity, Commands, Mut, World, Query},
    system::SystemState,
};

use crate::{
    AnyBufferStorageAccess, AnyMessageRef, AnyMessageMut, BoxedMessage,
    BoxedMessageError, BufferAccessMut, DynBufferViewer, AnyBufferKey,
    DynBufferManagement, GateState, BufferStorage,
};

use thiserror::Error as ThisError;

/// Similar to [`BufferMut`][crate::BufferMut], but this can be unlocked with a
/// [`DynBufferKey`], so it can work for any buffer regardless of the data type
/// inside.
pub struct AnyBufferMut<'w, 's, 'a> {
    storage: Box<dyn DynBufferManagement<'a, AnyMessageRef<'a>, AnyMessageMut<'a>, BoxedMessage, BoxedMessageError> + 'a>,
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

    /// Get how many messages are in this buffer.
    pub fn len(&self) -> usize {
        self.storage.dyn_count(self.session)
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
        key: AnyBufferKey,
        f: impl FnOnce(AnyBufferMut) -> U,
    ) -> Result<U, AnyBufferError>;
}

impl AnyBufferWorldAccess for World {
    fn any_buffer_mut<U>(
        &mut self,
        key: AnyBufferKey,
        f: impl FnOnce(AnyBufferMut) -> U,
    ) -> Result<U, AnyBufferError> {
        let create_state = self.get::<AnyBufferStorageAccess>(key.buffer)
            .ok_or(AnyBufferError::BufferMissing)?
            .create_any_buffer_access_mut_state;

        let mut state = create_state(self);
        let mut access = state.get_buffer_access_mut(self);
        let buffer_mut = access.as_any_buffer_mut(key)?;
        Ok(f(buffer_mut))
    }
}

pub(crate) trait AnyBufferAccessMutState {
    fn get_buffer_access_mut<'s, 'w: 's>(&'s mut self, world: &'w mut World) -> Box<dyn AnyBufferAccessMut<'w, 's> + 's>;
}

impl<T: 'static + Send + Sync + Any> AnyBufferAccessMutState for SystemState<BufferAccessMut<'static, 'static, T>> {
    fn get_buffer_access_mut<'s, 'w: 's>(&'s mut self, world: &'w mut World) -> Box<dyn AnyBufferAccessMut<'w, 's> + 's> {
        Box::new(self.get_mut(world))
    }
}

trait AnyBufferAccessMut<'w, 's> {
    fn as_any_buffer_mut<'a>(&'a mut self, key: AnyBufferKey) -> Result<AnyBufferMut<'w, 's, 'a>, AnyBufferError>;
}

impl<'w, 's, T: 'static + Send + Sync + Any> AnyBufferAccessMut<'w, 's> for BufferAccessMut<'w, 's, T> {
    fn as_any_buffer_mut<'a>(&'a mut self, key: AnyBufferKey) -> Result<AnyBufferMut<'w, 's, 'a>, AnyBufferError> {
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
