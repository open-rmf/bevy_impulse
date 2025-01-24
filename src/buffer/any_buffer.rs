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
    AnyMessageRef, AnyMessageMut, BoxedMessage, BoxedMessageError, BufferAccessMut,
    DynBufferViewer, DynBufferKey, DynBufferManagement, GateState, BufferStorage,
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
    commands: Commands<'w, 's>,
    modified: bool,
}

#[derive(ThisError, Debug, Clone)]
pub enum AnyBufferError {
    #[error("The key was unable to identify a buffer")]
    BufferMissing,
}

pub(crate) fn access_any_buffer_mut<T: 'static + Send + Sync + Any>(
    key: DynBufferKey,
    f: Box<dyn FnOnce(AnyBufferMut) + '_>,
    world: &mut World,
) -> Result<(), AnyBufferError> {
    let mut state: SystemState<BufferAccessMut<T>> = SystemState::new(world);
    let BufferAccessMut { mut query, commands } = state.get_mut(world);
    let (storage, gate) = query.get_mut(key.buffer).map_err(|_| AnyBufferError::BufferMissing)?;
    let buffer_mut = AnyBufferMut {
        storage: Box::new(storage),
        gate,
        buffer: key.buffer,
        session: key.session,
        accessor: Some(key.accessor),
        commands,
        modified: false,
    };

    f(buffer_mut);
    Ok(())
}
