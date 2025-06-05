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

use bevy_ecs::{
    change_detection::Mut,
    prelude::{Commands, Entity, Query},
    query::QueryEntityError,
    system::SystemParam,
};

use crate::{AnyBufferKey, Gate, GateState, NotifyBufferUpdate};

/// This system parameter lets you get read-only access to the gate of a buffer
/// that exists within a workflow. Use a [`BufferKey`][1] or [`AnyBufferKey`]
/// to unlock the access.
///
/// See [`BufferGateAccessMut`] for mutable access.
///
/// [1]: crate::BufferKey
#[derive(SystemParam)]
pub struct BufferGateAccess<'w, 's> {
    query: Query<'w, 's, &'static GateState>,
}

impl<'w, 's> BufferGateAccess<'w, 's> {
    pub fn get<'a>(
        &'a self,
        key: impl Into<AnyBufferKey>,
    ) -> Result<BufferGateView<'a>, QueryEntityError> {
        let key: AnyBufferKey = key.into();
        let session = key.session();
        self.query
            .get(key.id())
            .map(|gate| BufferGateView { gate, session })
    }
}

/// Access to view the gate of a buffer that exists inside a workflow.
pub struct BufferGateView<'a> {
    pub(crate) gate: &'a GateState,
    pub(crate) session: Entity,
}

impl<'a> BufferGateView<'a> {
    /// Check whether the gate of this buffer is open or closed
    pub fn gate(&self) -> Gate {
        // Gate buffers are open by default, so pretend it's open if a session
        // has never touched this buffer gate.
        self.gate
            .map
            .get(&self.session)
            .copied()
            .unwrap_or(Gate::Open)
    }
}

/// This system parameter lets you get mutable access to the gate of a buffer
/// that exists within a workflow. Use a [`BufferKey`][1] or [`AnyBufferKey`]
/// to unlock the access.
///
/// See [`BufferGateAccess`] for read-only access.
#[derive(SystemParam)]
pub struct BufferGateAccessMut<'w, 's> {
    query: Query<'w, 's, &'static mut GateState>,
    commands: Commands<'w, 's>,
}

impl<'w, 's> BufferGateAccessMut<'w, 's> {
    pub fn get<'a>(
        &'a self,
        key: impl Into<AnyBufferKey>,
    ) -> Result<BufferGateView<'a>, QueryEntityError> {
        let key: AnyBufferKey = key.into();
        let session = key.session();
        self.query
            .get(key.id())
            .map(|gate| BufferGateView { gate, session })
    }

    pub fn get_mut<'a>(
        &'a mut self,
        key: impl Into<AnyBufferKey>,
    ) -> Result<BufferGateMut<'w, 's, 'a>, QueryEntityError> {
        let key: AnyBufferKey = key.into();
        let buffer = key.id();
        let session = key.session();
        let accessor = key.tag.accessor;
        self.query
            .get_mut(buffer)
            .map(|gate| BufferGateMut::new(gate, buffer, session, accessor, &mut self.commands))
    }
}

/// Access to mutate the gate of a buffer that exists inside a workflow.
pub struct BufferGateMut<'w, 's, 'a> {
    gate: Mut<'a, GateState>,
    buffer: Entity,
    session: Entity,
    accessor: Option<Entity>,
    commands: &'a mut Commands<'w, 's>,
    modified: bool,
}

impl<'w, 's, 'a> BufferGateMut<'w, 's, 'a> {
    /// See documentation of [`crate::BufferMut::allow_closed_loops`].
    pub fn allow_closed_loops(mut self) -> Self {
        self.accessor = None;
        self
    }

    /// Check whether the gate of this buffer is open or closed.
    pub fn get(&self) -> Gate {
        self.gate
            .map
            .get(&self.session)
            .copied()
            .unwrap_or(Gate::Open)
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

    fn new(
        gate: Mut<'a, GateState>,
        buffer: Entity,
        session: Entity,
        accessor: Entity,
        commands: &'a mut Commands<'w, 's>,
    ) -> Self {
        Self {
            gate,
            buffer,
            session,
            accessor: Some(accessor),
            commands,
            modified: false,
        }
    }
}

impl<'w, 's, 'a> Drop for BufferGateMut<'w, 's, 'a> {
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
