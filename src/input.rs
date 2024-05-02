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
    ecs::world::EntityMut,
};

use smallvec::SmallVec;

use crate::{OperationRoster, OperationError, OrBroken};

/// Marker trait to indicate when an input is ready without knowing the type of
/// input.
#[derive(Component, Default)]
pub(crate) struct InputReady {
    requesters: SmallVec<[Entity; 16]>,
}

pub struct Input<T> {
    pub requester: Entity,
    pub data: T,
}

#[derive(Component)]
pub struct InputStorage<T> {
    pub reverse_queue: SmallVec<[Input<T>; 16]>,
}

impl<T> InputStorage<T> {
    pub fn new() -> Self {
        Self { reverse_queue: Default::default() }
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

    fn take_input<T: 'static + Send + Sync>(&mut self) -> Result<Input<T>, OperationError>;
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
        storage.reverse_queue.pop().or_broken()
    }
}
