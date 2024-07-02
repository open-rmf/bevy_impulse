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

use bevy::prelude::{Entity, Component};

use crate::{
    Impulsive, OperationSetup, OperationRequest, Storage,
    OperationResult, OrBroken, Input, ManageInput,
    Collection, InputBundle,
    add_lifecycle_dependency,
};

#[derive(Component)]
pub(crate) struct InsertResponse<T> {
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> InsertResponse<T> {
    pub(crate) fn new(target: Entity) -> Self {
        Self { target, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Impulsive for InsertResponse<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            self,
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;

    }
}
