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

use bevy::prelude::{Entity, Component, DespawnRecursiveExt};

use crate::{
    Impulsive, OperationSetup, OperationRequest, Storage,
    OperationResult, OrBroken, Input, ManageInput, InputBundle,
    add_lifecycle_dependency,
};

#[derive(Component)]
pub(crate) struct StoreResponse<Response> {
    target: Entity,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response> StoreResponse<Response> {
    pub(crate) fn new(target: Entity) -> Self {
        Self { target, _ignore: Default::default() }
    }
}

impl<Response: 'static + Send + Sync> Impulsive for StoreResponse<Response> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        add_lifecycle_dependency(source, self.target, world);
        world.entity_mut(source).insert((
            InputBundle::<Response>::new(),
            self
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<Response>()?;
        let target = source_mut.get::<StoreResponse<Response>>().or_broken()?.target;
        world.get_entity_mut(target).or_broken()?.insert(Storage { data, session });
        world.entity_mut(source).despawn_recursive();
        Ok(())
    }
}
