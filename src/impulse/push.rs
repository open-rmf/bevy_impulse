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
    OperationResult, OrBroken, Input, ManageInput,
    Collection, InputBundle,
    add_lifecycle_dependency,
};

#[derive(Component)]
pub(crate) struct Push<T> {
    target: Entity,
    is_stream: bool,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Push<T> {
    pub(crate) fn new(target: Entity, is_stream: bool) -> Self {
        Self { target, is_stream, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Impulsive for Push<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        if !self.is_stream {
            add_lifecycle_dependency(source, self.target, world);
        }
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            self
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<T>()?;
        let target = source_mut.get::<Push<T>>().or_broken()?.target;
        let mut target_mut = world.get_entity_mut(target).or_broken()?;
        if let Some(mut collection) = target_mut.get_mut::<Collection<T>>() {
            collection.items.push(Storage { session, data });
        } else {
            let mut collection = Collection::default();
            collection.items.push(Storage { session, data });
            target_mut.insert(collection);
        }
        world.entity_mut(source).despawn_recursive();
        Ok(())
    }
}

