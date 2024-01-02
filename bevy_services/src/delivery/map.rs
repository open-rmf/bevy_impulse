/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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

use bevy::prelude::{Entity, World, Component};
use std::collections::VecDeque;

use crate::{TargetStorage, InputStorage, InputBundle, Operation, OperationStatus};

pub(crate) struct Map<T, U> {
    target: Entity,
    f: Box<dyn FnOnce(T) -> U + Send + Sync>,
}

impl<T, U> Map<T, U> {
    pub(crate) fn new(
        target: Entity,
        f: Box<dyn FnOnce(T) -> U + Send + Sync>,
    ) -> Self {
        Map { target, f }
    }
}

impl<T: 'static + Send + Sync, U: 'static + Send + Sync> Operation for Map<T, U> {
    type Parameters = (TargetStorage, MapStorage<T, U>);

    fn parameters(self) -> Self::Parameters {
        (
            TargetStorage(self.target),
            MapStorage(self.f),
        )
    }

    fn execute(
        source: Entity,
        world: &mut World,
        queue: &mut VecDeque<Entity>,
    ) -> Result<super::OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let InputStorage(input) = source_mut.take::<InputStorage<T>>().ok_or(())?;
        let MapStorage(f) = source_mut.take::<MapStorage<T, U>>().ok_or(())?;
        let TargetStorage(target) = source_mut.get::<TargetStorage>().ok_or(())?;
        let mut target_mut = world.get_entity_mut(*target).ok_or(())?;

        let u = (f)(input);
        target_mut.insert(InputBundle::new(u));
        Ok(OperationStatus::Finished)
    }
}

#[derive(Component)]
struct MapStorage<T, U>(Box<dyn FnOnce(T) -> U + Send + Sync>);
