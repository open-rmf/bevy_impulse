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

use crate::{TargetStorage, InputStorage, InputBundle, Operation, OperationStatus, OperationRoster};

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
    fn set_parameters(self, entity: Entity, world: &mut World) {
        world.entity_mut(entity).insert((
            TargetStorage(self.target),
            MapStorage(self.f),
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<super::OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let InputStorage(input) = source_mut.take::<InputStorage<T>>().ok_or(())?;
        let f = source_mut.take::<MapStorage<T, U>>().ok_or(())?.0;
        let TargetStorage(target) = source_mut.get().ok_or(())?;
        let target = *target;
        let mut target_mut = world.get_entity_mut(target).ok_or(())?;

        let u = f(input);
        target_mut.insert(InputBundle::new(u));
        roster.queue(target);
        Ok(OperationStatus::Finished)
    }
}

#[derive(Component)]
struct MapStorage<T, U>(Box<dyn FnOnce(T) -> U + Send + Sync>);
