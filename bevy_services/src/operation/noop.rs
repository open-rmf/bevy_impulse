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

use bevy::prelude::{Entity, World};

use crate::{
    Operation, SingleSourceStorage, SingleTargetStorage, InputStorage, InputBundle,
    OperationStatus,
};

pub(crate) struct Noop<T> {
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Noop<T> {
    pub(crate) fn new(target: Entity) -> Self {
        Noop { target, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Operation for Noop<T> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleSourceStorage(entity));
        }
        world.entity_mut(entity).insert(SingleTargetStorage(self.target));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut crate::OperationRoster,
    ) -> Result<crate::OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let target = source_mut.get::<SingleTargetStorage>().ok_or(())?.0;
        let value = source_mut.take::<InputStorage<T>>().ok_or(())?.0;
        let mut target_mut = world.get_entity_mut(target).ok_or(())?;
        target_mut.insert(InputBundle::new(value));
        roster.queue(target);
        Ok(OperationStatus::Finished)
    }
}
