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
    ForkTargetStorage, Operation, Unzippable, SingleSourceStorage, InputStorage, InputBundle,
    OperationStatus,
};

use smallvec::SmallVec;

pub(crate) struct ForkUnzip<T> {
    targets: SmallVec<[Entity; 8]>,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> ForkUnzip<T> {
    pub(crate) fn new(targets: SmallVec<[Entity; 8]>) -> Self {
        Self { targets, _ignore: Default::default() }
    }
}

impl<T: Unzippable + 'static + Send + Sync> Operation for ForkUnzip<T> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        for target in &self.targets {
            if let Some(mut target_mut) = world.get_entity_mut(*target) {
                target_mut.insert(SingleSourceStorage(entity));
            }
        }
        world.entity_mut(entity).insert(ForkTargetStorage::from_iter(self.targets));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut crate::OperationRoster,
    ) -> Result<crate::OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let values = source_mut.take::<InputStorage<T>>().ok_or(())?.0;
        let targets = source_mut.take::<ForkTargetStorage>().ok_or(())?;
        values.distribute_values(&targets, world, roster);
        Ok(OperationStatus::Finished)
    }
}
