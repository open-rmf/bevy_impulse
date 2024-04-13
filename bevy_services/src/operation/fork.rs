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

use bevy::prelude::{Entity, World};

use crate::{InputStorage, InputBundle, Operation, OperationStatus, OperationRoster, ForkStorage, SourceStorage};

pub(crate) struct Fork<Response: 'static + Send + Sync + Clone> {
    targets: [Entity; 2],
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response: 'static + Send + Sync + Clone> Fork<Response> {
    pub(crate) fn new(targets: [Entity; 2]) -> Self {
        Self { targets, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync + Clone> Operation for Fork<T> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        for target in &self.targets {
            if let Some(mut target_mut) = world.get_entity_mut(*target) {
                target_mut.insert(SourceStorage(entity));
            }
        }
        world.entity_mut(entity).insert(ForkStorage(self.targets));

    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<super::OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let InputStorage::<T>(input) = source_mut.take().ok_or(())?;
        let ForkStorage(targets) = source_mut.take().ok_or(())?;
        if let Some(mut target_mut) = world.get_entity_mut(targets[0]) {
            target_mut.insert(InputBundle::new(input.clone()));
            roster.queue(targets[0]);
        } else {
            roster.cancel(targets[0]);
        }

        if let Some(mut target_mut) = world.get_entity_mut(targets[1]) {
            target_mut.insert(InputBundle::new(input));
            roster.queue(targets[1]);
        } else {
            roster.cancel(targets[1]);
        }

        Ok(OperationStatus::Finished)
    }
}
