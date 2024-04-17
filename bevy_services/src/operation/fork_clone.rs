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

use crate::{
    InputStorage, InputBundle, Operation, OperationStatus, OperationRoster,
    ForkTargetStorage, SingleSourceStorage, Cancel,
};

pub(crate) struct ForkClone<Response: 'static + Send + Sync + Clone> {
    targets: ForkTargetStorage,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response: 'static + Send + Sync + Clone> ForkClone<Response> {
    pub(crate) fn new(targets: ForkTargetStorage) -> Self {
        Self { targets, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync + Clone> Operation for ForkClone<T> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        for target in &self.targets.0 {
            if let Some(mut target_mut) = world.get_entity_mut(*target) {
                target_mut.insert(SingleSourceStorage(entity));
            }
        }
        world.entity_mut(entity).insert(self.targets);
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let InputStorage::<T>(input) = source_mut.take().ok_or(())?;
        let ForkTargetStorage(targets) = source_mut.take().ok_or(())?;

        let mut send_value = |value: T, target: Entity| {
            if let Some(mut target_mut) = world.get_entity_mut(target) {
                target_mut.insert(InputBundle::new(value));
                roster.queue(target);
            } else {
                roster.cancel(Cancel::broken(target));
            }
        };

        // Distributing the values like this is a bit convoluted, but it ensures
        // that we are not producing any more clones of the input than what is
        // strictly necessary. This may be valuable if cloning the value is
        // expensive.
        for target in targets[..targets.len()-1].iter() {
            send_value(input.clone(), *target);
        }

        if let Some(last_target) = targets.last() {
            send_value(input, *last_target);
        }

        Ok(OperationStatus::Finished)
    }
}
