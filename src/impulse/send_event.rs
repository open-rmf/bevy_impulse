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

use bevy_ecs::prelude::Event;
use bevy_hierarchy::DespawnRecursiveExt;

use crate::{
    Impulsive, Input, InputBundle, ManageInput, OperationRequest, OperationResult, OperationSetup,
    OrBroken,
};

pub(crate) struct SendEvent<T> {
    _ignore: std::marker::PhantomData<fn(T)>,
}

impl<T> SendEvent<T> {
    pub(crate) fn new() -> Self {
        Self {
            _ignore: Default::default(),
        }
    }
}

impl<T: 'static + Send + Sync + Event> Impulsive for SendEvent<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert(InputBundle::<T>::new());
        Ok(())
    }

    fn execute(OperationRequest { source, world, .. }: OperationRequest) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { data, .. } = source_mut.take_input::<T>()?;
        source_mut.despawn_recursive();
        world.send_event(data);
        Ok(())
    }
}
