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

use crate::{
    Operation, TargetStorage, Provider, Dispatch, OperationRoster,
    OperationStatus, DispatchCommand, dispatch_service,
};

use bevy::prelude::{Component, Entity, World};

use std::collections::VecDeque;

pub(crate) struct Serve {
    provider: Entity,
    target: Entity,
}

impl Serve {
    pub(crate) fn new<Request, Response, Streams>(
        provider: Provider<Request, Response, Streams>,
        target: Entity,
    ) -> Self {
        Self {
            provider: provider.get(),
            target,
        }
    }
}

impl Operation for Serve {
    fn set_parameters(self, entity: Entity, world: &mut World) {
        world.entity_mut(entity).insert((
            ProviderStorage(self.provider),
            TargetStorage(self.target),
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        queue: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let source_ref = world.get_entity(source).ok_or(())?;
        let target = source_ref.get::<TargetStorage>().ok_or(())?.0;
        let provider = source_ref.get::<ProviderStorage>().ok_or(())?.0;

        dispatch_service(DispatchCommand::new(provider, source, target), world, queue);
        Ok(OperationStatus::Queued(provider))
    }
}

#[derive(Component)]
struct ProviderStorage(Entity);
