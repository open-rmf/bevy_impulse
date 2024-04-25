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
    Operation, SingleTargetStorage, Service, OperationRoster, ServiceRequest,
    SingleSourceStorage, OperationStatus, dispatch_service, Cancel,
};

use bevy::{
    prelude::{Component, Entity, World, Query},
    ecs::system::SystemState,
};

pub(crate) struct OperateService {
    provider: Entity,
    target: Entity,
}

impl OperateService {
    pub(crate) fn new<Request, Response, Streams>(
        provider: Service<Request, Response, Streams>,
        target: Entity,
    ) -> Self {
        Self {
            provider: provider.get(),
            target,
        }
    }
}

impl Operation for OperateService {
    fn set_parameters(self, entity: Entity, world: &mut World) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleSourceStorage(entity));
        }
        world.entity_mut(entity).insert((
            ProviderStorage(self.provider),
            SingleTargetStorage(self.target),
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let source_ref = world.get_entity(source).ok_or(())?;
        let target = source_ref.get::<SingleTargetStorage>().ok_or(())?.0;
        let provider = source_ref.get::<ProviderStorage>().ok_or(())?.0;

        dispatch_service(ServiceRequest { provider, source, target, world, roster });
        Ok(OperationStatus::Unfinished)
    }
}

#[derive(Component)]
struct ProviderStorage(Entity);

pub(crate) fn cancel_service(
    cancelled_provider: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let mut providers_state: SystemState<Query<(Entity, &ProviderStorage)>> =
        SystemState::new(world);
    let providers = providers_state.get(world);
    for (source, ProviderStorage(provider)) in &providers {
        if *provider == cancelled_provider {
            roster.cancel(Cancel::service_unavailable(source, cancelled_provider));
        }
    }
}
