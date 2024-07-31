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
    SingleInputStorage, dispatch_service, OperationCleanup,
    OperationResult, OrBroken, OperationSetup, OperationRequest,
    ActiveTasksStorage, OperationReachability, ReachabilityResult,
    InputBundle, Input, ManageDisposal, Disposal, ManageInput, UnhandledErrors,
    DisposalFailure, ActiveContinuousSessions, DeliveryInstructions,
};

use bevy::{
    prelude::{Component, Entity, World, Query},
    ecs::system::SystemState,
};

use smallvec::SmallVec;

use backtrace::Backtrace;

pub(crate) struct OperateService<Request> {
    provider: Entity,
    instructions: Option<DeliveryInstructions>,
    target: Entity,
    _ignore: std::marker::PhantomData<Request>,
}

impl<Request: 'static + Send + Sync> OperateService<Request> {
    pub(crate) fn new<Response, Streams>(
        service: Service<Request, Response, Streams>,
        target: Entity,
    ) -> Self {
        Self {
            provider: service.provider(),
            instructions: service.instructions().copied(),
            target,
            _ignore: Default::default(),
        }
    }
}

impl<Request: 'static + Send + Sync> Operation for OperateService<Request> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<Request>::new(),
            ProviderStorage(self.provider),
            SingleTargetStorage::new(self.target),
            ActiveTasksStorage::default(),
            DisposeForUnavailableService(dispose_for_unavailable_service::<Request>),
        ));
        if let Some(instructions) = self.instructions {
            world.entity_mut(source).insert(instructions);
        }
        Ok(())
    }

    fn execute(operation: OperationRequest) -> OperationResult {
        let source_ref = operation.world.get_entity(operation.source).or_broken()?;
        let target = source_ref.get::<SingleTargetStorage>().or_broken()?.0;
        let provider = source_ref.get::<ProviderStorage>().or_broken()?.0;

        dispatch_service(ServiceRequest { provider, target, operation });
        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<Request>()?;
        clean.cleanup_disposals()?;
        ActiveTasksStorage::cleanup(clean)
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<Request>()? {
            return Ok(true);
        }
        if ActiveTasksStorage::contains_session(&reachability)? {
            return Ok(true);
        }
        if ActiveContinuousSessions::contains_session(&reachability)? {
            return Ok(true);
        }
        SingleInputStorage::is_reachable(&mut reachability)
    }
}

#[derive(Component)]
pub(crate) struct ProviderStorage(Entity);

impl ProviderStorage {
    pub(crate) fn get(&self) -> Entity {
        self.0
    }
}

pub(crate) fn dispose_for_despawned_service(
    despawned_service: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let mut providers_state: SystemState<Query<
        (Entity, &ProviderStorage, Option<&DisposeForUnavailableService>)
    >> = SystemState::new(world);
    let providers = providers_state.get(world);
    let mut needs_disposal: SmallVec<[_; 16]> = SmallVec::new();
    for (source, ProviderStorage(provider), disposer) in &providers {
        if *provider == despawned_service {
            needs_disposal.push((source, disposer.copied()));
        }
    }

    for (source, disposer) in needs_disposal {
        if let Some(disposer) = disposer {
            (disposer.0)(source, despawned_service, world, roster);
        } else {
            world
            .get_resource_or_insert_with(|| UnhandledErrors::default())
            .disposals
            .push(DisposalFailure {
                disposal: Disposal::service_unavailable(despawned_service, source),
                broken_node: source,
                backtrace: Some(Backtrace::new()),
            });
        }
    }
}

#[derive(Component, Clone, Copy)]
pub(crate) struct DisposeForUnavailableService(fn(Entity, Entity, &mut World, &mut OperationRoster));

fn dispose_for_unavailable_service<T: 'static + Send + Sync>(
    source: Entity,
    service: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    let disposal = Disposal::service_unavailable(service, source);
    if let Some(mut source_mut) = world.get_entity_mut(source) {
        while let Ok(Input { session, .. }) = source_mut.take_input::<T>() {
            source_mut.emit_disposal(session, disposal.clone(), roster);
        }
    } else {
        world
        .get_resource_or_insert_with(|| UnhandledErrors::default())
        .disposals
        .push(DisposalFailure {
            disposal,
            broken_node: source,
            backtrace: Some(Backtrace::new()),
        });
    }
}
