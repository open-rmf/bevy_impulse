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

use crate::{
    Operation, SingleTargetStorage, Service, OperationRoster, ServiceRequest,
    SingleInputStorage, dispatch_service, OperationCleanup, WorkflowHooks,
    OperationResult, OrBroken, OperationSetup, OperationRequest, StreamPack,
    ActiveTasksStorage, OperationReachability, ReachabilityResult,
    InputBundle, Input, ManageDisposal, Disposal, ManageInput, UnhandledErrors,
    DisposalFailure, ActiveContinuousSessions, DeliveryInstructions, Delivery,
    DisposeForUnavailableService,
};

use bevy_ecs::{
    prelude::{Component, Entity, World, Query},
    system::SystemState,
};

use smallvec::SmallVec;

pub(crate) struct Injection<Request, Response, Streams> {
    target: Entity,
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

// impl<Request, Response, Streams> Operation for Injection<Request, Response, Streams>
// where
//     Request: 'static + Send + Sync,
//     Response: 'static + Send + Sync,
//     Streams: StreamPack,
// {
//     fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
//         world.get_entity_mut(self.target).or_broken()?
//             .insert(SingleInputStorage::new(source));

//         world.entity_mut(source).insert((
//             InjectionStorage::default(),
//             InputBundle::<Request>::new(),
//             SingleTargetStorage::new(self.target),
//             ActiveTasksStorage::default(),
//             DisposeForUnavailableService::new::<Request>(),
//         ));

//         Ok(())
//     }
// }

#[derive(Component, Default)]
struct InjectionStorage {
    list: SmallVec<[Injected; 16]>,
}

struct Injected {
    provider: Entity,
    session: Entity,
    task_id: Entity,
}
