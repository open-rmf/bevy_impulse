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
    Operation, Input, ManageInput, InputBundle, OperationRequest, OperationResult,
    OperationReachability, ReachabilityResult, OperationSetup, StreamPack,
    SingleInputStorage, SingleTargetStorage, OrBroken, OperationCleanup,
    Cancellation, Unreachability, InspectDisposals, execute_operation,
    Cancellable, OperationRoster, ManageCancellation,
    OperationError, OperationCancel, Cancel, UnhandledErrors, check_reachability,
    Blocker, Stream, StreamTargetStorage, StreamRequest, AddOperation,
    ScopeSettings, StreamTargetMap, ClearBufferFn, UnusedTarget, Cleanup,
    Buffered, BufferKeyBuilder, TrimBranch, TrimPolicy,
};

use bevy::prelude::{Component, Entity, World, Commands, BuildChildren};

use smallvec::SmallVec;

use std::collections::{HashMap, hash_map::Entry};


pub(crate) struct Trim<T> {
    /// The branches to be trimmed, as defined by the user.
    branches: SmallVec<[TrimBranch; 16]>,

    /// The target that will be notified when the trimming is finished and all
    /// the nodes report that they're clean.
    target: Entity,

    _ignore: std::marker::PhantomData<T>,
}

#[derive(Component)]
struct TrimStorage {
    /// The branches to be trimmed, as defined by the user
    branches: SmallVec<[TrimBranch; 16]>,

    /// The actual operations which need to be cancelled, as calculated the
    /// first time the trim operation gets run. Before the first run, this will
    /// contain a None value.
    ///
    /// We wait until the first run of the operation before calculating this
    /// because we can't be certain that the workflow is fully defined until the
    /// first time it runs. After that the workflow is fixed, so we can just
    /// reuse this.
    operations: Option<SmallVec<[Entity; 16]>>,
}

impl TrimStorage {
    fn new(branches: SmallVec<[TrimBranch; 16]>) -> Self {
        Self { branches, operations: None }
    }
}

// impl<T: 'static + Send + Sync> Operation for Trim<T> {
//     fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
//         world.get_entity_mut(self.target).or_broken()?
//             .insert(SingleInputStorage::new(source));

//         world.entity_mut(source).insert((
//             TrimStorage::new(self.branches),
//             SingleTargetStorage::new(self.target),
//             InputBundle::<T>::new(),
//         ));

//         Ok(())
//     }

//     fn execute(
//         OperationRequest { source, world, roster }: OperationRequest,
//     ) -> OperationResult {

//     }
// }
