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
    Buffered, BufferKeyBuilder, InputSlot,
};

use bevy::prelude::{Component, Entity, World, Commands, BuildChildren};

use smallvec::SmallVec;

use std::collections::{HashMap, hash_map::Entry};

/// Define a branch for the trim operator to cancel all activity along.
#[derive(Clone, Debug)]
pub struct TrimBranch {
    from_point: TrimPoint,
    policy: TrimPolicy,
}

impl TrimBranch {
    /// Just cancel a single node in the workflow. If the provided [`TrimPoint`]
    /// is not inclusive then this will do nothing at all.
    pub fn single_point(point: TrimPoint) -> Self {
        Self { from_point: point, policy: TrimPolicy::Span(Default::default()) }
    }

    /// Trim everything downstream from the initial point.
    ///
    /// In the event of any cycles, any nodes between the scope entry point and
    /// the initial trim point will not be included.
    pub fn downstream(from_point: TrimPoint) -> Self {
        Self { from_point, policy: TrimPolicy::Downstream }
    }

    /// Trim the shortest path between the two points.
    pub fn between(from_point: TrimPoint, to_point: TrimPoint) -> Self {
        Self { from_point, policy: TrimPolicy::ShortestPathTo(to_point) }
    }

    /// Trim every node that exists along some path between the initial point and
    /// any point in the set of endpoints.
    ///
    /// In the event of any cycles, any nodes which lead back to the initial
    /// point without also leading to one of the endpoints will not be included.
    ///
    /// If the set of endpoints are emtpy, this behaves the same as [`Self::single_point`].
    pub fn span(from_point: TrimPoint, endpoints: SmallVec<[TrimPoint; 16]>) -> Self {
        Self { from_point, policy: TrimPolicy::Span(endpoints) }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TrimPoint {
    id: Entity,
    inclusive: bool,
}

impl TrimPoint {
    /// Define where a trim will begin or end
    //
    // TODO(@mxgrey): It would be good if we could also accept an Output<T> as
    // a reference point, but there is a risk that the output ID will be
    // invalidated after it gets connected to another node. We would need to do
    // additional bookkeeping to update every trim operation about the change
    // during the connection command. This is doable but seems error prone, so
    // we are deprioritizing it for now.
    pub fn new<T>(input: &InputSlot<T>, inclusive: bool) -> Self {
        Self { id: input.id(), inclusive }
    }

    /// Define where a trim will begin or end, and include the point as part of
    /// the trimming.
    pub fn inclusive<T>(input: &InputSlot<T>) -> Self {
        Self::new(input, true)
    }

    /// Define where a trim will begin or end, and exclude the point from being
    /// trimmed.
    pub fn exclusive<T>(input: &InputSlot<T>) -> Self {
        Self::new(input, false)
    }

    pub fn id(&self) -> Entity {
        self.id
    }

    pub fn is_inclusive(&self) -> bool {
        self.inclusive
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TrimPolicy {
    Downstream,
    ShortestPathTo(TrimPoint),
    Span(SmallVec<[TrimPoint; 16]>),
}
