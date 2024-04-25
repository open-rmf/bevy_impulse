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

use bevy::prelude::Entity;

use backtrace::Backtrace;

use std::sync::Arc;

use crate::FunnelInputStatus;

/// Response type that gets sent when a cancellation occurs.
#[derive(Debug)]
pub struct Cancelled<Signal> {
    pub signal: Signal,
    pub cancellation: Cancellation,
}

/// Information about the cancellation that occurred.
#[derive(Debug, Clone)]
pub struct Cancellation {
    pub cause: Arc<CancellationCause>,
}

impl Cancellation {
    pub fn from_cause(cause: CancellationCause) -> Self {
        Self { cause: Arc::new(cause) }
    }
}

/// Get an explanation for why a cancellation occurred. In most cases the
/// entities provided by these enums will already be despawned by the time you
/// receive this information, but it may be useful to look at them if you need
/// to debug.
#[derive(Debug)]
pub enum CancellationCause {
    /// The target at the end of a chain is unused, meaning a chain was built
    /// but the builder did not end with a [`Chain::detach()`] or a
    /// [`Chain::take()`]. The entity provided in the variant is the unused
    /// target.
    UnusedTarget(Entity),

    /// A [`Service`](crate::Service) provider needed by the chain was despawned
    /// or had a critical component removed. The entity provided in the variant
    /// is the unavailable service.
    ServiceUnavailable(Entity),

    /// The final target of the chain was dropped without detaching, which
    /// implyies that this chain is no longer needed.
    TargetDropped(Entity),

    /// Async services with serial delivery will queue up requests to deliver
    /// them one at a time. Depending on the [label settings](crate::LabelBuilder)
    /// of the incoming requests, a new request might supplant an earlier one,
    /// causing the earlier request to be cancelled.
    Supplanted(Supplanted),

    /// A link in the chain was broken, for example despawned or missing a
    /// component. This type of cancellation indicates that you are modifying
    /// the entities in chain in an unsupported way. If you believe that you are
    /// not doing anything unsupported then this could indicate a bug in
    /// `bevy_impulse`, and you encouraged to open an issue with a minimal
    /// reproducible example.
    ///
    /// The entity provided in the variant is the link where the breakage was
    /// detected.
    BrokenLink(BrokenLink),

    /// A link in the chain filtered out a response.
    Filtered(Entity),

    /// All the branches of a fork were cancelled.
    ForkCancelled(ForkCancelled),

    /// A join was cancelled due to one of these scenarios:
    /// * At least one of its inputs was cancelled
    /// * At least one of its inputs was delivered but one or more of the inputs
    ///   were cancelled or disposed.
    ///
    /// Note that if all inputs for the join are disposed instead of cancelled,
    /// then the join will disposed and not cancelled.
    JoinCancelled(JoinCancelled),

    /// A race was cancelled because all of its inputs were either cancelled or
    /// disposed, with at least one of them being a cancel.
    ///
    /// Note that if all of the inputs for a race are disposed instead of
    /// cancelled, then the race will be disposed and not cancelled.
    RaceCancelled(RaceCancelled),

    /// The chain lost a race so it is being cancelled.
    RaceLost(RaceLost),
}

#[derive(Debug, Clone)]
pub struct BrokenLink {
    pub source: Entity,
    pub backtrace: Option<Backtrace>,
}

impl From<BrokenLink> for CancellationCause {
    fn from(value: BrokenLink) -> Self {
        CancellationCause::BrokenLink(value)
    }
}

#[derive(Debug)]
pub struct Supplanted {
    /// Entity of the link in the chain that was supplanted
    pub cancelled_at: Entity,
    /// Entity of the link in a different chain that did the supplanting
    pub supplanter: Entity,
}

impl From<Supplanted> for CancellationCause {
    fn from(value: Supplanted) -> Self {
        CancellationCause::Supplanted(value)
    }
}

/// A description of why a fork was cancelled.
#[derive(Debug, Clone)]
pub struct ForkCancelled {
    /// The source link of the fork
    pub fork: Entity,
    /// The cancellation cause of each downstream branch of a fork.
    pub cancelled: Vec<Cancellation>,
}

/// A description of why a join was cancelled.
#[derive(Debug, Clone)]
pub struct JoinCancelled {
    /// The source link of the join
    pub join: Entity,
    /// The statuses for the inputs of the join.
    pub input_statuses: Vec<(Entity, FunnelInputStatus)>,
}

impl From<JoinCancelled> for CancellationCause {
    fn from(value: JoinCancelled) -> Self {
        CancellationCause::JoinCancelled(value)
    }
}

/// A description of why a race was cancelled.
#[derive(Debug, Clone)]
pub struct RaceCancelled {
    /// The source link of the race
    pub race: Entity,
    /// The statuses of the inputs for this race.
    pub input_statuses: Vec<(Entity, FunnelInputStatus)>,
}

impl From<RaceCancelled> for CancellationCause {
    fn from(value: RaceCancelled) -> Self {
        CancellationCause::RaceCancelled(value)
    }
}

/// A description of the input status while losing a race.
#[derive(Debug, Clone)]
pub struct RaceLost {
    /// The source link of the race.
    pub race: Entity,
    /// The input entity for the race.
    pub input: Entity,
    /// The status of the losing input entity at the time that it lost.
    pub status: FunnelInputStatus,
}

impl From<RaceLost> for CancellationCause {
    fn from(value: RaceLost) -> Self {
        CancellationCause::RaceLost(value)
    }
}

/// Passed into the [`OperationRoster`](crate::OperationRoster) to indicate when
/// a link needs to be cancelled.
pub struct Cancel {
    pub apply_to: Entity,
    pub cause: Cancellation,
}

impl Cancel {
    /// Create a new [`Cancel`] operation
    pub fn new(apply_to: Entity, cause: CancellationCause) -> Self {
        Self { apply_to, cause: Cancellation::from_cause(cause) }
    }

    /// Create a broken link cancel operation
    pub fn broken(source: Entity, backtrace: Option<Backtrace>) -> Self {
        Self::new(source, BrokenLink { source, backtrace }.into())
    }

    /// Create a broken link cancel operation with a backtrace for this current
    /// location.
    pub fn broken_here(source: Entity) -> Self {
        Self::broken(source, Some(Backtrace::new()))
    }

    /// Create an unavailable service cancel operation
    pub fn service_unavailable(source: Entity, service: Entity) -> Self {
        Self::new(source, CancellationCause::ServiceUnavailable(service))
    }

    /// Create a supplanted request cancellation operation
    pub fn supplanted(cancelled_at: Entity, supplanter: Entity) -> Self {
        Self::new(cancelled_at, Supplanted { cancelled_at, supplanter }.into())
    }

    /// Create an unused target cancel operation
    pub fn unused_target(target: Entity) -> Self {
        Self::new(target, CancellationCause::UnusedTarget(target))
    }

    /// Create a dropped target cancel operation
    pub fn dropped(target: Entity) -> Self {
        Self::new(target, CancellationCause::TargetDropped(target))
    }

    /// Create a filtered cancel operation
    pub fn filtered(source: Entity) -> Self {
        Self::new(source, CancellationCause::Filtered(source))
    }

    /// A fork was cancelled because all of its dependents were dropped.
    pub fn fork(source: Entity, cancelled: Vec<Cancellation>) -> Self {
        Self::new(source, CancellationCause::ForkCancelled(
            ForkCancelled { fork: source, cancelled }
        ))
    }

    /// The chain lost a race so it gets cancelled.
    pub fn race_lost(race: Entity, input: Entity, status: FunnelInputStatus) -> Self {
        Self::new(input, CancellationCause::RaceLost(RaceLost { race, input, status }))
    }
}
