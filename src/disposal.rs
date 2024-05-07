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

use std::sync::Arc;


#[derive(Debug)]
pub struct Disposal {
    pub cause: Arc<DisposalCause>,
}

impl<T: Into<DisposalCause>> From<T> for Disposal {
    fn from(value: T) -> Self {
        Disposal { cause: Arc::new(value.into())}
    }
}

#[derive(Debug)]
pub enum DisposalCause {
    /// Async services with serial delivery will queue up requests to deliver
    /// them one at a time. Depending on the label of the incoming requests, a
    /// new request might supplant an earlier one, causing the earlier request
    /// to be disposed.
    Supplanted(Supplanted),

    /// A node filtered out a response.
    Filtered(DisposalFilter),

    /// A node disposed of one of its output branches.
    Branching(DisposedBranch),

    /// A join was halted because one or more of its inputs became unreachable.
    JoinUnreachable(JoinUnreachable),

    /// A [`Service`](crate::Service) provider needed by the chain was despawned
    /// or had a critical component removed. The entity provided in the variant
    /// is the unavailable service.
    ServiceUnavailable(ServiceUnavailable),

    /// An output was disposed because a mutex was poisoned.
    PoisonedMutex(PoisonedMutexDisposal),

    /// A scope became unreachable, causing its output to be disposed.
    Scope(Unreachability),
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct Supplanted {
    /// ID of the node whose service request was supplanted
    pub cancelled_at_node: Entity,
    /// ID of the node that did the supplanting
    pub supplanting_node: Entity,
    /// ID of the session that did the supplanting
    pub supplanting_session: Entity,
}

impl Supplanted {
    pub fn new(
        cancelled_at_node: Entity,
        supplanting_node: Entity,
        supplanting_session: Entity,
    ) -> Self {
        Self { cancelled_at_node, supplanting_node, supplanting_session }
    }
}

impl From<Supplanted> for DisposalCause {
    fn from(value: Supplanted) -> Self {
        DisposalCause::Supplanted(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct DisposalFilter {
    /// ID of the node that did the filtering
    pub filtered_at_node: Entity,
    /// Optionally, a reason given for why the filtering happened.
    pub reason: Option<anyhow::Error>,
}

impl DisposalFilter {
    pub fn new(filtered_at_node: Entity, reason: Option<anyhow::Error>) -> Self {
        Self { filtered_at_node, reason }
    }
}

impl From<DisposalFilter> for DisposalCause {
    fn from(value: DisposalFilter) -> Self {
        Self::Filtered(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct DisposedBranch {
    /// The node where the branching happened
    pub branched_at_node: Entity,
    /// The target node whose input was disposed
    pub disposed_for_node: Entity,
    /// Optionally, a reason given for the branching
    pub reason: Option<anyhow::Error>,
}

impl From<DisposedBranch> for DisposalCause {
    fn from(value: DisposedBranch) -> Self {
        Self::Branching(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug, Clone)]
pub struct JoinUnreachable {
    /// The source node of the join
    pub join: Entity,
    /// The unreachable input nodes
    pub unreachable: Vec<Entity>,
}

impl From<JoinUnreachable> for DisposalCause {
    fn from(value: JoinUnreachable) -> Self {
        DisposalCause::JoinUnreachable(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct ServiceUnavailable {
    /// The service that is no longer available
    pub service: Entity,
    /// The node that intended to use the service
    pub for_node: Entity,
}

impl From<ServiceUnavailable> for DisposalCause {
    fn from(value: ServiceUnavailable) -> Self {
        Self::ServiceUnavailable(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct PoisonedMutexDisposal {
    /// The node containing the poisoned mutex
    pub for_node: Entity,
}

impl From<PoisonedMutexDisposal> for DisposalCause {
    fn from(value: PoisonedMutexDisposal) -> Self {
        Self::PoisonedMutex(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct Unreachability {
    /// The ID of the scope whose termination became unreachable.
    pub scope: Entity,
    /// The ID of the session whose termination became unreachable.
    pub session: Entity,
    /// A list of the disposals that occurred for this session.
    pub disposals: Vec<Disposal>,
}

impl From<Unreachability> for DisposalCause {
    fn from(value: Unreachability) -> Self {
        Self::Scope(value)
    }
}
