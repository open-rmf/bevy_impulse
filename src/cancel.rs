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

use crate::Unreachability;

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

/// Get an explanation for why a cancellation occurred.
#[derive(Debug)]
pub enum CancellationCause {
    /// The promise taken by the requester was dropped without being detached.
    TargetDropped(Entity),

    /// There are no terminating nodes for the workflow that can be reached
    /// anymore.
    Unreachable(Unreachability),

    /// A node in the workflow was broken, for example despawned or missing a
    /// component. This type of cancellation indicates that you are modifying
    /// the entities in a workflow in an unsupported way. If you believe that
    /// you are not doing anything unsupported then this could indicate a bug in
    /// `bevy_impulse` itself, and you encouraged to open an issue with a minimal
    /// reproducible example.
    ///
    /// The entity provided in [`BrokenLink`] is the link where the breakage was
    /// detected.
    BrokenLink(BrokenLink),
}

#[derive(Debug, Clone)]
pub struct BrokenLink {
    pub node: Entity,
    pub backtrace: Option<Backtrace>,
}

impl From<BrokenLink> for CancellationCause {
    fn from(value: BrokenLink) -> Self {
        CancellationCause::BrokenLink(value)
    }
}

/// Passed into the [`OperationRoster`](crate::OperationRoster) to indicate when
/// a link needs to be cancelled.
#[derive(Debug, Clone)]
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
        Self::new(source, BrokenLink { node: source, backtrace }.into())
    }

    /// Create a broken link cancel operation with a backtrace for this current
    /// location.
    pub fn broken_here(source: Entity) -> Self {
        Self::broken(source, Some(Backtrace::new()))
    }

    /// Create a dropped target cancel operation
    pub fn dropped(target: Entity) -> Self {
        Self::new(target, CancellationCause::TargetDropped(target))
    }
}
