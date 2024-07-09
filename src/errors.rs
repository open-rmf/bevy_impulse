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

use bevy::prelude::{Resource, Entity};

use backtrace::Backtrace;

use anyhow::Error as Anyhow;

use std::sync::Arc;

use crate::{OperationError, Cancel, Disposal, Broken};

/// This resource stores errors that have occurred that could not be handled
/// internally or communicated to the user by any other means.
#[derive(Resource, Default, Clone, Debug)]
pub struct UnhandledErrors {
    pub setup: Vec<SetupFailure>,
    pub cancellations: Vec<CancelFailure>,
    pub operations: Vec<OperationError>,
    pub disposals: Vec<DisposalFailure>,
    pub stop_tasks: Vec<StopTaskFailure>,
    pub broken: Vec<Broken>,
    pub unused_targets: Vec<UnusedTargetDrop>,
    pub connections: Vec<ConnectionFailure>,
    pub miscellaneous: Vec<MiscellaneousFailure>,
}

#[derive(Clone, Debug)]
pub struct SetupFailure {
    pub broken_node: Entity,
    pub error: OperationError,
}

#[derive(Clone, Debug)]
pub struct CancelFailure {
    /// The error produced while the cancellation was happening
    pub error: OperationError,
    /// The cancellation that was being emitted
    pub cancel: Cancel,
}

impl CancelFailure {
    pub fn new(
        error: OperationError,
        cancel: Cancel,
    ) -> Self {
        Self { error, cancel }
    }
}

/// When it is impossible for some reason to perform a disposal, the incident
/// will be logged in this resource. This may happen if a node somehow gets
/// despawned while its service is attempting to dispose a request.
#[derive(Clone, Debug)]
pub struct DisposalFailure {
    /// The disposal that was attempted
    pub disposal: Disposal,
    /// The node which was attempting to report the disposal
    pub broken_node: Entity,
    /// The backtrace indicating what led up to the failure
    pub backtrace: Option<Backtrace>,
}

/// An error happened, causing the task of a provider to be unable to stop.
#[derive(Clone, Debug)]
pub struct StopTaskFailure {
    /// The task that was unable to be stopped
    pub task: Entity,
    /// The backtrace to indicate why it failed
    pub backtrace: Option<Backtrace>,
}

/// An impulse chain was dropped because its final target was unused but `detach()`
/// was not called on it. This is almost always a usage error, so we report it here.
#[derive(Clone, Debug)]
pub struct UnusedTargetDrop {
    /// Which target was dropped.
    pub unused_target: Entity,
    /// Which impulses were dropped as a consequence of the unused target.
    pub dropped_impulses: Vec<Entity>,
}

/// Something went wrong while trying to connect a target into a source.
#[derive(Clone, Debug)]
pub struct ConnectionFailure {
    pub original_target: Entity,
    pub new_target: Entity,
    pub backtrace: Backtrace,
}

/// Use this for any failures that are not covered by the other categories
#[derive(Clone, Debug)]
pub struct MiscellaneousFailure {
    pub error: Arc<Anyhow>,
    pub backtrace: Option<Backtrace>,
}
