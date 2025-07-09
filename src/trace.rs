/*
 * Copyright (C) 2025 Open Source Robotics Foundation
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

use crate::{ConstructionInfo, OperationRef, JsonMessage, TypeInfo};

use bevy_ecs::prelude::{Component, Entity, Event};
use schemars::JsonSchema;
use serde::{Serialize, Deserialize};
use std::{
    any::Any,
    borrow::Cow,
    sync::Arc,
};
use thiserror::Error as ThisError;

/// A component attached to workflow operation entities in order to trace their
/// activities.
#[derive(Component)]
pub struct Trace {
    info: Arc<OperationInfo>,
    serialize_value: Option<fn(&dyn Any) -> Result<JsonMessage, GetValueError>>,
}

impl Trace {
    /// Create trace information for an entity. By default this will not serialize
    /// the messages passing through.
    pub fn new(info: Arc<OperationInfo>) -> Self {
        Trace { info, serialize_value: None }
    }

    /// Enable the trace for this operation to also send out the data of the
    /// messages that are passing through.
    pub fn enable_value_serialization<T: Any + Serialize>(&mut self) {
        self.serialize_value = Some(get_serialize_value::<T>);
    }

    /// Get the information for this workflow operation.
    pub fn info(&self) -> &Arc<OperationInfo> {
        &self.info
    }

    /// Attempt to serialize the value. This will return a None if the trace is
    /// not set up to serialize the values.
    pub fn serialize_value(&self, value: &dyn Any) -> Option<Result<JsonMessage, GetValueError>> {
        self.serialize_value.map(|f| f(value))
    }
}

fn get_serialize_value<T: Any + Serialize>(value: &dyn Any) -> Result<JsonMessage, GetValueError> {
    let Some(value_ref) = value.downcast_ref::<T>() else {
        return Err(GetValueError::FailedDowncast {
            expected: TypeInfo::of::<T>(),
            received: std::any::TypeId::of::<T>(),
        });
    };

    serde_json::to_value(value_ref)
    .map_err(GetValueError::FailedSerialization)
}

#[derive(ThisError, Debug)]
pub enum GetValueError {
    #[error("The downcast was incompatible. Expected {expected:?}, received {received:?}")]
    FailedDowncast {
        expected: TypeInfo,
        received: std::any::TypeId,
    },
    #[error("The serialization into json failed: {0}")]
    FailedSerialization(serde_json::Error),
}

/// An event that gets transmitted whenever an operation receives an input or is
/// activated some other way while tracing is on (meaning the operation entity
/// has a [`Trace`] component).
#[derive(Event)]
pub struct OperationStarted {
    /// The entity of the operation that was triggered.
    pub entity: Entity,
    /// Information about the operation that was triggered.
    pub info: Arc<OperationInfo>,
    /// The message that triggered the operation, if serialization is enabled
    /// for it.
    pub message: Option<JsonMessage>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug)]
pub struct OperationInfo {
    id: Option<OperationRef>,
    message_type: Option<Cow<'static, str>>,
    construction: Option<ConstructionInfo>,
}

impl OperationInfo {
    /// The unique identifier for this operation within the workflow.
    pub fn id(&self) -> &Option<OperationRef> {
        &self.id
    }

    /// Get the message type that this operation uses, if one is available.
    pub fn message_type(&self) -> &Option<Cow<'static, str>> {
        &self.message_type
    }

    /// If this operation was created by a builder, this is the ID of that
    /// builder
    pub fn construction(&self) -> &Option<ConstructionInfo> {
        &self.construction
    }
}
