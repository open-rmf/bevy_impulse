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

use std::sync::Arc;

use crate::{OperationName, OperationRef, BuilderId};

use bevy_ecs::prelude::Component;

use schemars::JsonSchema;
use serde::{Serialize, Deserialize};

#[derive(Component)]
pub struct Trace {
    info: Arc<OperationInfo>,

}

#[derive(Serialize, Deserialize, JsonSchema, Debug)]
pub struct OperationInfo {
    op_id: Option<OperationRef>,
    construction: Option<ConstructionInfo>,
    op_label: Option<OperationName>,
}


impl OperationInfo {
    /// The unique identifier for this operation within the workflow.
    pub fn op_id(&self) -> &Option<OperationRef> {
        &self.op_id
    }

    /// If this operation was created by a builder, this is the ID of that
    /// builder
    pub fn construction(&self) -> &Option<ConstructionInfo> {
        &self.construction
    }

    pub fn op_name(&self) -> &Option<Arc<str>> {
        &self.op_label
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug)]
pub enum ConstructionInfo {
    NodeBuilder(BuilderId),
    SectionBuilder(BuilderId),
    Template(OperationName),
}
