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

use std::{collections::HashMap, sync::Arc};

use schemars::{schema::Schema, JsonSchema};
use serde::{Deserialize, Serialize};

use crate::{
    AnyBuffer, AnyMessageBox, Buffer, Builder, InputSlot, JsonBuffer, JsonMessage, Output,
};

use super::{
    BuildDiagramOperation, BuildStatus, BuilderId, DiagramContext, DiagramElementRegistry,
    DiagramErrorCode, DynInputSlot, DynOutput, NamespacedOperation, NextOperation, OperationName,
    Operations, TypeInfo,
};

/// The schema to define a scope within a diagram.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ScopeSchema {
    /// Indicates which node inside the scope should receive the input into the
    /// scope.
    pub start: NextOperation,

    /// To simplify diagram definitions, the diagram workflow builder will
    /// sometimes insert implicit operations into the workflow, such as implicit
    /// serializing and deserializing. These implicit operations may be fallible.
    ///
    /// This field indicates how a failed implicit operation should be handled.
    /// If left unspecified, an implicit error will cause the entire workflow to
    /// be cancelled.
    #[serde(default)]
    pub on_implicit_error: Option<NextOperation>,

    /// Operations that exist inside this scope.
    pub ops: Operations,

    /// Where to connect streams that are coming out of this scope.
    #[serde(default)]
    pub stream_out: HashMap<OperationName, NextOperation>,

    /// Where to connect the output of this scope.
    pub next: NextOperation,
}

impl BuildDiagramOperation for ScopeSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        Ok(BuildStatus::Finished)
    }
}
