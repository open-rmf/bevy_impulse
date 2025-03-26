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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Builder;

use super::{
    BuildDiagramOperation, BuildStatus, BuilderId, DiagramContext, DiagramErrorCode, NextOperation,
    OperationId,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeSchema {
    pub(super) builder: BuilderId,
    #[serde(default)]
    pub(super) config: serde_json::Value,
    pub(super) next: NextOperation,
}

impl BuildDiagramOperation for NodeSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let node_registration = ctx.registry.get_node_registration(&self.builder)?;
        let node = node_registration.create_node(builder, self.config.clone())?;

        ctx.set_input_for_target(id, node.input.into())?;
        ctx.add_output_into_target(self.next.clone(), node.output);
        Ok(BuildStatus::Finished)
    }
}
