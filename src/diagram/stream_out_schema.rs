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
use std::sync::Arc;

use super::{
    BuildDiagramOperation, BuildStatus, DiagramContext, DiagramErrorCode, OperationName,
    RedirectConnection, StreamOutRef,
};

/// Declare a stream output for the current scope. Outputs that you connect
/// to this operation will be streamed out of the scope that this operation
/// is declared in.
///
/// For the root-level scope, make sure you use a stream pack that is
/// compatible with all stream out operations that you declare, otherwise
/// you may get a connection error at runtime.
///
/// # Examples
/// ```
/// # crossflow::Diagram::from_json_str(r#"
/// {
///     "version": "0.1.0",
///     "start": "plan",
///     "ops": {
///         "progress_stream": {
///             "type": "stream_out",
///             "name": "progress"
///         },
///         "plan": {
///             "type": "node",
///             "builder": "planner",
///             "next": "drive",
///             "stream_out" : {
///                 "progress": "progress_stream"
///             }
///         },
///         "drive": {
///             "type": "node",
///             "builder": "navigation",
///             "next": { "builtin": "terminate" },
///             "stream_out": {
///                 "progress": "progress_stream"
///             }
///         }
///     }
/// }
/// # "#)?;
/// # Ok::<_, serde_json::Error>(())
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct StreamOutSchema {
    /// The name of the stream exiting the workflow or scope.
    pub(super) name: OperationName,
}

impl BuildDiagramOperation for StreamOutSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let redirect_to =
            ctx.into_operation_ref(StreamOutRef::new_for_root(Arc::clone(&self.name)));
        ctx.set_connect_into_target(id, RedirectConnection::new(redirect_to))?;

        Ok(BuildStatus::Finished)
    }
}
