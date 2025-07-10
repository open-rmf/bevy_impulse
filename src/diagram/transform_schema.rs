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

use std::error::Error;

use cel_interpreter::{Context, ExecutionError, ParseError, Program};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ForkResultOutput, JsonMessage};

use super::{
    BuildDiagramOperation, BuildStatus, DiagramContext, DiagramErrorCode, NextOperation,
    OperationName, TraceInfo, TraceSettings,
};

#[derive(Error, Debug)]
pub enum TransformError {
    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error(transparent)]
    Execution(#[from] ExecutionError),

    #[error(transparent)]
    Other(#[from] Box<dyn Error + Send + Sync + 'static>),
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TransformSchema {
    pub cel: String,
    pub next: NextOperation,
    /// Specify what happens if an error occurs during the transformation. If
    /// you specify a target for on_error, then an error message will be sent to
    /// that target. You can set this to `{ "builtin": "dispose" }` to simply
    /// ignore errors.
    ///
    /// If left unspecified, a failure will be treated like an implicit operation
    /// failure and behave according to `on_implicit_error`.
    #[serde(default)]
    pub on_error: Option<NextOperation>,
    #[serde(flatten)]
    pub trace_settings: TraceSettings,
}

impl BuildDiagramOperation for TransformSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let program = Program::compile(&self.cel).map_err(TransformError::Parse)?;
        let node = ctx.builder.create_map_block(
            move |req: JsonMessage| -> Result<JsonMessage, TransformError> {
                let mut context = Context::default();
                context
                    .add_variable("request", req)
                    // cannot keep the original error because it is not Send + Sync
                    .map_err(|err| TransformError::Other(err.to_string().into()))?;
                program
                    .execute(&context)?
                    .json()
                    // cel_interpreter::json is private so we have to type erase ConvertToJsonError
                    .map_err(|err| TransformError::Other(err.to_string().into()))
            },
        );

        let error_target = self
            .on_error
            .as_ref()
            .map(|on_error| ctx.into_operation_ref(on_error))
            .unwrap_or(
                // If no error target was explicitly given then treat this as an
                // implicit error.
                ctx.get_implicit_error_target(),
            );

        let (fork_input, ForkResultOutput { ok, err }) = ctx.builder.create_fork_result();
        ctx.builder.connect(node.output, fork_input);
        ctx.add_output_into_target(error_target.clone(), err.into());

        let trace = TraceInfo::for_basic_op("transform", &self.trace_settings);
        ctx.set_input_for_target(id, node.input.into(), trace)?;
        ctx.add_output_into_target(&self.next, ok.into());
        Ok(BuildStatus::Finished)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram, JsonMessage};

    #[test]
    fn test_transform_node_response() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "transform",
                },
                "transform": {
                    "type": "transform",
                    "cel": "777",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 777);
    }

    #[test]
    fn test_transform_scope_start() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "transform",
            "ops": {
                "transform": {
                    "type": "transform",
                    "cel": "777",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 777);
    }

    #[test]
    fn test_cel_multiply() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "transform",
            "ops": {
                "transform": {
                    "type": "transform",
                    "cel": "int(request) * 3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 12);
    }

    #[test]
    fn test_cel_compose() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "transform",
            "ops": {
                "transform": {
                    "type": "transform",
                    "cel": "{ \"request\": request, \"seven\": 7 }",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result["request"], 4);
        assert_eq!(result["seven"], 7);
    }

    #[test]
    fn test_cel_destructure() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "transform",
            "ops": {
                "transform": {
                    "type": "transform",
                    "cel": "request.age",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let request = json!({
            "name": "John",
            "age": 40,
        });

        let result: JsonMessage = fixture.spawn_and_run(&diagram, request).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 40);
    }
}
