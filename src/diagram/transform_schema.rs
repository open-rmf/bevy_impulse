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

use crate::{JsonMessage, Builder};

use super::{
    BuildDiagramOperation, BuildStatus, DiagramContext,
    DiagramErrorCode, NextOperation, OperationId, TypeInfo,
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
    pub(super) cel: String,
    pub(super) next: NextOperation,
    /// Specify what happens if an error occurs during the transformation. By
    /// default an error will cause the entire workflow to cancel. If you specify
    /// a target for on_error, then an error message will be sent to that target.
    /// You can set this to `{ "builtin": "dispose" }` to simply ignore errors.
    #[serde(default)]
    pub(super) on_error: Option<NextOperation>,
}

impl BuildDiagramOperation for TransformSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationId,
        builder: &mut Builder,
        ctx: DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let program = Program::compile(&self.cel).map_err(TransformError::Parse)?;
        let node = builder.create_map_block(
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
            }
        );

        let output = if let Some(on_error) = &self.on_error {
            let (ok, _) = node.output.chain(builder).fork_result(
                |ok| ok.output(),
                |err| {
                    ctx.construction.add_output_into_target(
                        on_error.clone(),
                        err.output().into(),
                    );
                }
            );
            ok
        } else {
            node.output.chain(builder).cancel_on_err().output()
        };

        let input = node.input;
        ctx.construction.set_connect_into_target(
            id,
            move |_, output, builder, ctx| {
                let json_output = if output.message_info() == &TypeInfo::of::<JsonMessage>() {
                    output.into_output()?
                } else {
                    ctx.registry.messages.serialize(builder, output)?
                };

                builder.connect(json_output, input);
                Ok(())
            }
        )?;

        ctx.construction.add_output_into_target(self.next.clone(), output.into());
        Ok(BuildStatus::Finished)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram};

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

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
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

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
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

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
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

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
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

        let result = fixture.spawn_and_run(&diagram, request).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 40);
    }
}
