use std::{any::TypeId, error::Error};

use cel_interpreter::{Context, ExecutionError, ParseError, Program};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Builder, Output};

use super::{DiagramError, DynOutput, NodeRegistry};

#[derive(Error, Debug)]
pub enum TransformError {
    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error(transparent)]
    Execution(#[from] ExecutionError),

    #[error(transparent)]
    Other(#[from] Box<dyn Error + Send + Sync + 'static>),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TransformOp {
    pub(super) cel: String,
    pub(super) next: String,
}

pub(super) fn transform_output(
    builder: &mut Builder,
    registry: &NodeRegistry,
    output: DynOutput,
    transform_op: &TransformOp,
) -> Result<Output<serde_json::Value>, DiagramError> {
    let json_output = if output.type_id == TypeId::of::<serde_json::Value>() {
        output.into_output()
    } else {
        let serialize = registry
            .serialize_impls
            .get(&output.type_id)
            .ok_or(DiagramError::NotSerializable)?;
        serialize(builder, output)
    };

    let program = Program::compile(&transform_op.cel).map_err(|err| TransformError::Parse(err))?;
    let transform_node = builder.create_map_block(
        move |req: serde_json::Value| -> Result<serde_json::Value, TransformError> {
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
    builder.connect(json_output, transform_node.input);
    Ok(transform_node
        .output
        .chain(builder)
        .cancel_on_err()
        .output())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use super::*;
    use crate::{
        diagram::testing::{new_registry_with_basic_nodes, unwrap_promise},
        testing::TestingContext,
        Diagram, DiagramOperation, NodeOp, RequestExt, StartOp, TerminateOp,
    };

    #[test]
    fn test_transform_node_response() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "op_1".to_string(),
                    }),
                ),
                (
                    "op_1".to_string(),
                    DiagramOperation::Node(NodeOp {
                        node_id: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "transform".to_string(),
                    }),
                ),
                (
                    "transform".to_string(),
                    DiagramOperation::Transform(TransformOp {
                        cel: "777".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = unwrap_promise(promise);
        assert_eq!(result, 777);
    }

    #[test]
    fn test_transform_scope_start() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "transform".to_string(),
                    }),
                ),
                (
                    "transform".to_string(),
                    DiagramOperation::Transform(TransformOp {
                        cel: "777".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = unwrap_promise(promise);
        assert_eq!(result, 777);
    }

    #[test]
    fn test_cel_multiply() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "transform".to_string(),
                    }),
                ),
                (
                    "transform".to_string(),
                    DiagramOperation::Transform(TransformOp {
                        // serde_json always serializes a postive int as u64, "request * 3"
                        // would result in a operation between unsigned and signed, which
                        // is not supported in CEL.
                        cel: "int(request) * 3".to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = unwrap_promise(promise);
        assert_eq!(result, 12);
    }

    #[test]
    fn test_cel_compose() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "transform".to_string(),
                    }),
                ),
                (
                    "transform".to_string(),
                    DiagramOperation::Transform(TransformOp {
                        cel: r#"{ "request": request, "seven": 7 }"#.to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let mut promise =
            context.command(|cmds| cmds.request(serde_json::Value::from(4), w).take_response());
        context.run_while_pending(&mut promise);
        let result = unwrap_promise(promise);
        assert_eq!(result["request"], 4);
        assert_eq!(result["seven"], 7);
    }

    #[test]
    fn test_cel_destructure() {
        let mut context = TestingContext::minimal_plugins();
        let registry = new_registry_with_basic_nodes();

        let diagram = Diagram {
            ops: HashMap::from([
                (
                    "start".to_string(),
                    DiagramOperation::Start(StartOp {
                        next: "transform".to_string(),
                    }),
                ),
                (
                    "transform".to_string(),
                    DiagramOperation::Transform(TransformOp {
                        cel: r#"request.age"#.to_string(),
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let w = diagram
            .spawn_io_workflow(&mut context.app, &registry)
            .unwrap();
        let request = serde_json::Value::from_str(
            r#"
            {
                "name": "John",
                "age": 40
            }
            "#,
        )
        .unwrap();

        let mut promise = context.command(|cmds| cmds.request(request, w).take_response());
        context.run_while_pending(&mut promise);
        let result = unwrap_promise(promise);
        assert_eq!(result, 40);
    }
}
