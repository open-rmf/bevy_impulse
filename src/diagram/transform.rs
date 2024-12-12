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
    use serde_json::json;

    use crate::{diagram::testing::DiagramTestFixture, Diagram};

    #[test]
    fn test_transform_node_response() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": {
                "type": "start",
                "next": "op1",
            },
            "op1": {
                "type": "node",
                "nodeId": "multiply3",
                "next": "transform",
            },
            "transform": {
                "type": "transform",
                "cel": "777",
                "next": "terminate",
            },
            "terminate": {
                "type": "terminate",
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 777);
    }

    #[test]
    fn test_transform_scope_start() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": {
                "type": "start",
                "next": "transform",
            },
            "transform": {
                "type": "transform",
                "cel": "777",
                "next": "terminate",
            },
            "terminate": {
                "type": "terminate",
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 777);
    }

    #[test]
    fn test_cel_multiply() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": {
                "type": "start",
                "next": "transform",
            },
            "transform": {
                "type": "transform",
                "cel": "int(request) * 3",
                "next": "terminate",
            },
            "terminate": {
                "type": "terminate",
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 12);
    }

    #[test]
    fn test_cel_compose() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": {
                "type": "start",
                "next": "transform",
            },
            "transform": {
                "type": "transform",
                "cel": "{ \"request\": request, \"seven\": 7 }",
                "next": "terminate",
            },
            "terminate": {
                "type": "terminate",
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result["request"], 4);
        assert_eq!(result["seven"], 7);
    }

    #[test]
    fn test_cel_destructure() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": {
                "type": "start",
                "next": "transform",
            },
            "transform": {
                "type": "transform",
                "cel": "request.age",
                "next": "terminate",
            },
            "terminate": {
                "type": "terminate",
            },
        }))
        .unwrap();

        let request = json!({
            "name": "John",
            "age": 40,
        });

        let result = fixture.spawn_and_run(&diagram, request).unwrap();
        assert_eq!(result, 40);
    }
}
