use std::error::Error;

use cel_interpreter::{Context, ExecutionError, ParseError, Program};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;

use crate::{diagram::type_info::TypeInfo, Builder, Output};

use super::{
    validate_single_input, workflow_builder::EdgeBuilder, DiagramErrorCode, DynOutput,
    MessageRegistry, NextOperation, Vertex,
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
pub struct TransformOp {
    pub(super) cel: String,
    pub(super) next: NextOperation,
}

impl TransformOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        builder.add_output_edge(self.next.clone(), None, None)?;
        Ok(())
    }

    pub(super) fn try_connect<'b>(
        &self,
        target: &Vertex,
        builder: &mut Builder,
        registry: &MessageRegistry,
    ) -> Result<bool, DiagramErrorCode> {
        let output = validate_single_input(target)?;
        let transformed_output = transform_output(builder, registry, output, self)?;
        let mut out_edge = target.out_edges[0].try_lock().unwrap();
        out_edge.output = Some(transformed_output.into());
        Ok(true)
    }
}

pub(super) fn transform_output(
    builder: &mut Builder,
    registry: &MessageRegistry,
    output: DynOutput,
    transform_op: &TransformOp,
) -> Result<Output<serde_json::Value>, DiagramErrorCode> {
    debug!("transform output: {:?}, op: {:?}", output, transform_op);

    let json_output = if output.type_info == TypeInfo::of::<serde_json::Value>() {
        output.into_output()
    } else {
        registry.serialize(builder, output)
    }?;

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
    let transformed_output = transform_node
        .output
        .chain(builder)
        .cancel_on_err()
        .output();
    debug!("transformed output: {:?}", transformed_output);
    Ok(transformed_output)
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
        assert_eq!(result, 40);
    }
}
