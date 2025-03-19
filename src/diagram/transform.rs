use std::error::Error;

use cel_interpreter::{Context, ExecutionError, ParseError, Program};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;

use crate::{Builder, Output};

use super::{
    validate_single_input, DiagramErrorCode, DynOutput, MessageRegistry, NextOperation,
    SerializationOptions, Vertex, WorkflowBuilder,
};

#[derive(Error, Debug)]
pub enum TransformError {
    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error(transparent)]
    Execution(#[from] ExecutionError),

    #[error("not supported")]
    NotSupported,

    #[error(transparent)]
    Other(#[from] Box<dyn Error + Send + Sync>),
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TransformOp {
    pub(super) cel: String,
    pub(super) next: NextOperation,
}

impl TransformOp {
    pub(super) fn add_vertices<'a, SerializationOptionsT>(
        &'a self,
        wf_builder: &mut WorkflowBuilder<'a, SerializationOptionsT>,
        op_id: String,
    ) where
        SerializationOptionsT: SerializationOptions,
    {
        wf_builder
            .add_vertex(op_id.clone(), move |vertex, builder, registry, _| {
                self.try_connect(vertex, builder, &registry.messages)
            })
            .add_output_edge(self.next.clone(), None);
    }

    pub(super) fn try_connect<SerializationOptionsT>(
        &self,
        vertex: &Vertex,
        builder: &mut Builder,
        registry: &MessageRegistry<SerializationOptionsT>,
    ) -> Result<bool, DiagramErrorCode>
    where
        SerializationOptionsT: SerializationOptions,
    {
        let output = validate_single_input(vertex)?;
        let transformed_output = transform_output(builder, registry, output, self)?;
        vertex.out_edges[0].set_output(transformed_output.into());
        Ok(true)
    }
}

pub(super) fn transform_output<SerializationOptionsT>(
    builder: &mut Builder,
    registry: &MessageRegistry<SerializationOptionsT>,
    output: DynOutput,
    transform_op: &TransformOp,
) -> Result<Output<cel_interpreter::Value>, DiagramErrorCode>
where
    SerializationOptionsT: SerializationOptions,
{
    debug!("transform output: {:?}, op: {:?}", output, transform_op);

    let cel_output = registry.to_cel_value(builder, output)?;
    let program = Program::compile(&transform_op.cel).map_err(|err| TransformError::Parse(err))?;
    let transform_node = builder.create_map_block(move |req| -> Result<_, TransformError> {
        let mut context = Context::default();
        context
            .add_variable("request", req)
            // cannot keep the original error because it is not Send + Sync
            .map_err(|err| TransformError::Other(err.to_string().into()))?;
        Ok(program.execute(&context)?)
    });
    builder.connect(cel_output, transform_node.input);
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
