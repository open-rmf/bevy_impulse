use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::debug;

use crate::{diagram::type_info::TypeInfo, unknown_diagram_error, Builder, IterBufferable, Output};

use super::{
    workflow_builder::{Edge, EdgeBuilder, Vertex},
    DiagramErrorCode, DynOutput, MessageRegistry, NextOperation, SerializeMessage, SourceOperation,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct JoinOp {
    pub(super) next: NextOperation,

    /// Controls the order of the resulting join. Each item must be an operation id of one of the
    /// incoming outputs.
    pub(super) inputs: Vec<SourceOperation>,

    /// Do not serialize before performing the join. If true, joins can only be done
    /// on outputs of the same type.
    pub(super) no_serialize: Option<bool>,
}

impl JoinOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        builder.add_output_edge(&self.next, None)?;
        Ok(())
    }

    pub(super) fn try_connect<'b>(
        &self,
        builder: &mut Builder,
        vertex: &Vertex,
        mut edges: HashMap<&usize, &mut Edge>,
        registry: &MessageRegistry,
    ) -> Result<(), DiagramErrorCode> {
        if vertex.in_edges.is_empty() {
            return Err(DiagramErrorCode::EmptyJoin);
        }
        let mut outputs: HashMap<SourceOperation, DynOutput> = vertex
            .in_edges
            .iter()
            .map(|e| {
                let edge = edges.remove(e).ok_or(unknown_diagram_error!())?;
                match edge.output.take() {
                    Some(output) => Ok((edge.source.clone(), output)),
                    // "expected all incoming edges to be ready"
                    _ => Err(unknown_diagram_error!()),
                }
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        let mut ordered_outputs: Vec<DynOutput> = Vec::with_capacity(vertex.in_edges.len());
        for source_id in self.inputs.iter() {
            let o = outputs
                .remove(source_id)
                .ok_or(DiagramErrorCode::OperationNotFound(source_id.to_string()))?;
            ordered_outputs.push(o);
        }

        let joined_output = if self.no_serialize.unwrap_or(false) {
            registry.join(builder, ordered_outputs)?
        } else {
            serialize_and_join(builder, &registry, ordered_outputs)?.into()
        };

        let out_edge = edges
            .get_mut(
                vertex
                    .out_edges
                    .get(0)
                    .ok_or_else(|| unknown_diagram_error!())?,
            )
            .ok_or(unknown_diagram_error!())?;
        out_edge.output = Some(joined_output);
        Ok(())
    }
}

pub(super) fn register_join_impl<T, Serializer>(registry: &mut MessageRegistry)
where
    T: Send + Sync + 'static,
    Serializer: SerializeMessage<Vec<T>>,
{
    registry.register_join::<T>(Box::new(join_impl::<T>));
}

/// Serialize the outputs before joining them, and convert the resulting joined output into a
/// [`serde_json::Value`].
pub(super) fn serialize_and_join(
    builder: &mut Builder,
    registry: &MessageRegistry,
    outputs: Vec<DynOutput>,
) -> Result<Output<serde_json::Value>, DiagramErrorCode> {
    debug!("serialize and join outputs {:?}", outputs);

    if outputs.is_empty() {
        // do not allow empty joins
        return Err(DiagramErrorCode::EmptyJoin);
    }

    let outputs = outputs
        .into_iter()
        .map(|o| registry.serialize(builder, o))
        .collect::<Result<Vec<_>, DiagramErrorCode>>()?;

    // we need to convert the joined output to [`serde_json::Value`] in order for it to be
    // serializable.
    let joined_output = outputs.join_vec::<4>(builder).output();
    let json_output = joined_output
        .chain(builder)
        .map_block(|o| serde_json::to_value(o))
        .cancel_on_err()
        .output();
    Ok(json_output)
}

fn join_impl<T>(
    builder: &mut Builder,
    outputs: Vec<DynOutput>,
) -> Result<DynOutput, DiagramErrorCode>
where
    T: Send + Sync + 'static,
{
    debug!("join outputs {:?}", outputs);

    if outputs.is_empty() {
        // do a empty join, in practice, this branch is never ran because [`WorkflowBuilder`]
        // should error out if there is an empty join.
        return Err(DiagramErrorCode::EmptyJoin);
    }

    let first_type = outputs[0].type_info;

    let outputs = outputs
        .into_iter()
        .map(|o| {
            if o.type_info != first_type {
                Err(DiagramErrorCode::TypeMismatch {
                    source_type: o.type_info,
                    target_type: TypeInfo::of::<T>(),
                })
            } else {
                Ok(o.into_output::<T>()?)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    // we don't know the number of items at compile time, so we just use a sensible number.
    // NOTE: Be sure to update `JoinOutput` if this changes.
    Ok(outputs.join_vec::<4>(builder).output().into())
}

/// The resulting type of a `join` operation. Nodes receiving a join output must have request
/// of this type. Note that the join output is NOT serializable. If you would like to serialize it,
/// convert it to a `Vec` first.
pub type JoinOutput<T> = SmallVec<[T; 4]>;

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use super::*;
    use crate::{
        diagram::testing::DiagramTestFixture, Diagram, DiagramErrorCode, JsonPosition,
        NodeBuilderOptions,
    };

    #[test]
    fn test_join() {
        let mut fixture = DiagramTestFixture::new();

        fn get_split_value(pair: (JsonPosition, serde_json::Value)) -> serde_json::Value {
            pair.1
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("get_split_value".to_string()),
            |builder, _config: ()| builder.create_map_block(get_split_value),
        );

        fn serialize_join_output(join_output: JoinOutput<i64>) -> serde_json::Value {
            serde_json::to_value(join_output).unwrap()
        }

        fixture
            .registry
            .opt_out()
            .no_request_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("serialize_join_output".to_string()),
                |builder, _config: ()| builder.create_map_block(serialize_join_output),
            );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "split",
            "ops": {
                "split": {
                    "type": "split",
                    "sequential": ["get_split_value1", "get_split_value2"]
                },
                "get_split_value1": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "join",
                },
                "get_split_value2": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "join",
                },
                "join": {
                    "type": "join",
                    "inputs": ["op1", "op2"],
                    "next": "serialize_join_output",
                    "no_serialize": true,
                },
                "serialize_join_output": {
                    "type": "node",
                    "builder": "serialize_join_output",
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from([1, 2]))
            .unwrap();
        assert_eq!(result.as_array().unwrap().len(), 2);
        assert_eq!(result[0], 3);
        assert_eq!(result[1], 6);
    }

    /// This test is to ensure that the order of split and join operations are stable.
    #[test]
    fn test_join_stress() {
        for _ in 1..20 {
            test_join();
        }
    }

    #[test]
    fn test_empty_join() {
        let mut fixture = DiagramTestFixture::new();

        fn get_split_value(pair: (JsonPosition, serde_json::Value)) -> serde_json::Value {
            pair.1
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("get_split_value".to_string()),
            |builder, _config: ()| builder.create_map_block(get_split_value),
        );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "split",
            "ops": {
                "split": {
                    "type": "split",
                    "sequential": ["get_split_value1", "get_split_value2"]
                },
                "get_split_value1": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
                "get_split_value2": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
                "join": {
                    "type": "join",
                    "inputs": [],
                    "next": { "builtin": "terminate" },
                    "no_serialize": true,
                },
            }
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(matches!(err.code, DiagramErrorCode::EmptyJoin));
    }

    #[test]
    fn test_serialize_and_join() {
        let mut fixture = DiagramTestFixture::new();

        fn num_output(_: serde_json::Value) -> i64 {
            1
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("num_output".to_string()),
            |builder, _config: ()| builder.create_map_block(num_output),
        );

        fn string_output(_: serde_json::Value) -> String {
            "hello".to_string()
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("string_output".to_string()),
            |builder, _config: ()| builder.create_map_block(string_output),
        );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["op1", "op2"]
                },
                "op1": {
                    "type": "node",
                    "builder": "num_output",
                    "next": "join",
                },
                "op2": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "join",
                },
                "join": {
                    "type": "join",
                    "inputs": ["op1", "op2"],
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert_eq!(result.as_array().unwrap().len(), 2);
        assert_eq!(result[0], 1);
        assert_eq!(result[1], "hello");
    }
}
