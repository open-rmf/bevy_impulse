use std::any::TypeId;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::debug;

use crate::{Builder, IterBufferable, Output};

use super::{DiagramError, DynOutput, NextOperation, NodeRegistry, SerializeMessage};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct JoinOp {
    pub(super) next: NextOperation,

    /// Do not serialize before performing the join. If true, joins can only be done
    /// on outputs of the same type.
    pub(super) no_serialize: Option<bool>,
}

pub(super) fn register_join_impl<T, Serializer>(registry: &mut NodeRegistry)
where
    T: Send + Sync + 'static,
    Serializer: SerializeMessage<Vec<T>>,
{
    if registry.join_impls.contains_key(&TypeId::of::<T>()) {
        return;
    }

    registry
        .join_impls
        .insert(TypeId::of::<T>(), Box::new(join_impl::<T>));

    // FIXME(koonpeng): join_vec results in a SmallVec<[T; N]>, we can't serialize it because
    // it doesn't implement JsonSchema, and we can't impl it because of orphan rule. We would need
    // to create our own trait that covers `JsonSchema` and `Serialize`.
    // register_serialize::<Vec<T>, Serializer>(registry);
}

/// Serialize the outputs before joining them, and convert the resulting joined output into a
/// [`serde_json::Value`].
pub(super) fn serialize_and_join(
    builder: &mut Builder,
    registry: &NodeRegistry,
    outputs: Vec<DynOutput>,
) -> Result<Output<serde_json::Value>, DiagramError> {
    debug!("serialize and join outputs {:?}", outputs);

    if outputs.is_empty() {
        // do not allow empty joins
        return Err(DiagramError::EmptyJoin);
    }

    let outputs = outputs
        .into_iter()
        .map(|o| {
            let serialize_impl = registry
                .serialize_impls
                .get(&o.type_id)
                .ok_or(DiagramError::NotSerializable)?;
            let serialized_output = serialize_impl(builder, o)?;
            Ok(serialized_output)
        })
        .collect::<Result<Vec<_>, DiagramError>>()?;

    // we need to convert the joined output to [`serde_json::Value`] in order for it to be
    // serializable.
    let joined_output = outputs.join_vec::<4>(builder).output();
    let json_output = joined_output
        .chain(builder)
        .map_block(|o| serde_json::to_value(o).unwrap())
        .output();
    Ok(json_output)
}

fn join_impl<T>(builder: &mut Builder, outputs: Vec<DynOutput>) -> Result<DynOutput, DiagramError>
where
    T: Send + Sync + 'static,
{
    debug!("join outputs {:?}", outputs);

    if outputs.is_empty() {
        // do a empty join, in practice, this branch is never ran because [`WorkflowBuilder`]
        // should error out if there is an empty join.
        return Err(DiagramError::EmptyJoin);
    }

    let first_type = outputs[0].type_id;

    let outputs = outputs
        .into_iter()
        .map(|o| {
            if o.type_id != first_type {
                Err(DiagramError::TypeMismatch)
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
    use crate::{diagram::testing::DiagramTestFixture, Diagram, DiagramError, JsonPosition};

    #[test]
    fn test_join() {
        let mut fixture = DiagramTestFixture::new();

        fn get_split_value(pair: (JsonPosition, serde_json::Value)) -> serde_json::Value {
            pair.1
        }

        fixture.registry.register_node_builder(
            "get_split_value".to_string(),
            "get_split_value".to_string(),
            |builder, _config: ()| builder.create_map_block(get_split_value),
        );

        fn serialize_join_output(join_output: JoinOutput<i64>) -> serde_json::Value {
            serde_json::to_value(join_output).unwrap()
        }

        fixture
            .registry
            .registration_builder()
            .with_opaque_request()
            .register_node_builder(
                "serialize_join_output".to_string(),
                "serialize_join_output".to_string(),
                |builder, _config: ()| builder.create_map_block(serialize_join_output),
            );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "split",
            "ops": {
                "split": {
                    "type": "split",
                    "index": ["getSplitValue1", "getSplitValue2"]
                },
                "getSplitValue1": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": "join",
                },
                "getSplitValue2": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": "join",
                },
                "join": {
                    "type": "join",
                    "next": "serializeJoinOutput",
                    "no_serialize": true,
                },
                "serializeJoinOutput": {
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
        // order is not guaranteed so need to test for both possibility
        assert!(result[0] == 3 || result[0] == 6);
        assert!(result[1] == 3 || result[1] == 6);
        assert!(result[0] != result[1]);
    }

    #[test]
    fn test_empty_join() {
        let mut fixture = DiagramTestFixture::new();

        fn get_split_value(pair: (JsonPosition, serde_json::Value)) -> serde_json::Value {
            pair.1
        }

        fixture.registry.register_node_builder(
            "get_split_value".to_string(),
            "get_split_value".to_string(),
            |builder, _config: ()| builder.create_map_block(get_split_value),
        );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "split",
            "ops": {
                "split": {
                    "type": "split",
                    "index": ["getSplitValue1", "getSplitValue2"]
                },
                "getSplitValue1": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
                },
                "getSplitValue2": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
                },
                "join": {
                    "type": "join",
                    "next": { "builtin": "terminate" },
                    "no_serialize": true,
                },
            }
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(matches!(err, DiagramError::EmptyJoin));
    }

    #[test]
    fn test_serialize_and_join() {
        let mut fixture = DiagramTestFixture::new();

        fn num_output(_: serde_json::Value) -> i64 {
            1
        }

        fixture.registry.register_node_builder(
            "num_output".to_string(),
            "num_output".to_string(),
            |builder, _config: ()| builder.create_map_block(num_output),
        );

        fn string_output(_: serde_json::Value) -> String {
            "hello".to_string()
        }

        fixture.registry.register_node_builder(
            "string_output".to_string(),
            "string_output".to_string(),
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
                    "next": { "builtin": "terminate" },
                },
            }
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert_eq!(result.as_array().unwrap().len(), 2);
        // order is not guaranteed so need to test for both possibility
        assert!(result[0] == 1 || result[0] == "hello");
        assert!(result[1] == 1 || result[1] == "hello");
        assert!(result[0] != result[1]);
    }
}
