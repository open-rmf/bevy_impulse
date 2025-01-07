use std::any::TypeId;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::debug;

use crate::{Builder, IterBufferable};

use super::{DiagramError, DynOutput, NextOperation, NodeRegistry, SerializeMessage};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct JoinOp {
    pub(super) next: NextOperation,
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
            // joins is only supported for outputs of the same type. This is because joins of
            // different types produces a tuple and we cannot output a tuple as we don't
            // know the number and order of join inputs at compile time.
            // A workaround is to serialize them all the `serde_json::Value` or convert them to `Box<dyn Any>`.
            // But the problem with `Box<dyn Any>` is that we can't convert it back to the original type,
            // so nodes need to take a request of `JoinOutput<Box<dyn Any>>`.
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
                },
            }
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(matches!(err, DiagramError::EmptyJoin));
    }
}
