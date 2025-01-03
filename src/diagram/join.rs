use std::any::TypeId;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{Builder, IterBufferable, Output};

use super::{DiagramError, DynOutput, NodeRegistry, SerializeMessage};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct JoinOp {
    pub(super) next: String,
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
        // do a empty join
        return Ok(([] as [Output<()>; 0])
            .join_vec::<4>(builder)
            .output()
            .into());
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
    Ok(outputs.join_vec::<4>(builder).output().into())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use smallvec::SmallVec;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram, JsonPosition};

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

        fn serialize_join_output(small_vec: SmallVec<[i64; 4]>) -> serde_json::Value {
            serde_json::to_value(small_vec).unwrap()
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
            "ops": {
                "start": {
                    "type": "start",
                    "next": "split",
                },
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
                    "builder": "multiply3",
                    "next": "join",
                },
                "getSplitValue2": {
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
                    "next": "serializeJoinOutput",
                },
                "serializeJoinOutput": {
                    "type": "node",
                    "builder": "serialize_join_output",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
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
}
