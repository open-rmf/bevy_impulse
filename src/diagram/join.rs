use std::any::TypeId;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{Builder, IterBufferable};

use super::{
    impls::DefaultImpl, register_serialize, DiagramError, DynOutput, NodeRegistry, SerializeMessage,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
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

    // also need to register serialize for Vec<T>
    register_serialize::<Vec<T>, Serializer>(registry);
}

fn join_impl<T>(builder: &mut Builder, outputs: Vec<DynOutput>) -> Result<DynOutput, DiagramError>
where
    T: Send + Sync + 'static,
{
    debug!("join outputs");

    let first_type = outputs[0].type_id;

    let outputs = outputs
        .into_iter()
        .map(|o| {
            if o.type_id != first_type {
                Err(DiagramError::TypeMismatch)
            } else {
                Ok(o.into_output::<T>())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    // we don't know the number of items at compile time, so we just use a sensible number.
    Ok(outputs.join_vec::<4>(builder).output().into())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram};

    #[test]
    fn test_join() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "split"
                },
                "unzip": {
                    "type": "unzip",
                    "next": ["op1", "op2"]
                },
                "op1": {
                    "type": "node",
                    "nodeId": "multiply3",
                    "next": "join"
                },
                "op2": {
                    "type": "node",
                    "nodeId": "multiply3",
                    "next": "join"
                },
                "join": {
                    "type": "join",
                    "next": "terminate"
                },
                "terminate": {
                    "type": "terminate"
                }
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
}
