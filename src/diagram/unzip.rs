use bevy_utils::all_tuples_with_size;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Builder;

use super::{
    impls::{DefaultImpl, NotSupported},
    join::register_join_impl,
    register_serialize as register_serialize_impl, DiagramError, DynOutput, NodeRegistry,
    OperationId, SerializeMessage,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UnzipOp {
    pub(super) next: Vec<OperationId>,
}

pub trait DynUnzip<T, Serializer> {
    const UNZIP_SLOTS: usize;

    fn dyn_unzip(builder: &mut Builder, output: DynOutput) -> Result<Vec<DynOutput>, DiagramError>;

    /// Called when a node is registered.
    fn on_register(registry: &mut NodeRegistry);
}

impl<T, Serializer> DynUnzip<T, Serializer> for NotSupported {
    const UNZIP_SLOTS: usize = 0;

    fn dyn_unzip(
        _builder: &mut Builder,
        _output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramError> {
        Err(DiagramError::NotUnzippable)
    }

    fn on_register(_registry: &mut NodeRegistry) {}
}

macro_rules! dyn_unzip_impl {
    ($len:literal, $(($P:ident, $o:ident)),*) => {
        impl<$($P),*, Serializer> DynUnzip<($($P,)*), Serializer> for DefaultImpl
        where
            $($P: Send + Sync + 'static),*,
            Serializer: $(SerializeMessage<$P> +)* $(SerializeMessage<Vec<$P>> +)*,
        {
            const UNZIP_SLOTS: usize = $len;

            fn dyn_unzip(
                builder: &mut Builder,
                output: DynOutput
            ) -> Result<Vec<DynOutput>, DiagramError> {
                debug!("unzip output: {:?}", output);
                let mut outputs: Vec<DynOutput> = Vec::with_capacity($len);
                let chain = output.into_output::<($($P,)*)>().chain(builder);
                let ($($o,)*) = chain.unzip();

                $({
                    outputs.push($o.into());
                })*

                debug!("unzipped outputs: {:?}", outputs);
                Ok(outputs)
            }

            fn on_register(registry: &mut NodeRegistry)
            {
                // Register serialize functions for all items in the tuple.
                // For a tuple of (T1, T2, T3), registers serialize for T1, T2 and T3.
                $(
                    register_serialize_impl::<$P, Serializer>(registry);
                )*

                // Register join impls for T1, T2, T3...
                $(
                    register_join_impl::<$P, Serializer>(registry);
                )*
            }
        }
    };
}

all_tuples_with_size!(dyn_unzip_impl, 1, 12, R, o);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;
    use test_log::test;

    use crate::{
        diagram::{testing::DiagramTestFixture, unzip::UnzipOp},
        Diagram, DiagramError, DiagramOperation, NodeOp, StartOp, TerminateOp,
    };

    #[test]
    fn test_unzip_not_unzippable() {
        let mut fixture = DiagramTestFixture::new();

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
                        builder: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["terminate".to_string()],
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut fixture.context.app, &fixture.registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotUnzippable), "{}", err);
    }

    #[test]
    fn test_unzip_to_too_many_slots() {
        let mut fixture = DiagramTestFixture::new();

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
                        builder: "multiply3_5".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["op_2".to_string(), "op_3".to_string(), "op_4".to_string()],
                    }),
                ),
                (
                    "op_2".to_string(),
                    DiagramOperation::Node(NodeOp {
                        builder: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "op_3".to_string(),
                    DiagramOperation::Node(NodeOp {
                        builder: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "op_4".to_string(),
                    DiagramOperation::Node(NodeOp {
                        builder: "multiply3".to_string(),
                        config: serde_json::Value::Null,
                        next: "terminate".to_string(),
                    }),
                ),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let err = diagram
            .spawn_io_workflow(&mut fixture.context.app, &fixture.registry)
            .unwrap_err();
        assert!(matches!(err, DiagramError::NotUnzippable));
    }

    #[test]
    fn test_unzip_to_terminate() {
        let mut fixture = DiagramTestFixture::new();

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
                        builder: "multiply3_5".to_string(),
                        config: serde_json::Value::Null,
                        next: "unzip".to_string(),
                    }),
                ),
                (
                    "unzip".to_string(),
                    DiagramOperation::Unzip(UnzipOp {
                        next: vec!["dispose".to_string(), "terminate".to_string()],
                    }),
                ),
                ("dispose".to_string(), DiagramOperation::Dispose),
                (
                    "terminate".to_string(),
                    DiagramOperation::Terminate(TerminateOp {}),
                ),
            ]),
        };

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 20);
    }

    #[test]
    fn test_unzip() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip",
                },
                "unzip": {
                    "type": "unzip",
                    "next": ["op2"],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 36);
    }

    #[test]
    fn test_unzip_with_dispose() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip",
                },
                "unzip": {
                    "type": "unzip",
                    "next": ["dispose", "op2"],
                },
                "dispose": {
                    "type": "dispose",
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 60);
    }
}
