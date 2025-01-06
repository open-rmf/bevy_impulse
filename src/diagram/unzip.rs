use bevy_utils::all_tuples_with_size;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Builder;

use super::{
    impls::{DefaultImpl, NotSupported},
    join::register_join_impl,
    register_serialize as register_serialize_impl, DiagramError, DynOutput, NextOperation,
    NodeRegistry, SerializeMessage,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UnzipOp {
    pub(super) next: Vec<NextOperation>,
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
                let chain = output.into_output::<($($P,)*)>()?.chain(builder);
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
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram, DiagramError};

    #[test]
    fn test_unzip_not_unzippable() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": "unzip"
                },
                "unzip": {
                    "type": "unzip",
                    "next": [{ "builtin": "terminate" }],
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(matches!(err, DiagramError::NotUnzippable), "{}", err);
    }

    #[test]
    fn test_unzip_to_too_many_slots() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip"
                },
                "unzip": {
                    "type": "unzip",
                    "next": ["op2", "op3", "op4"],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
                },
                "op3": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
                },
                "op4": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(matches!(err, DiagramError::NotUnzippable));
    }

    #[test]
    fn test_unzip_to_terminate() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip"
                },
                "unzip": {
                    "type": "unzip",
                    "next": [{ "builtin": "dispose" }, { "builtin": "terminate" }],
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 20);
    }

    #[test]
    fn test_unzip() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "start": "op1",
            "ops": {
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
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
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
            "start": "op1",
            "ops": {
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
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
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
