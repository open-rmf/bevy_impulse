use std::iter::zip;

use bevy_utils::all_tuples_with_size;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::Builder;

use super::{
    impls::{DefaultImplMarker, NotSupportedMarker},
    workflow_builder::{Edge, EdgeBuilder},
    DiagramErrorCode, DynOutput, MessageRegistry, NextOperation,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UnzipOp {
    pub(super) next: Vec<NextOperation>,
}

impl UnzipOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        for target in &self.next {
            builder.add_output_edge(target, None)?;
        }
        Ok(())
    }

    pub(super) fn try_connect<'b>(
        &self,
        builder: &mut Builder,
        output: DynOutput,
        out_edges: Vec<&mut Edge>,
        registry: &MessageRegistry,
    ) -> Result<(), DiagramErrorCode> {
        let outputs = registry.unzip(builder, output)?;

        if outputs.len() < out_edges.len() {
            return Err(DiagramErrorCode::NotUnzippable);
        }

        for (output, out_edge) in zip(outputs, out_edges) {
            out_edge.output = Some(output);
        }

        Ok(())
    }
}

pub trait DynUnzip {
    /// Returns a list of type names that this message unzips to.
    fn output_types(&self) -> Vec<&'static str>;

    fn dyn_unzip(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode>;
}

impl<T> DynUnzip for NotSupportedMarker<T> {
    fn output_types(&self) -> Vec<&'static str> {
        Vec::new()
    }

    fn dyn_unzip(
        &self,
        _builder: &mut Builder,
        _output: DynOutput,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode> {
        Err(DiagramErrorCode::NotUnzippable)
    }
}

macro_rules! dyn_unzip_impl {
    ($len:literal, $(($P:ident, $o:ident)),*) => {
        impl<$($P),*> DynUnzip for DefaultImplMarker<($($P,)*)>
        where
            $($P: Send + Sync + 'static),*,
        {
            fn output_types(&self) -> Vec<&'static str> {
                vec![$(
                    std::any::type_name::<$P>(),
                )*]
            }

            fn dyn_unzip(
                &self,
                builder: &mut Builder,
                output: DynOutput
            ) -> Result<Vec<DynOutput>, DiagramErrorCode> {
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
        }
    };
}

all_tuples_with_size!(dyn_unzip_impl, 1, 12, R, o);

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram, DiagramErrorCode};

    #[test]
    fn test_unzip_not_unzippable() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
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
        assert!(
            matches!(err.code, DiagramErrorCode::NotUnzippable),
            "{}",
            err
        );
    }

    #[test]
    fn test_unzip_to_too_many_slots() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
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
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
                "op3": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
                "op4": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(matches!(err.code, DiagramErrorCode::NotUnzippable));
    }

    #[test]
    fn test_unzip_to_terminate() {
        let mut fixture = DiagramTestFixture::new();

        fixture
            .registry
            .opt_out()
            .register_message::<i64>("i64")
            .unwrap();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
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
            "version": "0.1.0",
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
                    "builder": "multiply3",
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
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip",
                },
                "unzip": {
                    "type": "unzip",
                    "next": [{ "builtin": "dispose" }, "op2"],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
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
