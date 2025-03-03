use std::iter::zip;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{diagram::type_info::TypeInfo, Builder};

use super::{
    impls::{DefaultImpl, NotSupported},
    workflow_builder::{Edge, EdgeBuilder},
    DiagramErrorCode, DynOutput, MessageRegistry, NextOperation,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ForkCloneOp {
    pub(super) next: Vec<NextOperation>,
}

impl ForkCloneOp {
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        for target in &self.next {
            builder.add_output_edge(target, None, None)?;
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
        let outputs = if output.type_info == TypeInfo::of::<serde_json::Value>() {
            <DefaultImpl as DynForkClone<serde_json::Value>>::dyn_fork_clone(
                builder,
                output,
                out_edges.len(),
            )
        } else {
            registry.fork_clone(builder, output, out_edges.len())
        }?;

        for (output, out_edge) in zip(outputs, out_edges) {
            out_edge.output = Some(output);
        }

        Ok(())
    }
}

pub trait DynForkClone<T> {
    const CLONEABLE: bool;

    fn dyn_fork_clone(
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode>;
}

impl<T> DynForkClone<T> for NotSupported {
    const CLONEABLE: bool = false;

    fn dyn_fork_clone(
        _builder: &mut Builder,
        _output: DynOutput,
        _amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode> {
        Err(DiagramErrorCode::NotCloneable)
    }
}

impl<T> DynForkClone<T> for DefaultImpl
where
    T: Send + Sync + 'static + Clone,
{
    const CLONEABLE: bool = true;

    fn dyn_fork_clone(
        builder: &mut Builder,
        output: DynOutput,
        amount: usize,
    ) -> Result<Vec<DynOutput>, DiagramErrorCode> {
        debug!("fork clone: {:?}", output);
        assert_eq!(output.type_info, TypeInfo::of::<T>());

        let fork_clone = output.into_output::<T>()?.fork_clone(builder);
        let outputs = (0..amount)
            .map(|_| fork_clone.clone_output(builder).into())
            .collect();
        debug!("forked outputs: {:?}", outputs);
        Ok(outputs)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use test_log::test;

    use crate::{diagram::testing::DiagramTestFixture, Diagram};

    use super::*;

    #[test]
    fn test_fork_clone_uncloneable() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": "fork_clone"
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["op2"]
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3_uncloneable",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();
        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotCloneable),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_fork_clone() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "fork_clone"
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["op2"]
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
}
