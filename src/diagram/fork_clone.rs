use std::iter::zip;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{diagram::type_info::TypeInfo, Builder};

use super::{
    impls::{DefaultImpl, NotSupported},
    validate_single_input, DiagramErrorCode, DynOutput, MessageRegistry, NextOperation,
    SerializationOptions, Vertex, WorkflowBuilder,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ForkCloneOp {
    pub(super) next: Vec<NextOperation>,
}

impl ForkCloneOp {
    pub(super) fn add_vertices<'a, SerializationOptionsT>(
        &'a self,
        wf_builder: &mut WorkflowBuilder<'a, SerializationOptionsT>,
        op_id: String,
    ) where
        SerializationOptionsT: SerializationOptions,
    {
        let mut edge_builder =
            wf_builder.add_vertex(op_id.clone(), move |vertex, builder, registry, _| {
                self.try_connect(vertex, builder, &registry.messages)
            });
        for target in &self.next {
            edge_builder.add_output_edge(target.clone(), None);
        }
    }

    fn try_connect<SerializationOptionsT>(
        &self,
        vertex: &Vertex,
        builder: &mut Builder,
        registry: &MessageRegistry<SerializationOptionsT>,
    ) -> Result<bool, DiagramErrorCode>
    where
        SerializationOptionsT: SerializationOptions,
    {
        let output = validate_single_input(vertex)?;

        let outputs = registry.fork_clone(builder, output, vertex.out_edges.len())?;

        for (output, out_edge) in zip(outputs, &vertex.out_edges) {
            out_edge.set_output(output);
        }

        Ok(true)
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
