use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Builder;

use super::{
    workflow_builder::dyn_connect, BuilderId, DiagramElementRegistry, DiagramErrorCode,
    NextOperation, WorkflowBuilder,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeOp {
    pub(super) builder: BuilderId,
    #[serde(default)]
    pub(super) config: serde_json::Value,
    pub(super) next: NextOperation,
}

impl NodeOp {
    pub(super) fn add_vertices<'a>(
        &'a self,
        builder: &mut Builder,
        wf_builder: &mut WorkflowBuilder<'a>,
        op_id: String,
        registry: &DiagramElementRegistry,
    ) -> Result<(), DiagramErrorCode> {
        let reg = registry.get_node_registration(&self.builder)?;
        let node = reg.create_node(builder, self.config.clone())?;

        let mut edge_builder =
            wf_builder.add_vertex(op_id.clone(), move |vertex, builder, registry, _| {
                for edge in &vertex.in_edges {
                    let output = edge.take_output();
                    dyn_connect(builder, output, node.input.into(), &registry.messages)?;
                }
                Ok(true)
            });
        edge_builder.add_output_edge(self.next.clone(), Some(node.output));

        Ok(())
    }
}
