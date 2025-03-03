use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Builder;

use super::{
    workflow_builder::{dyn_connect, EdgeBuilder, Vertex},
    BuilderId, DiagramElementRegistry, DiagramErrorCode, DynInputSlot, MessageRegistry,
    NextOperation, OperationId,
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
    pub(super) fn build_edges<'a>(
        &'a self,
        builder: &mut Builder,
        registry: &DiagramElementRegistry,
        inputs: &mut HashMap<&'a OperationId, DynInputSlot>,
        op_id: &'a OperationId,
        mut edge_builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        let reg = registry.get_node_registration(&self.builder)?;
        let n = reg.create_node(builder, self.config.clone())?;
        inputs.insert(op_id, n.input);
        edge_builder.add_output_edge(self.next.clone(), Some(n.output), None)?;
        Ok(())
    }

    pub(super) fn try_connect(
        &self,
        target: &Vertex,
        builder: &mut Builder,
        inputs: &HashMap<&OperationId, DynInputSlot>,
        registry: &MessageRegistry,
    ) -> Result<bool, DiagramErrorCode> {
        for edge in &target.in_edges {
            let mut edge = edge.try_lock().unwrap();
            let output = edge.output.take().unwrap();
            let input = inputs[&target.op_id];
            let deserialized_output = registry.deserialize(&input.type_info, builder, output)?;
            dyn_connect(builder, deserialized_output, input)?;
        }
        Ok(true)
    }
}
