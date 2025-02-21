use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{unknown_diagram_error, Builder};

use super::{
    workflow_builder::{dyn_connect, Edge, EdgeBuilder, Vertex},
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
        edge_builder.add_output_edge(&self.next, Some(n.output))?;
        Ok(())
    }

    pub(super) fn try_connect(
        &self,
        builder: &mut Builder,
        target: &Vertex,
        mut edges: HashMap<&usize, &mut Edge>,
        inputs: &HashMap<&OperationId, DynInputSlot>,
        registry: &MessageRegistry,
    ) -> Result<bool, DiagramErrorCode> {
        let in_edge = target
            .in_edges
            .get(0)
            .ok_or(DiagramErrorCode::OnlySingleInput)?;
        let output = if let Some(output) = edges
            .get_mut(in_edge)
            .ok_or_else(|| unknown_diagram_error!())?
            .output
            .take()
        {
            output
        } else {
            return Ok(false);
        };

        let input = inputs[target.op_id];
        let deserialized_output = registry.deserialize(&input.type_info, builder, output)?;
        dyn_connect(builder, deserialized_output, input)?;
        Ok(true)
    }
}
