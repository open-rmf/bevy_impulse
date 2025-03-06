use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Builder;

use super::{
    workflow_builder::dyn_connect, BuilderId, DiagramElementRegistry, DiagramErrorCode,
    DynInputSlot, Edge, NextOperation, OperationId, Vertex, WorkflowBuilder,
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
    pub(super) fn add_edges<'a>(
        &self,
        op_id: &'a OperationId,
        workflow_builder: &mut WorkflowBuilder,
        builder: &mut Builder,
        registry: &DiagramElementRegistry,
        inputs: &mut HashMap<OperationId, DynInputSlot>,
    ) -> Result<(), DiagramErrorCode> {
        let reg = registry.get_node_registration(&self.builder)?;
        let n = reg.create_node(builder, self.config.clone())?;
        inputs.insert(op_id.clone(), n.input.into());
        workflow_builder.add_edge(Edge {
            source: op_id.clone().into(),
            target: self.next.clone(),
            output: Some(n.output.into()),
            tag: None,
        })?;
        Ok(())
    }

    pub(super) fn try_connect(
        &self,
        vertex: &Vertex,
        builder: &mut Builder,
        registry: &DiagramElementRegistry,
        op_id: &OperationId,
        inputs: &HashMap<OperationId, DynInputSlot>,
    ) -> Result<bool, DiagramErrorCode> {
        for edge in &vertex.in_edges {
            let mut edge = edge.try_lock().unwrap();
            let output = edge.output.take().unwrap();
            let input = inputs[op_id];
            dyn_connect(builder, output, input.into(), &registry.messages)?;
        }

        Ok(true)
    }
}
