use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::AnyBuffer;

use super::{BuilderId, DiagramErrorCode, DynInputSlot, DynOutput, NextOperation, OperationId};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SectionBuilder {
    Builder(BuilderId),
    Template(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SectionOp {
    pub(super) builder: SectionBuilder,
    #[serde(default)]
    pub(super) config: serde_json::Value,
    pub(super) buffers: HashMap<String, OperationId>,
    pub(super) connect: HashMap<String, NextOperation>,
}

impl SectionOp {
    fn build_edges(&self) -> Result<(), DiagramErrorCode> {
        // behavior of this depends on if the section is a "builder" or "template".
        //   1. if it is builder, simply create the section from the registered builder id, then create the output edges.
        //   2. if it is a template, create a sub diagram, create and connect all the nodes, then create the output edges.
        //     2.1. it is likely that diagrams will need to support multiple inputs, outputs and input buffers.
        //     2.2. alternatively, use the same diagram but namespace the operation ids.
        panic!("TODO")
    }

    fn try_connect(&self) -> Result<(), DiagramErrorCode> {
        panic!("TODO")
    }
}

pub trait Section {
    fn try_connect(
        self,
        // outputs from outside that are connected to this section
        inputs: HashMap<String, DynOutput>,
        // inputs from outside that this section connects to
        outputs: HashMap<String, DynInputSlot>,
        // buffers from outside that are connected to this section
        buffers: HashMap<String, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode>;

    fn get_input(&self, key: &str) -> Option<&DynInputSlot>;

    fn take_output(&self, key: &str) -> Option<DynOutput>;

    fn get_buffer(&self, key: &str) -> Option<&AnyBuffer>;
}

pub(super) struct SectionTemplate {
    inputs: HashMap<String, DynInputSlot>,
    outputs: HashMap<String, DynOutput>,
    buffers: HashMap<String, AnyBuffer>,
}

impl SectionTemplate {
    fn new() -> Self {
        Self {
            inputs: HashMap::new(),
            outputs: HashMap::new(),
            buffers: HashMap::new(),
        }
    }
}

impl Section for SectionTemplate {
    fn try_connect(
        self,
        // outputs from outside that are connected to this section
        inputs: HashMap<String, DynOutput>,
        // inputs from outside that this section connects to
        outputs: HashMap<String, DynInputSlot>,
        // buffers from outside that are connected to this section
        buffers: HashMap<String, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        panic!("TODO")
    }

    fn get_input(&self, key: &str) -> Option<&DynInputSlot> {
        panic!("TODO")
    }

    fn take_output(&self, key: &str) -> Option<DynOutput> {
        panic!("TODO")
    }

    fn get_buffer(&self, key: &str) -> Option<&AnyBuffer> {
        panic!("TODO")
    }
}
