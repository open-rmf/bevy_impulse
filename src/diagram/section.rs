use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{AnyBuffer, Buffer, Builder, InputSlot, Output};

use super::{
    dyn_connect, BuilderId, DiagramErrorCode, DynInputSlot, DynOutput, NextOperation, OperationId,
};

pub use bevy_impulse_derive::Section;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SectionProvider {
    Builder(BuilderId),
    Template(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SectionOp {
    pub(super) builder: SectionProvider,
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
        builder: &mut Builder,
        // outputs from outside that are connected to this section
        inputs: HashMap<String, DynOutput>,
        // inputs from outside that this section connects to
        outputs: HashMap<String, DynInputSlot>,
    ) -> Result<(), DiagramErrorCode>;
}

pub trait SectionItem {
    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str);

    fn try_connect(
        self,
        key: &'static str,
        builder: &mut Builder,
        inputs: &mut HashMap<String, DynOutput>,
        outputs: &mut HashMap<String, DynInputSlot>,
    ) -> Result<(), DiagramErrorCode>;
}

impl<T: 'static> SectionItem for InputSlot<T> {
    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str) {
        metadata.inputs.push(key);
    }

    fn try_connect(
        self,
        key: &'static str,
        builder: &mut Builder,
        inputs: &mut HashMap<String, DynOutput>,
        _outputs: &mut HashMap<String, DynInputSlot>,
    ) -> Result<(), DiagramErrorCode> {
        let output = inputs.remove(key).ok_or(SectionError::MissingInput(key))?;
        dyn_connect(builder, output, self.into())
    }
}

impl<T: Send + Sync + 'static> SectionItem for Output<T> {
    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str) {
        metadata.outputs.push(key);
    }

    fn try_connect(
        self,
        key: &'static str,
        builder: &mut Builder,
        _inputs: &mut HashMap<String, DynOutput>,
        outputs: &mut HashMap<String, DynInputSlot>,
    ) -> Result<(), DiagramErrorCode> {
        let input = outputs
            .remove(key)
            .ok_or(SectionError::MissingOutput(key))?;
        dyn_connect(builder, self.into(), input)
    }
}

impl<T> SectionItem for Buffer<T> {
    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str) {
        metadata.buffers.push(key)
    }

    fn try_connect(
        self,
        _key: &'static str,
        _builder: &mut Builder,
        _inputs: &mut HashMap<String, DynOutput>,
        _outputs: &mut HashMap<String, DynInputSlot>,
    ) -> Result<(), DiagramErrorCode> {
        Ok(())
    }
}

pub struct SectionMetadata {
    inputs: Vec<&'static str>,
    outputs: Vec<&'static str>,
    buffers: Vec<&'static str>,
}

pub trait SectionMetadataProvider {
    fn metadata() -> &'static SectionMetadata;
}

pub trait SectionBuilder {
    fn section_metadata(&self) -> &'static SectionMetadata;

    fn create_section(
        &mut self,
        builder: &mut Builder,
        buffers: HashMap<String, AnyBuffer>,
    ) -> impl Section;
}

impl<F, S> SectionBuilder for F
where
    F: FnMut(&mut Builder, HashMap<String, AnyBuffer>) -> S,
    S: Section + SectionMetadataProvider,
{
    fn section_metadata(&self) -> &'static SectionMetadata {
        S::metadata()
    }

    fn create_section(
        &mut self,
        builder: &mut Builder,
        buffers: HashMap<String, AnyBuffer>,
    ) -> impl Section {
        self(builder, buffers)
    }
}

pub(super) struct DynSection;

impl Section for DynSection {
    fn try_connect(
        self,
        builder: &mut Builder,
        // outputs from outside that are connected to this section
        inputs: HashMap<String, DynOutput>,
        // inputs from outside that this section connects to
        outputs: HashMap<String, DynInputSlot>,
    ) -> Result<(), DiagramErrorCode> {
        panic!("TODO")
    }
}

pub(super) struct SectionTemplate;

impl SectionBuilder for SectionTemplate {
    fn section_metadata(&self) -> &'static SectionMetadata {
        panic!("TODO")
    }

    fn create_section(
        &mut self,
        builder: &mut Builder,
        buffers: HashMap<String, AnyBuffer>,
    ) -> impl Section {
        DynSection {}
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SectionError {
    #[error("section does not have input [{0}]")]
    MissingInput(&'static str),

    #[error("section does not have output [{0}] or output is already connected")]
    MissingOutput(&'static str),

    #[error("section does not have buffer [{0}]")]
    MissingBuffer(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Section)]
    struct TestSection {
        foo: InputSlot<i64>,
        bar: Output<i64>,
        baz: Buffer<i64>,
    }
}
