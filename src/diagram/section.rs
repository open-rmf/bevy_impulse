use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{AnyBuffer, AsAnyBuffer, Buffer, BufferMap, Builder, InputSlot, Output};

use super::{
    dyn_connect, type_info::TypeInfo, BuilderId, DiagramErrorCode, DynInputSlot, DynOutput,
    MessageRegistrationBuilder, MessageRegistry, NextOperation, OperationId, SectionRegistration,
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

    fn try_connect(
        &self,
        op_id: &OperationId,
        builder: &mut Builder,
        config: serde_json::Value,
        sections: &HashMap<BuilderId, SectionRegistration>,
        inputs: HashMap<String, DynOutput>,
        outputs: HashMap<String, DynInputSlot>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<bool, DiagramErrorCode> {
        match &self.builder {
            SectionProvider::Builder(builder_id) => {
                let reg = sections
                    .get(builder_id)
                    .ok_or(DiagramErrorCode::BuilderNotFound(builder_id.clone()))?;

                // note that unlike nodes, sections are created only when trying to connect
                // because we need all the buffers to be created first, and buffers are
                // created at connect time.
                let section = reg.create_section(builder, config)?;
                let mut section_buffers = HashMap::with_capacity(self.buffers.len());
                section.try_connect(builder, inputs, outputs, &mut section_buffers)?;
                // for now we just do namespacing by prefixing, if this is insufficient,
                // a more concrete impl may be done in `OperationId`.
                for (key, buffer) in section_buffers {
                    buffers.insert(format!("{}/{}", op_id, key), buffer);
                }

                Ok(true)
            }
            SectionProvider::Template(_template) => panic!("TODO"),
        }
    }
}

#[derive(Serialize, Clone)]
pub struct SectionMetadata {
    inputs: HashMap<String, SectionInput>,
    outputs: HashMap<String, SectionOutput>,
    buffers: HashMap<String, SectionBuffer>,
}

impl SectionMetadata {
    pub fn new() -> Self {
        Self {
            inputs: HashMap::new(),
            outputs: HashMap::new(),
            buffers: HashMap::new(),
        }
    }
}

pub trait SectionMetadataProvider {
    fn metadata() -> &'static SectionMetadata;
}

pub trait Section {
    fn try_connect(
        self: Box<Self>,
        builder: &mut Builder,
        inputs: HashMap<String, DynOutput>,
        outputs: HashMap<String, DynInputSlot>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode>;

    fn on_register(registry: &mut MessageRegistry)
    where
        Self: Sized;
}

pub trait SectionItem {
    type MessageType;

    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str);

    fn try_connect(
        self,
        key: &String,
        builder: &mut Builder,
        inputs: &mut HashMap<String, DynOutput>,
        outputs: &mut HashMap<String, DynInputSlot>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode>;

    fn message_registration(
        registry: &mut MessageRegistry,
    ) -> MessageRegistrationBuilder<Self::MessageType>;
}

impl<T> SectionItem for InputSlot<T>
where
    T: Send + Sync + 'static,
{
    type MessageType = T;

    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str) {
        metadata.inputs.insert(
            key.to_string(),
            SectionInput {
                message_type: TypeInfo::of::<T>(),
            },
        );
    }

    fn try_connect(
        self,
        key: &String,
        builder: &mut Builder,
        inputs: &mut HashMap<String, DynOutput>,
        _outputs: &mut HashMap<String, DynInputSlot>,
        _buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        let output = inputs
            .remove(key)
            .ok_or(SectionError::MissingInput(key.clone()))?;
        dyn_connect(builder, output, self.into())
    }

    fn message_registration(
        registry: &mut MessageRegistry,
    ) -> MessageRegistrationBuilder<Self::MessageType> {
        MessageRegistrationBuilder::new(registry)
    }
}

impl<T> SectionItem for Output<T>
where
    T: Send + Sync + 'static,
{
    type MessageType = T;

    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str) {
        metadata.outputs.insert(
            key.to_string(),
            SectionOutput {
                message_type: TypeInfo::of::<T>(),
            },
        );
    }

    fn try_connect(
        self,
        key: &String,
        builder: &mut Builder,
        _inputs: &mut HashMap<String, DynOutput>,
        outputs: &mut HashMap<String, DynInputSlot>,
        _buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        let input = outputs
            .remove(key)
            .ok_or(SectionError::MissingOutput(key.clone()))?;
        dyn_connect(builder, self.into(), input)
    }

    fn message_registration(
        registry: &mut MessageRegistry,
    ) -> MessageRegistrationBuilder<Self::MessageType> {
        MessageRegistrationBuilder::new(registry)
    }
}

impl<T> SectionItem for Buffer<T>
where
    T: Send + Sync + 'static,
{
    type MessageType = T;

    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str) {
        metadata.buffers.insert(
            key.to_string(),
            SectionBuffer {
                item_type: TypeInfo::of::<T>(),
            },
        );
    }

    fn try_connect(
        self,
        key: &String,
        _builder: &mut Builder,
        _inputs: &mut HashMap<String, DynOutput>,
        _outputs: &mut HashMap<String, DynInputSlot>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        buffers.insert(key.to_string(), self.as_any_buffer());
        Ok(())
    }

    fn message_registration(
        registry: &mut MessageRegistry,
    ) -> MessageRegistrationBuilder<Self::MessageType> {
        MessageRegistrationBuilder::new(registry)
    }
}

#[derive(Serialize, Clone)]
pub struct SectionInput {
    pub(super) message_type: TypeInfo,
}

#[derive(Serialize, Clone)]
pub struct SectionOutput {
    pub(super) message_type: TypeInfo,
}

#[derive(Serialize, Clone)]
pub struct SectionBuffer {
    pub(super) item_type: TypeInfo,
}

trait CreateSection {
    fn create_section(
        &mut self,
        builder: &mut Builder,
        buffers: HashMap<String, AnyBuffer>,
    ) -> Box<dyn Section>;
}

pub(super) struct DynSection;

impl Section for DynSection {
    fn try_connect(
        self: Box<Self>,
        builder: &mut Builder,
        inputs: HashMap<String, DynOutput>,
        outputs: HashMap<String, DynInputSlot>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        panic!("TODO")
    }

    fn on_register(_registry: &mut MessageRegistry)
    where
        Self: Sized,
    {
        // dyn section cannot register message operations
    }
}

pub(super) struct SectionTemplate;

impl SectionTemplate {
    fn create_section(&self) -> DynSection {
        panic!("TODO")
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SectionError {
    #[error("section does not have input [{0}]")]
    MissingInput(String),

    #[error("section does not have output [{0}] or output is already connected")]
    MissingOutput(String),

    #[error("section does not have buffer [{0}]")]
    MissingBuffer(String),
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
