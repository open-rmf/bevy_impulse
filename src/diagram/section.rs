use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{unknown_diagram_error, AnyBuffer, AsAnyBuffer, Buffer, Builder, InputSlot, Output};

use super::{
    dyn_connect, type_info::TypeInfo, BuilderId, DiagramElementRegistry, DiagramErrorCode,
    DynOutput, Edge, EdgeBuilder, NextOperation, OperationId, Vertex,
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
    pub(super) fn build_edges<'a>(
        &'a self,
        mut builder: EdgeBuilder<'a, '_>,
    ) -> Result<(), DiagramErrorCode> {
        match &self.builder {
            SectionProvider::Builder(_) => {
                for (key, next_op) in &self.connect {
                    builder.add_output_edge(next_op, None, Some(key.clone()))?;
                }
                Ok(())
            }
            SectionProvider::Template(_) => {
                panic!("TODO")
            }
        }
    }

    pub(super) fn try_connect(
        &self,
        vertex: &Vertex,
        builder: &mut Builder,
        registry: &DiagramElementRegistry,
        mut edges: HashMap<&usize, &mut Edge>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<bool, DiagramErrorCode> {
        let mut inputs = HashMap::with_capacity(vertex.in_edges.len());
        for edge_id in &vertex.in_edges {
            let edge = edges
                .remove(edge_id)
                .ok_or_else(|| unknown_diagram_error!())?;
            if let Some(output) = edge.output.take() {
                match edge.target {
                    NextOperation::Section { section: _, input } => {
                        inputs.insert(input, output);
                    }
                    _ => return Err(DiagramErrorCode::UnknownTarget),
                }
            } else {
                return Ok(false);
            }
        }

        // let mut outputs = HashMap::with_capacity(self.connect.len());
        // for edge_id

        match &self.builder {
            SectionProvider::Builder(builder_id) => {
                let reg = registry.get_section_registration(builder_id)?;

                // note that unlike nodes, sections are created only when trying to connect
                // because we need all the buffers to be created first, and buffers are
                // created at connect time.
                let section = reg.create_section(builder, self.config.clone())?;
                let mut section_buffers = HashMap::with_capacity(self.buffers.len());
                let mut outputs = HashMap::new();
                section.try_connect(builder, inputs, &mut outputs, &mut section_buffers)?;

                for edge_id in &vertex.out_edges {
                    let edge = edges
                        .remove(edge_id)
                        .ok_or_else(|| unknown_diagram_error!())?;
                    if let Some(name) = &edge.tag {
                        let output = outputs
                            .remove(name)
                            .ok_or(DiagramErrorCode::IncompleteDiagram)?;
                        edge.output = Some(output);
                    }
                }

                // for now we just do namespacing by prefixing, if this is insufficient,
                // a more concrete impl may be done in `OperationId`.
                for (key, buffer) in section_buffers {
                    buffers.insert(format!("{}/{}", vertex.op_id, key), buffer);
                }

                Ok(true)
            }
            SectionProvider::Template(_template) => panic!("TODO"),
        }
    }
}

#[derive(Serialize, Clone)]
pub struct SectionMetadata {
    pub(super) inputs: HashMap<String, SectionInput>,
    pub(super) outputs: HashMap<String, SectionOutput>,
    pub(super) buffers: HashMap<String, SectionBuffer>,
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
        inputs: HashMap<&String, DynOutput>,
        outputs: &mut HashMap<String, DynOutput>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode>;

    fn on_register(registry: &mut DiagramElementRegistry)
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
        inputs: &mut HashMap<&String, DynOutput>,
        outputs: &mut HashMap<String, DynOutput>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode>;
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
        inputs: &mut HashMap<&String, DynOutput>,
        _outputs: &mut HashMap<String, DynOutput>,
        _buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        let output = inputs
            .remove(key)
            .ok_or(SectionError::MissingInput(key.clone()))?;
        dyn_connect(builder, output, self.into())
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
        _builder: &mut Builder,
        _inputs: &mut HashMap<&String, DynOutput>,
        outputs: &mut HashMap<String, DynOutput>,
        _buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        outputs.insert(key.clone(), self.into());
        Ok(())
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
        _inputs: &mut HashMap<&String, DynOutput>,
        _outputs: &mut HashMap<String, DynOutput>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        buffers.insert(key.to_string(), self.as_any_buffer());
        Ok(())
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

pub(super) struct DynSection;

impl Section for DynSection {
    fn try_connect(
        self: Box<Self>,
        builder: &mut Builder,
        inputs: HashMap<&String, DynOutput>,
        outputs: &mut HashMap<String, DynOutput>,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<(), DiagramErrorCode> {
        panic!("TODO")
    }

    fn on_register(_registry: &mut DiagramElementRegistry)
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
    use crate::{BufferKey, BufferSettings, SectionBuilderOptions};

    use super::*;

    #[derive(Section)]
    struct TestSection {
        foo: InputSlot<i64>,
        bar: Output<f64>,
        baz: Buffer<String>,
    }

    #[test]
    fn test_register_section() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_name("TestSection"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|_: i64| 1_f64);
                let buffer = builder.create_buffer(BufferSettings::default());
                TestSection {
                    foo: node.input,
                    bar: node.output,
                    baz: buffer,
                }
            },
        );

        let reg = registry.get_section_registration("test_section").unwrap();
        assert_eq!(reg.name, "TestSection");
        let metadata = &reg.metadata;
        assert_eq!(metadata.inputs.len(), 1);
        assert_eq!(metadata.inputs["foo"].message_type, TypeInfo::of::<i64>());
        assert_eq!(metadata.outputs.len(), 1);
        assert_eq!(metadata.outputs["bar"].message_type, TypeInfo::of::<f64>());
        assert_eq!(metadata.buffers.len(), 1);
        assert_eq!(metadata.buffers["baz"].item_type, TypeInfo::of::<String>());
    }

    struct OpaqueMessage;

    /// A test compile that opaque messages can be used in sections.
    #[derive(Section)]
    struct TestSectionNoDeserialize {
        #[section(no_deserialize, no_serialize, no_clone)]
        msg: InputSlot<OpaqueMessage>,
    }

    #[derive(Section)]
    struct TestSectionUnzip {
        input: InputSlot<()>,
        #[section(unzip)]
        output: Output<(i64, i64)>,
    }

    #[test]
    fn test_section_unzip() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_name("TestSection"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|_: ()| (1, 2));
                TestSectionUnzip {
                    input: node.input,
                    output: node.output,
                }
            },
        );
        let reg = registry.get_message_registration::<(i64, i64)>().unwrap();
        assert!(reg.operations.unzip_impl.is_some());
    }

    #[derive(Section)]
    struct TestSectionForkResult {
        input: InputSlot<()>,
        #[section(fork_result)]
        output: Output<Result<i64, String>>,
    }

    #[test]
    fn test_section_fork_result() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_name("TestSection"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|_: ()| Ok(1));
                TestSectionForkResult {
                    input: node.input,
                    output: node.output,
                }
            },
        );
        let reg = registry
            .get_message_registration::<Result<i64, String>>()
            .unwrap();
        assert!(reg.operations.fork_result_impl.is_some());
    }

    #[derive(Section)]
    struct TestSectionSplit {
        input: InputSlot<()>,
        #[section(split)]
        output: Output<Vec<i64>>,
    }

    #[test]
    fn test_section_split() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_name("TestSection"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|_: ()| vec![]);
                TestSectionSplit {
                    input: node.input,
                    output: node.output,
                }
            },
        );
        let reg = registry.get_message_registration::<Vec<i64>>().unwrap();
        assert!(reg.operations.split_impl.is_some());
    }

    #[derive(Section)]
    struct TestSectionJoin {
        #[section(join)]
        input: InputSlot<Vec<i64>>,
        output: Output<()>,
    }

    #[test]
    fn test_section_join() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_name("TestSection"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|_: Vec<i64>| {});
                TestSectionJoin {
                    input: node.input,
                    output: node.output,
                }
            },
        );
        let reg = registry.get_message_registration::<Vec<i64>>().unwrap();
        assert!(reg.operations.join_impl.is_some());
    }

    #[derive(Section)]
    struct TestSectionBufferAccess {
        #[section(buffer_access, no_deserialize, no_serialize)]
        input: InputSlot<(i64, Vec<BufferKey<i64>>)>,
        output: Output<()>,
    }

    #[test]
    fn test_section_buffer_access() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_name("TestSection"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|_: (i64, Vec<BufferKey<i64>>)| {});
                TestSectionBufferAccess {
                    input: node.input,
                    output: node.output,
                }
            },
        );
        let reg = registry
            .get_message_registration::<(i64, Vec<BufferKey<i64>>)>()
            .unwrap();
        assert!(reg.operations.buffer_access_impl.is_some());
    }

    #[derive(Section)]
    struct TestSectionListen {
        #[section(listen, no_deserialize, no_serialize)]
        input: InputSlot<Vec<BufferKey<i64>>>,
        output: Output<()>,
    }

    #[test]
    fn test_section_listen() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_name("TestSection"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|_: Vec<BufferKey<i64>>| {});
                TestSectionListen {
                    input: node.input,
                    output: node.output,
                }
            },
        );
        let reg = registry
            .get_message_registration::<Vec<BufferKey<i64>>>()
            .unwrap();
        assert!(reg.operations.listen_impl.is_some());
    }
}
