use std::{
    borrow::Cow,
    collections::HashMap
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{AnyBuffer, AnyMessageBox, Buffer, Builder, InputSlot, JsonBuffer, JsonMessage, Output};

use super::{
    type_info::TypeInfo, BuilderId,
    DiagramElementRegistry, DiagramErrorCode, DiagramOperation, DynInputSlot, DynOutput,
    NextOperation, OperationName, BuildDiagramOperation, DiagramContext,
    BuildStatus, NamespacedOperation,
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
pub struct SectionSchema {
    #[serde(flatten)]
    pub(super) provider: SectionProvider,
    #[serde(default)]
    pub(super) config: serde_json::Value,
    pub(super) connect: HashMap<String, NextOperation>,
}

impl BuildDiagramOperation for SectionSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        match &self.provider {
            SectionProvider::Builder(section_builder) => {
                let section = ctx
                    .registry
                    .get_section_registration(section_builder)?
                    .create_section(builder, self.config.clone())?
                    .into_slots();

                for (op, input) in section.inputs {
                    ctx.set_input_for_target(
                        NextOperation::Namespace(NamespacedOperation {
                            namespace: id.clone(),
                            operation: op.clone(),
                        }),
                        input
                    )?;
                }

                for (target, output) in section.outputs {
                    if let Some(target) = self.connect.get(&target) {
                        ctx.add_output_into_target(target.clone(), output);
                    }
                }

                for (op, buffer) in section.buffers {
                    ctx.set_buffer_for_operation(
                        NextOperation::Namespace(NamespacedOperation {
                            namespace: id.clone(),
                            operation: op.clone()
                        }),
                        buffer,
                    )?;
                }
            }
            SectionProvider::Template(section_template) => {
                let section = ctx
                    .templates
                    .get_template(section_template)?;


            }
        }

        Ok(BuildStatus::Finished)
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

pub struct SectionSlots {
    inputs: HashMap<String, DynInputSlot>,
    outputs: HashMap<String, DynOutput>,
    buffers: HashMap<String, AnyBuffer>,
}

impl SectionSlots {
    pub fn new() -> Self {
        Self {
            inputs: HashMap::new(),
            outputs: HashMap::new(),
            buffers: HashMap::new(),
        }
    }
}

pub trait Section {
    fn into_slots(self: Box<Self>) -> SectionSlots;

    fn on_register(registry: &mut DiagramElementRegistry)
    where
        Self: Sized;
}

pub trait SectionItem {
    type MessageType;

    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str);

    fn insert_into_slots(self, key: String, slots: &mut SectionSlots);
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

    fn insert_into_slots(self, key: String, slots: &mut SectionSlots) {
        slots.inputs.insert(key, self.into());
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

    fn insert_into_slots(self, key: String, slots: &mut SectionSlots) {
        slots.outputs.insert(key, self.into());
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
                item_type: Some(TypeInfo::of::<T>()),
            },
        );
    }

    fn insert_into_slots(self, key: String, slots: &mut SectionSlots) {
        slots.buffers.insert(key, self.into());
    }
}

impl SectionItem for AnyBuffer {
    type MessageType = AnyMessageBox;

    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str) {
        metadata.buffers.insert(
            key.to_string(),
            SectionBuffer { item_type: None },
        );
    }

    fn insert_into_slots(self, key: String, slots: &mut SectionSlots) {
        slots.buffers.insert(key, self);
    }
}

impl SectionItem for JsonBuffer {
    type MessageType = JsonMessage;

    fn build_metadata(metadata: &mut SectionMetadata, key: &'static str) {
        metadata.buffers.insert(
            key.to_string(),
            SectionBuffer { item_type: None },
        );
    }

    fn insert_into_slots(self, key: String, slots: &mut SectionSlots) {
        slots.buffers.insert(key, self.into());
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
    pub(super) item_type: Option<TypeInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SectionTemplate {
    /// These are the inputs that the section is exposing for its sibling
    /// operations to send outputs into.
    #[serde(default)]
    pub inputs: Vec<InputRemapping>,
    /// These are the outputs that the section is exposing so you can connect
    /// them into siblings of the section.
    #[serde(default)]
    pub outputs: Vec<String>,
    /// These are the buffers that the section is exposing for you to read,
    /// write, join, or listen to.
    #[serde(default)]
    pub buffers: Vec<InputRemapping>,
    /// Operations that define the behavior of the section.
    pub ops: HashMap<OperationName, DiagramOperation>,
}

/// This defines how sections remap their inner operations (inputs and buffers)
/// to expose them to operations that are siblings to the section.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub enum InputRemapping {
    /// Do a simple 1:1 forwarding of the names listed in the array
    Forward(Vec<OperationName>),
    /// Rename an operation inside the section to expose it externally. The key
    /// of the map is what siblings of the section can connect to, and the value
    /// of the entry is the identifier of the input inside the section that is
    /// being exposed.
    ///
    /// This allows a section to expose inputs and buffers that are provided
    /// by inner sections.
    Remap(HashMap<OperationName, NextOperation>),
}

#[derive(thiserror::Error, Debug)]
pub enum SectionError {
    #[error("section does not have output [{0}]")]
    MissingOutput(String),

    #[error("operation has extra output [{0}] that is not in the section")]
    ExtraOutput(String),
}

#[cfg(test)]
mod tests {
    use bevy_ecs::system::In;
    use serde_json::json;

    use crate::{
        diagram::testing::DiagramTestFixture, testing::TestingContext, BufferAccess, BufferKey,
        BufferSettings, Diagram, IntoBlockingCallback, Node, NodeBuilderOptions, RequestExt,
        RunCommandsOnWorldExt, SectionBuilderOptions, JsonMessage,
    };

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
        assert_eq!(metadata.buffers["baz"].item_type, Some(TypeInfo::of::<String>()));
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

    #[derive(Section)]
    struct TestAddOne {
        test_input: InputSlot<i64>,
        test_output: Output<i64>,
    }

    fn register_add_one(registry: &mut DiagramElementRegistry) {
        registry.register_section_builder(
            SectionBuilderOptions::new("add_one"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|i: i64| i + 1);
                TestAddOne {
                    test_input: node.input,
                    test_output: node.output,
                }
            },
        );
    }

    #[test]
    fn test_section_workflow() {
        let mut registry = DiagramElementRegistry::new();
        register_add_one(&mut registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "add_one": "test_input" },
            "ops": {
                "add_one": {
                    "type": "section",
                    "builder": "add_one",
                    "connect": {
                        "test_output": { "builtin": "terminate" },
                    },
                },
            },
        }))
        .unwrap();

        let mut context = TestingContext::minimal_plugins();
        let mut promise = context.app.world.command(|cmds| {
            let workflow = diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry).unwrap();
            cmds.request(serde_json::to_value(1).unwrap(), workflow)
                .take_response()
        });
        context.run_while_pending(&mut promise);
        let result = promise.take().available().unwrap();
        assert_eq!(result, 2);
    }

    #[test]
    fn test_section_workflow_missing_output() {
        let mut registry = DiagramElementRegistry::new();
        register_add_one(&mut registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "add_one": "test_input" },
            "ops": {
                "add_one": {
                    "type": "section",
                    "builder": "add_one",
                    "connect": {},
                },
            },
        }))
        .unwrap();

        let mut context = TestingContext::minimal_plugins();
        let err = context
            .app
            .world
            .command(|cmds| diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry))
            .unwrap_err();
        let section_err = match err.code {
            DiagramErrorCode::SectionError(section_err) => section_err,
            _ => panic!("expected SectionError"),
        };
        assert!(matches!(section_err, SectionError::MissingOutput(_)));
    }

    #[test]
    fn test_section_workflow_extra_output() {
        let mut registry = DiagramElementRegistry::new();
        register_add_one(&mut registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "add_one": "test_input" },
            "ops": {
                "add_one": {
                    "type": "section",
                    "builder": "add_one",
                    "connect": {
                        "extra": { "builtin": "dispose" },
                        "test_output": { "builtin": "terminate" },
                    },
                },
            },
        }))
        .unwrap();

        let mut context = TestingContext::minimal_plugins();
        let err = context
            .app
            .world
            .command(|cmds| diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry))
            .unwrap_err();
        let section_err = match err.code {
            DiagramErrorCode::SectionError(section_err) => section_err,
            _ => panic!("expected SectionError"),
        };
        assert!(matches!(section_err, SectionError::ExtraOutput(_)));
    }

    #[test]
    fn test_section_workflow_missing_input() {
        let mut registry = DiagramElementRegistry::new();
        register_add_one(&mut registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "builtin": "terminate" },
            "ops": {
                "add_one": {
                    "type": "section",
                    "builder": "add_one",
                    "connect": {
                        "test_output": { "builtin": "terminate" },
                    },
                },
            },
        }))
        .unwrap();

        let mut context = TestingContext::minimal_plugins();
        let err = context
            .app
            .world
            .command(|cmds| diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry))
            .unwrap_err();
        assert!(matches!(err.code, DiagramErrorCode::OnlySingleInput));
    }

    #[test]
    fn test_section_workflow_extra_input() {
        let mut fixture = DiagramTestFixture::new();
        register_add_one(&mut fixture.registry);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "multiply3",
            "ops": {
                "multiply3": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "fork_clone",
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": [
                        { "add_one": "test_input" },
                        { "add_one": "extra_input" },
                    ]
                },
                "add_one": {
                    "type": "section",
                    "builder": "add_one",
                    "connect": {
                        "test_output": { "builtin": "terminate" },
                    },
                },
            },
        }))
        .unwrap();

        let mut context = TestingContext::minimal_plugins();
        let err = context
            .app
            .world
            .command(|cmds| diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &fixture.registry))
            .unwrap_err();
        assert!(matches!(err.code, DiagramErrorCode::OperationNotFound(_)));
    }

    #[derive(Section)]
    struct TestSectionAddToBuffer {
        test_input: InputSlot<i64>,
        test_buffer: Buffer<i64>,
    }

    #[test]
    fn test_section_workflow_buffer() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("add_one_to_buffer"),
            |builder: &mut Builder, _config: ()| {
                let node = builder.create_map_block(|i: i64| i + 1);
                let buffer = builder.create_buffer(BufferSettings::default());
                builder.connect(node.output, buffer.input_slot());
                TestSectionAddToBuffer {
                    test_input: node.input,
                    test_buffer: buffer,
                }
            },
        );
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("buffer_length"),
                |builder: &mut Builder, _config: ()| -> Node<Vec<BufferKey<i64>>, usize, ()> {
                    {
                        builder.create_node(
                            (|In(request): In<Vec<BufferKey<i64>>>, access: BufferAccess<i64>| {
                                access.get(&request[0]).unwrap().len()
                            })
                            .into_blocking_callback(),
                        )
                    }
                },
            )
            .with_listen()
            .with_common_response();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "add_one_to_buffer": "test_input" },
            "ops": {
                "add_one_to_buffer": {
                    "type": "section",
                    "builder": "add_one_to_buffer",
                    "connect": {},
                },
                "listen": {
                    "type": "listen",
                    "buffers": ["add_one_to_buffer/test_buffer"],
                    "next": "buffer_length",
                },
                "buffer_length": {
                    "type": "node",
                    "builder": "buffer_length",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let mut context = TestingContext::minimal_plugins();
        let mut promise = context.app.world.command(|cmds| {
            let workflow = diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry).unwrap();
            cmds.request(serde_json::to_value(1).unwrap(), workflow)
                .take_response()
        });
        context.run_while_pending(&mut promise);
        let result = promise.take().available().unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_section_template() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "test_template": {
                    "inputs": ["input1", "input2"],
                    "outputs": ["output1", "output2"],
                    "ops": {
                        "input1": {
                            "type": "node",
                            "builder": "multiply3",
                            "next": "output1",
                        },
                        "input2": {
                            "type": "node",
                            "builder": "multiply3",
                            "next": "output2",
                        },
                    },
                },
            },
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": [
                        { "test_tmpl": "input1" },
                        { "test_tmpl": "input2" },
                    ],
                },
                "test_tmpl": {
                    "type": "section",
                    "template": "test_template",
                    "connect": {
                        "output1": { "builtin": "terminate" },
                        "output2": { "builtin": "terminate" },
                    },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result, 12);
    }
}
