/*
 * Copyright (C) 2025 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

use std::{collections::HashMap, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{AnyBuffer, AnyMessageBox, Buffer, InputSlot, JsonBuffer, JsonMessage, Output};

use super::{
    BuildDiagramOperation, BuildStatus, BuilderId, DiagramContext, DiagramElementRegistry,
    DiagramErrorCode, DynInputSlot, DynOutput, NamespacedOperation, NextOperation, OperationName,
    OperationRef, Operations, RedirectConnection, TraceInfo, TraceSettings, TypeInfo,
};

pub use bevy_impulse_derive::Section;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SectionProvider {
    Builder(BuilderId),
    Template(OperationName),
}

/// Connect the request to a registered section.
///
/// ```
/// # bevy_impulse::Diagram::from_json_str(r#"
/// {
///     "version": "0.1.0",
///     "start": "section_op",
///     "ops": {
///         "section_op": {
///             "type": "section",
///             "builder": "my_section_builder",
///             "connect": {
///                 "my_section_output": { "builtin": "terminate" }
///             }
///         }
///     }
/// }
/// # "#)?;
/// # Ok::<_, serde_json::Error>(())
/// ```
///
/// Custom sections can also be created via templates
/// ```
/// # bevy_impulse::Diagram::from_json_str(r#"
/// {
///     "version": "0.1.0",
///     "templates": {
///         "my_template": {
///             "inputs": ["section_input"],
///             "outputs": ["section_output"],
///             "buffers": [],
///             "ops": {
///                 "section_input": {
///                     "type": "node",
///                     "builder": "my_node",
///                     "next": "section_output"
///                 }
///             }
///         }
///     },
///     "start": "section_op",
///     "ops": {
///         "section_op": {
///             "type": "section",
///             "template": "my_template",
///             "connect": {
///                 "section_output": { "builtin": "terminate" }
///             }
///         }
///     }
/// }
/// # "#)?;
/// # Ok::<_, serde_json::Error>(())
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SectionSchema {
    #[serde(flatten)]
    pub provider: SectionProvider,
    #[serde(default)]
    pub config: Arc<JsonMessage>,
    #[serde(default)]
    pub connect: HashMap<Arc<str>, NextOperation>,
    #[serde(flatten)]
    pub trace_settings: TraceSettings,
}

impl BuildDiagramOperation for SectionSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        match &self.provider {
            SectionProvider::Builder(section_builder) => {
                let section_registration =
                    ctx.registry.get_section_registration(section_builder)?;

                let section = section_registration
                    .create_section(ctx.builder, (*self.config).clone())?
                    .into_slots();

                // TODO(@mxgrey): Figure out how to automatically trace operations
                // that are built by the section builder.
                let trace = TraceInfo::new(self, self.trace_settings.trace)?;
                for (op, input) in section.inputs {
                    ctx.set_input_for_target(
                        &NextOperation::Namespace(NamespacedOperation {
                            namespace: id.clone(),
                            operation: op.clone(),
                        }),
                        input,
                        trace.clone(),
                    )?;
                }

                for expected_output in self.connect.keys() {
                    if !section.outputs.contains_key(expected_output) {
                        return Err(SectionError::UnknownOutput(Arc::clone(expected_output)).into());
                    }
                }

                for (target, output) in section.outputs {
                    if let Some(target) = self.connect.get(&target) {
                        ctx.add_output_into_target(target, output);
                    }
                }

                for (op, buffer) in section.buffers {
                    ctx.set_buffer_for_operation(
                        &NextOperation::Namespace(NamespacedOperation {
                            namespace: id.clone(),
                            operation: op.clone(),
                        }),
                        buffer,
                        trace.clone(),
                    )?;
                }
            }
            SectionProvider::Template(section_template) => {
                let section = ctx.templates.get_template(section_template)?;

                for (child_id, op) in section.ops.iter() {
                    ctx.add_child_operation(id, child_id, op, section.ops.clone(), None);
                }

                section
                    .inputs
                    .redirect(|op, next| ctx.redirect_to_child_input(id, op, next))?;

                section
                    .buffers
                    .redirect(|op, next| ctx.redirect_to_child_buffer(id, op, next))?;

                for expected_output in self.connect.keys() {
                    if !section.outputs.contains(expected_output) {
                        return Err(SectionError::UnknownOutput(Arc::clone(expected_output)).into());
                    }
                }

                for output in &section.outputs {
                    if let Some(target) = self.connect.get(output) {
                        ctx.redirect_exposed_output_to_sibling(id, output, target)?;
                    } else {
                        ctx.redirect_exposed_output_to_sibling(
                            id,
                            output,
                            &NextOperation::dispose(),
                        )?;
                    }
                }
            }
        }

        ctx.set_connect_into_target(
            OperationRef::terminate_for(id.clone()),
            RedirectConnection::new(ctx.into_operation_ref(&NextOperation::terminate())),
        )?;

        Ok(BuildStatus::Finished)
    }
}

#[derive(Serialize, Clone, JsonSchema)]
pub struct SectionMetadata {
    pub(super) inputs: HashMap<OperationName, SectionInput>,
    pub(super) outputs: HashMap<OperationName, SectionOutput>,
    pub(super) buffers: HashMap<OperationName, SectionBuffer>,
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
    inputs: HashMap<OperationName, DynInputSlot>,
    outputs: HashMap<OperationName, DynOutput>,
    buffers: HashMap<OperationName, AnyBuffer>,
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

    fn build_metadata(metadata: &mut SectionMetadata, key: &str);

    fn insert_into_slots(self, key: &str, slots: &mut SectionSlots);
}

impl<T> SectionItem for InputSlot<T>
where
    T: Send + Sync + 'static,
{
    type MessageType = T;

    fn build_metadata(metadata: &mut SectionMetadata, key: &str) {
        metadata.inputs.insert(
            key.into(),
            SectionInput {
                message_type: TypeInfo::of::<T>(),
            },
        );
    }

    fn insert_into_slots(self, key: &str, slots: &mut SectionSlots) {
        slots.inputs.insert(key.into(), self.into());
    }
}

impl<T> SectionItem for Output<T>
where
    T: Send + Sync + 'static,
{
    type MessageType = T;

    fn build_metadata(metadata: &mut SectionMetadata, key: &str) {
        metadata.outputs.insert(
            key.into(),
            SectionOutput {
                message_type: TypeInfo::of::<T>(),
            },
        );
    }

    fn insert_into_slots(self, key: &str, slots: &mut SectionSlots) {
        slots.outputs.insert(key.into(), self.into());
    }
}

impl<T> SectionItem for Buffer<T>
where
    T: Send + Sync + 'static,
{
    type MessageType = T;

    fn build_metadata(metadata: &mut SectionMetadata, key: &str) {
        metadata.buffers.insert(
            key.into(),
            SectionBuffer {
                item_type: Some(TypeInfo::of::<T>()),
            },
        );
    }

    fn insert_into_slots(self, key: &str, slots: &mut SectionSlots) {
        slots.buffers.insert(key.into(), self.into());
    }
}

impl SectionItem for AnyBuffer {
    type MessageType = AnyMessageBox;

    fn build_metadata(metadata: &mut SectionMetadata, key: &str) {
        metadata
            .buffers
            .insert(key.into(), SectionBuffer { item_type: None });
    }

    fn insert_into_slots(self, key: &str, slots: &mut SectionSlots) {
        slots.buffers.insert(key.into(), self);
    }
}

impl SectionItem for JsonBuffer {
    type MessageType = JsonMessage;

    fn build_metadata(metadata: &mut SectionMetadata, key: &str) {
        metadata
            .buffers
            .insert(key.into(), SectionBuffer { item_type: None });
    }

    fn insert_into_slots(self, key: &str, slots: &mut SectionSlots) {
        slots.buffers.insert(key.into(), self.into());
    }
}

#[derive(Serialize, Clone, JsonSchema)]
pub struct SectionInput {
    pub(super) message_type: TypeInfo,
}

#[derive(Serialize, Clone, JsonSchema)]
pub struct SectionOutput {
    pub(super) message_type: TypeInfo,
}

#[derive(Serialize, Clone, JsonSchema)]
pub struct SectionBuffer {
    pub(super) item_type: Option<TypeInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SectionTemplate {
    /// These are the inputs that the section is exposing for its sibling
    /// operations to send outputs into.
    #[serde(default)]
    pub inputs: InputRemapping,
    /// These are the outputs that the section is exposing so you can connect
    /// them into siblings of the section.
    #[serde(default)]
    pub outputs: Vec<OperationName>,
    /// These are the buffers that the section is exposing for you to read,
    /// write, join, or listen to.
    #[serde(default)]
    pub buffers: InputRemapping,
    /// Operations that define the behavior of the section.
    pub ops: Operations,
}

/// This defines how sections remap their inner operations (inputs and buffers)
/// to expose them to operations that are siblings to the section.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
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

impl Default for InputRemapping {
    fn default() -> Self {
        Self::Forward(Vec::new())
    }
}

impl InputRemapping {
    pub fn get_inner(&self, op: &OperationName) -> Option<NextOperation> {
        match self {
            Self::Forward(forward) => {
                if forward.contains(op) {
                    return Some(NextOperation::Name(Arc::clone(op)));
                }
            }
            Self::Remap(remap) => {
                if let Some(next) = remap.get(op) {
                    return Some(next.clone());
                }
            }
        }

        None
    }

    pub fn redirect(
        &self,
        mut f: impl FnMut(&OperationName, &NextOperation) -> Result<(), DiagramErrorCode>,
    ) -> Result<(), DiagramErrorCode> {
        match self {
            Self::Forward(operations) => {
                for op in operations {
                    f(op, &NextOperation::Name(Arc::clone(op)))?;
                }
            }
            Self::Remap(remap) => {
                for (op, next) in remap {
                    f(op, next)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SectionError {
    #[error("operation has extra output [{0}] that is not in the section")]
    UnknownOutput(OperationName),
}

#[cfg(test)]
mod tests {
    use bevy_ecs::system::In;
    use serde_json::json;

    use crate::{
        diagram::testing::DiagramTestFixture, testing::TestingContext, BufferAccess,
        BufferAccessMut, BufferKey, BufferSettings, Builder, Diagram, IntoBlockingCallback,
        JsonMessage, Node, NodeBuilderOptions, RequestExt, RunCommandsOnWorldExt,
        SectionBuilderOptions,
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
            SectionBuilderOptions::new("test_section").with_default_display_text("TestSection"),
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
        assert_eq!(reg.default_display_text.as_ref(), "TestSection");
        let metadata = &reg.metadata;
        assert_eq!(metadata.inputs.len(), 1);
        assert_eq!(metadata.inputs["foo"].message_type, TypeInfo::of::<i64>());
        assert_eq!(metadata.outputs.len(), 1);
        assert_eq!(metadata.outputs["bar"].message_type, TypeInfo::of::<f64>());
        assert_eq!(metadata.buffers.len(), 1);
        assert_eq!(
            metadata.buffers["baz"].item_type,
            Some(TypeInfo::of::<String>())
        );
    }

    #[derive(Section)]
    struct TestSectionUnzip {
        input: InputSlot<()>,
        #[message(unzip)]
        output: Output<(i64, i64)>,
    }

    #[test]
    fn test_section_unzip() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_default_display_text("TestSection"),
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
        #[message(result)]
        output: Output<Result<i64, String>>,
    }

    #[test]
    fn test_section_fork_result() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_default_display_text("TestSection"),
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
        #[message(split)]
        output: Output<Vec<i64>>,
    }

    #[test]
    fn test_section_split() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_default_display_text("TestSection"),
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
        #[message(join)]
        input: InputSlot<Vec<i64>>,
        output: Output<()>,
    }

    #[test]
    fn test_section_join() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_default_display_text("TestSection"),
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
        #[message(buffer_access, no_deserialize, no_serialize)]
        input: InputSlot<(i64, Vec<BufferKey<i64>>)>,
        output: Output<()>,
    }

    #[test]
    fn test_section_buffer_access() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_default_display_text("TestSection"),
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
        #[message(listen, no_deserialize, no_serialize)]
        input: InputSlot<Vec<BufferKey<i64>>>,
        output: Output<()>,
    }

    #[test]
    fn test_section_listen() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_section_builder(
            SectionBuilderOptions::new("test_section").with_default_display_text("TestSection"),
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
            let workflow = diagram
                .spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry)
                .unwrap();
            cmds.request(serde_json::to_value(1).unwrap(), workflow)
                .take_response()
        });
        context.run_while_pending(&mut promise);
        let result = promise.take().available().unwrap();
        assert_eq!(result, 2);
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
        assert!(matches!(section_err, SectionError::UnknownOutput(_)));
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
            .command(|cmds| {
                diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &fixture.registry)
            })
            .unwrap_err();
        assert!(matches!(err.code, DiagramErrorCode::UnknownOperation(_)));
    }

    #[derive(Section)]
    struct TestSectionAddToBuffer {
        test_input: InputSlot<i64>,
        test_buffer: Buffer<i64>,
    }

    #[test]
    fn test_section_workflow_buffer() {
        let mut fixture = DiagramTestFixture::new();

        fixture.registry.register_section_builder(
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
        fixture
            .registry
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
                    "buffers": [{"add_one_to_buffer" : "test_buffer"}],
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

        let count: usize = fixture.spawn_and_run(&diagram, 1_i64).unwrap();
        assert_eq!(count, 1);

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(NodeBuilderOptions::new("pull"), |builder, _: ()| {
                builder.create_node(pull_from_buffer.into_blocking_callback())
            })
            .with_listen();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "multiply_10_to_buffer": {
                    "inputs": ["input"],
                    "buffers": ["buffer"],
                    "ops": {
                        "input": {
                            "type": "node",
                            "builder": "multiply_by",
                            "config": 10,
                            "next": "buffer",
                        },
                        "buffer": { "type": "buffer" },
                    }
                }
            },
            "start": { "multiply": "input" },
            "ops": {
                "multiply": {
                    "type": "section",
                    "template": "multiply_10_to_buffer",
                    "connect": {

                    }
                },
                "listen": {
                    "type": "listen",
                    "buffers": [{ "multiply": "buffer" }],
                    "next": "pull",
                },
                "pull": {
                    "type": "node",
                    "builder": "pull",
                    "next": { "builtin": "terminate" },
                }
            }
        }))
        .unwrap();

        let result: i64 = fixture.spawn_and_run(&diagram, 2_i64).unwrap();
        assert_eq!(result, 20);
    }

    fn pull_from_buffer(In(key): In<BufferKey<i64>>, mut access: BufferAccessMut<i64>) -> i64 {
        access.get_mut(&key).unwrap().pull().unwrap()
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

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert_eq!(result, 12);
    }

    #[test]
    fn test_template_input_remap() {
        let mut fixture = DiagramTestFixture::new();

        // Testing that we can remap inputs to both internal operations within
        // the template and also to builtin operations.
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "test_template": {
                    "inputs": {
                        "multiply": "multiply",
                        "terminate": { "builtin": "terminate" },
                    },
                    "outputs": ["output"],
                    "ops": {
                        "multiply": {
                            "type": "node",
                            "builder": "multiply3",
                            "next": "output",
                        }
                    }
                }
            },
            "start": { "section": "multiply" },
            "ops": {
                "section": {
                    "type": "section",
                    "template": "test_template",
                    "connect": {
                        "output": { "section": "terminate" },
                    },
                },
            },
        }))
        .unwrap();

        let result: i64 = fixture.spawn_and_run(&diagram, 4_i64).unwrap();

        assert_eq!(result, 12);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "multiply_then_add": {
                    "inputs": {
                        "input": "multiply",
                    },
                    "outputs": ["output"],
                    "ops": {
                        "multiply": {
                            "type": "node",
                            "builder": "multiply_by",
                            "config": 10,
                            "next": { "add": "input" },
                        },
                        "add": {
                            "type": "section",
                            "template": "adding_template",
                            "connect": {
                                "added": "output",
                            }
                        }
                    }
                },
                "adding_template": {
                    "inputs": ["input"],
                    "outputs": ["added"],
                    "ops": {
                        "input": {
                            "type": "node",
                            "builder": "add_to",
                            "config": 1,
                            "next": "added",
                        }
                    }
                }
            },
            "start": { "multiply": "input" },
            "ops": {
                "multiply": {
                    "type": "section",
                    "template": "multiply_then_add",
                    "connect": {
                        "output": { "builtin": "terminate" },
                    }
                }
            }
        }))
        .unwrap();

        let result: i64 = fixture.spawn_and_run(&diagram, 5_i64).unwrap();

        assert_eq!(result, 51);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "calculate": {
                    "inputs": ["add", "multiply"],
                    "outputs": ["added", "multiplied"],
                    "ops": {
                        "multiply": {
                            "type": "node",
                            "builder": "multiply_by",
                            "config": 10,
                            "next": "multiplied",
                        },
                        "add": {
                            "type": "node",
                            "builder": "add_to",
                            "config": 10,
                            "next": "added",
                        }
                    }
                }
            },
            "start": "start",
            "ops": {
                "start": {
                    "type": "fork_clone",
                    "next": [
                        { "calc": "add" },
                        { "calc": "multiply" },
                    ],
                },
                "calc": {
                    "type": "section",
                    "template": "calculate",
                    "connect": {
                        "added": "added",
                        "multiplied": "multiplied",
                    },
                },
                "added": { "type": "buffer" },
                "multiplied": { "type": "buffer" },
                "join": {
                    "type": "join",
                    "buffers": ["added", "multiplied"],
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: Vec<i64> = fixture.spawn_and_run(&diagram, 2_i64).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 12);
        assert_eq!(result[1], 20);

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "redirect_buffers": {
                    "buffers": {
                        "added": { "inner": "added" },
                        "multiplied": { "inner": "multiplied" },
                    },
                    "ops": {
                        "inner": {
                            "type": "section",
                            "template": "calculate",
                        }
                    }
                },
                "calculate": {
                    "inputs": ["add", "multiply"],
                    "buffers": ["added", "multiplied"],
                    "ops": {
                        "multiply": {
                            "type": "node",
                            "builder": "multiply_by",
                            "config": 10,
                            "next": "multiplied",
                        },
                        "multiplied": { "type": "buffer" },
                        "add": {
                            "type": "node",
                            "builder": "add_to",
                            "config": 10,
                            "next": "added",
                        },
                        "added": { "type": "buffer" },
                    },
                }
            },
            "start": "start",
            "ops": {
                "start": {
                    "type": "fork_clone",
                    "next": [
                        { "calc": "add" },
                        { "calc": "multiply" },
                    ],
                },
                "calc": {
                    "type": "section",
                    "template": "calculate",
                },
                "join": {
                    "type": "join",
                    "buffers": [
                        { "calc": "added" },
                        { "calc": "multiplied" },
                    ],
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: Vec<i64> = fixture.spawn_and_run(&diagram, 3_i64).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 13);
        assert_eq!(result[1], 30);
    }

    #[test]
    fn test_detect_circular_redirect() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "test_template": {
                    "inputs": {
                        "input": "output"
                    },
                    "outputs": ["output"],
                    "ops": {
                    }
                }
            },
            "start": "fork",
            "ops": {
                "fork": {
                    "type": "fork_clone",
                    "next": [
                        { "section": "input" },
                        { "builtin": "terminate" },
                    ]
                },
                "section": {
                    "type": "section",
                    "template": "test_template",
                    "connect": {
                        "output": { "section": "input" },
                    },
                }
            }
        }))
        .unwrap();

        let result = fixture.spawn_json_io_workflow(&diagram).unwrap_err();

        assert!(matches!(result.code, DiagramErrorCode::CircularRedirect(_)));
    }

    #[test]
    fn test_circular_template_dependency() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "recursive_template": {
                    "inputs": ["input"],
                    "outputs": ["output"],
                    "ops": {
                        "input": {
                            "type": "fork_clone",
                            "next": [
                                { "recursive_self": "input" },
                                "output",
                            ]
                        },
                        "recursive_self": {
                            "type": "section",
                            "template": "recursive_template",
                            "connect": {
                                "output": "output",
                            }
                        }
                    }
                }
            },
            "start": { "start": "input" },
            "ops": {
                "start": {
                    "type": "section",
                    "template": "recursive_template",
                    "connect": {
                        "output": { "builtin": "terminate" },
                    }
                }
            }
        }))
        .unwrap();

        let result = fixture.spawn_json_io_workflow(&diagram).unwrap_err();

        assert!(matches!(
            result.code,
            DiagramErrorCode::CircularTemplateDependency(_),
        ));

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "parent_template": {
                    "inputs": ["input"],
                    "outputs": ["output"],
                    "ops": {
                        "input": {
                            "type": "fork_clone",
                            "next": [
                                { "child": "input" },
                                "output",
                            ]
                        },
                        "child": {
                            "type": "section",
                            "template": "child_template",
                            "connect": {
                                "output": "output",
                            }
                        }
                    },
                },
                "child_template": {
                    "inputs": ["input"],
                    "outputs": ["output"],
                    "ops": {
                        "input": {
                            "type": "node",
                            "builder": "multiply3",
                            "next": "grandchild",
                        },
                        "grandchild": {
                            "type": "section",
                            "template": "grandchild_template",
                            "connect": {
                                "output": "output",
                            }
                        }
                    }
                },
                "grandchild_template": {
                    "inputs": ["input"],
                    "outputs": ["output"],
                    "ops": {
                        "input": {
                            "type": "section",
                            "template": "parent_template",
                            "connect": {
                                "output": "output",
                            },
                        },
                    },
                },
            },
            "start": { "start": "input" },
            "ops": {
                "start": {
                    "type": "section",
                    "template": "parent_template",
                    "connect": {
                        "output": { "builtin": "terminate" },
                    },
                },
            },
        }))
        .unwrap();

        let result = fixture.spawn_json_io_workflow(&diagram).unwrap_err();

        assert!(matches!(
            result.code,
            DiagramErrorCode::CircularTemplateDependency(_),
        ));
    }
}
