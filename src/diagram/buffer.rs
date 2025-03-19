use std::collections::{hash_map::Entry, HashMap};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    Accessor, AnyBuffer, BufferIdentifier, BufferMap, BufferSettings, Builder, InputSlot, Output,
};

use super::{
    type_info::TypeInfo, BuiltinTarget, Diagram, DiagramElementRegistry, DiagramErrorCode,
    DiagramOperation, DynOutput, MessageRegistry, NextOperation, OperationId, SerializationOptions,
    Vertex, WorkflowBuilder,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct BufferOp {
    #[serde(default)]
    pub(super) settings: BufferSettings,

    /// If true, messages will be serialized before sending into the buffer.
    pub(super) serialize: Option<bool>,
}

impl BufferOp {
    pub(super) fn add_vertices<'a, SerializationOptionsT>(
        &'a self,
        wf_builder: &mut WorkflowBuilder<'a, SerializationOptionsT>,
        op_id: String,
    ) where
        SerializationOptionsT: SerializationOptions,
    {
        wf_builder.add_vertex(op_id.clone(), move |vertex, builder, registry, buffers| {
            self.try_connect(vertex, builder, &registry.messages, &op_id, buffers)
        });
    }

    pub(super) fn try_connect<SerializationOptionsT>(
        &self,
        vertex: &Vertex,
        builder: &mut Builder,
        registry: &MessageRegistry<SerializationOptionsT>,
        op_id: &OperationId,
        buffers: &mut HashMap<OperationId, AnyBuffer>,
    ) -> Result<bool, DiagramErrorCode>
    where
        SerializationOptionsT: SerializationOptions,
    {
        if vertex.in_edges.is_empty() {
            // this will eventually cause workflow builder to return a [`DiagramErrorCode::IncompleteDiagram`] error.
            return Ok(false);
        }

        let first_output = vertex.in_edges[0].take_output();
        let first_output = if self.serialize.unwrap_or(false) {
            registry.serialize(builder, first_output)?.into()
        } else {
            first_output
        };

        let rest_outputs: Vec<DynOutput> = vertex.in_edges[1..]
            .iter()
            .map(|edge| -> Result<_, DiagramErrorCode> {
                let output = edge.take_output();
                if self.serialize.unwrap_or(false) {
                    Ok(registry.serialize(builder, output)?.into())
                } else {
                    Ok(output)
                }
            })
            .collect::<Result<_, _>>()?;

        // check that all inputs are the same type
        let expected_type = first_output.type_info;
        for output in &rest_outputs {
            if output.type_info != expected_type {
                return Err(DiagramErrorCode::TypeMismatch {
                    source_type: output.type_info,
                    target_type: expected_type,
                });
            }
        }

        // convert the first output into a buffer
        let buffer = first_output.into_any_buffer(builder, self.settings)?;
        let buffer = match buffers.entry(op_id.clone()) {
            Entry::Occupied(mut entry) => {
                entry.insert(buffer);
                entry.into_mut()
            }
            Entry::Vacant(entry) => entry.insert(buffer),
        };

        // connect the rest of the outputs to the buffer
        for output in rest_outputs {
            buffer.receive_output(builder, output)?;
        }

        Ok(true)
    }
}

/// if `target` has a value, return the request type of the operation, else, return the request type of `next`
pub(super) fn get_node_request_type<SerializationOptionsT>(
    target: &Option<NextOperation>,
    next: &NextOperation,
    diagram: &Diagram,
    registry: &DiagramElementRegistry<SerializationOptionsT>,
) -> Result<TypeInfo, DiagramErrorCode>
where
    SerializationOptionsT: SerializationOptions,
{
    let target_op = if let Some(op) = target { op } else { next };
    match target_op {
        NextOperation::Target(op_id) => match diagram.get_op(op_id)? {
            DiagramOperation::Node(op) => Ok(registry.get_node_registration(&op.builder)?.request),
            _ => Err(DiagramErrorCode::UnknownTarget),
        },
        NextOperation::Section { section, input } => {
            let reg = registry.get_section_registration(section)?;
            let input = reg.metadata.inputs.get(input).unwrap();
            Ok(input.message_type)
        }
        NextOperation::Builtin { builtin } => match builtin {
            BuiltinTarget::Terminate => Ok(TypeInfo::of::<serde_json::Value>()),
            BuiltinTarget::Dispose => Err(DiagramErrorCode::UnknownTarget),
        },
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", untagged)]
pub enum BufferInputs {
    Dict(HashMap<String, OperationId>),
    Array(Vec<OperationId>),
}

impl BufferInputs {
    /// Creates a [`BufferMap`] from the buffer inputs.
    /// Returns `None` if one or more buffer does not exist.
    pub(super) fn as_buffer_map(
        &self,
        buffers: &HashMap<OperationId, AnyBuffer>,
    ) -> Option<BufferMap> {
        match self {
            Self::Dict(mapping) => {
                let mut buffer_map = BufferMap::with_capacity(mapping.len());
                for (k, op_id) in mapping {
                    let buffer = if let Some(buffer) = buffers.get(op_id) {
                        buffer
                    } else {
                        return None;
                    };
                    buffer_map.insert(BufferIdentifier::Name(k.clone().into()), *buffer);
                }
                Some(buffer_map)
            }
            Self::Array(arr) => {
                let mut buffer_map = BufferMap::with_capacity(arr.len());
                for (i, op_id) in arr.into_iter().enumerate() {
                    let buffer = if let Some(buffer) = buffers.get(op_id) {
                        buffer
                    } else {
                        return None;
                    };
                    buffer_map.insert(BufferIdentifier::Index(i), *buffer);
                }
                Some(buffer_map)
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Dict(d) => d.is_empty(),
            Self::Array(a) => a.is_empty(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct BufferAccessOp {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,

    /// The id of an operation that this operation is for. The id must be a `node` operation. Optional if `next` is a node operation.
    pub(super) target_node: Option<NextOperation>,
}

impl BufferAccessOp {
    pub(super) fn add_vertices<'a, SerializationOptionsT>(
        &'a self,
        wf_builder: &mut WorkflowBuilder<'a, SerializationOptionsT>,
        op_id: String,
        diagram: &'a Diagram,
    ) where
        SerializationOptionsT: SerializationOptions,
    {
        wf_builder
            .add_vertex(op_id.clone(), move |vertex, builder, registry, buffers| {
                self.try_connect(vertex, builder, registry, buffers, diagram)
            })
            .add_output_edge(self.next.clone(), None);
    }

    pub(super) fn try_connect<SerializationOptionsT>(
        &self,
        vertex: &Vertex,
        builder: &mut Builder,
        registry: &DiagramElementRegistry<SerializationOptionsT>,
        buffers: &HashMap<OperationId, AnyBuffer>,
        diagram: &Diagram,
    ) -> Result<bool, DiagramErrorCode>
    where
        SerializationOptionsT: SerializationOptions,
    {
        let buffers = if let Some(buffers) = self.buffers.as_buffer_map(buffers) {
            buffers
        } else {
            return Ok(false);
        };

        let output = vertex.in_edges[0].take_output();

        let target_type = get_node_request_type(&self.target_node, &self.next, diagram, registry)?;
        let output =
            registry
                .messages
                .with_buffer_access(builder, output, &buffers, target_type)?;
        vertex.out_edges[0].set_output(output);
        Ok(true)
    }
}

pub trait BufferAccessRequest {
    type Message: Send + Sync + 'static;
    type BufferKeys: Accessor;
}

impl<T, B> BufferAccessRequest for (T, B)
where
    T: Send + Sync + 'static,
    B: Accessor,
{
    type Message = T;
    type BufferKeys = B;
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ListenOp {
    pub(super) next: NextOperation,

    /// Map of buffer keys and buffers.
    pub(super) buffers: BufferInputs,

    /// The id of an operation that this operation is for. The id must be a `node` operation. Optional if `next` is a node operation.
    pub(super) target_node: Option<NextOperation>,
}

impl ListenOp {
    pub(super) fn add_vertices<'a, SerializationOptionsT>(
        &'a self,
        wf_builder: &mut WorkflowBuilder<'a, SerializationOptionsT>,
        op_id: String,
        diagram: &'a Diagram,
    ) where
        SerializationOptionsT: SerializationOptions,
    {
        wf_builder
            .add_vertex(op_id.clone(), move |vertex, builder, registry, buffers| {
                self.try_connect(vertex, builder, registry, buffers, diagram)
            })
            .add_output_edge(self.next.clone(), None);
    }

    pub(super) fn try_connect<SerializationOptionsT>(
        &self,
        vertex: &Vertex,
        builder: &mut Builder,
        registry: &DiagramElementRegistry<SerializationOptionsT>,
        buffers: &HashMap<OperationId, AnyBuffer>,
        diagram: &Diagram,
    ) -> Result<bool, DiagramErrorCode>
    where
        SerializationOptionsT: SerializationOptions,
    {
        let buffers = if let Some(buffers) = self.buffers.as_buffer_map(buffers) {
            buffers
        } else {
            return Ok(false);
        };

        let target_type = get_node_request_type(&self.target_node, &self.next, diagram, registry)?;
        let output = registry.messages.listen(builder, &buffers, target_type)?;
        vertex.out_edges[0].set_output(output);
        Ok(true)
    }
}

trait ReceiveOutput {
    fn receive_output(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(), DiagramErrorCode>;
}

impl ReceiveOutput for AnyBuffer {
    fn receive_output(
        &self,
        builder: &mut Builder,
        output: DynOutput,
    ) -> Result<(), DiagramErrorCode> {
        if self.message_type_id() != output.type_info.type_id {
            return Err(DiagramErrorCode::TypeMismatch {
                source_type: TypeInfo {
                    type_id: self.message_type_id(),
                    type_name: self.message_type_name(),
                },
                target_type: output.type_info,
            });
        }
        // FIXME(koonpeng): This can potentially cause the workflow to run forever and never halt.
        // For now this works because neither of these operations creates or use any bevy Components.
        let input_slot = InputSlot::<()>::new(self.location.scope, self.location.source);
        let output = Output::<()>::new(output.scope(), output.id());
        builder.connect(output, input_slot);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bevy_ecs::system::In;
    use serde_json::json;

    use crate::{
        diagram::testing::DiagramTestFixture, BufferAccess, BufferKey, Diagram, DiagramErrorCode,
        IntoBlockingCallback, Node, NodeBuilderOptions,
    };

    /// create a new [`DiagramTestFixture`] with some extra builders.
    fn new_fixture() -> DiagramTestFixture {
        let mut fixture = DiagramTestFixture::new();

        fn num_output(_: serde_json::Value) -> i64 {
            1
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("num_output".to_string()),
            |builder, _config: ()| builder.create_map_block(num_output),
        );

        fn string_output(_: serde_json::Value) -> String {
            "hello".to_string()
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("string_output".to_string()),
            |builder, _config: ()| builder.create_map_block(string_output),
        );

        fixture
    }

    #[test]
    fn test_buffer_mismatch_type() {
        let mut fixture = new_fixture();
        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("join_i64"),
                |builder, _config: ()| builder.create_map_block(|i: Vec<i64>| i[0]),
            )
            .with_join();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "string_output",
            "ops": {
                "string_output": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "buffer",
                },
                "buffer": {
                    "type": "buffer",
                },
                "join": {
                    "type": "join",
                    "buffers": ["buffer"],
                    "target_node": "op1",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "join_i64",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::IncompatibleBuffers(_)),
            "{:#?}",
            err
        );
    }

    #[test]
    fn test_buffer_multiple_inputs() {
        let mut fixture = new_fixture();
        fixture
            .registry
            .opt_out()
            .no_request_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("wait_2_strings"),
                |builder, _config: ()| {
                    let n = builder.create_node(
                        (|In(req): In<Vec<BufferKey<String>>>, access: BufferAccess<String>| {
                            if access.get(&req[0]).unwrap().len() < 2 {
                                None
                            } else {
                                Some("hello world".to_string())
                            }
                        })
                        .into_blocking_callback(),
                    );
                    let output = n.output.chain(builder).dispose_on_none().output();
                    Node::<Vec<BufferKey<String>>, String> {
                        input: n.input,
                        output,
                        streams: n.streams,
                    }
                },
            )
            .with_listen();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["string_output", "string_output2"],
                },
                "string_output": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "buffer",
                },
                "string_output2": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "buffer",
                },
                "buffer": {
                    "type": "buffer",
                    "settings": {
                        "retention": "keep_all",
                    },
                },
                "listen": {
                    "type": "listen",
                    "buffers": ["buffer"],
                    "target_node": "wait_2_strings",
                    "next": "wait_2_strings",
                },
                "wait_2_strings": {
                    "type": "node",
                    "builder": "wait_2_strings",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_buffer_access() {
        let mut fixture = new_fixture();

        fixture
            .registry
            .opt_out()
            .no_request_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("with_buffer_access"),
                |builder, _config: ()| {
                    builder.create_map_block(|req: (i64, Vec<BufferKey<String>>)| req.0)
                },
            )
            .with_buffer_access();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fork_clone",
            "ops": {
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["num_output", "string_output"],
                },
                "num_output": {
                    "type": "node",
                    "builder": "num_output",
                    "next": "buffer_access",
                },
                "string_output": {
                    "type": "node",
                    "builder": "string_output",
                    "next": "string_buffer",
                },
                "string_buffer": {
                    "type": "buffer",
                },
                "buffer_access": {
                    "type": "buffer_access",
                    "buffers": ["string_buffer"],
                    "target_node": "with_buffer_access",
                    "next": "with_buffer_access",
                },
                "with_buffer_access": {
                    "type": "node",
                    "builder": "with_buffer_access",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_listen() {
        let mut fixture = new_fixture();

        fn listen_buffer(In(request): In<Vec<BufferKey<i64>>>, access: BufferAccess<i64>) -> usize {
            access.get(&request[0]).unwrap().len()
        }

        fixture
            .registry
            .opt_out()
            .no_request_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_buffer"),
                |builder, _config: ()| -> Node<Vec<BufferKey<i64>>, usize, ()> {
                    builder.create_node(listen_buffer.into_blocking_callback())
                },
            )
            .with_listen();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "num_output",
            "ops": {
                "buffer": {
                    "type": "buffer",
                },
                "num_output": {
                    "type": "node",
                    "builder": "num_output",
                    "next": "buffer",
                },
                "listen": {
                    "type": "listen",
                    "buffers": ["buffer"],
                    "target_node": "listen_buffer",
                    "next": "listen_buffer",
                },
                "listen_buffer": {
                    "type": "node",
                    "builder": "listen_buffer",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::Null)
            .unwrap();
        assert_eq!(result, 1);
    }
}
