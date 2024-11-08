use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::Display,
    io::Read,
};

use bevy_app::App;
use bevy_impulse::{Builder, Scope, Service, SpawnWorkflowExt, StreamPack};
use serde::{Deserialize, Serialize};

use crate::{DynInputSlot, DynNode, DynOutput, NodeRegistration, NodeRegistry};

pub type NodeId = String;
pub type VertexId = String;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Blueprint {
    #[serde(flatten)]
    vertices: HashMap<String, Vertex>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Vertex {
    edges: Vec<String>,

    #[serde(flatten)]
    desc: VertexDescription,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum VertexDescription {
    Start,
    Terminate,
    #[serde(rename_all = "camelCase")]
    Node {
        node_id: NodeId,
    },
}

impl Blueprint {
    pub fn spawn_workflow<Streams>(
        &self,
        app: &mut App,
        registry: &NodeRegistry,
    ) -> Result<Service<serde_json::Value, serde_json::Value, Streams>, BlueprintError>
    where
        Streams: StreamPack,
    {
        let (start_id, _terminate_id, node_vertices_ids) = self.validate(registry)?;
        let start = self.get_vertex(start_id)?;

        let w = app.world.spawn_workflow(
            |scope: Scope<serde_json::Value, serde_json::Value, Streams>, builder: &mut Builder| {
                let mut dyn_builder = DynWorkflowBuilder { builder };

                // nodes and outputs cannot be cloned, but input can be cloned, so we
                // first create all the nodes, store them in a map, then store the inputs
                // in another map. `connect` takes ownership of input and output so we must pop/take
                // from the nodes map but we can borrow and clone the inputs.
                let mut nodes: HashMap<&VertexId, DynNode> = self
                    .vertices
                    .values()
                    .filter_map(|v| match &v.desc {
                        VertexDescription::Node { node_id } => Some(node_id),
                        _ => None,
                    })
                    .map(|node_id| {
                        (
                            node_id,
                            (registry.get_registration(node_id).unwrap().create_node)(
                                dyn_builder.builder,
                            ),
                        )
                    })
                    .collect();
                let node_inputs: HashMap<&VertexId, DynInputSlot> =
                    nodes.iter().map(|(k, v)| (*k, v.input.clone())).collect();

                // connect the start vertex
                let target_inputs: Vec<DynInputSlot> = start
                    .edges
                    .iter()
                    .map(|target_vertex_id| {
                        let target_vertex = self.vertices.get(target_vertex_id).unwrap();
                        match &target_vertex.desc {
                            VertexDescription::Node {
                                node_id: target_node_id,
                            } => {
                                let target_registration =
                                    registry.get_registration(target_node_id).unwrap();
                                let receiver =
                                    (target_registration.create_receiver)(dyn_builder.builder);
                                let target_input = node_inputs.get(target_node_id).unwrap();
                                dyn_builder.connect(receiver.output, target_input.clone());
                                receiver.input
                            }
                            VertexDescription::Terminate => scope.terminate.into(),
                            _ => panic!(),
                        }
                    })
                    .collect();

                if target_inputs.len() == 1 {
                    dyn_builder.connect(scope.input.into(), *target_inputs.get(0).unwrap());
                } else {
                    dyn_builder.connect_fork(scope.input.into(), target_inputs);
                }

                // connect other vertices
                for vertex_id in node_vertices_ids {
                    let vertex = self.get_vertex(vertex_id).unwrap();
                    let source_node_id = match &vertex.desc {
                        VertexDescription::Node { node_id } => node_id,
                        _ => panic!("expected node vertex"),
                    };
                    let source_node = nodes.remove(source_node_id).unwrap();

                    let target_inputs: Vec<DynInputSlot> = vertex
                        .edges
                        .iter()
                        .map(|target_vertex_id| {
                            let target_vertex = self.vertices.get(target_vertex_id).unwrap();
                            match &target_vertex.desc {
                                VertexDescription::Node {
                                    node_id: target_node_id,
                                } => {
                                    let target_node = nodes.get(target_node_id).unwrap();
                                    target_node.input.clone()
                                }
                                VertexDescription::Terminate => {
                                    let source_registration =
                                        registry.get_registration(source_node_id).unwrap();
                                    let sender =
                                        (source_registration.create_sender)(dyn_builder.builder);
                                    dyn_builder.connect(sender.output, scope.terminate.into());
                                    sender.input
                                }
                                _ => panic!("cannot connect to start"),
                            }
                        })
                        .collect();

                    if target_inputs.len() == 1 {
                        dyn_builder
                            .connect(source_node.output, target_inputs.get(0).unwrap().clone());
                    } else {
                        dyn_builder.connect_fork(source_node.output, target_inputs);
                    }
                }
            },
        );

        Ok(w)
    }

    pub fn spawn_io_workflow(
        &self,
        app: &mut App,
        registry: &NodeRegistry,
    ) -> Result<Service<serde_json::Value, serde_json::Value>, BlueprintError> {
        self.spawn_workflow::<()>(app, registry)
    }

    pub fn from_json_str(s: &str) -> Result<Self, BlueprintError> {
        Ok(serde_json::from_str(s)?)
    }

    pub fn from_reader<R>(r: R) -> Result<Self, BlueprintError>
    where
        R: Read,
    {
        Ok(serde_json::from_reader(r)?)
    }

    /// Checks that the workflow is valid, returns the start and terminate vertices.
    ///
    /// More formally, it checks that
    /// 1. All nodes used are registered.
    /// 2. There is a path from start to terminate.
    /// 3. The terminate vertex is no edges to other vertices.
    /// 4. There are no edges into the start vertex.
    /// 5. All the connections are valid (their request and response type matches).
    fn validate(
        &self,
        registry: &NodeRegistry,
    ) -> Result<(&VertexId, &VertexId, Vec<&VertexId>), BlueprintError> {
        let mut start = None;
        let mut terminate = None;
        let mut nodes = Vec::with_capacity(self.vertices.len());

        for (id, v) in self.vertices.iter() {
            match &v.desc {
                VertexDescription::Start => {
                    if start.is_some() {
                        return Err(BlueprintError::MultipleStartTerminate);
                    }
                    self.validate_start(id, registry)?;
                    start = Some(id);
                }
                VertexDescription::Terminate => {
                    if terminate.is_some() {
                        return Err(BlueprintError::MultipleStartTerminate);
                    }
                    self.validate_terminate(v)?;
                    terminate = Some(id);
                }
                VertexDescription::Node { node_id } => {
                    self.validate_node(id, &node_id, registry)?;
                    nodes.push(id);
                }
            }
        }

        if let (Some(start), Some(terminate)) = (start, terminate) {
            self.check_connected(start, terminate)?;
            return Ok((start, terminate, nodes));
        }
        Err(BlueprintError::DisconnectedGraph)
    }

    /// Checks if there is a path from start to terminate
    fn check_connected(
        &self,
        start_id: &VertexId,
        terminate_id: &VertexId,
    ) -> Result<(), BlueprintError> {
        let mut visited: HashSet<&VertexId> = HashSet::new();
        let mut search: Vec<&VertexId> = vec![&start_id];

        while let Some(vertex_id) = search.pop() {
            let v = self.get_vertex(vertex_id)?;
            if vertex_id == terminate_id {
                return Ok(());
            }

            visited.insert(&vertex_id);
            for e in &v.edges {
                if visited.insert(e) {
                    search.push(e);
                }
            }
        }

        Err(BlueprintError::DisconnectedGraph)
    }

    fn get_vertex(&self, id: &VertexId) -> Result<&Vertex, BlueprintError> {
        self.vertices
            .get(id)
            .ok_or_else(|| BlueprintError::VertexNotFound(id.clone()))
    }

    fn get_node_registration<'a>(
        id: &NodeId,
        registry: &'a NodeRegistry,
    ) -> Result<&'a NodeRegistration, BlueprintError> {
        registry
            .get_registration(id)
            .ok_or_else(|| BlueprintError::NodeNotFound(id.clone()))
    }

    fn validate_start(
        &self,
        start_id: &VertexId,
        registry: &NodeRegistry,
    ) -> Result<(), BlueprintError> {
        let start = self.get_vertex(start_id)?;
        for e in &start.edges {
            let target_vertex = self.get_vertex(e)?;
            match &target_vertex.desc {
                VertexDescription::Start => return Err(BlueprintError::MultipleStartTerminate),
                VertexDescription::Node { node_id } => {
                    let target_node = Self::get_node_registration(&node_id, registry)?;
                    if !target_node.metadata.request.serializable {
                        return Err(BlueprintError::UnserializableEdge(start_id.clone()));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_terminate(&self, terminate: &Vertex) -> Result<(), BlueprintError> {
        if !terminate.edges.is_empty() {
            return Err(BlueprintError::InvalidTerminateEdges);
        }
        Ok(())
    }

    fn validate_node(
        &self,
        vertex_id: &VertexId,
        node_id: &NodeId,
        registry: &NodeRegistry,
    ) -> Result<(), BlueprintError> {
        let vertex = self.get_vertex(vertex_id)?;
        for target_vertex_id in &vertex.edges {
            let target_vertex = self.get_vertex(target_vertex_id)?;
            // let source_node = Self::get_node_registration(s, registry)
            match &target_vertex.desc {
                VertexDescription::Start => return Err(BlueprintError::InvalidStartEdges),
                VertexDescription::Terminate => {
                    let target_node = Self::get_node_registration(&node_id, registry)?;
                    if !target_node.metadata.response.serializable {
                        return Err(BlueprintError::UnserializableEdge(vertex_id.clone()));
                    }
                }
                VertexDescription::Node {
                    node_id: target_node_id,
                } => {
                    let source_node = Self::get_node_registration(&node_id, registry)?;
                    let target_node = Self::get_node_registration(&target_node_id, registry)?;
                    let output_type = &source_node.metadata.response.r#type;
                    let input_type = &target_node.metadata.request.r#type;
                    if output_type != input_type {
                        return Err(BlueprintError::TypeMismatch(TypeMismatch {
                            source_id: vertex_id.clone(),
                            target_id: target_vertex_id.clone(),
                            output_type: output_type.clone(),
                            input_type: input_type.clone(),
                        }));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TypeMismatch {
    source_id: VertexId,
    target_id: VertexId,
    output_type: String,
    input_type: String,
}

#[derive(Debug)]
pub enum BlueprintError {
    TypeMismatch(TypeMismatch),
    NodeNotFound(NodeId),
    VertexNotFound(VertexId),
    MultipleStartTerminate,
    InvalidStartEdges,
    InvalidTerminateEdges,
    DisconnectedGraph,
    UnserializableEdge(VertexId),
    JsonError(serde_json::Error),
}

impl Display for BlueprintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TypeMismatch(data) => write!(f,
                "cannot connect [{}] to [{}], output type [{}] does not match input type [{}]",
                data.source_id, data.target_id, data.output_type, data.input_type
            ),
            Self::NodeNotFound(node_id) => {
                write!(f, "node [{}] not is not registered", node_id)
            }
            Self::VertexNotFound(node_id) => {
                write!(f, "vertex [{}] is not found", node_id)
            }
            Self::MultipleStartTerminate => {
                f.write_str("there must be exactly one start and terminate")
            }
            Self::InvalidStartEdges => f.write_str("[start] cannot receive inputs"),
            Self::InvalidTerminateEdges => f.write_str("[terminate] cannot send any outputs"),
            Self::DisconnectedGraph => f.write_str("No path reaches terminate"),
            Self::UnserializableEdge(vertex_id) => write!(f, "[{}] is connected to start or terminate but it's request or response cannot be serialized", vertex_id),
            Self::JsonError(err) => err.fmt(f),
        }
    }
}

impl Error for BlueprintError {}

impl From<serde_json::Error> for BlueprintError {
    fn from(json_err: serde_json::Error) -> Self {
        Self::JsonError(json_err)
    }
}

pub struct DynWorkflowBuilder<'b, 'w, 's, 'a> {
    builder: &'b mut Builder<'w, 's, 'a>,
}

impl<'b, 'w, 's, 'a> DynWorkflowBuilder<'b, 'w, 's, 'a> {
    fn connect(&mut self, output: DynOutput, input: DynInputSlot) {
        self.builder
            .connect(output.into_output::<()>(), input.into_input::<()>());
    }

    fn connect_fork<InputIter>(&mut self, output: DynOutput, inputs: InputIter)
    where
        InputIter: IntoIterator<Item = DynInputSlot>,
    {
        let fork = output.into_output::<()>().fork_clone(self.builder);
        for input in inputs {
            let cloned_output = fork.clone_output(self.builder);
            self.connect(cloned_output.into(), input);
        }
    }
}

#[cfg(test)]
mod tests {
    use bevy_impulse::{testing::TestingContext, RequestExt};

    use super::*;

    fn multiply3(i: i64) -> i64 {
        i * 3
    }

    struct Unserializable;

    fn opaque(_: Unserializable) -> Unserializable {
        Unserializable {}
    }

    fn opaque_request(_: Unserializable) {}

    fn opaque_response(_: i64) -> Unserializable {
        Unserializable {}
    }

    /// create a new node registry with some basic nodes registered
    fn new_registry_with_basic_nodes() -> NodeRegistry {
        let mut registry = NodeRegistry::default();
        registry.register_node("multiply3", "multiply3", |builder| {
            builder.create_map_block(multiply3)
        });
        registry.register_opaque_node("opaque", "opaque", |builder| {
            builder.create_map_block(opaque)
        });
        registry.register_opaque_request_node("opaque_request", "opaque_request", |builder| {
            builder.create_map_block(opaque_request)
        });
        registry.register_opaque_response_node("opaque_response", "opaque_response", |builder| {
            builder.create_map_block(opaque_response)
        });
        registry
    }

    #[test]
    fn test_validate_blueprint_no_terminate() {
        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "start".to_string(),
                    Vertex {
                        edges: vec!["multiply3".to_string()],
                        desc: VertexDescription::Start,
                    },
                ),
                (
                    "multiply3".to_string(),
                    Vertex {
                        edges: vec![],
                        desc: VertexDescription::Node {
                            node_id: "multiply3".to_string(),
                        },
                    },
                ),
            ]),
        };

        let registry = new_registry_with_basic_nodes();
        assert!(bp
            .validate(&registry)
            .is_err_and(|err| matches!(err, BlueprintError::DisconnectedGraph)));
    }

    #[test]
    fn test_validate_blueprint_no_start() {
        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "multiply3".to_string(),
                    Vertex {
                        edges: vec!["terminate".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "multiply3".to_string(),
                        },
                    },
                ),
                (
                    "terminate".to_string(),
                    Vertex {
                        edges: vec![],
                        desc: VertexDescription::Terminate,
                    },
                ),
            ]),
        };

        let registry = new_registry_with_basic_nodes();
        assert!(bp
            .validate(&registry)
            .is_err_and(|err| matches!(err, BlueprintError::DisconnectedGraph)));
    }

    #[test]
    fn test_validate_blueprint_extra_terminate_edges() {
        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "start".to_string(),
                    Vertex {
                        edges: vec!["multiply3".to_string()],
                        desc: VertexDescription::Start,
                    },
                ),
                (
                    "multiply3".to_string(),
                    Vertex {
                        edges: vec!["terminate".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "multiply3".to_string(),
                        },
                    },
                ),
                (
                    "terminate".to_string(),
                    Vertex {
                        edges: vec!["multiply3".to_string()],
                        desc: VertexDescription::Terminate,
                    },
                ),
            ]),
        };

        let registry = new_registry_with_basic_nodes();
        assert!(bp
            .validate(&registry)
            .is_err_and(|err| matches!(err, BlueprintError::InvalidTerminateEdges)));
    }

    #[test]
    fn test_validate_blueprint_connect_to_start() {
        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "start".to_string(),
                    Vertex {
                        edges: vec!["multiply3".to_string()],
                        desc: VertexDescription::Start,
                    },
                ),
                (
                    "multiply3".to_string(),
                    Vertex {
                        edges: vec!["start".to_string(), "terminate".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "multiply3".to_string(),
                        },
                    },
                ),
                (
                    "terminate".to_string(),
                    Vertex {
                        edges: vec![],
                        desc: VertexDescription::Terminate,
                    },
                ),
            ]),
        };

        let registry = new_registry_with_basic_nodes();
        assert!(bp
            .validate(&registry)
            .is_err_and(|err| matches!(err, BlueprintError::InvalidStartEdges)));
    }

    #[test]
    fn test_validate_blueprint_unserializable_start() {
        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "start".to_string(),
                    Vertex {
                        edges: vec!["opaque_request".to_string()],
                        desc: VertexDescription::Start,
                    },
                ),
                (
                    "opaque_request".to_string(),
                    Vertex {
                        edges: vec!["terminate".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "opaque_request".to_string(),
                        },
                    },
                ),
                (
                    "terminate".to_string(),
                    Vertex {
                        edges: vec![],
                        desc: VertexDescription::Terminate,
                    },
                ),
            ]),
        };

        let registry = new_registry_with_basic_nodes();
        assert!(bp
            .validate(&registry)
            .is_err_and(|err| matches!(err, BlueprintError::UnserializableEdge(_))));
    }

    #[test]
    fn test_validate_blueprint_unserializable_terminate() {
        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "start".to_string(),
                    Vertex {
                        edges: vec!["opaque_response".to_string()],
                        desc: VertexDescription::Start,
                    },
                ),
                (
                    "opaque_response".to_string(),
                    Vertex {
                        edges: vec!["terminate".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "opaque_response".to_string(),
                        },
                    },
                ),
                (
                    "terminate".to_string(),
                    Vertex {
                        edges: vec![],
                        desc: VertexDescription::Terminate,
                    },
                ),
            ]),
        };

        let registry = new_registry_with_basic_nodes();
        assert!(bp
            .validate(&registry)
            .is_err_and(|err| matches!(err, BlueprintError::UnserializableEdge(_))));
    }

    #[test]
    fn test_validate_blueprint_mismatch_types() {
        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "start".to_string(),
                    Vertex {
                        edges: vec!["multiply3".to_string()],
                        desc: VertexDescription::Start,
                    },
                ),
                (
                    "multiply3".to_string(),
                    Vertex {
                        edges: vec!["opaque_request".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "multiply3".to_string(),
                        },
                    },
                ),
                (
                    "opaque_request".to_string(),
                    Vertex {
                        edges: vec!["terminate".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "opaque_request".to_string(),
                        },
                    },
                ),
                (
                    "terminate".to_string(),
                    Vertex {
                        edges: vec![],
                        desc: VertexDescription::Terminate,
                    },
                ),
            ]),
        };

        let registry = new_registry_with_basic_nodes();
        assert!(bp
            .validate(&registry)
            .is_err_and(|err| matches!(err, BlueprintError::TypeMismatch(_))));
    }

    #[test]
    fn test_validate_blueprint_disconnected() {
        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "start".to_string(),
                    Vertex {
                        edges: vec!["multiply3".to_string()],
                        desc: VertexDescription::Start,
                    },
                ),
                (
                    "multiply3".to_string(),
                    Vertex {
                        edges: vec!["multiply3_2".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "multiply3".to_string(),
                        },
                    },
                ),
                (
                    "multiply3_2".to_string(),
                    Vertex {
                        edges: vec!["multiply3".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "multiply3".to_string(),
                        },
                    },
                ),
                (
                    "terminate".to_string(),
                    Vertex {
                        edges: vec![],
                        desc: VertexDescription::Terminate,
                    },
                ),
            ]),
        };

        let registry = new_registry_with_basic_nodes();
        assert!(bp
            .validate(&registry)
            .is_err_and(|err| matches!(err, BlueprintError::DisconnectedGraph)));
    }

    #[test]
    fn test_run_blueprint() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let bp = Blueprint {
            vertices: HashMap::from([
                (
                    "start".to_string(),
                    Vertex {
                        edges: vec!["multiply3".to_string()],
                        desc: VertexDescription::Start,
                    },
                ),
                (
                    "multiply3".to_string(),
                    Vertex {
                        edges: vec!["terminate".to_string()],
                        desc: VertexDescription::Node {
                            node_id: "multiply3".to_string(),
                        },
                    },
                ),
                (
                    "terminate".to_string(),
                    Vertex {
                        edges: vec![],
                        desc: VertexDescription::Terminate,
                    },
                ),
            ]),
        };

        let workflow = bp.spawn_io_workflow(&mut context.app, &registry).unwrap();

        let mut promise = context.command(|cmds| {
            cmds.request(serde_json::Value::from(4), workflow)
                .take_response()
        });
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 12);
    }

    #[test]
    fn test_run_serialized_blueprint() {
        let registry = new_registry_with_basic_nodes();
        let mut context = TestingContext::minimal_plugins();

        let json_str = r#"
        {
            "start": {
                "type": "start",
                "edges": ["multiply3"]
            },
            "multiply3": {
                "type": "node",
                "nodeId": "multiply3",
                "edges": ["terminate"]
            },
            "terminate": {
                "type": "terminate",
                "edges": []
            }
        }
        "#;

        let bp = Blueprint::from_json_str(json_str).unwrap();
        let workflow = bp.spawn_io_workflow(&mut context.app, &registry).unwrap();

        let mut promise = context.command(|cmds| {
            cmds.request(serde_json::Value::from(4), workflow)
                .take_response()
        });
        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available().unwrap(), 12);
    }
}
