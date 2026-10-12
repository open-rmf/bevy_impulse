use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{self, Response},
};
#[cfg(feature = "router")]
use axum::{Router, routing::post};
#[cfg(feature = "router")]
use axum::{
    extract::ws,
    routing::{self},
};
use bevy_ecs::{prelude::Entity, schedule::IntoScheduleConfigs};
#[cfg(feature = "router")]
use crossflow::TracedEventKind;
use crossflow::{
    Diagram, DiagramElementRegistry, DiagramError, DiagramErrorCode, DiagramOperation,
    InferenceBoundaryConditions, MetadataAccess, Outcome, PortRef, RequestExt, TracedEvent, trace,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::error;
#[cfg(feature = "router")]
use tracing::warn;

#[cfg(feature = "router")]
use super::websocket::{WebsocketSinkExt, WebsocketStreamExt};
use crate::api::error_responses::WorkflowCancelledResponse;

#[cfg(feature = "router")]
type BroadcastRecvError = tokio::sync::broadcast::error::RecvError;

type WorkflowResponseResult =
    Result<(Outcome<serde_json::Value>, Entity), Box<dyn Error + Send + Sync>>;
type WorkflowResponseSender = tokio::sync::oneshot::Sender<WorkflowResponseResult>;

type WorkflowFeedback = TracedEvent;

#[derive(bevy_ecs::component::Component)]
struct FeedbackSender(tokio::sync::broadcast::Sender<WorkflowFeedback>);

pub struct Context {
    diagram: Diagram,
    request: serde_json::Value,
    registry: Arc<Mutex<DiagramElementRegistry>>,
    response_tx: WorkflowResponseSender,
    feedback_tx: Option<FeedbackSender>,
}

#[derive(Clone)]
pub struct ExecutorState {
    pub registry: Arc<Mutex<DiagramElementRegistry>>,
    pub send_chan: tokio::sync::mpsc::Sender<Context>,
    pub despawn_chan: tokio::sync::mpsc::Sender<Entity>,
    pub response_timeout: Duration,
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Serialize))]
#[derive(Deserialize)]
pub struct PostRunRequest {
    pub diagram: Diagram,
    pub request: serde_json::Value,
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityRequest {
    pub diagram: Diagram,
    pub connections: Vec<CompatibilityConnection>,
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityConnection {
    pub id: String,
    #[serde(default)]
    pub focus_ports: Vec<PortRef>,
    #[serde(default)]
    pub source_port: Option<PortRef>,
    #[serde(default)]
    pub target_port: Option<PortRef>,
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityResponse {
    pub results: Vec<CompatibilityResult>,
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompatibilityResult {
    pub id: String,
    pub status: CompatibilityStatus,
    pub reason: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub provisional: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<String>,
}

impl CompatibilityResult {
    fn terminal(id: String, status: CompatibilityStatus, reason: impl Into<String>) -> Self {
        Self {
            id,
            status,
            reason: reason.into(),
            provisional: false,
            source_type: None,
            target_type: None,
        }
    }

    fn with_types(
        id: String,
        status: CompatibilityStatus,
        reason: impl Into<String>,
        source_type: Option<String>,
        target_type: Option<String>,
    ) -> Self {
        Self {
            id,
            status,
            reason: reason.into(),
            provisional: false,
            source_type,
            target_type,
        }
    }

    fn provisional_with_types(
        id: String,
        reason: impl Into<String>,
        source_type: Option<String>,
        target_type: Option<String>,
    ) -> Self {
        Self {
            provisional: true,
            ..Self::with_types(
                id,
                CompatibilityStatus::Unknown,
                reason,
                source_type,
                target_type,
            )
        }
    }
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum CompatibilityStatus {
    Compatible,
    Incompatible,
    Unknown,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Sends a request to the executor system and wait for the response.
pub async fn post_run(
    state: State<ExecutorState>,
    Json(body): Json<PostRunRequest>,
) -> response::Result<Json<serde_json::Value>> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    if let Err(err) = state
        .send_chan
        .send(Context {
            registry: state.registry.clone(),
            diagram: body.diagram,
            request: body.request,
            response_tx,
            feedback_tx: None,
        })
        .await
    {
        error!("{}", err);
        return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
    }

    let workflow_response = match response_rx.await {
        Ok(response) => response,
        Err(err) => {
            error!("{}", err);
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
        }
    };

    let response = (match workflow_response {
        Ok((outcome, workflow)) => {
            let result = outcome.await;
            if let Err(err) = state.despawn_chan.send(workflow).await {
                error!("Failed to request workflow despawn: {err}");
            }

            match result {
                Ok(response) => Ok(response),
                Err(err) => Err(WorkflowCancelledResponse(&err).into()),
            }
        }
        Err(err) => Err(Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .body(err.to_string())
            .map_or(StatusCode::INTERNAL_SERVER_ERROR.into(), |resp| resp.into())),
    } as response::Result<serde_json::Value>)?;

    Ok(Json(response))
}

pub async fn post_compatibility(
    state: State<ExecutorState>,
    Json(body): Json<CompatibilityRequest>,
) -> response::Result<Json<CompatibilityResponse>> {
    let registry = state.registry.lock().map_err(|err| {
        error!("failed to lock registry for compatibility check: {err}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(check_connections(&registry, body)))
}

impl CompatibilityConnection {
    fn ports(&self) -> impl Iterator<Item = &PortRef> {
        self.focus_ports
            .iter()
            .chain(self.source_port.iter())
            .chain(self.target_port.iter())
    }
}

fn check_connections(
    registry: &DiagramElementRegistry,
    request: CompatibilityRequest,
) -> CompatibilityResponse {
    let inference =
        InferenceBoundaryConditions::json_messages(registry, root_stream_names(&request.diagram))
            .map_err(DiagramError::from)
            .and_then(|boundary| {
                request.diagram.infer_message_types_for_ports_individually(
                    registry,
                    boundary,
                    request
                        .connections
                        .iter()
                        .flat_map(|connection| connection.ports().cloned()),
                )
            });
    let results = request
        .connections
        .into_iter()
        .map(|connection| match &inference {
            Ok(inference) => check_connection(registry, connection, inference),
            Err(error) => CompatibilityResult::terminal(
                connection.id,
                CompatibilityStatus::Unknown,
                error.to_string(),
            ),
        })
        .collect();
    CompatibilityResponse { results }
}

fn check_connection(
    registry: &DiagramElementRegistry,
    connection: CompatibilityConnection,
    inference: &HashMap<PortRef, Vec<Result<(usize, bool), DiagramError>>>,
) -> CompatibilityResult {
    let mut focus_ports: Vec<_> = connection.ports().collect();
    focus_ports.sort();
    focus_ports.dedup();
    if focus_ports.is_empty() {
        return CompatibilityResult::terminal(
            connection.id,
            CompatibilityStatus::Unknown,
            "No ports were provided for compatibility checking",
        );
    }
    let mut unknown = None;
    for inferred in focus_ports.iter().flat_map(|port| &inference[*port]) {
        match inferred {
            Err(err) => {
                if matches!(
                    &err.code,
                    DiagramErrorCode::NotCloneable(_)
                        | DiagramErrorCode::NotUnzippable(_)
                        | DiagramErrorCode::InvalidUnzip { .. }
                        | DiagramErrorCode::CannotForkResult(_)
                        | DiagramErrorCode::NotSplittable(_)
                        | DiagramErrorCode::NotJoinable(_)
                        | DiagramErrorCode::CannotAccessBuffers(_)
                        | DiagramErrorCode::CannotListen(_)
                        | DiagramErrorCode::IncompatibleBufferType { .. }
                        | DiagramErrorCode::IncompatibleBuffers(_)
                        | DiagramErrorCode::EmptyJoin
                        | DiagramErrorCode::UnknownJoinField { .. }
                        | DiagramErrorCode::InvalidOperation(_)
                ) {
                    return CompatibilityResult::terminal(
                        connection.id,
                        CompatibilityStatus::Incompatible,
                        err.to_string(),
                    );
                }
                let provisional = is_missing_context_error(err);
                let reason = if provisional {
                    format!("Connection needs more type context: {err}")
                } else {
                    err.to_string()
                };
                unknown.get_or_insert((reason, provisional));
            }
            Ok((_, false)) => {
                unknown.get_or_insert((
                    "Inferred message types still depend on unresolved ports".to_string(),
                    true,
                ));
            }
            Ok(_) => {}
        }
    }

    let port_types = |port: &Option<PortRef>| {
        port.iter()
            .flat_map(|port| &inference[port])
            .filter_map(|result| result.as_ref().ok().copied())
            .collect::<Vec<_>>()
    };
    let source_types = port_types(&connection.source_port);
    let target_types = port_types(&connection.target_port);
    let type_name = |message_type| {
        registry
            .message_type_name(message_type)
            .ok()
            .map(ToOwned::to_owned)
    };

    // A section output can forward several internal producers. Check each real
    // type; an unresolved producer must not hide a proven mismatch in another.
    let mut reason = "All source message types can be delivered to the target".to_string();
    for &(source_type, source_certain) in &source_types {
        for &(target_type, target_certain) in &target_types {
            if !source_certain || !target_certain {
                continue;
            }
            match can_connect_message_types(registry, source_type, target_type) {
                Ok(Some(message)) => {
                    if source_types.len() == 1 && target_types.len() == 1 {
                        reason = message;
                    }
                }
                Ok(None) => {
                    let source_type_name = type_name(source_type);
                    let target_type_name = type_name(target_type);
                    return CompatibilityResult::with_types(
                        connection.id,
                        CompatibilityStatus::Incompatible,
                        format!(
                            "{} cannot be delivered to {}",
                            source_type_name
                                .as_deref()
                                .unwrap_or("[unknown source type]"),
                            target_type_name
                                .as_deref()
                                .unwrap_or("[unknown target type]"),
                        ),
                        source_type_name,
                        target_type_name,
                    );
                }
                Err(error) => {
                    unknown.get_or_insert((error.to_string(), false));
                }
            }
        }
    }

    let type_names = |types: &[(usize, bool)]| {
        let mut names: Vec<_> = types.iter().filter_map(|(ty, _)| type_name(*ty)).collect();
        names.sort();
        names.dedup();
        (!names.is_empty()).then(|| names.join(" | "))
    };
    let source_type_name = type_names(&source_types);
    let target_type_name = type_names(&target_types);
    if let Some((reason, provisional)) = unknown {
        return CompatibilityResult {
            provisional,
            ..CompatibilityResult::with_types(
                connection.id,
                CompatibilityStatus::Unknown,
                reason,
                source_type_name,
                target_type_name,
            )
        };
    }

    if connection.source_port.is_none() || connection.target_port.is_none() {
        let provisional = connection.source_port.is_some() || connection.target_port.is_some();
        if provisional {
            return CompatibilityResult::provisional_with_types(
                connection.id,
                "Focused ports can be inferred, but the connection needs more peer type context",
                source_type_name,
                target_type_name,
            );
        }

        return CompatibilityResult::with_types(
            connection.id,
            CompatibilityStatus::Compatible,
            "Focused ports can be inferred",
            source_type_name,
            target_type_name,
        );
    }
    CompatibilityResult::with_types(
        connection.id,
        CompatibilityStatus::Compatible,
        reason,
        source_type_name,
        target_type_name,
    )
}

fn is_missing_context_error(error: &DiagramError) -> bool {
    matches!(
        &error.code,
        DiagramErrorCode::CannotInferType(_)
            | DiagramErrorCode::NoConnection(_)
            | DiagramErrorCode::UnknownPort(_)
    )
}

fn can_connect_message_types(
    registry: &DiagramElementRegistry,
    source_type: usize,
    target_type: usize,
) -> Result<Option<String>, DiagramErrorCode> {
    if source_type == target_type {
        return Ok(Some("Message types match exactly".to_string()));
    }

    if registry.can_convert(source_type, target_type)? {
        return Ok(Some(
            "Registered message conversion is available".to_string(),
        ));
    }

    if registry
        .json_message_index()
        .is_ok_and(|json_type| target_type == json_type)
        && registry.can_seralize(source_type)?
    {
        return Ok(Some(
            "Source can be implicitly serialized to JSON".to_string(),
        ));
    }

    if registry
        .json_message_index()
        .is_ok_and(|json_type| source_type == json_type)
        && registry.can_deserialize(target_type)?
    {
        return Ok(Some(
            "JSON can be implicitly deserialized for the target".to_string(),
        ));
    }

    if registry
        .script_message_index()
        .is_ok_and(|script_type| target_type == script_type)
        && registry.into_script_message(source_type)?
    {
        return Ok(Some(
            "Source can be implicitly converted to ScriptMessage".to_string(),
        ));
    }

    if registry
        .script_message_index()
        .is_ok_and(|script_type| source_type == script_type)
        && registry.from_script_message(target_type)?
    {
        return Ok(Some(
            "ScriptMessage can be implicitly converted for the target".to_string(),
        ));
    }

    Ok(None)
}

fn root_stream_names(diagram: &Diagram) -> Vec<String> {
    diagram
        .ops
        .values()
        .filter_map(|op| match op.as_ref() {
            DiagramOperation::StreamOut(stream_out) => Some(stream_out.name.to_string()),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;
    use CompatibilityStatus::{Compatible, Incompatible, Unknown};
    use crossflow::{
        Blocking, BufferAccess, BufferKey, Builder, IntoCallback, JsonMessage, NextOperation, Node,
        NodeBuilderOptions, OperationRef, output_ref,
    };
    use serde_json::json;

    fn check_compatibility_request(
        registry: &DiagramElementRegistry,
        request: CompatibilityRequest,
    ) -> CompatibilityResult {
        check_connections(registry, request)
            .results
            .into_iter()
            .next()
            .unwrap()
    }

    fn input_port(name: &str) -> PortRef {
        (&NextOperation::Name(name.into())).into()
    }

    fn output_port(name: &str) -> PortRef {
        output_ref(&name.into()).next().into()
    }

    fn connection(id: &str, source: PortRef, target: PortRef) -> CompatibilityConnection {
        CompatibilityConnection {
            id: id.into(),
            focus_ports: vec![],
            source_port: Some(source),
            target_port: Some(target),
        }
    }

    fn assert_status(
        result: &CompatibilityResult,
        expected: CompatibilityStatus,
        provisional: bool,
    ) {
        assert_eq!(result.status, expected, "{}: {}", result.id, result.reason);
        assert_eq!(result.provisional, provisional, "{}", result.id);
    }

    fn test_registry() -> DiagramElementRegistry {
        let mut registry = DiagramElementRegistry::new();
        registry
            .register_message::<i64>()
            .with_mapping_into::<f64>(|value| value as f64);
        registry.register_node_builder(
            NodeBuilderOptions::new("json_to_i64"),
            |builder: &mut Builder, _config: ()| {
                builder.create_map_block(|request: JsonMessage| request.as_i64().unwrap_or(0))
            },
        );
        registry.register_node_builder(
            NodeBuilderOptions::new("json_identity"),
            |builder: &mut Builder, _config: ()| {
                builder.create_map_block(|request: JsonMessage| request)
            },
        );
        registry.register_node_builder(
            NodeBuilderOptions::new("i64_to_json"),
            |builder: &mut Builder, _config: ()| {
                builder.create_map_block(|request: i64| JsonMessage::from(request))
            },
        );
        registry.register_node_builder(
            NodeBuilderOptions::new("f64_to_json"),
            |builder: &mut Builder, _config: ()| {
                builder.create_map_block(|request: f64| JsonMessage::from(request))
            },
        );
        registry.register_node_builder(
            NodeBuilderOptions::new("bool_to_json"),
            |builder: &mut Builder, _config: ()| {
                builder.create_map_block(|request: bool| JsonMessage::from(request))
            },
        );
        registry
    }

    fn node_pair_diagram(source_builder: &str, target_builder: &str) -> Diagram {
        Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "source",
            "ops": {
                "source": {
                    "type": "node",
                    "builder": source_builder,
                    "next": "target"
                },
                "target": {
                    "type": "node",
                    "builder": target_builder,
                    "next": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap()
    }

    fn node_pair_request(
        id: &str,
        source_builder: &str,
        target_builder: &str,
    ) -> CompatibilityRequest {
        let source_port: PortRef = output_ref(&"source".into()).next().into();
        let target_port: PortRef = (&NextOperation::Name("target".into())).into();
        CompatibilityRequest {
            diagram: node_pair_diagram(source_builder, target_builder),
            connections: vec![CompatibilityConnection {
                id: id.to_string(),
                focus_ports: vec![source_port.clone(), target_port.clone()],
                source_port: Some(source_port),
                target_port: Some(target_port),
            }],
        }
    }

    fn status_for(source_builder: &str, target_builder: &str) -> CompatibilityResult {
        check_compatibility_request(
            &test_registry(),
            node_pair_request("request", source_builder, target_builder),
        )
    }

    fn operation_request(
        source_builder: &str,
        operation: serde_json::Value,
        target_builder: &str,
    ) -> CompatibilityRequest {
        CompatibilityRequest {
            diagram: Diagram::from_json(json!({
                "version": "0.1.0",
                "start": "source",
                "ops": {
                    "source": { "type": "node", "builder": source_builder, "next": "operation" },
                    "operation": operation,
                    "target": {
                        "type": "node", "builder": target_builder,
                        "next": { "builtin": "terminate" }
                    }
                }
            }))
            .unwrap(),
            connections: vec![connection(
                "operation",
                output_port("source"),
                input_port("operation"),
            )],
        }
    }

    #[test]
    fn compatibility_keeps_connection_results_independent() {
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "source",
            "ops": {
                "source": { "type": "node", "builder": "json_to_i64", "next": "unzip" },
                "unzip": { "type": "unzip", "next": [{ "builtin": "dispose" }] },
                "good": { "type": "node", "builder": "json_to_i64", "next": "target" },
                "target": { "type": "node", "builder": "i64_to_json", "next": { "builtin": "terminate" } },
                "mismatch": { "type": "node", "builder": "bool_to_json", "next": { "builtin": "terminate" } },
                "unfinished": { "type": "buffer" }
            }
        }))
        .unwrap();
        let request: CompatibilityRequest = serde_json::from_value(json!({
            "diagram": diagram,
            "connections": [
                {
                    "id": "bad",
                    "sourcePort": PortRef::from(output_ref(&"source".into()).next()),
                    "targetPort": PortRef::from(&NextOperation::Name("unzip".into()))
                },
                {
                    "id": "good",
                    "sourcePort": PortRef::from(output_ref(&"good".into()).next()),
                    "targetPort": PortRef::from(&NextOperation::Name("target".into()))
                },
                {
                    "id": "mismatch",
                    "sourcePort": PortRef::from(output_ref(&"good".into()).next()),
                    "targetPort": PortRef::from(&NextOperation::Name("mismatch".into()))
                },
                {
                    "id": "unknown",
                    "focusPorts": [PortRef::from(&NextOperation::Name("missing".into()))]
                }
            ]
        }))
        .unwrap();
        let response = check_connections(&test_registry(), request);
        assert_eq!(response.results.len(), 4);
        for (result, expected) in
            response
                .results
                .iter()
                .zip([Incompatible, Compatible, Incompatible])
        {
            assert_status(result, expected, false);
        }
        assert_eq!(response.results[3].status, Unknown);
    }

    #[test]
    fn compatibility_exact_node_to_node_match() {
        let result = status_for("json_to_i64", "i64_to_json");
        assert_eq!(result.status, CompatibilityStatus::Compatible);
        assert!(!result.provisional);
        assert!(result.reason.contains("match"));
    }

    #[test]
    fn compatibility_scope_boundaries_use_converted_types_in_nested_sections() {
        for (target_builder, target_type, inner_status) in [
            ("f64_to_json", "f64", Compatible),
            ("bool_to_json", "bool", Incompatible),
        ] {
            let diagram = Diagram::from_json(json!({
                "version": "0.1.0",
                "templates": {
                    "producer": {
                        "inputs": { "request": "source" },
                        "outputs": ["result"],
                        "ops": {
                            "source": { "type": "node", "builder": "json_to_i64", "next": "result" }
                        }
                    }
                },
                "start": "outer",
                "ops": {
                    "outer": {
                        "type": "scope", "start": "inner", "next": "target",
                        "stream_out": { "updates": "target" },
                        "ops": {
                            "inner": {
                                "type": "scope", "start": { "section": "request" },
                                "next": { "builtin": "terminate" },
                                "stream_out": { "updates": "stream" },
                                "ops": {
                                    "section": {
                                        "type": "section", "template": "producer",
                                        "connect": { "result": { "builtin": "terminate" } }
                                    },
                                    "producer": { "type": "node", "builder": "json_to_i64", "next": "stream" },
                                    "stream": { "type": "stream_out", "name": "updates" }
                                }
                            },
                            "stream": { "type": "stream_out", "name": "updates" }
                        }
                    },
                    "target": { "type": "node", "builder": target_builder, "next": { "builtin": "terminate" } },
                    "unfinished": { "type": "buffer" }
                }
            })).unwrap();
            let output = |name: &str, namespaces: &[&str]| {
                PortRef::from(output_ref(&name.into()).next()).in_namespaces(
                    &namespaces
                        .iter()
                        .map(|name| (*name).into())
                        .collect::<Vec<_>>(),
                )
            };
            let input = |name: &str, namespaces: &[&str]| {
                PortRef::from(OperationRef::from(&NextOperation::Name(name.into()))).in_namespaces(
                    &namespaces
                        .iter()
                        .map(|name| (*name).into())
                        .collect::<Vec<_>>(),
                )
            };
            let inner_namespaces = ["outer".into(), "inner".into()];
            let inner_terminate: PortRef = OperationRef::Terminate(Default::default())
                .in_namespaces(&inner_namespaces)
                .into();
            let ports = [
                (
                    "main",
                    output("outer", &[]),
                    input("target", &[]),
                    Compatible,
                ),
                (
                    "nested-main",
                    output("inner", &["outer"]),
                    OperationRef::terminate_for(&"outer".into()).into(),
                    Compatible,
                ),
                (
                    "stream",
                    output_ref(&"outer".into()).stream_out(&"updates").into(),
                    input("target", &[]),
                    Compatible,
                ),
                (
                    "nested-stream",
                    PortRef::from(output_ref(&"inner".into()).stream_out(&"updates"))
                        .in_namespaces(&["outer".into()]),
                    input("stream", &["outer"]),
                    Compatible,
                ),
                (
                    "start",
                    crossflow::OutputRef::start()
                        .in_namespaces(&["outer".into()])
                        .into(),
                    input("inner", &["outer"]),
                    Compatible,
                ),
                (
                    "nested-start",
                    crossflow::OutputRef::start()
                        .in_namespaces(&inner_namespaces)
                        .into(),
                    OperationRef::exposed_input(&"section".into(), &"request".into())
                        .in_namespaces(&inner_namespaces)
                        .into(),
                    Compatible,
                ),
                (
                    "inner-main",
                    PortRef::from(output_ref(&"section".into()).section_output(&"result"))
                        .in_namespaces(&inner_namespaces),
                    inner_terminate,
                    inner_status,
                ),
                (
                    "inner-stream",
                    output("producer", &["outer", "inner"]),
                    input("stream", &["outer", "inner"]),
                    inner_status,
                ),
                (
                    "unknown",
                    output("outer", &[]),
                    input("unfinished", &[]),
                    Unknown,
                ),
            ];
            let response = check_connections(
                &test_registry(),
                CompatibilityRequest {
                    diagram,
                    connections: ports
                        .iter()
                        .map(|(id, source, target, _)| {
                            connection(id, source.clone(), target.clone())
                        })
                        .collect(),
                },
            );
            for (result, (id, _, _, expected)) in response.results.iter().zip(&ports) {
                assert_eq!(result.status, *expected, "{id}: {}", result.reason);
                assert_eq!(result.provisional, *expected == Unknown, "{id}");
                if matches!(*id, "main" | "nested-main" | "stream" | "nested-stream") {
                    assert_eq!(result.source_type.as_deref(), Some(target_type), "{id}");
                    assert_eq!(result.source_type, result.target_type, "{id}");
                }
                if matches!(*id, "inner-main" | "inner-stream") {
                    assert_eq!(result.source_type.as_deref(), Some("i64"), "{id}");
                    assert_eq!(result.target_type.as_deref(), Some(target_type), "{id}");
                }
                if matches!(*id, "start" | "nested-start") {
                    assert_eq!(
                        result.source_type.as_deref(),
                        Some(std::any::type_name::<JsonMessage>()),
                        "{id}"
                    );
                    assert_eq!(result.source_type, result.target_type, "{id}");
                }
            }
        }
    }

    #[test]
    fn compatibility_template_output_stops_at_scope_conversion_boundary() {
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "templates": {
                "scoped": {
                    "inputs": ["scope"], "outputs": ["result"],
                    "ops": {
                        "scope": {
                            "type": "scope", "start": "producer", "next": "result",
                            "ops": {
                                "producer": { "type": "node", "builder": "json_to_i64", "next": { "builtin": "terminate" } }
                            }
                        }
                    }
                }
            },
            "start": { "section": "scope" },
            "ops": {
                "section": { "type": "section", "template": "scoped", "connect": { "result": "target" } },
                "target": { "type": "node", "builder": "bool_to_json", "next": { "builtin": "terminate" } }
            }
        })).unwrap();
        let result = check_compatibility_request(
            &test_registry(),
            CompatibilityRequest {
                diagram,
                connections: vec![connection(
                    "scope-through-section",
                    output_ref(&"section".into())
                        .section_output(&"result")
                        .into(),
                    input_port("target"),
                )],
            },
        );
        // The i64 producer is incompatible with its scope's bool termination input.
        // After that boundary, the section forwards the scope's bool output.
        assert_eq!(result.status, Compatible, "{}", result.reason);
        assert_eq!(result.source_type.as_deref(), Some("bool"));
        assert_eq!(result.target_type.as_deref(), Some("bool"));
    }

    #[test]
    fn compatibility_template_output_checks_each_internal_producer() {
        for (extra, target, expected) in [
            (
                json!({ "type": "node", "builder": "json_identity", "next": "result" }),
                "f64_to_json",
                Compatible,
            ),
            (
                json!({ "type": "node", "builder": "json_identity", "next": "result" }),
                "bool_to_json",
                Incompatible,
            ),
            (
                json!({ "type": "fork_clone", "next": ["result"] }),
                "f64_to_json",
                Unknown,
            ),
            (
                json!({ "type": "fork_clone", "next": ["result"] }),
                "bool_to_json",
                Incompatible,
            ),
        ] {
            let diagram = Diagram::from_json(json!({
                "version": "0.1.0",
                "templates": {
                    "receiver": {
                        "inputs": { "request": "node" },
                        "outputs": ["response"],
                        "ops": {
                            "node": { "type": "node", "builder": target, "next": "response" }
                        }
                    },
                    "producer": {
                        "inputs": { "request": "source" },
                        "outputs": ["result", "other"],
                        "ops": {
                            "source": { "type": "node", "builder": "json_to_i64", "next": "result" },
                            "extra": extra,
                            "unrelated": { "type": "node", "builder": "json_identity", "next": "other" }
                        }
                    }
                },
                "start": { "section": "request" },
                "ops": {
                    "section": { "type": "section", "template": "producer", "connect": { "result": { "target": "request" }, "other": { "target": "request" } } },
                    "target": { "type": "section", "template": "receiver", "connect": { "response": { "builtin": "terminate" } } }
                }
            })).unwrap();
            let response = check_connections(
                &test_registry(),
                CompatibilityRequest {
                    diagram,
                    connections: ["result", "other"]
                        .into_iter()
                        .map(|output| {
                            connection(
                                output,
                                output_ref(&"section".into()).section_output(&output).into(),
                                OperationRef::exposed_input(&"target".into(), &"request".into())
                                    .into(),
                            )
                        })
                        .chain([connection(
                            "to-terminate",
                            output_ref(&"target".into())
                                .section_output(&"response")
                                .into(),
                            OperationRef::Terminate(Default::default()).into(),
                        )])
                        .collect(),
                },
            );
            let result = &response.results[0];
            assert_eq!(result.status, expected, "{}", result.reason);
            assert_eq!(result.provisional, expected == Unknown);
            if expected == Incompatible {
                assert_eq!(result.source_type.as_deref(), Some("i64"));
                assert_eq!(result.target_type.as_deref(), Some("bool"));
            }
            assert_eq!(response.results[1].status, Compatible);
            assert_status(&response.results[2], Compatible, false);
            assert_eq!(
                response.results[2].source_type,
                response.results[2].target_type
            );
        }
    }

    #[test]
    fn compatibility_registered_conversion() {
        let result = status_for("json_to_i64", "f64_to_json");
        assert_eq!(result.status, CompatibilityStatus::Compatible);
        assert!(!result.provisional);
        assert!(result.reason.contains("conversion"));
    }

    #[test]
    fn compatibility_implicit_json_serialization() {
        let result = status_for("json_to_i64", "json_identity");
        assert_eq!(result.status, CompatibilityStatus::Compatible);
        assert!(result.reason.contains("serialized"));
    }

    #[test]
    fn compatibility_implicit_json_deserialization() {
        let result = status_for("json_identity", "i64_to_json");
        assert_eq!(result.status, CompatibilityStatus::Compatible);
        assert!(result.reason.contains("deserialized"));
    }

    #[test]
    fn compatibility_incompatible_custom_node_pair() {
        let result = check_compatibility_request(
            &test_registry(),
            node_pair_request("request", "json_to_i64", "bool_to_json"),
        );
        assert_eq!(result.status, CompatibilityStatus::Incompatible);
        assert!(!result.provisional);
    }

    #[test]
    fn compatibility_known_inputs_reject_missing_operation_capabilities() {
        struct Opaque;
        let mut registry = test_registry();
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("opaque_output"),
                |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(|_: JsonMessage| Opaque)
                },
            );

        for (source, operation) in [
            (
                "json_to_i64",
                json!({ "type": "unzip", "next": ["target"] }),
            ),
            (
                "json_to_i64",
                json!({ "type": "fork_result", "ok": "target", "err": "target" }),
            ),
            (
                "opaque_output",
                json!({ "type": "fork_clone", "next": ["target"] }),
            ),
            (
                "opaque_output",
                json!({ "type": "split", "sequential": ["target"] }),
            ),
        ] {
            let request = operation_request(source, operation, "i64_to_json");
            let result = check_compatibility_request(&registry, request);
            assert_status(&result, Incompatible, false);
        }
    }

    #[test]
    fn compatibility_determined_operation_outputs_check_delivery() {
        let mut registry = test_registry();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("tuple_output"),
                |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(|_: JsonMessage| (1_i64, true))
                },
            )
            .with_unzip();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("result_output"),
                |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(|_: JsonMessage| Ok::<i64, bool>(1))
                },
            )
            .with_result();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("list_output"),
                |builder: &mut Builder, _config: ()| {
                    builder.create_map_block(|_: JsonMessage| vec![1_i64])
                },
            )
            .with_split();

        let operation_name = "operation".into();
        let cases = [
            (
                "json_to_i64",
                json!({ "type": "fork_clone", "next": ["target"] }),
                output_ref(&operation_name).next_index(0),
                "i64_to_json",
                Compatible,
            ),
            (
                "json_to_i64",
                json!({ "type": "fork_clone", "next": ["target"] }),
                output_ref(&operation_name).next_index(0),
                "bool_to_json",
                Incompatible,
            ),
            (
                "tuple_output",
                json!({ "type": "unzip", "next": [{ "builtin": "dispose" }, { "builtin": "dispose" }, "target"] }),
                output_ref(&operation_name).next_index(2),
                "i64_to_json",
                Incompatible,
            ),
            (
                "tuple_output",
                json!({ "type": "unzip", "next": ["target"] }),
                output_ref(&operation_name).next_index(0),
                "bool_to_json",
                Incompatible,
            ),
            (
                "tuple_output",
                json!({ "type": "unzip", "next": ["target"] }),
                output_ref(&operation_name).next_index(0),
                "f64_to_json",
                Compatible,
            ),
            (
                "result_output",
                json!({ "type": "fork_result", "ok": "target", "err": { "builtin": "dispose" } }),
                output_ref(&operation_name).ok(),
                "bool_to_json",
                Incompatible,
            ),
            (
                "result_output",
                json!({ "type": "fork_result", "ok": { "builtin": "dispose" }, "err": "target" }),
                output_ref(&operation_name).err(),
                "i64_to_json",
                Incompatible,
            ),
            (
                "list_output",
                json!({ "type": "split", "sequential": ["target"] }),
                output_ref(&operation_name).next_index(0),
                "bool_to_json",
                Incompatible,
            ),
            (
                "list_output",
                json!({ "type": "split", "keyed": { "0": "target" } }),
                output_ref(&operation_name).keyed(&"0".into()),
                "bool_to_json",
                Incompatible,
            ),
            (
                "list_output",
                json!({ "type": "split", "remaining": "target" }),
                output_ref(&operation_name).remaining(),
                "bool_to_json",
                Incompatible,
            ),
            (
                "json_to_i64",
                json!({ "type": "split", "sequential": ["target"] }),
                output_ref(&operation_name).next_index(0),
                "bool_to_json",
                Compatible,
            ),
        ];
        for (source, operation, output, target, expected) in cases {
            let is_fork = operation["type"] == "fork_clone";
            let mut request = operation_request(source, operation, target);
            request.connections[0] = connection(source, output.into(), input_port("target"));
            let result = check_compatibility_request(&registry, request);
            assert_status(&result, expected, false);
            if is_fork {
                assert_eq!(result.source_type.as_deref(), Some("i64"));
            }
        }
    }

    #[test]
    fn compatibility_unresolved_upstream_keeps_fork_output_provisional() {
        let mut request = operation_request(
            "json_to_i64",
            json!({ "type": "fork_clone", "next": ["target"] }),
            "bool_to_json",
        );
        let unresolved = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "builtin": "dispose" },
            "ops": { "pending": { "type": "fork_clone", "next": ["operation"] } }
        }))
        .unwrap();
        Arc::make_mut(&mut request.diagram.ops)
            .insert("pending".into(), unresolved.ops["pending"].clone());
        request.connections[0].source_port =
            Some(output_ref(&"operation".into()).next_index(0).into());
        request.connections[0].target_port = Some(input_port("target"));
        let result = check_compatibility_request(&test_registry(), request);
        assert_status(&result, Unknown, true);
    }

    #[test]
    fn compatibility_waits_for_upstream_before_rejecting_fork_capability() {
        let mut registry = test_registry();
        registry.opt_out().no_cloning().register_node_builder(
            NodeBuilderOptions::new("uncloneable_input"),
            |builder: &mut Builder, _config: ()| {
                builder.create_map_block(|_: Vec<i16>| JsonMessage::Null)
            },
        );
        for (connected, expected) in [(true, Compatible), (false, Unknown)] {
            let diagram = Diagram::from_json(json!({
                "version": "0.1.0",
                "start": "source",
                "ops": {
                    "source": {
                        "type": "node", "builder": "json_identity",
                        "next": if connected { json!("hop1") } else { json!({ "builtin": "dispose" }) }
                    },
                    "hop1": { "type": "fork_clone", "next": ["hop2"] },
                    "hop2": { "type": "fork_clone", "next": ["operation"] },
                    "operation": { "type": "fork_clone", "next": ["target"] },
                    "target": {
                        "type": "node", "builder": "uncloneable_input",
                        "next": { "builtin": "terminate" }
                    }
                }
            }))
            .unwrap();
            let result = check_compatibility_request(
                &registry,
                CompatibilityRequest {
                    diagram,
                    connections: vec![connection(
                        &connected.to_string(),
                        output_ref(&"hop2".into()).next_index(0).into(),
                        input_port("operation"),
                    )],
                },
            );
            assert_eq!(result.status, expected, "{}", result.reason);
            assert_eq!(result.provisional, !connected);
        }
    }

    #[test]
    fn compatibility_feedback_remains_provisional_without_hanging() {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut registry = test_registry();
            registry
                .register_node_builder(
                    NodeBuilderOptions::new("result_output"),
                    |builder: &mut Builder, _config: ()| {
                        builder.create_map_block(|_: JsonMessage| Ok::<i64, bool>(1))
                    },
                )
                .with_result();
            let mut request = operation_request(
                "result_output",
                json!({ "type": "fork_result", "ok": "feedback", "err": { "builtin": "dispose" } }),
                "i64_to_json",
            );
            Arc::make_mut(&mut request.diagram.ops).insert(
                "feedback".into(),
                serde_json::from_value(
                    json!({ "type": "fork_clone", "next": ["operation", "buffer"] }),
                )
                .unwrap(),
            );
            Arc::make_mut(&mut request.diagram.ops).insert(
                "buffer".into(),
                serde_json::from_value(json!({ "type": "buffer" })).unwrap(),
            );
            request.connections[0].source_port = Some(output_ref(&"operation".into()).ok().into());
            request.connections[0].target_port = Some(input_port("feedback"));
            request.connections.push(CompatibilityConnection {
                id: "buffer".into(),
                focus_ports: vec![input_port("buffer")],
                source_port: None,
                target_port: None,
            });
            for result in check_connections(&registry, request).results {
                let _ = sender.send(result);
            }
        });
        for _ in 0..2 {
            let result = receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("Compatibility inference did not settle");
            assert_status(&result, Unknown, true);
        }
    }

    #[test]
    fn compatibility_ignores_unfocused_unfinished_ports() {
        let registry = test_registry();
        let source_port: PortRef = output_ref(&"source".into()).next().into();
        let target_port: PortRef = (&NextOperation::Name("target".into())).into();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "source",
            "ops": {
                "source": {
                    "type": "node",
                    "builder": "json_to_i64",
                    "next": "target"
                },
                "target": {
                    "type": "node",
                    "builder": "i64_to_json",
                    "next": { "builtin": "terminate" }
                },
                "unfinished": {
                    "type": "buffer"
                }
            }
        }))
        .unwrap();
        let result = check_compatibility_request(
            &registry,
            CompatibilityRequest {
                diagram,
                connections: vec![CompatibilityConnection {
                    id: "request".to_string(),
                    focus_ports: vec![source_port.clone(), target_port.clone()],
                    source_port: Some(source_port),
                    target_port: Some(target_port),
                }],
            },
        );
        assert_eq!(result.status, CompatibilityStatus::Compatible);
    }

    #[test]
    fn compatibility_focused_unknown_builder_is_unknown() {
        let result = check_compatibility_request(
            &test_registry(),
            node_pair_request("request", "json_to_i64", "missing_builder"),
        );
        assert_eq!(result.status, CompatibilityStatus::Unknown);
        assert!(!result.provisional);
        assert!(result.reason.contains("missing_builder"));
    }

    #[test]
    fn compatibility_setup_errors_do_not_hide_unrelated_matches() {
        for unfinished in [
            json!({ "type": "node", "builder": "", "next": { "builtin": "dispose" } }),
            json!({ "type": "node", "builder": "missing_builder", "next": { "builtin": "dispose" } }),
            json!({ "type": "section", "template": "missing", "connect": {} }),
            json!({ "type": "section", "template": "recursive", "connect": {} }),
            json!({ "type": "section", "template": "redirect_cycle", "connect": { "result": { "unfinished": "input" } } }),
            json!({ "type": "scope", "start": { "builtin": "terminate" }, "next": "unfinished", "ops": {} }),
            json!({ "type": "section", "template": "producer", "connect": { "": { "builtin": "dispose" } } }),
        ] {
            let mut request = node_pair_request("good", "json_to_i64", "i64_to_json");
            request.diagram.templates = serde_json::from_value(json!({
                "producer": { "inputs": ["source"], "outputs": ["result"], "ops": {
                    "source": { "type": "node", "builder": "json_to_i64", "next": "result" }
                } },
                "recursive": { "inputs": [], "outputs": [], "ops": {
                    "again": {"type": "section", "template": "recursive", "connect": {}}
                } },
                "redirect_cycle": { "inputs": {"input": "result"}, "outputs": ["result"], "ops": {} }
            }))
            .unwrap();
            Arc::make_mut(&mut request.diagram.ops).insert(
                "unfinished".into(),
                serde_json::from_value(unfinished).unwrap(),
            );
            request.connections.push(connection(
                "unfinished",
                output_port("unfinished"),
                input_port("target"),
            ));
            let registry = test_registry();
            assert!(
                request
                    .diagram
                    .infer_message_types(
                        &registry,
                        InferenceBoundaryConditions::json_messages(&registry, []).unwrap(),
                    )
                    .is_err()
            );
            let results = check_connections(&registry, request).results;
            assert_eq!(results[0].status, Compatible, "{}", results[0].reason);
            assert_eq!(results[1].status, Unknown);
            assert!(!results[1].provisional);
        }
    }

    #[test]
    fn compatibility_one_sided_message_port_is_provisional() {
        let source_port: PortRef = output_ref(&"source".into()).next().into();
        let result = check_compatibility_request(
            &test_registry(),
            CompatibilityRequest {
                diagram: node_pair_diagram("json_to_i64", "i64_to_json"),
                connections: vec![CompatibilityConnection {
                    id: "one-sided".to_string(),
                    focus_ports: vec![source_port.clone()],
                    source_port: Some(source_port),
                    target_port: None,
                }],
            },
        );

        assert_eq!(result.status, CompatibilityStatus::Unknown);
        assert!(result.provisional);
    }

    #[test]
    fn compatibility_allows_incomplete_buffer_connections_provisionally() {
        for operation_type in ["listen", "join", "buffer_access"] {
            let buffer_port: PortRef = (&NextOperation::Name("buffer".into())).into();
            let diagram = Diagram::from_json(json!({
                "version": "0.1.0",
                "start": { "builtin": "dispose" },
                "ops": {
                    "buffer": {
                        "type": "buffer"
                    },
                    "consumer": {
                        "type": operation_type,
                        "buffers": ["buffer"],
                        "next": { "builtin": "dispose" }
                    }
                }
            }))
            .unwrap();

            let result = check_compatibility_request(
                &test_registry(),
                CompatibilityRequest {
                    diagram,
                    connections: vec![CompatibilityConnection {
                        id: operation_type.to_string(),
                        focus_ports: vec![buffer_port.clone()],
                        source_port: None,
                        target_port: None,
                    }],
                },
            );

            assert_eq!(result.status, CompatibilityStatus::Unknown);
            assert!(result.provisional);
            assert!(result.reason.contains("more type context"));
        }
    }

    #[test]
    fn compatibility_allows_listen_output_missing_context_provisionally() {
        let source_port: PortRef = output_ref(&"listen".into()).next().into();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "buffer",
            "ops": {
                "buffer": {
                    "type": "buffer"
                },
                "listen": {
                    "type": "listen",
                    "buffers": ["buffer"],
                    "next": { "builtin": "dispose" }
                }
            }
        }))
        .unwrap();

        let result = check_compatibility_request(
            &test_registry(),
            CompatibilityRequest {
                diagram,
                connections: vec![CompatibilityConnection {
                    id: "listen-output".to_string(),
                    focus_ports: vec![source_port.clone()],
                    source_port: Some(source_port),
                    target_port: None,
                }],
            },
        );

        assert_eq!(result.status, CompatibilityStatus::Unknown);
        assert!(result.provisional);
        assert!(result.reason.contains("more type context"));
    }

    #[test]
    fn compatibility_allows_buffer_access_output_missing_context_provisionally() {
        let source_port: PortRef = output_ref(&"buffer_access".into()).next().into();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "buffer",
            "ops": {
                "buffer": {
                    "type": "buffer"
                },
                "buffer_access": {
                    "type": "buffer_access",
                    "buffers": ["buffer"],
                    "next": { "builtin": "dispose" }
                }
            }
        }))
        .unwrap();

        let result = check_compatibility_request(
            &test_registry(),
            CompatibilityRequest {
                diagram,
                connections: vec![CompatibilityConnection {
                    id: "buffer-access-output".to_string(),
                    focus_ports: vec![source_port.clone()],
                    source_port: Some(source_port),
                    target_port: None,
                }],
            },
        );

        assert_eq!(result.status, CompatibilityStatus::Unknown);
        assert!(result.provisional);
        assert!(result.reason.contains("more type context"));
    }

    #[test]
    fn compatibility_buffer_consumers_validate_populated_buffers_and_outputs() {
        let mut registry = test_registry();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("join_i64"),
                |builder: &mut Builder, _: ()| {
                    builder.create_map_block(|_: Vec<i64>| JsonMessage::Null)
                },
            )
            .with_join();
        registry
            .register_node_builder(
                NodeBuilderOptions::new("join_bool"),
                |builder: &mut Builder, _: ()| {
                    builder.create_map_block(|_: Vec<bool>| JsonMessage::Null)
                },
            )
            .with_join();
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_pair"),
                |builder: &mut Builder, _: ()| {
                    builder
                        .create_map_block(|_: (BufferKey<i64>, BufferKey<i64>)| JsonMessage::Null)
                },
            )
            .with_listen();
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_i64"),
                |builder: &mut Builder, _: ()| {
                    builder.create_map_block(|_: Vec<BufferKey<i64>>| JsonMessage::Null)
                },
            )
            .with_listen();
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("access_i64"),
                |builder: &mut Builder, _: ()| {
                    builder
                        .create_map_block(|_: (JsonMessage, Vec<BufferKey<i64>>)| JsonMessage::Null)
                },
            )
            .with_buffer_access();
        for (operation, target, selection, expected) in [
            ("join", "bool_to_json", json!(["buffer"]), Incompatible),
            ("listen", "bool_to_json", json!(["buffer"]), Incompatible),
            (
                "buffer_access",
                "bool_to_json",
                json!(["buffer"]),
                Incompatible,
            ),
            ("join", "join_i64", json!(["buffer"]), Compatible),
            ("join", "join_bool", json!(["buffer"]), Incompatible),
            (
                "join",
                "join_bool",
                json!(["missing", "buffer"]),
                Incompatible,
            ),
            ("listen", "listen_pair", json!(["buffer"]), Incompatible),
            ("listen", "listen_i64", json!(["buffer"]), Compatible),
            ("buffer_access", "access_i64", json!(["buffer"]), Compatible),
            ("join", "join_i64", json!({"named": "buffer"}), Incompatible),
            (
                "listen",
                "listen_i64",
                json!({"named": "buffer"}),
                Incompatible,
            ),
            (
                "buffer_access",
                "access_i64",
                json!({"named": "buffer"}),
                Incompatible,
            ),
        ] {
            let diagram = Diagram::from_json(json!({
                "version": "0.1.0", "start": "source", "ops": {
                    "source": {"type": "node", "builder": "json_to_i64", "next": "buffer"},
                    "buffer": {"type": "buffer"},
                    "consumer": {"type": operation, "buffers": selection, "next": "target"},
                    "target": {"type": "node", "builder": target, "next": {"builtin": "terminate"}},
                    "good": {"type": "node", "builder": "json_identity", "next": {"builtin": "terminate"}}
                }
            })).unwrap();
            let response = check_connections(
                &registry,
                CompatibilityRequest {
                    diagram,
                    connections: vec![
                        CompatibilityConnection {
                            id: "buffer-selection".into(),
                            focus_ports: vec![input_port("buffer"), output_port("consumer")],
                            source_port: None,
                            target_port: None,
                        },
                        connection(
                            "consumer-output",
                            output_port("consumer"),
                            input_port("target"),
                        ),
                        connection(
                            "unrelated",
                            output_port("good"),
                            OperationRef::Terminate(Default::default()).into(),
                        ),
                        connection("buffer-input", output_port("source"), input_port("buffer")),
                    ],
                },
            );
            for result in &response.results[..2] {
                assert_eq!(
                    result.status, expected,
                    "{operation}/{target}/{}: {}",
                    result.id, result.reason
                );
                assert!(!result.provisional);
            }
            assert_eq!(response.results[2].status, Compatible);
            assert_eq!(response.results[3].status, Compatible);
        }
    }

    #[test]
    fn compatibility_buffer_selection_requires_an_exposed_buffer() {
        use crossflow::SectionInterfaceItem;
        #[derive(crossflow::Section)]
        struct Ports {
            request: crossflow::InputSlot<i64>,
            stored: crossflow::Buffer<i64>,
        }
        let mut registry = test_registry();
        registry.register_section_builder(
            crossflow::SectionBuilderOptions::new("ports"),
            |builder: &mut Builder, _: ()| Ports {
                request: builder.create_map_block(|_: i64| JsonMessage::Null).input,
                stored: builder.create_buffer(Default::default()),
            },
        );
        registry
            .register_node_builder(
                NodeBuilderOptions::new("joined"),
                |builder: &mut Builder, _: ()| {
                    builder.create_map_block(|_: Vec<i64>| JsonMessage::Null)
                },
            )
            .with_join();
        for provider in ["template", "builder"] {
            for (selected, expected) in [("stored", Compatible), ("request", Incompatible)] {
                let mut section = json!({"type": "section", "connect": {}});
                section[provider] = json!("ports");
                let request = CompatibilityRequest {
                    diagram: Diagram::from_json(json!({
                        "version": "0.1.0", "start": "source",
                        "templates": {"ports": {"inputs": {"request": "node"}, "buffers": ["stored"], "outputs": [], "ops": {
                            "node": {"type": "node", "builder": "i64_to_json", "next": {"builtin": "terminate"}},
                            "stored": {"type": "buffer"}
                        }}},
                        "ops": {
                            "source": {"type": "node", "builder": "json_to_i64", "next": {"section": "stored"}},
                            "section": section,
                            "consumer": {"type": "join", "buffers": [{"section": selected}], "next": "target"},
                            "target": {"type": "node", "builder": "joined", "next": {"builtin": "terminate"}}
                        }
                    })).unwrap(),
                    connections: vec![CompatibilityConnection { id: "selected-buffer".into(), focus_ports: vec![
                        OperationRef::exposed_input(&"section".into(), &selected.into()).into(),
                        output_port("consumer"),
                    ], source_port: None, target_port: None }],
                };
                let result = check_compatibility_request(&registry, request);
                assert_eq!(
                    result.status, expected,
                    "{provider}/{selected}: {}",
                    result.reason
                );
            }
        }
    }

    #[test]
    fn compatibility_join_requires_cloning_only_for_selected_clone_slots() {
        struct Uncloneable;
        let mut registry = test_registry();
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("uncloneable"),
                |builder: &mut Builder, _: ()| {
                    builder.create_map_block(|_: JsonMessage| Uncloneable)
                },
            );
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_node_builder(
                NodeBuilderOptions::new("join_uncloneable"),
                |builder: &mut Builder, _: ()| {
                    builder.create_map_block(|_: Vec<Uncloneable>| JsonMessage::Null)
                },
            )
            .with_join();
        for (clone, expected) in [
            (json!([]), Compatible),
            (json!([0]), Incompatible),
            (json!([1]), Incompatible),
        ] {
            let request = CompatibilityRequest {
                diagram: Diagram::from_json(json!({
                    "version": "0.1.0", "start": "source", "ops": {
                        "source": {"type": "node", "builder": "uncloneable", "next": "buffer"},
                        "buffer": {"type": "buffer"},
                        "consumer": {"type": "join", "buffers": ["buffer"], "clone": clone, "next": "target"},
                        "target": {"type": "node", "builder": "join_uncloneable", "next": {"builtin": "terminate"}}
                    }
                })).unwrap(),
                connections: vec![CompatibilityConnection {
                    id: "join".into(), focus_ports: vec![input_port("buffer")],
                    source_port: Some(output_port("consumer")),
                    target_port: Some(input_port("target")),
                }],
            };
            let result = check_compatibility_request(&registry, request);
            assert_status(&result, expected, false);
        }
    }

    #[test]
    fn compatibility_does_not_allow_hard_buffer_layout_mismatch_provisionally() {
        let mut registry = test_registry();
        registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("listen_string_buffer"),
                |builder: &mut Builder, _config: ()| -> Node<Vec<BufferKey<String>>, usize, ()> {
                    builder.create_node(
                        (|Blocking { request, .. }: Blocking<Vec<BufferKey<String>>>,
                          _access: BufferAccess<String>| {
                            request.len()
                        })
                        .into_callback(),
                    )
                },
            )
            .with_listen();

        let source_port: PortRef = output_port("listen");
        let target_port: PortRef = input_port("listen_string_buffer");
        let buffer_port: PortRef = input_port("buffer");
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "builtin": "dispose" },
            "ops": {
                "buffer": {
                    "type": "buffer"
                },
                "listen": {
                    "type": "listen",
                    "buffers": { "foo": "buffer" },
                    "next": "listen_string_buffer"
                },
                "listen_string_buffer": {
                    "type": "node",
                    "builder": "listen_string_buffer",
                    "next": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap();

        let result = check_compatibility_request(
            &registry,
            CompatibilityRequest {
                diagram,
                connections: vec![CompatibilityConnection {
                    id: "hard-buffer-mismatch".to_string(),
                    focus_ports: vec![
                        source_port.clone(),
                        target_port.clone(),
                        buffer_port.clone(),
                    ],
                    source_port: Some(source_port),
                    target_port: Some(target_port),
                }],
            },
        );

        assert_status(&result, Incompatible, false);
    }
    #[test]
    fn compatibility_allows_incomplete_buffer_connection() {
        let buffer_port: PortRef = (&NextOperation::Name("buffer".into())).into();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "builtin": "dispose" },
            "ops": {
                "buffer": {
                    "type": "buffer"
                },
                "listen": {
                    "type": "listen",
                    "buffers": ["buffer"],
                    "next": { "builtin": "dispose" }
                }
            }
        }))
        .unwrap();

        let result = check_compatibility_request(
            &test_registry(),
            CompatibilityRequest {
                diagram,
                connections: vec![CompatibilityConnection {
                    id: "request".to_string(),
                    focus_ports: vec![buffer_port],
                    source_port: None,
                    target_port: None,
                }],
            },
        );

        assert_eq!(result.status, CompatibilityStatus::Unknown);
        assert!(result.provisional);
        assert!(result.reason.contains("more type context"));
    }
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Deserialize))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub enum InteractionSessionEnd {
    Ok(serde_json::Value),
    Err(String),
}

#[cfg(feature = "router")]
impl InteractionSessionEnd {
    fn err_from_status_code(status_code: StatusCode) -> Self {
        Self::Err(status_code.to_string())
    }
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Deserialize))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub enum InteractionSessionFeedback {
    OperationStarted(String),
    OperationFinished(String),
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Deserialize))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum InteractionSessionMessage {
    Feedback(InteractionSessionFeedback),
    Finish(InteractionSessionEnd),
}

/// Start an interaction session.
#[cfg(feature = "router")]
async fn ws_interaction<W, R, Text>(mut write: W, mut read: R, state: State<ExecutorState>)
where
    W: WebsocketSinkExt<InteractionSessionMessage>,
    R: WebsocketStreamExt<PostRunRequest, Text>,
    Text: std::ops::Deref<Target = str>,
{
    let req: PostRunRequest = if let Some(req) = read.next_json().await {
        req
    } else {
        return;
    };

    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let (feedback_tx, mut feedback_rx) = tokio::sync::broadcast::channel(10);
    if let Err(err) = state
        .send_chan
        .send(Context {
            registry: state.registry.clone(),
            diagram: req.diagram,
            request: req.request,
            response_tx,
            feedback_tx: Some(FeedbackSender(feedback_tx)),
        })
        .await
    {
        error!("{}", err);
        write
            .send_json(&InteractionSessionMessage::Finish(
                InteractionSessionEnd::err_from_status_code(StatusCode::INTERNAL_SERVER_ERROR),
            ))
            .await;
        return;
    }

    let response = async {
        let response_result = response_rx.await;

        let workflow_response = match response_result {
            Ok(response) => response,
            Err(err) => {
                error!("{}", err);
                return InteractionSessionEnd::err_from_status_code(
                    StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        };

        match workflow_response {
            Ok((outcome, workflow)) => {
                let result = outcome.await;
                if let Err(err) = state.despawn_chan.send(workflow).await {
                    error!("Failed to request workflow despawn: {err}");
                }

                // Brief yield so already-queued feedback reaches the socket
                // before the finish message; drain_interaction_feedback handles
                // the rest.
                tokio::time::sleep(Duration::from_millis(100)).await;

                match result {
                    Ok(result) => InteractionSessionEnd::Ok(result),
                    Err(err) => InteractionSessionEnd::Err(err.to_string()),
                }
            }
            Err(err) => InteractionSessionEnd::Err(err.to_string()),
        }
    };
    tokio::pin!(response);

    let mut feedback_open = true;
    loop {
        if feedback_open {
            tokio::select! {
                feedback = feedback_rx.recv() => {
                    match feedback {
                        Ok(feedback) => {
                            send_interaction_feedback(&mut write, &feedback).await;
                        }
                        Err(e) => match e {
                            BroadcastRecvError::Closed => {
                                feedback_open = false;
                            }
                            BroadcastRecvError::Lagged(_) => {
                                warn!("{}", e);
                                feedback_open = false;
                            }
                        },
                    }
                }
                result = &mut response => {
                    drain_interaction_feedback(&mut write, &mut feedback_rx).await;
                    write
                        .send_json(&InteractionSessionMessage::Finish(result))
                        .await;
                    break;
                }
            }
        } else {
            let result = response.await;
            write
                .send_json(&InteractionSessionMessage::Finish(result))
                .await;
            break;
        }
    }
}

#[cfg(feature = "router")]
async fn drain_interaction_feedback<W>(
    write: &mut W,
    feedback_rx: &mut tokio::sync::broadcast::Receiver<WorkflowFeedback>,
) where
    W: WebsocketSinkExt<InteractionSessionMessage>,
{
    loop {
        match feedback_rx.try_recv() {
            Ok(feedback) => send_interaction_feedback(write, &feedback).await,
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                warn!("interaction feedback lagged by {skipped} messages");
            }
        }
    }
}

#[cfg(feature = "router")]
async fn send_interaction_feedback<W>(write: &mut W, feedback: &TracedEvent)
where
    W: WebsocketSinkExt<InteractionSessionMessage>,
{
    for op_id in operation_finished_ids(feedback) {
        write
            .send_json(&InteractionSessionMessage::Feedback(
                InteractionSessionFeedback::OperationFinished(op_id),
            ))
            .await;
    }

    if let Some(op_id) = operation_started_id(feedback) {
        write
            .send_json(&InteractionSessionMessage::Feedback(
                InteractionSessionFeedback::OperationStarted(op_id),
            ))
            .await;
    }
}

#[cfg(feature = "router")]
fn operation_started_id(feedback: &TracedEvent) -> Option<String> {
    match &feedback.event {
        TracedEventKind::MessageSent(message) => message
            .input
            .info
            .as_ref()
            .and_then(|info| info.id().as_ref())
            .map(ToString::to_string),
        TracedEventKind::BufferEvent(event) => event
            .accessor
            .info
            .as_ref()
            .and_then(|info| info.id().as_ref())
            .map(ToString::to_string),
        _ => None,
    }
}

#[cfg(feature = "router")]
fn operation_finished_ids(feedback: &TracedEvent) -> Vec<String> {
    match &feedback.event {
        TracedEventKind::MessageSent(message) => message
            .output
            .iter()
            .filter_map(|source| {
                source
                    .info
                    .as_ref()
                    .and_then(|info| info.id().as_ref())
                    .map(ToString::to_string)
            })
            .collect(),
        _ => Vec::new(),
    }
}

#[derive(bevy_ecs::prelude::Resource)]
struct RequestReceiver(tokio::sync::mpsc::Receiver<Context>);

/// Receiver for workflows that need to be despawned.
#[derive(bevy_ecs::prelude::Resource)]
struct WorkflowDespawnReceiver(tokio::sync::mpsc::Receiver<Entity>);

/// Receives a request from executor service and schedules the workflow.
fn execute_requests(
    mut rx: bevy_ecs::system::ResMut<RequestReceiver>,
    mut cmds: bevy_ecs::system::Commands,
    mut app_exit_events: bevy_ecs::event::EventWriter<bevy_app::AppExit>,
) {
    let rx = &mut rx.0;
    match rx.try_recv() {
        Ok(ctx) => {
            let registry = &*ctx.registry.lock().unwrap();
            let maybe_outcome = match ctx.diagram.spawn_io_workflow(&mut cmds, registry) {
                Ok(workflow) => {
                    let series = cmds.request(ctx.request, workflow);
                    let session = series.session_id();
                    let outcome: Outcome<serde_json::Value> = series.outcome();
                    if let Some(feedback_tx) = ctx.feedback_tx {
                        cmds.entity(session).insert(feedback_tx);
                    }
                    Ok((outcome, workflow.provider()))
                }
                Err(err) => Err(err.into()),
            };
            // assuming that workflows are automatically cancelled when the promise is dropped.
            if let Err(_) = ctx.response_tx.send(maybe_outcome) {
                error!("failed to send response")
            }
        }
        Err(err) => match err {
            TryRecvError::Empty => {}
            TryRecvError::Disconnected => {
                app_exit_events.write_default();
            }
        },
    }
}

fn interaction_feedback(
    trigger: bevy_ecs::prelude::Trigger<trace::TracedEvent>,
    feedback_query: bevy_ecs::system::Query<(Entity, &FeedbackSender)>,
) {
    let ev = trigger.event();
    for (session, channel) in &feedback_query {
        if ev.event.is_for_session(session) {
            let _ = channel.0.send(ev.clone());
        }
    }
}

fn despawn_workflows(
    mut receiver: bevy_ecs::system::ResMut<WorkflowDespawnReceiver>,
    mut commands: bevy_ecs::system::Commands,
) {
    while let Ok(workflow) = receiver.0.try_recv() {
        let Ok(mut e) = commands.get_entity(workflow) else {
            continue;
        };

        e.despawn();
    }
}

#[non_exhaustive]
pub struct ExecutorOptions {
    pub response_timeout: Duration,
}

impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            response_timeout: Duration::from_secs(15),
        }
    }
}

/// Use this to set up a full-fledged bevy App to be used as a diagram execution server.
/// Pass in just the main subapp using `&mut app.sub_apps_mut().main`.
pub fn setup_bevy_app(
    app: &mut bevy_app::SubApp,
    registry: DiagramElementRegistry,
    options: &ExecutorOptions,
) -> ExecutorState {
    let (request_tx, request_rx) = tokio::sync::mpsc::channel::<Context>(10);
    let (despawn_tx, despawn_rx) = tokio::sync::mpsc::channel(10);
    app.insert_resource(RequestReceiver(request_rx));
    app.insert_resource(WorkflowDespawnReceiver(despawn_rx));
    app.add_systems(bevy_app::Update, execute_requests);
    app.world_mut().add_observer(interaction_feedback);
    app.add_systems(bevy_app::Update, despawn_workflows);

    ExecutorState {
        registry: Arc::new(Mutex::new(registry)),
        send_chan: request_tx,
        despawn_chan: despawn_tx,
        response_timeout: options.response_timeout,
    }
}

/// Use this for WASM builds to set up a SubApp that does not belong to any App.
/// WASM builds need to use just a plain SubApp because the full-fledged App
/// struct no longer implements Send as of Bevy 0.16.
pub fn setup_bevy_app_wasm(
    app: &mut bevy_app::SubApp,
    registry: DiagramElementRegistry,
    options: &ExecutorOptions,
) -> ExecutorState {
    setup_subapp_defaults(app);
    setup_bevy_app(app, registry, options)
}

/// We need to manually setup the SubApp the way it would be setup by a regular
/// App, because we no longer get the benefit of a regular App in this highly
/// async environment.
///
/// This function definition is based on [`bevy_app::App::default()`]
fn setup_subapp_defaults(app: &mut bevy_app::SubApp) {
    use bevy_ecs::schedule::ScheduleLabel;
    app.update_schedule = Some(bevy_app::Main.intern());

    app.init_resource::<bevy_ecs::reflect::AppTypeRegistry>();
    app.register_type::<bevy_ecs::name::Name>();
    app.register_type::<bevy_ecs::hierarchy::ChildOf>();
    app.register_type::<bevy_ecs::hierarchy::Children>();

    app.add_plugins(bevy_app::MainSchedulePlugin);
    app.add_systems(
        bevy_app::First,
        bevy_ecs::event::event_update_system
            .in_set(bevy_ecs::event::EventUpdates)
            .run_if(bevy_ecs::event::event_update_condition),
    );
    app.add_event::<bevy_app::AppExit>();
}

#[cfg(feature = "router")]
pub(super) fn new_router(
    app: &mut bevy_app::App,
    registry: DiagramElementRegistry,
    options: ExecutorOptions,
) -> Router {
    let executor_state = setup_bevy_app(&mut app.sub_apps_mut().main, registry, &options);

    let router = Router::new()
        .route("/run", post(post_run))
        .route("/compatibility", post(post_compatibility));

    let router = router.route(
        "/interaction",
        routing::any(
            async |ws: ws::WebSocketUpgrade, state: State<ExecutorState>| {
                ws.on_upgrade(|socket| {
                    use futures_util::StreamExt;

                    let (write, read) = socket.split();
                    ws_interaction(write, read, state)
                })
            },
        ),
    );

    let router = router.with_state(executor_state);
    router
}

#[cfg(feature = "router")]
#[cfg(test)]
mod tests {
    use axum::extract::ws;
    use axum::{
        body,
        http::{Request, header},
    };
    use crossflow::{
        CrossflowExecutorApp, NextOperation, NodeBuilderOptions, OperationRef, output_ref,
    };
    use futures_util::SinkExt;
    use mime_guess::mime;
    use serde_json::json;
    use std::thread;
    use tower::ServiceExt;

    use super::*;

    struct TestFixture<CleanupFn> {
        router: Router,
        cleanup_test: CleanupFn,
    }

    async fn setup_test() -> TestFixture<impl FnOnce()> {
        let mut registry = DiagramElementRegistry::new();
        registry.register_node_builder(NodeBuilderOptions::new("add7"), |builder, _config: ()| {
            builder.create_map_block(|req: i32| req + 7)
        });

        let (send_stop, mut recv_stop) = tokio::sync::oneshot::channel::<()>();
        let (router_sender, router_receiver) = tokio::sync::oneshot::channel();

        let join_handle = thread::spawn(move || {
            // We need to instantiate the App inside the thread that it will run
            // inside because App is no longer Send as of Bevy 0.14.
            let mut app = bevy_app::App::new();
            app.add_plugins(CrossflowExecutorApp::default());
            app.add_systems(
                bevy_app::Update,
                move |mut app_exit: bevy_ecs::event::EventWriter<bevy_app::AppExit>| {
                    if let Ok(_) = recv_stop.try_recv() {
                        app_exit.write_default();
                    }
                },
            );

            let router = new_router(&mut app, registry, ExecutorOptions::default());
            let _ = router_sender.send(router);

            app.run();
        });

        let router = router_receiver.await.unwrap();

        TestFixture {
            router,
            cleanup_test: move || {
                send_stop.send(()).unwrap();
                join_handle.join().unwrap();
            },
        }
    }

    fn new_add7_diagram() -> Diagram {
        Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "add7",
            "ops": {
                "add7": {
                    "type": "node",
                    "builder": "add7",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap()
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_post_run() {
        let TestFixture {
            router,
            cleanup_test,
        } = setup_test().await;

        let diagram = new_add7_diagram();

        let request_body = PostRunRequest {
            diagram,
            request: serde_json::Value::from(5),
        };
        let response = router
            .oneshot(
                Request::post("/run")
                    .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string())
                    .body(serde_json::to_string(&request_body).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            mime::APPLICATION_JSON
        );
        let resp_bytes = body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let resp_str = str::from_utf8(&resp_bytes).unwrap();
        let resp: i32 = serde_json::from_str(resp_str).unwrap();
        assert_eq!(resp, 12);

        cleanup_test();
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_post_compatibility() {
        let TestFixture {
            router,
            cleanup_test,
        } = setup_test().await;

        let source_port: PortRef = output_ref(&"add7".into()).next().into();
        let target_port: PortRef = OperationRef::Terminate(Default::default()).into();
        let request_body = CompatibilityRequest {
            diagram: new_add7_diagram(),
            connections: vec![CompatibilityConnection {
                id: "add7-to-terminate".to_string(),
                focus_ports: vec![source_port.clone(), target_port.clone()],
                source_port: Some(source_port),
                target_port: Some(target_port),
            }],
        };
        let response = router
            .oneshot(
                Request::post("/compatibility")
                    .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string())
                    .body(serde_json::to_string(&request_body).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let resp_bytes = body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let resp_str = str::from_utf8(&resp_bytes).unwrap();
        let resp: CompatibilityResponse = serde_json::from_str(resp_str).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].status, CompatibilityStatus::Compatible);
        assert!(!resp.results[0].provisional);

        cleanup_test();
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_post_compatibility_serializes_provisional_result() {
        let TestFixture {
            router,
            cleanup_test,
        } = setup_test().await;

        let buffer_port: PortRef = (&NextOperation::Name("buffer".into())).into();
        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "builtin": "dispose" },
            "ops": {
                "buffer": {
                    "type": "buffer"
                },
                "listen": {
                    "type": "listen",
                    "buffers": ["buffer"],
                    "next": { "builtin": "dispose" }
                }
            }
        }))
        .unwrap();
        let request_body = CompatibilityRequest {
            diagram,
            connections: vec![CompatibilityConnection {
                id: "buffer-to-listen".to_string(),
                focus_ports: vec![buffer_port],
                source_port: None,
                target_port: None,
            }],
        };
        let response = router
            .oneshot(
                Request::post("/compatibility")
                    .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string())
                    .body(serde_json::to_string(&request_body).unwrap())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let resp_bytes = body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let resp_str = str::from_utf8(&resp_bytes).unwrap();
        assert!(resp_str.contains("\"provisional\":true"));
        let resp: CompatibilityResponse = serde_json::from_str(resp_str).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].status, CompatibilityStatus::Unknown);
        assert!(resp.results[0].provisional);

        cleanup_test();
    }

    struct WsTestFixture<CleanupFn> {
        executor_state: ExecutorState,
        cleanup_test: CleanupFn,
    }

    fn setup_ws_test() -> WsTestFixture<impl FnOnce()> {
        let (send_stop, mut recv_stop) = tokio::sync::oneshot::channel::<()>();
        let (state_sender, state_receiver) = std::sync::mpsc::channel();

        let join_handle = thread::spawn(move || {
            let mut app = bevy_app::App::new();
            app.add_plugins(CrossflowExecutorApp::default());
            app.add_systems(
                bevy_app::Update,
                move |mut app_exit: bevy_ecs::event::EventWriter<bevy_app::AppExit>| {
                    if let Ok(_) = recv_stop.try_recv() {
                        app_exit.write_default();
                    }
                },
            );

            let mut registry = DiagramElementRegistry::new();
            registry
                .register_node_builder(NodeBuilderOptions::new("add7"), |builder, _config: ()| {
                    builder.create_map_block(|req: i32| req + 7)
                });
            let executor_state = setup_bevy_app(
                &mut app.sub_apps_mut().main,
                registry,
                &ExecutorOptions::default(),
            );
            state_sender.send(executor_state).unwrap();

            app.run();
        });

        let executor_state = state_receiver.recv().unwrap();

        WsTestFixture {
            executor_state,
            cleanup_test: move || {
                send_stop.send(()).unwrap();
                join_handle.join().unwrap();
            },
        }
    }

    #[ignore = "tracing events in `crossflow` is delayed"]
    #[tokio::test]
    #[test_log::test]
    async fn test_ws_interaction() {
        use futures_util::StreamExt;

        let WsTestFixture {
            executor_state,
            cleanup_test,
        } = setup_ws_test();

        let mut diagram = new_add7_diagram();
        diagram.default_trace = crossflow::TraceToggle::On;

        let request_body = PostRunRequest {
            diagram,
            request: serde_json::Value::from(5),
        };

        // Need to use "futures" channels rather than "tokio" channels as they implement `Sink` and
        // `Stream`
        let (socket_write, mut test_rx) = futures_channel::mpsc::channel(1024);
        let (mut test_tx, socket_read) = futures_channel::mpsc::channel(1024);

        tokio::spawn(ws_interaction(
            socket_write,
            socket_read,
            State(executor_state),
        ));

        test_tx
            .send(Ok(ws::Message::Text(
                serde_json::to_string(&request_body).unwrap().into(),
            )))
            .await
            .unwrap();

        // There should be 4 feedback messages: add7 starts, add7 finishes,
        // terminate starts, and terminate finishes.
        for _ in 0..4 {
            let msg = test_rx.next().await.unwrap();
            let feedback_msg: InteractionSessionMessage =
                serde_json::from_slice(msg.into_text().unwrap().as_bytes()).unwrap();
            let feedback = match feedback_msg {
                InteractionSessionMessage::Feedback(feedback) => feedback,
                _ => {
                    panic!("expected feedback message");
                }
            };
            assert!(matches!(
                feedback,
                InteractionSessionFeedback::OperationStarted(_)
                    | InteractionSessionFeedback::OperationFinished(_)
            ));
        }

        let resp_msg = test_rx.next().await.unwrap();
        let resp_text = resp_msg.into_text().unwrap();
        let resp_msg: InteractionSessionMessage =
            serde_json::from_slice(resp_text.as_bytes()).unwrap();
        let resp = match resp_msg {
            InteractionSessionMessage::Finish(InteractionSessionEnd::Ok(resp)) => resp,
            _ => {
                panic!("expected response to be Ok");
            }
        };
        assert_eq!(resp, serde_json::Value::from(12));

        cleanup_test();
    }
}
