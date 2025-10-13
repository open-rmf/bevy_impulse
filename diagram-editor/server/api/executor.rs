#[cfg(feature = "debug")]
use axum::{
    extract::ws,
    routing::{self},
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{self, Response},
    Json,
};
#[cfg(feature = "router")]
use axum::{routing::post, Router};
use bevy_ecs::{prelude::Entity, schedule::IntoScheduleConfigs};
use bevy_impulse::{trace, Diagram, DiagramElementRegistry, OperationStarted, Promise, RequestExt};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::error;
#[cfg(feature = "debug")]
use tracing::warn;

#[cfg(feature = "debug")]
use super::websocket::{WebsocketSinkExt, WebsocketStreamExt};
use crate::api::error_responses::WorkflowCancelledResponse;

#[cfg(feature = "debug")]
type BroadcastRecvError = tokio::sync::broadcast::error::RecvError;

type WorkflowResponseResult =
    Result<(Promise<serde_json::Value>, Entity), Box<dyn Error + Send + Sync>>;
type WorkflowResponseSender = tokio::sync::oneshot::Sender<WorkflowResponseResult>;

type WorkflowFeedback = OperationStarted;

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
        Ok((promise, workflow)) => {
            let promise_state = promise.await;
            if let Err(err) = state.despawn_chan.send(workflow).await {
                error!("Failed to request workflow despawn: {err}");
            }

            if promise_state.is_available() {
                if let Some(result) = promise_state.available() {
                    Ok(result)
                } else {
                    Err(StatusCode::INTERNAL_SERVER_ERROR.into())
                }
            } else if promise_state.is_cancelled() {
                if let Some(cancellation) = promise_state.cancellation() {
                    Err(WorkflowCancelledResponse(cancellation).into())
                } else {
                    Err(StatusCode::INTERNAL_SERVER_ERROR.into())
                }
            } else {
                Err(StatusCode::INTERNAL_SERVER_ERROR.into())
            }
        }
        Err(err) => Err(Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .body(err.to_string())
            .map_or(StatusCode::INTERNAL_SERVER_ERROR.into(), |resp| resp.into())),
    } as response::Result<serde_json::Value>)?;

    Ok(Json(response))
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Deserialize))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DebugSessionEnd {
    Ok(serde_json::Value),
    Err(String),
}

#[cfg(feature = "debug")]
impl DebugSessionEnd {
    fn err_from_status_code(status_code: StatusCode) -> Self {
        Self::Err(status_code.to_string())
    }
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Deserialize))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DebugSessionFeedback {
    OperationStarted(String),
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Deserialize))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum DebugSessionMessage {
    Feedback(DebugSessionFeedback),
    Finish(DebugSessionEnd),
}

/// Start a debug session.
#[cfg(feature = "debug")]
async fn ws_debug<W, R, Text>(mut write: W, mut read: R, state: State<ExecutorState>)
where
    W: WebsocketSinkExt<DebugSessionMessage>,
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
            .send_json(&DebugSessionMessage::Finish(
                DebugSessionEnd::err_from_status_code(StatusCode::INTERNAL_SERVER_ERROR),
            ))
            .await;
        return;
    }

    let write = tokio::sync::Mutex::new(write);

    let process_response = async || {
        let response_result = response_rx.await;

        let workflow_response = match response_result {
            Ok(response) => response,
            Err(err) => {
                error!("{}", err);
                write
                    .lock()
                    .await
                    .send_json(&DebugSessionMessage::Finish(
                        DebugSessionEnd::err_from_status_code(StatusCode::INTERNAL_SERVER_ERROR),
                    ))
                    .await;
                return;
            }
        };

        match workflow_response {
            // type annotations needed on `promise.await`
            Ok(promise) => match promise.await.available() {
                Some(result) => {
                    write
                        .lock()
                        .await
                        .send_json(&DebugSessionMessage::Finish(DebugSessionEnd::Ok(result)))
                        .await;
                }
                None => {
                    write
                        .lock()
                        .await
                        .send_json(&DebugSessionMessage::Finish(
                            DebugSessionEnd::err_from_status_code(
                                StatusCode::INTERNAL_SERVER_ERROR,
                            ),
                        ))
                        .await;
                    return;
                }
            },
            Err(err) => {
                write
                    .lock()
                    .await
                    .send_json(&DebugSessionMessage::Finish(DebugSessionEnd::Err(
                        err.to_string(),
                    )))
                    .await;
                return;
            }
        };
    };

    let mut process_feedback = async || loop {
        let feedback = feedback_rx.recv().await;

        match feedback {
            Ok(feedback) => {
                let op_id = if let Some(id) = feedback.info.id() {
                    id.to_string()
                } else {
                    "[unknown]".to_string()
                };

                write
                    .lock()
                    .await
                    .send_json(&DebugSessionMessage::Feedback(
                        DebugSessionFeedback::OperationStarted(op_id),
                    ))
                    .await;
            }
            Err(e) => match e {
                BroadcastRecvError::Closed => {
                    break;
                }
                BroadcastRecvError::Lagged(_) => {
                    warn!("{}", e);
                    break;
                }
            },
        }
    };

    tokio::select! {
        _ = process_response() => {},
        _ = process_feedback() => {},
    };
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
            let maybe_promise = match ctx.diagram.spawn_io_workflow(&mut cmds, registry) {
                Ok(workflow) => {
                    let impulse = cmds.request(ctx.request, workflow);
                    let session = impulse.session_id();
                    let promise: Promise<serde_json::Value> = impulse.take_response();
                    if let Some(feedback_tx) = ctx.feedback_tx {
                        cmds.entity(session).insert(feedback_tx);
                    }
                    Ok((promise, workflow.provider()))
                }
                Err(err) => Err(err.into()),
            };
            // assuming that workflows are automatically cancelled when the promise is dropped.
            if let Err(_) = ctx.response_tx.send(maybe_promise) {
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

fn debug_feedback(
    mut op_started: bevy_ecs::event::EventReader<trace::OperationStarted>,
    feedback_query: bevy_ecs::system::Query<&FeedbackSender>,
) {
    for ev in op_started.read() {
        // not sure if it is working as intended, but the root session is in the 2nd last
        // item, not the last as described in `session_stack` docs.
        let session = match ev.session_stack.iter().rev().skip(1).next() {
            Some(session) => session,
            None => {
                continue;
            }
        };
        match feedback_query.get(*session) {
            Ok(feedback_tx) => {
                if let Err(e) = feedback_tx.0.send(ev.clone()) {
                    error!("{}", e);
                }
            }
            Err(_) => {
                // the session has no feedback channel
            }
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
    app.add_systems(bevy_app::Update, debug_feedback.after(execute_requests));
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

    let router = Router::new().route("/run", post(post_run));

    #[cfg(feature = "debug")]
    let router = router.route(
        "/debug",
        routing::any(
            async |ws: ws::WebSocketUpgrade, state: State<ExecutorState>| {
                ws.on_upgrade(|socket| {
                    use futures_util::StreamExt;

                    let (write, read) = socket.split();
                    ws_debug(write, read, state)
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
    #[cfg(feature = "debug")]
    use axum::extract::ws;
    use axum::{
        body,
        http::{header, Request},
    };
    use bevy_impulse::{ImpulseAppPlugin, NodeBuilderOptions};
    #[cfg(feature = "debug")]
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
            app.add_plugins(ImpulseAppPlugin::default());
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

    #[cfg(feature = "debug")]
    struct WsTestFixture<CleanupFn> {
        executor_state: ExecutorState,
        cleanup_test: CleanupFn,
    }

    #[cfg(feature = "debug")]
    fn setup_ws_test() -> WsTestFixture<impl FnOnce()> {
        let mut app = bevy_app::App::new();
        app.add_plugins(ImpulseAppPlugin::default());
        let (send_stop, mut recv_stop) = tokio::sync::oneshot::channel::<()>();
        app.add_systems(
            bevy_app::Update,
            move |mut app_exit: bevy_ecs::event::EventWriter<bevy_app::AppExit>| {
                if let Ok(_) = recv_stop.try_recv() {
                    app_exit.send_default();
                }
            },
        );

        let mut registry = DiagramElementRegistry::new();
        registry.register_node_builder(NodeBuilderOptions::new("add7"), |builder, _config: ()| {
            builder.create_map_block(|req: i32| req + 7)
        });
        let executor_state = setup_bevy_app(&mut app, registry, &ExecutorOptions::default());

        let join_handle = thread::spawn(move || {
            app.run();
        });

        WsTestFixture {
            executor_state,
            cleanup_test: move || {
                send_stop.send(()).unwrap();
                join_handle.join().unwrap();
            },
        }
    }

    #[cfg(feature = "debug")]
    #[ignore = "tracing events in `bevy_impulse` is delayed"]
    #[tokio::test]
    #[test_log::test]
    async fn test_ws_debug() {
        use futures_util::StreamExt;

        let WsTestFixture {
            executor_state,
            cleanup_test,
        } = setup_ws_test();

        let mut diagram = new_add7_diagram();
        diagram.default_trace = bevy_impulse::TraceToggle::On;

        let request_body = PostRunRequest {
            diagram,
            request: serde_json::Value::from(5),
        };

        // Need to use "futures" channels rather than "tokio" channels as they implement `Sink` and
        // `Stream`
        let (socket_write, mut test_rx) = futures_channel::mpsc::channel(1024);
        let (mut test_tx, socket_read) = futures_channel::mpsc::channel(1024);

        tokio::spawn(ws_debug(socket_write, socket_read, State(executor_state)));

        test_tx
            .send(Ok(ws::Message::Text(
                serde_json::to_string(&request_body).unwrap().into(),
            )))
            .await
            .unwrap();

        // there should be 2 feedback messages
        for _ in 0..2 {
            let msg = test_rx.next().await.unwrap();
            let feedback_msg: DebugSessionMessage =
                serde_json::from_slice(msg.into_text().unwrap().as_bytes()).unwrap();
            let feedback = match feedback_msg {
                DebugSessionMessage::Feedback(feedback) => feedback,
                _ => {
                    panic!("expected feedback message");
                }
            };
            assert!(matches!(
                feedback,
                DebugSessionFeedback::OperationStarted(_)
            ));
        }

        let resp_msg = test_rx.next().await.unwrap();
        let resp_text = resp_msg.into_text().unwrap();
        let resp: DebugSessionEnd = serde_json::from_slice(resp_text.as_bytes()).unwrap();
        let resp = match resp {
            DebugSessionEnd::Ok(resp) => resp,
            _ => {
                panic!("expected response to be Ok");
            }
        };
        assert_eq!(resp, serde_json::Value::from(12));

        cleanup_test();
    }
}
