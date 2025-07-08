use super::websocket::{WebsocketSinkExt, WebsocketStreamExt};
use axum::{
    extract::{
        ws::{self},
        State,
    },
    http::StatusCode,
    routing::{self, post},
    Json, Router,
};
use bevy_ecs::{
    event::EventWriter,
    system::{ResMut, Resource},
};
use bevy_impulse::{Diagram, DiagramElementRegistry, Promise, RequestExt};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::error;

struct Context {
    diagram: Diagram,
    request: serde_json::Value,
    registry: Arc<Mutex<DiagramElementRegistry>>,
    response_tx: tokio::sync::oneshot::Sender<Promise<serde_json::Value>>,
}

#[derive(Clone)]
struct ExecutorState {
    registry: Arc<Mutex<DiagramElementRegistry>>,
    send_chan: tokio::sync::mpsc::Sender<Context>,
    response_timeout: Duration,
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Serialize))]
#[derive(Deserialize)]
pub struct PostRunRequest {
    diagram: Diagram,
    request: serde_json::Value,
}

/// Sends a request to the executor system and wait for the response.
async fn post_run(
    state: State<ExecutorState>,
    Json(body): Json<PostRunRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    if let Err(err) = state
        .send_chan
        .send(Context {
            registry: state.registry.clone(),
            diagram: body.diagram,
            request: body.request,
            response_tx,
        })
        .await
    {
        error!("{}", err);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let response = tokio::time::timeout(
        state.response_timeout,
        (async || {
            let response_promise = response_rx
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            response_promise
                .await
                .available()
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
        })(),
    )
    .await
    .map_err(|_| StatusCode::REQUEST_TIMEOUT)??;

    Ok(Json(response))
}

#[cfg_attr(feature = "json_schema", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(serde::Deserialize))]
#[derive(Serialize)]
pub enum DebugSessionEnd {
    Ok(serde_json::Value),
    Err(String),
}

/// Start a debug session.
async fn ws_debug<W, R>(mut write: W, mut read: R, state: State<ExecutorState>)
where
    W: WebsocketSinkExt,
    R: WebsocketStreamExt,
{
    let req: PostRunRequest = if let Some(req) = read.next_json().await {
        req
    } else {
        return;
    };

    // TODO: bevy_impulse does not support debugging yet, for now just run the diagram to terminate.
    let response = post_run(state, Json(req)).await;
    match response {
        Ok(resp) => {
            write.send_json(&DebugSessionEnd::Ok(resp.0)).await;
            return;
        }
        Err(status_code) => {
            write
                .send_json(&DebugSessionEnd::Err(status_code.to_string()))
                .await;
            return;
        }
    }
}

#[derive(Resource)]
struct RequestReceiver(tokio::sync::mpsc::Receiver<Context>);

/// Receives a request from executor service and schedules the workflow.
fn execute_requests(
    mut rx: ResMut<RequestReceiver>,
    mut cmds: bevy_ecs::system::Commands,
    mut app_exit_events: EventWriter<bevy_app::AppExit>,
) {
    let rx = &mut rx.0;
    match rx.try_recv() {
        Ok(ctx) => {
            let workflow = ctx
                .diagram
                .spawn_io_workflow(&mut cmds, &*ctx.registry.lock().unwrap())
                .unwrap();
            let promise: Promise<serde_json::Value> =
                cmds.request(ctx.request, workflow).take_response();
            // assuming that workflows are automatically cancelled when the promise is dropped.
            if let Err(_) = ctx.response_tx.send(promise) {
                error!("failed to send response")
            }
        }
        Err(err) => match err {
            TryRecvError::Empty => {}
            TryRecvError::Disconnected => {
                app_exit_events.send_default();
            }
        },
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

fn setup_bevy_app(
    app: &mut bevy_app::App,
    registry: DiagramElementRegistry,
    options: &ExecutorOptions,
) -> ExecutorState {
    let (request_tx, request_rx) = tokio::sync::mpsc::channel::<Context>(10);
    app.insert_resource(RequestReceiver(request_rx));
    app.add_systems(bevy_app::Update, execute_requests);
    ExecutorState {
        registry: Arc::new(Mutex::new(registry)),
        send_chan: request_tx,
        response_timeout: options.response_timeout,
    }
}

pub(super) fn new_router(
    app: &mut bevy_app::App,
    registry: DiagramElementRegistry,
    options: ExecutorOptions,
) -> Router {
    let executor_state = setup_bevy_app(app, registry, &options);

    Router::new()
        .route("/run", post(post_run))
        .route(
            "/debug",
            routing::any(
                async |ws: ws::WebSocketUpgrade, state: State<ExecutorState>| {
                    ws.on_upgrade(|socket| {
                        let (write, read) = socket.split();
                        ws_debug(write, read, state)
                    })
                },
            ),
        )
        .with_state(executor_state)
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use axum::{
        body,
        http::{header, Request},
    };
    use bevy_impulse::{ImpulseAppPlugin, NodeBuilderOptions};
    use futures_util::SinkExt;
    use mime_guess::mime;
    use serde_json::json;
    use tower::ServiceExt;

    struct TestFixture<CleanupFn> {
        router: Router,
        cleanup_test: CleanupFn,
    }

    fn setup_test() -> TestFixture<impl FnOnce()> {
        let mut registry = DiagramElementRegistry::new();
        registry.register_node_builder(NodeBuilderOptions::new("add7"), |builder, _config: ()| {
            builder.create_map_block(|req: i32| req + 7)
        });

        let mut app = bevy_app::App::new();
        app.add_plugins(ImpulseAppPlugin::default());
        let (send_stop, mut recv_stop) = tokio::sync::oneshot::channel::<()>();
        app.add_systems(
            bevy_app::Update,
            move |mut app_exit: EventWriter<bevy_app::AppExit>| {
                if let Ok(_) = recv_stop.try_recv() {
                    app_exit.send_default();
                }
            },
        );

        let router = new_router(&mut app, registry, ExecutorOptions::default());
        let join_handle = thread::spawn(move || {
            app.run();
        });

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
        } = setup_test();

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

    struct WsTestFixture<CleanupFn> {
        executor_state: ExecutorState,
        cleanup_test: CleanupFn,
    }

    fn setup_ws_test() -> WsTestFixture<impl FnOnce()> {
        let mut app = bevy_app::App::new();
        app.add_plugins(ImpulseAppPlugin::default());
        let (send_stop, mut recv_stop) = tokio::sync::oneshot::channel::<()>();
        app.add_systems(
            bevy_app::Update,
            move |mut app_exit: EventWriter<bevy_app::AppExit>| {
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

    #[tokio::test]
    #[test_log::test]
    async fn test_ws_debug() {
        let WsTestFixture {
            executor_state,
            cleanup_test,
        } = setup_ws_test();

        let diagram = new_add7_diagram();

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
