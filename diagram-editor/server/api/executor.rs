use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use bevy_app::{AppExit, Update};
use bevy_ecs::{
    event::EventWriter,
    system::{ResMut, Resource},
};
use bevy_impulse::{Diagram, DiagramElementRegistry, ImpulseAppPlugin, Promise, RequestExt};
use serde::Deserialize;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{debug, error};

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

#[cfg_attr(test, derive(serde::Serialize))]
#[derive(Deserialize)]
struct PostRunRequest {
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

#[derive(Resource)]
struct RequestReceiver(tokio::sync::mpsc::Receiver<Context>);

/// Receives a request from executor service and schedules the workflow.
fn execute_requests(
    mut rx: ResMut<RequestReceiver>,
    mut cmds: bevy_ecs::system::Commands,
    mut app_exit_events: EventWriter<AppExit>,
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

pub(super) fn new_router(registry: DiagramElementRegistry, options: ExecutorOptions) -> Router {
    let (tx, rx) = tokio::sync::mpsc::channel::<Context>(10);

    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());
    app.insert_resource(RequestReceiver(rx));
    app.add_systems(Update, execute_requests);
    thread::spawn(move || {
        debug!("bevy app started");
        app.run();
        debug!("bevy app exited");
    });

    Router::new()
        .route("/run", post(post_run))
        .with_state(ExecutorState {
            registry: Arc::new(Mutex::new(registry)),
            send_chan: tx,
            response_timeout: options.response_timeout,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body,
        http::{header, Request},
    };
    use bevy_impulse::NodeBuilderOptions;
    use mime_guess::mime;
    use serde_json::json;
    use tower::Service;

    #[tokio::test]
    #[test_log::test]
    async fn test_post_run() {
        let mut registry = DiagramElementRegistry::new();
        registry.register_node_builder(NodeBuilderOptions::new("add7"), |builder, _config: ()| {
            builder.create_map_block(|req: i32| req + 7)
        });

        let diagram = Diagram::from_json(json!({
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
        .unwrap();

        let mut router = new_router(registry, ExecutorOptions::default());
        let request_body = PostRunRequest {
            diagram,
            request: serde_json::Value::from(5),
        };
        let response = router
            .call(
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
    }
}
