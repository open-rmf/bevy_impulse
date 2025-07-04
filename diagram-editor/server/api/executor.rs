use std::sync::{Arc, Mutex};

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use bevy_app::Update;
use bevy_impulse::{Diagram, DiagramElementRegistry, ImpulseAppPlugin, Promise, RequestExt};
use serde::Deserialize;
use tracing::error;

struct Context {
    request: serde_json::Value,
    diagram: Diagram,
    registry: Arc<Mutex<DiagramElementRegistry>>,
    response_tx: tokio::sync::oneshot::Sender<Promise<serde_json::Value>>,
}

#[derive(Clone)]
struct ExecutorState {
    registry: Arc<Mutex<DiagramElementRegistry>>,
    send_chan: tokio::sync::mpsc::Sender<Context>,
}

#[derive(Deserialize)]
struct RunRequest {
    request: serde_json::Value,
    diagram: Diagram,
}

async fn post_run(
    state: State<ExecutorState>,
    Json(request_data): Json<RunRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    state
        .send_chan
        .send(Context {
            registry: state.registry.clone(),
            request: request_data.request,
            diagram: request_data.diagram,
            response_tx,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result = response_rx
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let result = result
        .await
        .available()
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(result))
}

pub fn router(registry: DiagramElementRegistry) -> Router {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Context>(
        1, // channel capacity, this controls the number of workflows that can execute in parallel
    );

    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());

    app.add_systems(Update, move |mut cmds: bevy_ecs::system::Commands| {
        if let Ok(ctx) = rx.try_recv() {
            let workflow = ctx
                .diagram
                .spawn_io_workflow(&mut cmds, &*ctx.registry.lock().unwrap())
                .unwrap();
            let promise: Promise<serde_json::Value> =
                cmds.request(ctx.request, workflow).take_response();
            if let Err(_) = ctx.response_tx.send(promise) {
                error!("failed to send response")
            }
        }
    });

    Router::new()
        .route("/run", post(post_run))
        .with_state(ExecutorState {
            registry: Arc::new(Mutex::new(registry)),
            send_chan: tx,
        })
}
