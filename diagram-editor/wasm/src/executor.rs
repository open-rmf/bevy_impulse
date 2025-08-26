use axum::{extract::State, Json};
use bevy_impulse_diagram_editor::api;
use wasm_bindgen::prelude::*;

use crate::errors::IntoJsResult;

#[wasm_bindgen]
pub async fn post_run(request: JsValue) -> Result<JsValue, JsValue> {
    let executor_state = super::globals::executor_state();
    api::executor::post_run(
        State(executor_state.clone()),
        Json(serde_wasm_bindgen::from_value(request).unwrap()),
    )
    .await
    .into_js_result()
    .await
}
