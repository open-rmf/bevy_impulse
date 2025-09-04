use std::{future::Future, task::Poll};

use axum::{extract::State, Json};
use bevy_impulse_diagram_editor::api;
use futures::task::noop_waker;
use wasm_bindgen::prelude::*;

use super::globals;
use crate::{errors::IntoJsResult, with_bevy_app_async};

#[wasm_bindgen]
pub async fn post_run(request: JsValue) -> Result<JsValue, JsValue> {
    let executor_state = globals::executor_state();
    // must convert to json first because `PostRunRequest` does not derive `#[wasm_bindgen]`.
    let json: serde_json::Value = serde_wasm_bindgen::from_value(request).unwrap();

    let mut fut = Box::pin(api::executor::post_run(
        State(executor_state.clone()),
        Json(serde_json::from_value(json).unwrap()),
    ));

    with_bevy_app_async(async |app| {
        let waker = noop_waker();
        let mut poll_ctx = std::task::Context::from_waker(&waker);
        loop {
            let poll = fut.as_mut().poll(&mut poll_ctx);
            match poll {
                Poll::Ready(response) => {
                    return response.into_js_result().await;
                }
                Poll::Pending => {}
            }
            app.update();
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use bevy_impulse::{Diagram, DiagramOperation, NextOperation, NodeSchema, TraceSettings};
    use serde_json::json;
    use wasm_bindgen_test::*;

    use super::*;
    use crate::test_utils::setup_test;

    #[wasm_bindgen_test]
    async fn test_post_run() {
        setup_test();

        let add3_op_id = Arc::from("add");
        let mut diagram = Diagram::new(NextOperation::Name(Arc::clone(&add3_op_id)));
        let add_op = Arc::new(DiagramOperation::Node(NodeSchema {
            builder: "add3".into(),
            config: serde_json::Value::Null.into(),
            next: NextOperation::Builtin {
                builtin: bevy_impulse::BuiltinTarget::Terminate,
            },
            stream_out: HashMap::new(),
            trace_settings: TraceSettings::default(),
        }));
        Arc::get_mut(&mut diagram.ops)
            .unwrap()
            .insert(Arc::clone(&add3_op_id), add_op);

        let request = json!({
            "diagram": diagram,
            "request": 5,
        });

        let result = post_run(serde_wasm_bindgen::to_value(&request).unwrap())
            .await
            .unwrap();
        assert_eq!(result.as_f64().unwrap(), 8.0);
    }
}
