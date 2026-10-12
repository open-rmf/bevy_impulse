use std::{future::Future, task::Poll};

use axum::{Json, extract::State};
use crossflow_diagram_editor::api::{
    self,
    executor::{CompatibilityRequest, PostRunRequest},
};
use futures::task::noop_waker;
use wasm_bindgen::prelude::*;

use super::globals;
use crate::{errors::IntoJsResult, with_bevy_sup_app_async};

#[wasm_bindgen(typescript_custom_section)]
const PostRunRequestTs: &'static str =
    r#"type PostRunRequest = import('../../types/api').PostRunRequest;"#;

#[wasm_bindgen(typescript_custom_section)]
const CompatibilityRequestTs: &'static str =
    r#"type CompatibilityRequest = import('../../types/api').CompatibilityRequest;"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "PostRunRequest")]
    pub type PostRunRequest_;

    #[wasm_bindgen(typescript_type = "CompatibilityRequest")]
    pub type CompatibilityRequest_;
}

#[wasm_bindgen]
pub struct PostRunRequestWasm(PostRunRequest);

#[wasm_bindgen]
impl PostRunRequestWasm {
    #[wasm_bindgen(constructor)]
    pub fn new(js: PostRunRequest_) -> Self {
        let request: PostRunRequest =
            serde_wasm_bindgen::from_value(js.obj).expect("failed to deserialize");
        Self(request)
    }
}

#[wasm_bindgen]
pub struct CompatibilityRequestWasm(CompatibilityRequest);

#[wasm_bindgen]
impl CompatibilityRequestWasm {
    #[wasm_bindgen(constructor)]
    pub fn new(js: CompatibilityRequest_) -> Self {
        let request: CompatibilityRequest =
            serde_wasm_bindgen::from_value(js.obj).expect("failed to deserialize");
        Self(request)
    }
}

#[wasm_bindgen]
pub async fn post_run(request: PostRunRequestWasm) -> Result<JsValue, JsValue> {
    let executor_state = globals::executor_state();

    let mut fut = Box::pin(api::executor::post_run(
        State(executor_state.clone()),
        Json(request.0),
    ));

    with_bevy_sup_app_async(async |app| {
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

#[wasm_bindgen]
pub async fn check_compatibility(request: CompatibilityRequestWasm) -> Result<JsValue, JsValue> {
    let executor_state = globals::executor_state();

    api::executor::post_compatibility(State(executor_state), Json(request.0))
        .await
        .into_js_result()
        .await
}

#[cfg(test)]
#[cfg(target_arch = "wasm32")]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crossflow::{
        Diagram, DiagramOperation, NextOperation, NodeSchema, OperationRef, PortRef, TraceSettings,
        output_ref,
    };
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
                builtin: crossflow::BuiltinTarget::Terminate,
            },
            stream_out: HashMap::new(),
            trace_settings: TraceSettings::default(),
        }));
        Arc::get_mut(&mut diagram.ops)
            .unwrap()
            .insert(Arc::clone(&add3_op_id), add_op);

        let result = post_run(PostRunRequestWasm(PostRunRequest {
            diagram,
            request: 5.into(),
        }))
        .await
        .unwrap();
        assert_eq!(result.as_f64().unwrap(), 8.0);
    }

    #[wasm_bindgen_test]
    async fn test_check_compatibility() {
        setup_test();

        let add3_op_id = Arc::from("add");
        let mut diagram = Diagram::new(NextOperation::Name(Arc::clone(&add3_op_id)));
        let add_op = Arc::new(DiagramOperation::Node(NodeSchema {
            builder: "add3".into(),
            config: serde_json::Value::Null.into(),
            next: NextOperation::Builtin {
                builtin: crossflow::BuiltinTarget::Terminate,
            },
            stream_out: HashMap::new(),
            trace_settings: TraceSettings::default(),
        }));
        Arc::get_mut(&mut diagram.ops)
            .unwrap()
            .insert(Arc::clone(&add3_op_id), add_op);

        let source_port: PortRef = output_ref(&add3_op_id).next().into();
        let target_port: PortRef = OperationRef::Terminate(Default::default()).into();
        let result = check_compatibility(CompatibilityRequestWasm(CompatibilityRequest {
            diagram,
            connections: vec![
                api::executor::CompatibilityConnection {
                    id: "unknown".to_string(),
                    focus_ports: vec![(&NextOperation::Name("missing".into())).into()],
                    source_port: None,
                    target_port: None,
                },
                api::executor::CompatibilityConnection {
                    id: "add-to-terminate".to_string(),
                    focus_ports: vec![source_port.clone(), target_port.clone()],
                    source_port: Some(source_port),
                    target_port: Some(target_port),
                },
            ],
        }))
        .await
        .unwrap();

        let response: api::executor::CompatibilityResponse =
            serde_wasm_bindgen::from_value(result).unwrap();
        assert_eq!(response.results.len(), 2);
        assert_eq!(
            response.results[0].status,
            api::executor::CompatibilityStatus::Unknown
        );
        assert_eq!(
            response.results[1].status,
            api::executor::CompatibilityStatus::Compatible
        );
        assert!(!response.results[1].provisional);
    }

    #[wasm_bindgen_test]
    async fn test_check_compatibility_provisional() {
        setup_test();

        let buffer_port: PortRef = (&NextOperation::Name("buffer".into())).into();
        let diagram = Diagram::from_json(serde_json::json!({
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

        let result = check_compatibility(CompatibilityRequestWasm(CompatibilityRequest {
            diagram,
            connections: vec![api::executor::CompatibilityConnection {
                id: "buffer-to-listen".to_string(),
                focus_ports: vec![buffer_port],
                source_port: None,
                target_port: None,
            }],
        }))
        .await
        .unwrap();

        let response: api::executor::CompatibilityResponse =
            serde_wasm_bindgen::from_value(result).unwrap();
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].status,
            api::executor::CompatibilityStatus::Unknown
        );
        assert!(response.results[0].provisional);
    }
}
