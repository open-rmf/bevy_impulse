use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bevy_impulse::{Cancellation, CancellationCause};
#[cfg(feature = "wasm")]
use wasm_bindgen::JsValue;

pub(super) struct WorkflowCancelledResponse<'a>(pub(super) &'a Cancellation);

impl<'a> IntoResponse for WorkflowCancelledResponse<'a> {
    fn into_response(self) -> Response {
        let msg = match *self.0.cause {
            CancellationCause::TargetDropped(_) => "target dropped",
            CancellationCause::Unreachable(_) => "unreachable",
            CancellationCause::Filtered(_) => "filtered",
            CancellationCause::Triggered(_) => "triggered",
            CancellationCause::Supplanted(_) => "supplanted",
            CancellationCause::InvalidSpan(_) => "invalid span",
            CancellationCause::CircularCollect(_) => "circular collect",
            CancellationCause::Undeliverable => "undeliverable",
            CancellationCause::PoisonedMutexInPromise => "poisoned mutex in promise",
            CancellationCause::Broken(_) => "broken",
        };
        Response::builder()
            .status(StatusCode::UNPROCESSABLE_ENTITY)
            .body(format!("workflow cancelled: {}", msg))
            .map_or(StatusCode::INTERNAL_SERVER_ERROR.into_response(), |resp| {
                resp.into_response()
            })
    }
}

#[cfg(feature = "wasm")]
pub(super) trait IntoJsResult {
    async fn into_js_result(self) -> Result<JsValue, JsValue>;
}

#[cfg(feature = "wasm")]
impl<T> IntoJsResult for T
where
    T: IntoResponse,
{
    async fn into_js_result(self) -> Result<JsValue, JsValue> {
        use axum::body::to_bytes;
        use axum::http::header;
        use mime_guess::mime;

        const BUFFER_SIZE: usize = 4 * 1024 * 1024 /* 4MiB */;

        let resp = self.into_response();
        if resp.status().is_success() {
            if let Some(val) = resp.headers().get(header::CONTENT_TYPE) {
                let content_type = val.to_str().map_err(|err| err.to_string())?;
                if content_type == mime::APPLICATION_JSON {
                    let body = to_bytes(resp.into_body(), BUFFER_SIZE)
                        .await
                        .map_err(|err| err.to_string())?;
                    let value: serde_json::Value =
                        serde_json::from_slice(&body).map_err(|err| err.to_string())?;
                    Ok(serde_wasm_bindgen::to_value(&value)?)
                } else {
                    Err(JsValue::from_str("response must be JSON"))
                }
            } else {
                Err(JsValue::from_str("response must be JSON"))
            }
        } else {
            let body_bytes = to_bytes(resp.into_body(), BUFFER_SIZE)
                .await
                .map_err(|err| JsValue::from_str(&err.to_string()))?;
            let body = String::from_utf8(body_bytes.to_vec()).map_err(|err| err.to_string())?;
            Ok(body.into())
        }
    }
}
