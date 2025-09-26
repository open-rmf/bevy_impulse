use axum::{body::to_bytes, http::header, response::IntoResponse};
use mime_guess::mime;
use wasm_bindgen::JsValue;

pub(super) trait IntoJsResult {
    async fn into_js_result(self) -> Result<JsValue, JsValue>;
}

impl<T> IntoJsResult for T
where
    T: IntoResponse,
{
    /// Transform the [`axum::response::Response`] into a [`JsValue`].
    ///
    /// This only works if the response's `Content-Type` is "application/json".
    /// It will read the body until the end and deserializes the JSON, so it may not work
    /// for responses that sends data in chunks over a long period of time, or responses
    /// that sends a huge amount of data (> 4MiB).
    async fn into_js_result(self) -> Result<JsValue, JsValue> {
        const MAX_SIZE: usize = 4 * 1024 * 1024; // 4MiB

        let resp = self.into_response();
        let status = resp.status();
        if resp.status().is_success() {
            if let Some(val) = resp.headers().get(header::CONTENT_TYPE) {
                let content_type = val.to_str().map_err(|err| err.to_string())?;
                if content_type == mime::APPLICATION_JSON {
                    let body = to_bytes(resp.into_body(), MAX_SIZE)
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
            let body_bytes = to_bytes(resp.into_body(), MAX_SIZE)
                .await
                .map_err(|err| err.to_string())?;
            let body = String::from_utf8(body_bytes.to_vec()).map_err(|err| err.to_string())?;
            if body.is_empty() {
                Err(status.to_string().into())
            } else {
                Err(format!("{}: {}", status, body).into())
            }
        }
    }
}
