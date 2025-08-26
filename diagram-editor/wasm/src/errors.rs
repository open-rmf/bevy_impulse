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
    async fn into_js_result(self) -> Result<JsValue, JsValue> {
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
