mod error_responses;
pub mod executor;
#[cfg(feature = "debug")]
mod websocket;

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
#[cfg(feature = "router")]
use axum::{routing::get, Router};
use bevy_impulse::DiagramElementRegistry;
use mime_guess::mime;

#[derive(Default)]
#[non_exhaustive]
pub struct ApiOptions {
    pub executor: executor::ExecutorOptions,
}

#[derive(Clone)]
pub struct RegistryResponse(String);

impl IntoResponse for RegistryResponse {
    fn into_response(self) -> Response {
        Response::builder()
            .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.as_ref())
            .body(self.0.into())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

#[cfg(feature = "json_schema")]
impl schemars::JsonSchema for RegistryResponse {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        DiagramElementRegistry::schema_name()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        DiagramElementRegistry::schema_id()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        DiagramElementRegistry::json_schema(generator)
    }

    fn inline_schema() -> bool {
        DiagramElementRegistry::inline_schema()
    }
}

impl RegistryResponse {
    pub fn new(value: &DiagramElementRegistry) -> serde_json::Result<Self> {
        let serialized = serde_json::to_string(value)?;
        Ok(Self(serialized))
    }
}

#[cfg(feature = "router")]
pub fn api_router(
    app: &mut bevy_app::App,
    registry: DiagramElementRegistry,
    options: ApiOptions,
) -> Router {
    let registry_resp = RegistryResponse::new(&registry).expect("failed to serialize registry");
    Router::new().route("/registry", get(registry_resp)).nest(
        "/executor",
        executor::new_router(app, registry, options.executor),
    )
}

#[cfg(feature = "router")]
#[cfg(test)]
mod tests {
    use axum::{
        body::{self, Body},
        http::{header, Request, StatusCode},
    };
    use bevy_impulse::NodeBuilderOptions;
    use mime_guess::mime;
    use tower::Service;

    use super::*;

    #[tokio::test]
    async fn test_serves_registry() {
        let mut app = bevy_app::App::new();
        let mut registry = DiagramElementRegistry::new();
        registry.register_node_builder(NodeBuilderOptions::new("x2"), |builder, _config: ()| {
            builder.create_map_block(|req: f64| req * 2.0)
        });
        let mut router = api_router(&mut app, registry, ApiOptions::default());

        let path = "/registry";
        let response = router
            .call(Request::builder().uri(path).body(Body::empty()).unwrap())
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
        let resp_registry: serde_json::Value = serde_json::from_str(resp_str).unwrap();
        assert!(resp_registry.get("nodes").unwrap().get("x2").is_some());
    }
}
