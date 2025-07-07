use axum::{routing::get, Router};
use bevy_impulse::DiagramElementRegistry;

pub mod executor;

#[derive(Default)]
#[non_exhaustive]
pub struct ApiOptions {
    pub executor: executor::ExecutorOptions,
}

pub fn api_router(registry: DiagramElementRegistry, options: ApiOptions) -> Router {
    Router::new()
        .route(
            "/registry",
            get(axum::Json(
                serde_json::to_value(&registry).expect("failed to serialize registry"),
            )),
        )
        .nest(
            "/executor",
            executor::new_router(registry, options.executor),
        )
}

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
        let mut registry = DiagramElementRegistry::new();
        registry.register_node_builder(NodeBuilderOptions::new("x2"), |builder, _config: ()| {
            builder.create_map_block(|req: f64| req * 2.0)
        });
        let mut router = api_router(registry, ApiOptions::default());

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
