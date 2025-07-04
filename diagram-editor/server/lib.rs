use axum::Router;
use bevy_impulse::DiagramElementRegistry;

use crate::api::api_router;

mod api;
#[cfg(feature = "frontend")]
mod frontend;

/// Create a new [`axum::Router`] with routes for the diagram editor.
pub fn new_router(registry: DiagramElementRegistry) -> Router {
    let router = Router::new();

    #[cfg(feature = "frontend")]
    let router = frontend::add_frontend_routes(router);

    router.nest("/api", api_router(registry))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{header, Request, StatusCode},
        response::Response,
    };
    use tower::Service;

    fn assert_index_response_headers(response: &Response) {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .expect("Content-Type header missing")
                .to_str()
                .unwrap(),
            "text/html"
        );
    }

    #[tokio::test]
    async fn test_serves_index_html_with_root_url() {
        let mut app = new_router(DiagramElementRegistry::new());
        let response = app
            .call(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_index_response_headers(&response);
    }

    #[tokio::test]
    async fn test_serves_index_html_with_direct_path() {
        let path = "/index.html";
        let mut app = new_router(DiagramElementRegistry::new());
        let response = app
            .call(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_index_response_headers(&response);
    }
}
