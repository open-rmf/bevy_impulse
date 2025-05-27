use axum::{
    body::Body,
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::io::Read;
use std::sync::LazyLock;
use tar::Archive;

// This will include the bytes of the dist.tar.gz file from the OUT_DIR
// The path is constructed at compile time.
const DIST_TAR_GZ: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/dist.tar.gz"));

static DIST: LazyLock<HashMap<std::path::PathBuf, Vec<u8>>> = LazyLock::new(|| {
    let mut archive = Archive::new(GzDecoder::new(DIST_TAR_GZ));
    let mut files = HashMap::new();

    for entry_result in archive
        .entries()
        .expect("Failed to read entries from tar.gz")
    {
        let mut entry = entry_result.expect("Failed to get entry from tar.gz");
        let path = entry
            .path()
            .expect("Failed to get path from entry")
            .into_owned();
        // Paths from tar, given build.rs `append_dir_all(".", "dist")`, will be like "index.html", "js/app.js"
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .expect("Failed to read entry data");
        files.insert(path, data);
    }
    if !files.contains_key(&std::path::PathBuf::from("index.html")) {
        eprintln!(
            "Warning: 'index.html' not found in embedded DIST assets. SPA fallback might not work."
        );
    }
    files
});

fn internal_server_error_response() -> Response {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("Internal Server Error"))
        .unwrap() // Should not fail
}

// Handles requests for assets, with SPA fallback logic.
async fn asset_request_handler(Path(path_str): Path<String>) -> impl IntoResponse {
    // Path is relative to the route, e.g., "main.js" or "some/spa/route".
    // It does not start with a '/'.
    let mut requested_file_path = std::path::PathBuf::from(path_str);

    // If the path is empty (e.g. from root_asset_handler), it means "index.html".
    if requested_file_path.as_os_str().is_empty() {
        requested_file_path = std::path::PathBuf::from("index.html");
    }

    // Attempt to serve the specific file
    if let Some(file_bytes) = DIST.get(&requested_file_path) {
        let mime_type = mime_guess::from_path(&requested_file_path)
            .first_or_octet_stream()
            .to_string();

        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, mime_type)
            .body(Body::from(file_bytes.clone()))
            .unwrap_or_else(|_| internal_server_error_response());
    }

    // If the specific file is not found:
    // 1. If it has an extension, it's likely a request for a missing asset (e.g., /js/nonexistent.js). Return 404.
    // 2. If it does not have an extension, it's likely an SPA route (e.g., /my-view). Serve index.html.
    if requested_file_path.extension().is_some() {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(format!(
                "Asset not found: {:?}",
                requested_file_path
            )))
            .unwrap_or_else(|_| internal_server_error_response());
    }

    // Fallback to serving index.html for SPA routing (path without extension not found)
    if let Some(index_bytes) = DIST.get(&std::path::PathBuf::from("index.html")) {
        let mime_type = mime_guess::from_path("index.html") // index.html always has this mime type
            .first_or_octet_stream()
            .to_string();

        Response::builder()
            .status(StatusCode::OK) // SPA fallback returns 200 with index.html content
            .header(header::CONTENT_TYPE, mime_type)
            .body(Body::from(index_bytes.clone()))
            .unwrap_or_else(|_| internal_server_error_response())
    } else {
        // This is a critical issue: index.html itself is missing from the embedded assets.
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(
                "Fatal error: index.html not found in embedded assets.",
            ))
            .unwrap_or_else(|_| internal_server_error_response())
    }
}

// Specific handler for the root of the mounted path, ensuring it serves index.html.
// It calls asset_request_handler with an empty path, which asset_request_handler interprets as "index.html".
async fn root_asset_handler() -> impl IntoResponse {
    asset_request_handler(Path(String::new())).await
}

/// Nests the diagram editor under `/diagram_editor`.
/// The router MUST be nested under `/diagram_editor` because it is fixed when the frontend is compiled.
pub fn nest_diagram_router(router: Router) -> Router {
    router.nest(
        "/diagram_editor",
        Router::new()
            .route("/", get(root_asset_handler))
            .route("/{*path}", get(asset_request_handler)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{header, Request, StatusCode},
    };
    use tower::Service;

    // Helper to get expected index.html content.
    // Panics if index.html is not found in the embedded DIST assets,
    // which would indicate an issue with the build process or frontend assets.
    fn get_expected_index_html_content() -> Vec<u8> {
        let index_html_path = std::path::PathBuf::from("index.html");
        DIST.get(&index_html_path)
            .expect("index.html not found in DIST. Ensure build.rs ran successfully and pnpm build produced index.html.")
            .clone()
    }

    #[tokio::test]
    async fn test_serves_index_html() {
        let expected_content = get_expected_index_html_content();
        let public_path = "/diagram_editor";
        let mut app = nest_diagram_router(Router::new());

        // Test request to "/diagram_editor"
        let response = app
            .call(
                Request::builder()
                    .uri(format!("{}", public_path))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

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
        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body_bytes, expected_content);
    }
}
