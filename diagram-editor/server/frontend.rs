use axum::{
    body::Body,
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use flate2::read::GzDecoder;
use std::{collections::HashMap, io::Read};
use tar::Archive;

// This will include the bytes of the dist.tar.gz file from the OUT_DIR
// The path is constructed at compile time.
const DIST_TAR_GZ: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/dist.tar.gz"));

fn load_dist() -> HashMap<std::path::PathBuf, Vec<u8>> {
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
}

async fn handle_text_html(html: Vec<u8>) -> impl IntoResponse {
    let mime_type = "text/html";
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime_type)
        .body(Body::from(html))
        .unwrap_or_else(|_| internal_server_error_response())
}

async fn handle_asset(
    dist: HashMap<std::path::PathBuf, Vec<u8>>,
    Path(path_str): Path<String>,
) -> impl IntoResponse {
    let requested_file_path = std::path::PathBuf::from(path_str);

    // Attempt to serve the specific file
    if let Some(file_bytes) = dist.get(&requested_file_path) {
        let mime_type = mime_guess::from_path(&requested_file_path)
            .first_or_octet_stream()
            .to_string();

        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, mime_type)
            .body(Body::from(file_bytes.clone()))
            .unwrap_or_else(|_| internal_server_error_response());
    }

    return Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from("Not found"))
        .unwrap_or_else(|_| internal_server_error_response());
}

fn internal_server_error_response() -> Response {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("Internal Server Error"))
        .unwrap() // Should not fail
}

pub fn with_frontend_routes(router: Router) -> Router {
    let dist = load_dist();
    let index_html = dist
        .get(std::path::Path::new("index.html"))
        .expect("index.html not found in dist")
        .clone();

    router
        .route("/", get(move || handle_text_html(index_html)))
        .route("/{*path}", get(move |path| handle_asset(dist, path)))
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
        let mut router = with_frontend_routes(Router::new());
        let response = router
            .call(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_index_response_headers(&response);
    }

    #[tokio::test]
    async fn test_serves_index_html_with_direct_path() {
        let path = "/index.html";
        let mut router = with_frontend_routes(Router::new());
        let response = router
            .call(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_index_response_headers(&response);
    }
}
