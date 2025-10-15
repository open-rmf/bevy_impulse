use crate::api::{api_router, ApiOptions};
#[cfg(feature = "frontend")]
use crate::frontend;
use axum::Router;
use crossflow::DiagramElementRegistry;

#[derive(Default)]
#[non_exhaustive]
pub struct ServerOptions {
    pub api: ApiOptions,
}

/// Create a new [`axum::Router`] with routes for the diagram editor.
pub fn new_router(
    app: &mut bevy_app::App,
    registry: DiagramElementRegistry,
    options: ServerOptions,
) -> Router {
    let router = Router::new();

    #[cfg(feature = "frontend")]
    let router = frontend::with_frontend_routes(router);

    router.nest("/api", api_router(app, registry, options.api))
}
