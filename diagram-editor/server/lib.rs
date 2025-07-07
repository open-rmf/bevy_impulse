use axum::Router;
use bevy_impulse::DiagramElementRegistry;

use crate::api::{api_router, ApiOptions};

pub mod api;
#[cfg(feature = "frontend")]
mod frontend;

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
