use crate::api::ApiOptions;

#[cfg(feature = "router")]
pub use router::*;
#[cfg(feature = "router")]
mod router;

pub mod api;

#[cfg(feature = "frontend")]
mod frontend;

#[derive(Default)]
#[non_exhaustive]
pub struct ServerOptions {
    pub api: ApiOptions,
}
