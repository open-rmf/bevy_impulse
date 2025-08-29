mod errors;
mod executor;
mod globals;
#[cfg(target_arch = "wasm32")]
#[cfg(test)]
mod test_utils;

pub use executor::*;
pub use globals::*;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
compile_error!("This crate only supports wasm32-unknown-unknown.");
