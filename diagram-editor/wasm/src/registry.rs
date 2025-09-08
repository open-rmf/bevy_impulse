use serde::Serialize;
use wasm_bindgen::prelude::*;

use super::globals;

#[wasm_bindgen]
pub fn get_registry() -> JsValue {
    let executor_state = globals::executor_state();
    executor_state
        .registry
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .expect("failed to serialize registry")
}

#[cfg(test)]
mod tests {
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::{registry::get_registry, test_utils::setup_test};

    #[wasm_bindgen_test]
    fn test_get_registry() {
        setup_test();

        serde_wasm_bindgen::from_value::<serde_json::Value>(get_registry()).unwrap();
    }
}
