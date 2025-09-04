use wasm_bindgen::prelude::*;

use super::globals;

#[wasm_bindgen]
pub fn get_registry() -> JsValue {
    let executor_state = globals::executor_state();
    serde_wasm_bindgen::to_value(&executor_state.registry).expect("failed to serialize registry")
}
