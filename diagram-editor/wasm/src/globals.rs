use std::sync::Mutex;

use bevy_impulse::DiagramElementRegistry;

use bevy_impulse_diagram_editor::api::executor::{setup_bevy_app, ExecutorOptions, ExecutorState};

pub(super) static EXECUTOR_STATE: Mutex<Option<ExecutorState>> = Mutex::new(None);
pub(super) static BEVY_APP: Mutex<Option<bevy_app::App>> = Mutex::new(None);

pub fn setup_wasm(
    mut app: bevy_app::App,
    registry: DiagramElementRegistry,
    executor_options: &ExecutorOptions,
) {
    let mut executor_state = EXECUTOR_STATE.lock().unwrap();
    *executor_state = Some(setup_bevy_app(&mut app, registry, executor_options));
    BEVY_APP.lock().unwrap().replace(app);
}

#[macro_export]
macro_rules! init_wasm {
    { $($statements:stmt)* } => {
        #[wasm_bindgen::prelude::wasm_bindgen]
        pub fn init_wasm() {
            $($statements)*
        }
    };
}

pub(super) fn executor_state() -> ExecutorState {
    EXECUTOR_STATE
        .lock()
        .unwrap()
        .as_ref()
        .expect("`init_wasm` not called")
        .clone()
}
