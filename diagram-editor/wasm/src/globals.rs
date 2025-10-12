use std::sync::Mutex;

use bevy_impulse::DiagramElementRegistry;

pub use bevy_impulse_diagram_editor::api::executor::ExecutorOptions;
use bevy_impulse_diagram_editor::api::executor::{setup_bevy_app_wasm, ExecutorState};

static EXECUTOR_STATE: Mutex<Option<ExecutorState>> = Mutex::new(None);
static BEVY_APP: Mutex<Option<bevy_app::SubApp>> = Mutex::new(None);

pub struct InitOptions {
    pub app: bevy_app::SubApp,
    pub registry: DiagramElementRegistry,
    pub executor_options: ExecutorOptions,
}

pub fn setup_wasm(
    InitOptions {
        mut app,
        registry,
        executor_options,
    }: InitOptions,
) {
    let mut executor_state = EXECUTOR_STATE.lock().unwrap();
    *executor_state = Some(setup_bevy_app_wasm(&mut app, registry, &executor_options));
    BEVY_APP.lock().unwrap().replace(app);
}

pub(super) async fn with_bevy_sup_app_async<R>(f: impl AsyncFnOnce(&mut bevy_app::SubApp) -> R) -> R {
    let mut mg = BEVY_APP.lock().unwrap();
    let app = mg.as_mut().expect("`init_wasm` not called");
    f(app).await
}

#[macro_export]
macro_rules! init_wasm {
    { $($statements:stmt)* } => {
        #[wasm_bindgen::prelude::wasm_bindgen]
        pub fn init_wasm() {
            let init_options: $crate::InitOptions = (|| {
                $($statements)*
            })();
            setup_wasm(init_options);
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
