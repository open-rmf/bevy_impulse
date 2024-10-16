use bevy_app::App;
use bevy_impulse::Callback;

use crate::{IntoOpaqueExt, OpaqueNode, ProviderRegistration, ProviderRegistry, Serializable};

pub trait SerializableCallback {
    type Request: Serializable;
    type Response: Serializable;

    fn insert_into_registry(registry: &mut ProviderRegistry, id: &'static str, name: &'static str) {
        registry.callbacks.insert(
            id,
            ProviderRegistration {
                id,
                name,
                request: Self::Request::insert_json_schema(&mut registry.gen),
                response: Self::Response::insert_json_schema(&mut registry.gen),
            },
        );
    }
}

impl<Request, Response, Streams> SerializableCallback for Callback<Request, Response, Streams>
where
    Request: Serializable,
    Response: Serializable,
{
    type Request = Request;
    type Response = Response;
}

struct OpaqueCallbackMarker;

impl<Request, Response, Streams> IntoOpaqueExt<Request, Response, OpaqueCallbackMarker>
    for Callback<Request, Response, Streams>
{
}

impl<Request, Response> SerializableCallback for OpaqueNode<Request, Response, OpaqueCallbackMarker>
where
    Request: Serializable,
    Response: Serializable,
{
    type Request = Request;
    type Response = Response;
}

pub trait RegisterCallbackExt {
    fn register_callback<T>(
        &mut self,
        callback: &T,
        id: &'static str,
        name: &'static str,
    ) -> &mut Self
    where
        T: SerializableCallback;

    fn callback_registration(&self, id: &'static str) -> Option<&ProviderRegistration>;
}

impl RegisterCallbackExt for App {
    fn register_callback<T>(
        &mut self,
        _callback: &T,
        id: &'static str,
        name: &'static str,
    ) -> &mut Self
    where
        T: SerializableCallback,
    {
        self.init_non_send_resource::<ProviderRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.non_send_resource_mut::<ProviderRegistry>();
        T::insert_into_registry(&mut registry, id, name);
        self
    }

    fn callback_registration(&self, id: &'static str) -> Option<&ProviderRegistration> {
        match self.world.get_non_send_resource::<ProviderRegistry>() {
            Some(registry) => registry.callbacks.get(id),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use bevy_impulse::IntoBlockingCallback;

    use crate::IntoOpaqueExt;

    use super::*;

    #[test]
    fn test_register_callback() {
        let mut app = App::new();

        fn test_cb() {}

        app.register_callback(&test_cb.into_blocking_callback(), "test_cb", "test_cb_name");
        let registration = app.callback_registration("test_cb").unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "test_cb_name");
    }

    #[test]
    fn test_register_opaque_callback() {
        let mut app = App::new();

        struct TestRequest {}
        fn opaque_req_cb(_: TestRequest) {}

        app.register_callback(
            &opaque_req_cb.into_blocking_callback().into_opaque_request(),
            "opaque_req_cb",
            "opaque_req_cb_name",
        );
        let registration = app.callback_registration("opaque_req_cb").unwrap();
        assert!(!registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "opaque_req_cb_name");

        fn opaque_resp_cb() -> TestRequest {
            TestRequest {}
        }
        app.register_callback(
            &opaque_resp_cb
                .into_blocking_callback()
                .into_opaque_response(),
            "opaque_resp_cb",
            "opaque_resp_cb_name",
        );
        let registration = app.callback_registration("opaque_resp_cb").unwrap();
        assert!(registration.request.serializable);
        assert!(!registration.response.serializable);
        assert!(registration.name == "opaque_resp_cb_name");

        fn opaque_req_resp_cb(_: TestRequest) -> TestRequest {
            TestRequest {}
        }
        app.register_callback(
            &opaque_req_resp_cb.into_blocking_callback().into_opaque(),
            "opaque_req_resp_cb",
            "opaque_req_resp_cb_name",
        );
        let registration = app.callback_registration("opaque_req_resp_cb").unwrap();
        assert!(!registration.request.serializable);
        assert!(!registration.response.serializable);
        assert!(registration.name == "opaque_req_resp_cb_name");
    }
}
