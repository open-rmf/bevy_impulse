use std::marker::PhantomData;

use bevy_app::App;
use bevy_impulse::{Callback, IntoBlockingCallback};

use crate::{IntoOpaqueExt, ProviderRegistration, ProviderRegistry, Serializable};

pub trait SerializableCallback<M> {
    type Source;
    type Request: Serializable;
    type Response: Serializable;

    fn provider_id() -> &'static str {
        std::any::type_name::<Self::Source>()
    }

    fn insert_into_registry(registry: &mut ProviderRegistry, name: &'static str) {
        let id = Self::provider_id();
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

struct IntoBlockingCallbackMarker<M> {
    _unused: PhantomData<M>,
}

impl<T, M> SerializableCallback<IntoBlockingCallbackMarker<M>> for T
where
    T: IntoBlockingCallback<M>,
    T::Request: Serializable,
    T::Response: Serializable,
{
    type Source = T;
    type Request = T::Request;
    type Response = T::Response;
}

impl<Request, Response, Streams>
    IntoOpaqueExt<Request, Response, Callback<Request, Response, Streams>>
    for Callback<Request, Response, Streams>
{
}

pub trait RegisterCallbackExt {
    fn register_callback<T, M>(&mut self, callback: &T, name: &'static str) -> &mut Self
    where
        T: SerializableCallback<M>;

    fn callback_registration<T, M>(&self, id: &T) -> Option<&ProviderRegistration>
    where
        T: SerializableCallback<M>;
}

impl RegisterCallbackExt for App {
    fn register_callback<T, M>(&mut self, _callback: &T, name: &'static str) -> &mut Self
    where
        T: SerializableCallback<M>,
    {
        self.init_non_send_resource::<ProviderRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.non_send_resource_mut::<ProviderRegistry>();
        T::insert_into_registry(&mut registry, name);
        self
    }

    fn callback_registration<T, M>(&self, _callback: &T) -> Option<&ProviderRegistration>
    where
        T: SerializableCallback<M>,
    {
        match self.world.get_non_send_resource::<ProviderRegistry>() {
            Some(registry) => registry.callbacks.get(T::provider_id()),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use schemars::JsonSchema;
    use serde::Serialize;

    use super::*;

    #[derive(JsonSchema, Serialize)]
    struct TestRequest {}

    #[derive(JsonSchema, Serialize)]
    struct TestResponse {}

    #[test]
    fn test_register_callback_with_no_req_resp() {
        let mut app = App::new();

        fn assert_register_cb<T, M>(app: &mut App, cb: &T, name: &'static str)
        where
            T: SerializableCallback<M>,
        {
            app.register_callback(cb, name);
            let registration = app.callback_registration(cb).unwrap();
            assert!(registration.request.serializable);
            assert!(registration.response.serializable);
            assert!(registration.name == name);
        }

        fn callback_with_no_req_resp() {}
        assert_register_cb(
            &mut app,
            &callback_with_no_req_resp,
            "callback_with_no_req_resp_name",
        );

        fn callback_with_req(_: TestRequest) {}
        assert_register_cb(&mut app, &callback_with_req, "callback_with_req_name");

        fn call_back_with_resp() -> TestResponse {
            TestResponse {}
        }
        assert_register_cb(&mut app, &call_back_with_resp, "call_back_with_resp_name");

        fn call_back_with_req_resp(_: TestRequest) -> TestResponse {
            TestResponse {}
        }
        assert_register_cb(
            &mut app,
            &call_back_with_req_resp,
            "call_back_with_req_resp_name",
        );
    }
}
