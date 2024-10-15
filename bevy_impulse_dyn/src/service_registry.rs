use std::marker::PhantomData;

use bevy_app::App;
use bevy_impulse::{
    AsyncService, BlockingService, ContinuousService, IntoContinuousService, IntoService,
    IntoServiceBuilder, StreamPack,
};
use schemars::JsonSchema;

use crate::{IntoOpaqueExt, OpaqueProvider, ProviderRegistry, Serializable, ServiceRegistration};

/// Helper trait to unwrap the request type of a wrapped request.
trait InferRequest<T> {}

impl<T> InferRequest<T> for T where T: JsonSchema {}

impl<Request, Streams> InferRequest<Request> for BlockingService<Request, Streams> where
    Streams: StreamPack
{
}

impl<Request, Streams> InferRequest<Request> for AsyncService<Request, Streams> where
    Streams: StreamPack
{
}

impl<Request, Response, Streams> InferRequest<Request>
    for ContinuousService<Request, Response, Streams>
where
    Streams: StreamPack,
{
}

pub trait SerializableService<M> {
    type Source;
    type Request: Serializable;
    type Response: Serializable;

    fn provider_id() -> &'static str {
        std::any::type_name::<Self::Source>()
    }

    fn insert_into_registry(registry: &mut ProviderRegistry, name: &'static str) {
        let id = Self::provider_id();
        registry.services.insert(
            id,
            ServiceRegistration {
                id,
                name,
                request: Self::Request::insert_json_schema(&mut registry.gen),
                response: Self::Response::insert_json_schema(&mut registry.gen),
                configure: None,
            },
        );
    }
}

// impl<T, M> SerializableService<M> for T where T: SerializableProvider<M> {}

struct IntoServiceBuilderMarker<M> {
    _unused: PhantomData<M>,
}

impl<T, Request, Response, M, M2>
    SerializableService<IntoServiceBuilderMarker<(Request, Response, M, M2)>> for T
where
    T: IntoServiceBuilder<M>,
    Request: Serializable,
    Response: Serializable,
    T::Service: IntoService<M2>,
    <T::Service as IntoService<M2>>::Request: InferRequest<Request>,
    <T::Service as IntoService<M2>>::Response: InferRequest<Response>,
{
    type Source = T;
    type Request = Request;
    type Response = Response;
}

struct IntoContinuousServiceBuilderMarker<M> {
    _unused: PhantomData<M>,
}

impl<T, Request, Response, M, M2>
    SerializableService<IntoContinuousServiceBuilderMarker<(Request, Response, M, M2)>> for T
where
    T: IntoServiceBuilder<M>,
    Request: Serializable,
    Response: Serializable,
    T::Service: IntoContinuousService<M2>,
    <T::Service as IntoContinuousService<M2>>::Request: InferRequest<Request>,
    <T::Service as IntoContinuousService<M2>>::Response: InferRequest<Response>,
{
    type Source = T;
    type Request = Request;
    type Response = Response;
}

struct OpaqueServiceMarker<M> {
    _unused: PhantomData<M>,
}

impl<T, M, M2>
    IntoOpaqueExt<
        <T::Service as IntoService<M2>>::Request,
        <T::Service as IntoService<M2>>::Response,
        OpaqueServiceMarker<(T::Service, M, M2)>,
    > for T
where
    T: IntoServiceBuilder<M>,
    T::Service: IntoService<M2>,
{
}

impl<Request, Response, Service, M, M2> SerializableService<OpaqueServiceMarker<(Service, M, M2)>>
    for OpaqueProvider<Request, Response, OpaqueServiceMarker<(Service, M, M2)>>
where
    Request: Serializable,
    Response: Serializable,
{
    type Source = Service;
    type Request = Request;
    type Response = Response;
}

pub trait RegisterServiceExt {
    /// Register a service into the service registry. In order to run a serialized workflow,
    /// all services in it must be registered.
    fn register_service<T, M>(&mut self, service: &T, name: &'static str) -> &mut Self
    where
        T: SerializableService<M>;

    /// Get a previous registered service registration.
    fn service_registration<T, M>(&self, service: &T) -> Option<&ServiceRegistration>
    where
        T: SerializableService<M>;
}

impl RegisterServiceExt for App {
    fn register_service<T, M>(&mut self, _service: &T, name: &'static str) -> &mut Self
    where
        T: SerializableService<M>,
    {
        self.init_non_send_resource::<ProviderRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.non_send_resource_mut::<ProviderRegistry>();
        T::insert_into_registry(&mut registry, name);
        self
    }

    fn service_registration<T, M>(&self, _service: &T) -> Option<&ServiceRegistration>
    where
        T: SerializableService<M>,
    {
        match self.world.get_non_send_resource::<ProviderRegistry>() {
            Some(registry) => registry.services.get(T::provider_id()),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use bevy_impulse::{
        AsyncServiceInput, BlockingServiceInput, ContinuousServiceInput, IntoAsyncService,
        IntoBlockingService,
    };
    use serde::Serialize;

    use crate::ProviderRegistryExt;

    use super::*;

    fn service_with_no_request() {}

    #[test]
    fn test_register_service() {
        let mut app = App::new();
        let srv = service_with_no_request.into_blocking_service();
        assert!(app.service_registration(&srv).is_none());
        app.register_service(&srv, "service_with_no_request");
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "service_with_no_request");
    }

    #[allow(dead_code)]
    #[derive(JsonSchema, Serialize)]
    struct TestServiceRequest {
        msg: String,
    }

    fn service_with_request(_: BlockingServiceInput<TestServiceRequest>) {}

    #[test]
    fn test_register_service_with_request() {
        let mut app = App::new();
        let srv = service_with_request.into_blocking_service();
        app.register_service(&srv, "service_with_request");
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.r#type == TestServiceRequest::schema_name());
        assert!(registration.request.serializable);
        assert!(registration.response.r#type == <()>::schema_name());
        assert!(registration.response.serializable);
        assert!(registration.name == "service_with_request");
    }

    #[allow(dead_code)]
    #[derive(JsonSchema, Serialize)]
    struct TestServiceResponse {
        ok: bool,
    }

    fn service_with_req_resp(_: BlockingServiceInput<TestServiceRequest>) -> TestServiceResponse {
        TestServiceResponse { ok: true }
    }

    #[test]
    fn test_service_with_req_resp() {
        let mut app = App::new();
        let srv = service_with_req_resp.into_blocking_service();
        app.register_service(&srv, "service_with_req_resp");
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.r#type == TestServiceRequest::schema_name());
        assert!(registration.request.serializable);
        assert!(registration.response.r#type == TestServiceResponse::schema_name());
        assert!(registration.response.serializable);
        assert!(registration.name == "service_with_req_resp");
    }

    struct TestNonSerializableRequest {}

    fn opaque_request_service(_: BlockingServiceInput<TestNonSerializableRequest>) {}

    #[test]
    fn test_register_opaque_request_service() {
        let mut app = App::new();
        let srv = opaque_request_service.into_opaque_request();
        app.register_service(&srv, "opaque_request_service");
        let registration = app.service_registration(&srv).unwrap();
        assert!(!registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "opaque_request_service");
    }

    fn opaque_response_service(
        _: BlockingServiceInput<TestServiceRequest>,
    ) -> TestNonSerializableRequest {
        TestNonSerializableRequest {}
    }

    #[test]
    fn test_opaque_response_service() {
        let mut app = App::new();
        let srv = opaque_response_service.into_opaque_response();
        app.register_service(&srv, "opaque_response_service");
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.serializable);
        assert!(!registration.response.serializable);
    }

    fn full_opaque_service(
        _: BlockingServiceInput<TestNonSerializableRequest>,
    ) -> TestNonSerializableRequest {
        TestNonSerializableRequest {}
    }

    #[test]
    fn test_full_opaque_service() {
        let mut app = App::new();
        let srv = full_opaque_service.into_opaque();
        app.register_service(&srv, "full_opaque_service");
        let registration = app.service_registration(&srv).unwrap();
        assert!(!registration.request.serializable);
        assert!(!registration.response.serializable);
    }

    fn async_service(_: AsyncServiceInput<()>) -> impl Future<Output = ()> {
        async move {}
    }

    #[test]
    fn test_register_async_service() {
        let mut app = App::new();
        let srv = async_service.into_async_service();
        app.register_service(&srv, "async_service");
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
    }

    fn continous_service(_: ContinuousServiceInput<(), ()>) {}

    #[test]
    fn test_register_continuous_service() {
        let mut app = App::new();
        let srv = continous_service;
        app.register_service(&srv, "continous_service");
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
    }

    #[test]
    fn test_serialize_service_registry() {
        let mut app = App::new();
        let srv = service_with_req_resp.into_blocking_service();
        app.register_service(&srv, "service_with_req_resp");
        let registry = app.provider_registry();
        let json = serde_json::to_string(registry).unwrap();
        let deserialized: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            deserialized
                .as_object()
                .unwrap()
                .get("services")
                .unwrap()
                .as_object()
                .unwrap()
                .len()
                == 1
        );
        assert!(
            deserialized
                .as_object()
                .unwrap()
                .get("types")
                .unwrap()
                .as_object()
                .unwrap()
                .len()
                == 2
        );
    }

    #[allow(dead_code)]
    #[derive(JsonSchema, Serialize)]
    struct TestNestedRequest {
        inner: TestServiceRequest,
    }

    fn nested_request_service(_: BlockingServiceInput<TestNestedRequest>) {}

    /// test that $ref pointers use the correct path
    #[test]
    fn test_type_definition_pointers() {
        let mut app = App::new();
        let srv = nested_request_service.into_blocking_service();
        app.register_service(&srv, "nested_request_service");
        let json = serde_json::to_value(app.provider_registry()).unwrap();
        let json_str = serde_json::to_string(&json).unwrap();
        assert!(json_str.contains("#/types/TestServiceRequest"));
    }
}
