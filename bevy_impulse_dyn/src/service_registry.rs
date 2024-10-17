use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use bevy_app::App;
use bevy_impulse::{
    AsyncService, BlockingService, ContinuousService, IntoContinuousService, IntoService,
    IntoServiceBuilder, StreamPack,
};
use schemars::gen::{SchemaGenerator, SchemaSettings};
use serde::Serialize;

use crate::{DynType, InferDynRequest, MessageMetadata, OpaqueMessage};

#[derive(Serialize)]
pub struct ServiceRegistration {
    pub id: &'static str,
    pub name: &'static str,
    pub request: MessageMetadata,
    pub response: MessageMetadata,
}

impl PartialEq for ServiceRegistration {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for ServiceRegistration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

fn serialize_service_registry_types<S>(gen: &SchemaGenerator, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    gen.definitions().serialize(s)
}

#[derive(Serialize)]
pub struct ServiceRegistry {
    /// List of services registered.
    pub services: HashMap<&'static str, ServiceRegistration>,

    /// List of all request and response types used in all registered services, this only
    /// contains serializable types, non serializable types are opaque and is only compatible
    /// with itself.
    #[serde(rename = "types", serialize_with = "serialize_service_registry_types")]
    pub(crate) gen: SchemaGenerator,
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/types/".to_string();
        ServiceRegistry {
            services: HashMap::<&'static str, ServiceRegistration>::default(),
            gen: SchemaGenerator::new(settings),
        }
    }
}

impl<Request, Streams> InferDynRequest<Request> for BlockingService<Request, Streams>
where
    Request: DynType,
    Streams: StreamPack,
{
}

impl<Request, Streams> InferDynRequest<Request> for AsyncService<Request, Streams>
where
    Request: DynType,
    Streams: StreamPack,
{
}

impl<Request, Response, Streams> InferDynRequest<Request>
    for ContinuousService<Request, Response, Streams>
where
    Request: DynType,
    Streams: StreamPack,
{
}

pub trait SerializableService<M> {
    type Source;
    type Request: DynType;
    type Response: DynType;

    fn provider_id() -> &'static str {
        std::any::type_name::<Self::Source>()
    }

    fn insert_into_registry(registry: &mut ServiceRegistry, name: &'static str) {
        let id = Self::provider_id();
        registry.services.insert(
            id,
            ServiceRegistration {
                id,
                name,
                request: Self::Request::insert_json_schema(&mut registry.gen),
                response: Self::Response::insert_json_schema(&mut registry.gen),
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
    Request: DynType,
    Response: DynType,
    T::Service: IntoService<M2>,
    <T::Service as IntoService<M2>>::Request: InferDynRequest<Request>,
    <T::Service as IntoService<M2>>::Response: InferDynRequest<Response>,
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
    Request: DynType,
    Response: DynType,
    T::Service: IntoContinuousService<M2>,
    <T::Service as IntoContinuousService<M2>>::Request: InferDynRequest<Request>,
    <T::Service as IntoContinuousService<M2>>::Response: InferDynRequest<Response>,
{
    type Source = T;
    type Request = Request;
    type Response = Response;
}

pub struct OpaqueService<Request, Response, M>
where
    Request: DynType,
    Response: DynType,
{
    _unused: PhantomData<(Request, Response, M)>,
}

pub type FullOpaqueService<Request, Response, M> =
    OpaqueService<OpaqueMessage<Request>, OpaqueMessage<Response>, M>;

pub type OpaqueRequestService<Request, Response, M> =
    OpaqueService<OpaqueMessage<Request>, Response, M>;

pub type OpaqueResponseService<Request, Response, M> =
    OpaqueService<Request, OpaqueMessage<Response>, M>;

pub trait IntoOpaqueExt<Request, Response, M> {
    /// Mark this provider as fully opaque, this means that both the request and response cannot
    /// be serialized. Opaque services can still be registered into the service registry but
    /// their request and response types are undefined and cannot be transformed.
    fn into_opaque(&self) -> FullOpaqueService<Request, Response, M> {
        FullOpaqueService::<Request, Response, M> {
            _unused: Default::default(),
        }
    }

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the request as opaque.
    fn into_opaque_request(&self) -> OpaqueRequestService<Request, Response, M>
    where
        Response: DynType,
    {
        OpaqueRequestService::<Request, Response, M> {
            _unused: PhantomData,
        }
    }

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the response as opaque.
    fn into_opaque_response(&self) -> OpaqueResponseService<Request, Response, M>
    where
        Request: DynType,
    {
        OpaqueResponseService::<Request, Response, M> {
            _unused: PhantomData,
        }
    }
}

impl<T, M, M2>
    IntoOpaqueExt<
        <T::Service as IntoService<M2>>::Request,
        <T::Service as IntoService<M2>>::Response,
        (T::Service, M, M2),
    > for T
where
    T: IntoServiceBuilder<M>,
    T::Service: IntoService<M2>,
{
}

impl<Request, Response, Service, M, M2> SerializableService<(Service, M, M2)>
    for OpaqueService<Request, Response, (Service, M, M2)>
where
    Request: DynType,
    Response: DynType,
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

    fn service_registry(&mut self) -> &ServiceRegistry;
}

impl RegisterServiceExt for App {
    fn register_service<T, M>(&mut self, _service: &T, name: &'static str) -> &mut Self
    where
        T: SerializableService<M>,
    {
        self.init_non_send_resource::<ServiceRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.non_send_resource_mut::<ServiceRegistry>();
        T::insert_into_registry(&mut registry, name);
        self
    }

    fn service_registration<T, M>(&self, _service: &T) -> Option<&ServiceRegistration>
    where
        T: SerializableService<M>,
    {
        match self.world.get_non_send_resource::<ServiceRegistry>() {
            Some(registry) => registry.services.get(T::provider_id()),
            None => None,
        }
    }

    fn service_registry(&mut self) -> &ServiceRegistry {
        self.init_non_send_resource::<ServiceRegistry>(); // nothing happens if the resource already exist
        self.world.non_send_resource::<ServiceRegistry>()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use bevy_impulse::{
        AsyncServiceInput, BlockingServiceInput, ContinuousServiceInput, IntoAsyncService,
        IntoBlockingService,
    };
    use schemars::JsonSchema;
    use serde::Deserialize;

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
    #[derive(JsonSchema, Deserialize)]
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
    #[derive(JsonSchema, Deserialize)]
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
        let registry = app.service_registry();
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
    #[derive(JsonSchema, Deserialize)]
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
        let json = serde_json::to_value(app.service_registry()).unwrap();
        let json_str = serde_json::to_string(&json).unwrap();
        assert!(json_str.contains("#/types/TestServiceRequest"));
    }
}
