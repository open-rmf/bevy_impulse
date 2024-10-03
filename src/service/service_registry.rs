use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use bevy_app::App;
use schemars::{
    gen::{SchemaGenerator, SchemaSettings},
    JsonSchema,
};
use serde::Serialize;

use crate::{AsyncService, BlockingService, ContinuousService, StreamPack};

use super::{
    service_builder::IntoBuilderMarker, IntoContinuousService, IntoContinuousServiceBuilderMarker,
    IntoService, IntoServiceBuilder,
};

#[derive(Debug, Serialize)]
pub struct ServiceRequestDefinition {
    /// The type of the request, if the request is serializable, this will be the json schema
    /// type, if it is not serializable, it will be the rust type.
    r#type: String,

    /// Indicates if the request is serializable.
    serializable: bool,
}

/// Helper trait to unwrap the request type of a wrapped request.
trait InferRequest {
    type Request;
}

impl<T> InferRequest for T
where
    T: JsonSchema,
{
    type Request = T;
}

impl<Request, Streams> InferRequest for BlockingService<Request, Streams>
where
    Request: JsonSchema,
    Streams: StreamPack,
{
    type Request = Request;
}

impl<Request, Streams> InferRequest for AsyncService<Request, Streams>
where
    Request: JsonSchema,
    Streams: StreamPack,
{
    type Request = Request;
}

impl<Request, Streams> InferRequest for ContinuousService<Request, Streams>
where
    Request: JsonSchema,
    Streams: StreamPack,
{
    type Request = Request;
}

pub trait SerializableServiceRequest {
    /// Returns the type name of the request, the type name must be unique across all services.
    fn type_name() -> String;

    /// Insert the request type into the schema generator and return the request definition.
    fn insert_json_schema(gen: &mut SchemaGenerator) -> ServiceRequestDefinition;
}

impl<T> SerializableServiceRequest for T
where
    T: InferRequest,
    T::Request: JsonSchema,
{
    fn type_name() -> String {
        // We need to use `schema_name` instead of `schema_id` as the definitions generated
        // by schemars using the name. But schemars will ensure the names are unique.
        T::Request::schema_name()
    }

    fn insert_json_schema(gen: &mut SchemaGenerator) -> ServiceRequestDefinition {
        gen.subschema_for::<T::Request>();
        ServiceRequestDefinition {
            r#type: T::Request::schema_name(),
            serializable: true,
        }
    }
}

pub trait SerializableService<M> {
    fn insert_into_registry(registry: &mut ServiceRegistry);
}

fn extract_service_name(id: ServiceId) -> String {
    // get the original function name, this is a very naive implementation that assumes that
    // there is never more than 1 param in any of the generics. If this proves to be
    // insufficient, consider using the `syn` crate to do actual parsing.
    match id.rsplit_once("::") {
        Some((_, suffix)) => suffix.trim_end_matches(">").to_string(),
        None => "".to_string(),
    }
}

fn insert_into_registry_impl<Service, Request, Response>(registry: &mut ServiceRegistry)
where
    Request: SerializableServiceRequest,
    Response: SerializableServiceRequest,
{
    let id = std::any::type_name::<Service>();
    let name = extract_service_name(id);
    registry.services.insert(
        id,
        ServiceRegistration {
            id,
            name,
            request: Request::insert_json_schema(&mut registry.gen),
            response: Response::insert_json_schema(&mut registry.gen),
        },
    );
}

impl<T, M, M2> SerializableService<IntoBuilderMarker<(M, M2)>> for T
where
    T: IntoServiceBuilder<M>,
    T::Service: IntoService<M2>,
    <T::Service as IntoService<M2>>::Request: SerializableServiceRequest,
    <T::Service as IntoService<M2>>::Response: SerializableServiceRequest,
{
    fn insert_into_registry(registry: &mut ServiceRegistry) {
        insert_into_registry_impl::<
            T,
            <T::Service as IntoService<M2>>::Request,
            <T::Service as IntoService<M2>>::Response,
        >(registry);
    }
}

impl<T, M, M2> SerializableService<IntoContinuousServiceBuilderMarker<(M, M2)>> for T
where
    T: IntoServiceBuilder<M>,
    T::Service: IntoContinuousService<M2>,
    <T::Service as IntoContinuousService<M2>>::Request: SerializableServiceRequest,
    <T::Service as IntoContinuousService<M2>>::Response: SerializableServiceRequest,
{
    fn insert_into_registry(registry: &mut ServiceRegistry) {
        insert_into_registry_impl::<
            T,
            <T::Service as IntoContinuousService<M2>>::Request,
            <T::Service as IntoContinuousService<M2>>::Response,
        >(registry);
    }
}

pub struct NonSerializableServiceRequest<T> {
    _unused: PhantomData<T>,
}

impl<T> NonSerializableServiceRequest<T> {
    pub fn request_definition() -> ServiceRequestDefinition {
        ServiceRequestDefinition {
            r#type: std::any::type_name::<T>().to_string(),
            serializable: false,
        }
    }
}

pub struct OpaqueService<M> {
    _unused: PhantomData<M>,
}

pub type OpaqueRequestService<Service, Request, Response> =
    OpaqueService<(Service, NonSerializableServiceRequest<Request>, Response)>;

impl<Service, Request, Response>
    SerializableService<OpaqueRequestService<Service, Request, Response>>
    for OpaqueRequestService<Service, Request, Response>
where
    Response: SerializableServiceRequest,
{
    fn insert_into_registry(registry: &mut ServiceRegistry) {
        let id = std::any::type_name::<OpaqueRequestService<Service, Request, Response>>();
        let name = extract_service_name(std::any::type_name::<Service>());
        registry.services.insert(
            id,
            ServiceRegistration {
                id,
                name,
                request: NonSerializableServiceRequest::<Request>::request_definition(),
                response: Response::insert_json_schema(&mut registry.gen),
            },
        );
    }
}

pub type OpaqueResponseService<Service, Request, Response> =
    OpaqueService<(Service, Request, NonSerializableServiceRequest<Response>)>;

impl<Service, Request, Response>
    SerializableService<OpaqueResponseService<Service, Request, Response>>
    for OpaqueResponseService<Service, Request, Response>
where
    Request: SerializableServiceRequest,
{
    fn insert_into_registry(registry: &mut ServiceRegistry) {
        let id = std::any::type_name::<OpaqueResponseService<Service, Request, Response>>();
        let name = extract_service_name(std::any::type_name::<Service>());
        registry.services.insert(
            id,
            ServiceRegistration {
                id,
                name,
                request: Request::insert_json_schema(&mut registry.gen),
                response: NonSerializableServiceRequest::<Response>::request_definition(),
            },
        );
    }
}

pub type FullOpaqueService<Service, Request, Response> = OpaqueService<(
    Service,
    NonSerializableServiceRequest<Request>,
    NonSerializableServiceRequest<Response>,
)>;

impl<Service, Request, Response> SerializableService<FullOpaqueService<Service, Request, Response>>
    for FullOpaqueService<Service, Request, Response>
{
    fn insert_into_registry(registry: &mut ServiceRegistry) {
        let id = std::any::type_name::<FullOpaqueService<Service, Request, Response>>();
        let name = extract_service_name(std::any::type_name::<Service>());
        registry.services.insert(
            id,
            ServiceRegistration {
                id,
                name,
                request: NonSerializableServiceRequest::<Request>::request_definition(),
                response: NonSerializableServiceRequest::<Response>::request_definition(),
            },
        );
    }
}

type RequestOf<B, M, M2> = <<B as IntoServiceBuilder<M>>::Service as IntoService<M2>>::Request;
type ResponseOf<B, M, M2> = <<B as IntoServiceBuilder<M>>::Service as IntoService<M2>>::Response;

pub trait OpaqueRequestExt<B, M, M2>
where
    B: IntoServiceBuilder<M>,
    B::Service: IntoService<M2>,
{
    /// Mark this service as fully opaque, this means that both the request and response cannot
    /// be serialized. Opaque services can still be registered into the service registry but
    /// their request and response types are undefined and cannot be transformed.
    fn into_opaque(
        &self,
    ) -> OpaqueService<(
        B::Service,
        NonSerializableServiceRequest<RequestOf<B, M, M2>>,
        NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
    )>;

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the request as opaque.
    fn into_opaque_request(
        self,
    ) -> OpaqueService<(
        B::Service,
        NonSerializableServiceRequest<RequestOf<B, M, M2>>,
        ResponseOf<B, M, M2>,
    )>;

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the response as opaque.
    fn into_opaque_response(
        &self,
    ) -> OpaqueService<(
        B::Service,
        RequestOf<B, M, M2>,
        NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
    )>;
}

impl<B, M, M2> OpaqueRequestExt<B, M, M2> for B
where
    B: IntoServiceBuilder<M>,
    B::Service: IntoService<M2>,
{
    fn into_opaque(
        &self,
    ) -> OpaqueService<(
        B::Service,
        NonSerializableServiceRequest<RequestOf<B, M, M2>>,
        NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
    )> {
        OpaqueService::<(
            B::Service,
            NonSerializableServiceRequest<RequestOf<B, M, M2>>,
            NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
        )> {
            _unused: PhantomData,
        }
    }

    fn into_opaque_request(
        self,
    ) -> OpaqueService<(
        B::Service,
        NonSerializableServiceRequest<RequestOf<B, M, M2>>,
        ResponseOf<B, M, M2>,
    )> {
        OpaqueService::<(
            B::Service,
            NonSerializableServiceRequest<RequestOf<B, M, M2>>,
            ResponseOf<B, M, M2>,
        )> {
            _unused: PhantomData,
        }
    }

    fn into_opaque_response(
        &self,
    ) -> OpaqueService<(
        B::Service,
        RequestOf<B, M, M2>,
        NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
    )> {
        OpaqueService::<(
            B::Service,
            RequestOf<B, M, M2>,
            NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
        )> {
            _unused: PhantomData,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ServiceRegistration {
    id: ServiceId,
    /// Friendly name for the service, may not be unique.
    name: String,
    request: ServiceRequestDefinition,
    response: ServiceRequestDefinition,
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

/// Service id must be fixed and (most likely) known at compile time.
type ServiceId = &'static str;

fn serialize_service_registry_types<S>(gen: &SchemaGenerator, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    gen.definitions().serialize(s)
}

#[derive(Serialize)]
pub struct ServiceRegistry {
    /// List of services registered.
    services: HashMap<ServiceId, ServiceRegistration>,

    /// List of all request and response types used in all registered services, this only
    /// contains serializable types, non serializable types are opaque and is only compatible
    /// with itself.
    #[serde(rename = "types", serialize_with = "serialize_service_registry_types")]
    gen: SchemaGenerator,
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/types/".to_string();
        ServiceRegistry {
            services: HashMap::<ServiceId, ServiceRegistration>::default(),
            gen: SchemaGenerator::new(settings),
        }
    }
}

pub trait RegisterServiceExt {
    /// Register a service into the service registry. In order to run a serialized workflow,
    /// all services in it must be registered.
    fn register_service<T, M>(&mut self, service: &T) -> &mut Self
    where
        T: SerializableService<M>;

    /// Get a previous registered service registration.
    fn service_registration<T, M>(&self, service: &T) -> Option<&ServiceRegistration>
    where
        T: SerializableService<M>;

    fn service_registry(&mut self) -> &ServiceRegistry;
}

impl RegisterServiceExt for App {
    fn register_service<T, M>(&mut self, _service: &T) -> &mut Self
    where
        T: SerializableService<M>,
    {
        self.init_non_send_resource::<ServiceRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.non_send_resource_mut::<ServiceRegistry>();
        T::insert_into_registry(&mut registry);
        self
    }

    fn service_registration<T, M>(&self, _service: &T) -> Option<&ServiceRegistration>
    where
        T: SerializableService<M>,
    {
        match self.world.get_non_send_resource::<ServiceRegistry>() {
            Some(registry) => registry.services.get(std::any::type_name::<T>()),
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

    use bevy_app::App;

    use crate::{
        AsyncServiceInput, BlockingServiceInput, ContinuousServiceInput, IntoAsyncService,
        IntoBlockingService,
    };

    use super::*;

    fn service_with_no_request() {}

    #[test]
    fn test_register_service() {
        let mut app = App::new();
        let srv = service_with_no_request.into_blocking_service();
        assert!(app.service_registration(&srv).is_none());
        app.register_service(&srv);
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "service_with_no_request");
    }

    #[allow(dead_code)]
    #[derive(JsonSchema)]
    struct TestServiceRequest {
        msg: String,
    }

    fn service_with_request(_: BlockingServiceInput<TestServiceRequest>) {}

    #[test]
    fn test_register_service_with_request() {
        let mut app = App::new();
        let srv = service_with_request.into_blocking_service();
        app.register_service(&srv);
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.r#type == TestServiceRequest::schema_name());
        assert!(registration.request.serializable);
        assert!(registration.response.r#type == <()>::schema_name());
        assert!(registration.response.serializable);
        assert!(registration.name == "service_with_request");
    }

    #[allow(dead_code)]
    #[derive(JsonSchema)]
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
        app.register_service(&srv);
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
        app.register_service(&srv);
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
        app.register_service(&srv);
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
        app.register_service(&srv);
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
        app.register_service(&srv);
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
    }

    fn continous_service(_: ContinuousServiceInput<(), ()>) {}

    #[test]
    fn test_register_continuous_service() {
        let mut app = App::new();
        let srv = continous_service;
        app.register_service(&srv);
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
    }

    #[test]
    fn test_serialize_service_registry() {
        let mut app = App::new();
        let srv = service_with_req_resp.into_blocking_service();
        app.register_service(&srv);
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
    #[derive(JsonSchema)]
    struct TestNestedRequest {
        inner: TestServiceRequest,
    }

    fn nested_request_service(_: BlockingServiceInput<TestNestedRequest>) {}

    /// test that $ref pointers use the correct path
    #[test]
    fn test_type_definition_pointers() {
        let mut app = App::new();
        let srv = nested_request_service.into_blocking_service();
        app.register_service(&srv);
        let json = serde_json::to_value(app.service_registry()).unwrap();
        let json_str = serde_json::to_string(&json).unwrap();
        assert!(json_str.contains("#/types/TestServiceRequest"));
    }
}
