use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use bevy_app::App;
use bevy_utils::HashMap;
use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};

use crate::{AsyncService, BlockingService};

use super::{IntoService, IntoServiceBuilder};

pub trait SerializableServiceRequest {
    type Request: JsonSchema;

    fn type_name() -> String {
        Self::Request::schema_id().to_string()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        gen.subschema_for::<Self::Request>()
    }
}

impl<T> SerializableServiceRequest for T
where
    T: JsonSchema,
{
    type Request = T;
}

impl<T> SerializableServiceRequest for BlockingService<T>
where
    T: JsonSchema,
{
    type Request = T;
}

impl<T> SerializableServiceRequest for AsyncService<T>
where
    T: JsonSchema,
{
    type Request = T;
}

pub trait MaybeSerializableServiceRequest {
    fn type_name() -> String;
    fn try_json_schema(_gen: &mut SchemaGenerator) -> Option<Schema>;
}

impl<T> MaybeSerializableServiceRequest for T
where
    T: SerializableServiceRequest,
{
    fn type_name() -> String {
        T::type_name()
    }

    fn try_json_schema(gen: &mut SchemaGenerator) -> Option<Schema> {
        Some(T::json_schema(gen))
    }
}

pub struct NonSerializableServiceRequest<T> {
    _unused: PhantomData<T>,
}

impl<T> MaybeSerializableServiceRequest for NonSerializableServiceRequest<T> {
    fn type_name() -> String {
        std::any::type_name::<T>().to_string()
    }

    fn try_json_schema(_gen: &mut SchemaGenerator) -> Option<Schema> {
        None
    }
}

pub struct OpaqueService<M> {
    _unused: PhantomData<M>,
}

impl<Request, Response> SerializableService<OpaqueService<(Request, Response)>>
    for OpaqueService<(Request, Response)>
where
    Request: MaybeSerializableServiceRequest,
    Response: MaybeSerializableServiceRequest,
{
    type Request = Request;
    type Response = Response;
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
        NonSerializableServiceRequest<RequestOf<B, M, M2>>,
        NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
    )>;

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the request as opaque.
    fn into_opaque_request(
        self,
    ) -> OpaqueService<(
        NonSerializableServiceRequest<RequestOf<B, M, M2>>,
        ResponseOf<B, M, M2>,
    )>;

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the response as opaque.
    fn into_opaque_response(
        &self,
    ) -> OpaqueService<(
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
        NonSerializableServiceRequest<RequestOf<B, M, M2>>,
        NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
    )> {
        OpaqueService::<(
            NonSerializableServiceRequest<RequestOf<B, M, M2>>,
            NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
        )> {
            _unused: PhantomData,
        }
    }

    fn into_opaque_request(
        self,
    ) -> OpaqueService<(
        NonSerializableServiceRequest<RequestOf<B, M, M2>>,
        ResponseOf<B, M, M2>,
    )> {
        OpaqueService::<(
            NonSerializableServiceRequest<RequestOf<B, M, M2>>,
            ResponseOf<B, M, M2>,
        )> {
            _unused: PhantomData,
        }
    }

    fn into_opaque_response(
        &self,
    ) -> OpaqueService<(
        RequestOf<B, M, M2>,
        NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
    )> {
        OpaqueService::<(
            RequestOf<B, M, M2>,
            NonSerializableServiceRequest<ResponseOf<B, M, M2>>,
        )> {
            _unused: PhantomData,
        }
    }
}

pub trait SerializableService<M> {
    type Request: MaybeSerializableServiceRequest;
    type Response: MaybeSerializableServiceRequest;
}

impl<T, M, M2> SerializableService<(M, M2)> for T
where
    T: IntoServiceBuilder<M>,
    T::Service: IntoService<M2>,
    <T::Service as IntoService<M2>>::Request: MaybeSerializableServiceRequest,
    <T::Service as IntoService<M2>>::Response: MaybeSerializableServiceRequest,
{
    type Request = <T::Service as IntoService<M2>>::Request;
    type Response = <T::Service as IntoService<M2>>::Response;
}

#[derive(Debug)]
pub struct ServiceRegistration {
    id: &'static str,
    request_type: String,
    request_serializable: bool,
    response_type: String,
    response_serializable: bool,
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

// koonpeng: Do we need Arc like in [`bevy_reflect::TypeRegistryArc`]?
#[derive(Default)]
pub struct ServiceRegistry {
    registrations: HashMap<&'static str, ServiceRegistration>,
    gen: SchemaGenerator,
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
}

impl RegisterServiceExt for App {
    fn register_service<T, M>(&mut self, _service: &T) -> &mut Self
    where
        T: SerializableService<M>,
    {
        self.init_non_send_resource::<ServiceRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.non_send_resource_mut::<ServiceRegistry>();
        let id = std::any::type_name::<T>();
        let request_type = <T::Request as MaybeSerializableServiceRequest>::type_name();
        let request_serializable = match T::Request::try_json_schema(&mut registry.gen) {
            Some(_) => true,
            None => false,
        };
        let response_type = <T::Response as MaybeSerializableServiceRequest>::type_name();
        let response_serializable = match T::Response::try_json_schema(&mut registry.gen) {
            Some(_) => true,
            None => false,
        };
        registry.registrations.insert(
            id,
            ServiceRegistration {
                id,
                request_type,
                request_serializable,
                response_type,
                response_serializable,
            },
        );
        self
    }

    fn service_registration<T, M>(&self, _service: &T) -> Option<&ServiceRegistration>
    where
        T: SerializableService<M>,
    {
        match self.world.get_non_send_resource::<ServiceRegistry>() {
            Some(registry) => registry.registrations.get(std::any::type_name::<T>()),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use bevy_app::App;

    use crate::{AsyncServiceInput, BlockingServiceInput, IntoAsyncService, IntoBlockingService};

    use super::*;

    fn service_with_no_request() {}

    #[test]
    fn test_register_service() {
        let mut app = App::new();
        let srv = service_with_no_request.into_blocking_service();
        assert!(app.service_registration(&srv).is_none());
        app.register_service(&srv);
        let registration = app.service_registration(&srv).unwrap();
        assert!(registration.request_serializable);
        assert!(registration.response_serializable);
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
        assert!(registration.request_type == TestServiceRequest::schema_id());
        assert!(registration.request_serializable);
        assert!(registration.response_type == <()>::schema_id());
        assert!(registration.response_serializable);
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
        assert!(registration.request_type == TestServiceRequest::schema_id());
        assert!(registration.request_serializable);
        assert!(registration.response_type == TestServiceResponse::schema_id());
        assert!(registration.response_serializable);
    }

    struct TestNonSerializableRequest {}

    fn opaque_request_service(_: BlockingServiceInput<TestNonSerializableRequest>) {}

    #[test]
    fn test_register_opaque_service() {
        let mut app = App::new();
        let srv = opaque_request_service.into_opaque_request();
        app.register_service(&srv);
        let registration = app.service_registration(&srv).unwrap();
        assert!(!registration.request_serializable);
        assert!(registration.response_serializable);
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
        assert!(registration.request_serializable);
        assert!(!registration.response_serializable);
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
        assert!(!registration.request_serializable);
        assert!(!registration.response_serializable);
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
        assert!(registration.request_serializable);
        assert!(registration.response_serializable);
    }
}
