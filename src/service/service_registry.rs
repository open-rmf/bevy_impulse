use std::{
    any::Any,
    hash::{Hash, Hasher},
};

use bevy_app::App;
use bevy_utils::HashMap;
use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};

use crate::BlockingService;

use super::{AsBlockingService, IntoBlockingService};

pub trait SerializableServiceRequest {
    type Request: JsonSchema;

    fn type_name() -> String {
        Self::Request::schema_name()
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

pub trait MaybeSerializableServiceRequest {
    type Request;

    fn type_name() -> String {
        std::any::type_name::<Self::Request>().to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Option<Schema> {
        None
    }
}

impl<T> MaybeSerializableServiceRequest for T
where
    T: SerializableServiceRequest,
{
    type Request = T;

    fn type_name() -> String {
        T::type_name()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Option<Schema> {
        Some(T::json_schema(gen))
    }
}

pub trait SerializableService<Request, Response, M> {}

impl<T, Request, Response, M> SerializableService<Request, Response, M> for T where
    T: IntoBlockingService<AsBlockingService<(Request, Response, M)>>
{
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
    fn register_service<T, M, Request, Response>(&mut self, service: T) -> &mut Self
    where
        Request: SerializableServiceRequest,
        Response: SerializableServiceRequest,
        T: SerializableService<Request, Response, M>,
    {
        self.register_opaque_service(service)
    }

    fn register_opaque_service<T, M, Request, Response>(&mut self, service: T) -> &mut Self
    where
        Request: MaybeSerializableServiceRequest,
        Response: MaybeSerializableServiceRequest,
        T: SerializableService<Request, Response, M>;

    fn service_registration<T>(&self, service: T) -> Option<&ServiceRegistration>
    where
        T: Any;
}

impl RegisterServiceExt for App {
    fn register_opaque_service<T, M, Request, Response>(&mut self, _service: T) -> &mut Self
    where
        Request: MaybeSerializableServiceRequest,
        Response: MaybeSerializableServiceRequest,
        T: SerializableService<Request, Response, M>,
    {
        self.init_non_send_resource::<ServiceRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.non_send_resource_mut::<ServiceRegistry>();
        let id = std::any::type_name::<T>();
        let request_type = Request::type_name();
        let request_serializable = match Request::json_schema(&mut registry.gen) {
            Some(_) => true,
            None => false,
        };
        let response_type = Response::type_name();
        let response_serializable = match Response::json_schema(&mut registry.gen) {
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

    fn service_registration<T>(&self, _service: T) -> Option<&ServiceRegistration>
    where
        T: Any,
    {
        match self.world.get_non_send_resource::<ServiceRegistry>() {
            Some(registry) => registry.registrations.get(std::any::type_name::<T>()),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use bevy_app::App;

    use crate::BlockingServiceInput;

    use super::*;

    fn service_with_no_request() {}

    #[test]
    fn test_register_service() {
        let mut app = App::new();
        assert!(app.service_registration(service_with_no_request).is_none());
        app.register_service(service_with_no_request);
        let registration = app.service_registration(service_with_no_request).unwrap();
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
        app.register_service(service_with_request);
        let registration = app.service_registration(service_with_request).unwrap();
        assert!(registration.request_type == TestServiceRequest::schema_name());
        assert!(registration.request_serializable);
        assert!(registration.response_type == <()>::schema_name());
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
        app.register_service(service_with_req_resp);
        let registration = app.service_registration(service_with_req_resp).unwrap();
        assert!(registration.request_type == TestServiceRequest::schema_name());
        assert!(registration.request_serializable);
        assert!(registration.response_type == TestServiceResponse::schema_name());
        assert!(registration.response_serializable);
    }

    struct TestNonSerializableRequest {}

    impl MaybeSerializableServiceRequest for TestNonSerializableRequest {
        type Request = TestNonSerializableRequest;
    }

    impl MaybeSerializableServiceRequest for BlockingService<TestNonSerializableRequest> {
        type Request = TestNonSerializableRequest;
    }

    fn opaque_service(_: BlockingServiceInput<TestNonSerializableRequest>) {}

    #[test]
    fn test_register_opaque_service() {
        let mut app = App::new();
        app.register_opaque_service(opaque_service);
        let registration = app.service_registration(opaque_service).unwrap();
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
        app.register_opaque_service(opaque_response_service);
        let registration = app.service_registration(opaque_response_service).unwrap();
        assert!(registration.request_serializable);
        assert!(!registration.response_serializable);
    }
}
