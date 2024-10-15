use std::future::Future;

use bevy_app::App;
use bevy_impulse::{AsyncMapDef, AsyncMapOnceDef, BlockingMapDef, BlockingMapOnceDef};

use crate::{IntoOpaqueExt, OpaqueProvider, ProviderRegistration, ProviderRegistry, Serializable};

pub trait SerializableMap {
    type Request: Serializable;
    type Response: Serializable;

    fn insert_into_registry(registry: &mut ProviderRegistry, id: &'static str, name: &'static str) {
        registry.maps.insert(
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

impl<Def, Request, Response, Streams> SerializableMap
    for BlockingMapDef<Def, Request, Response, Streams>
where
    Request: Serializable,
    Response: Serializable,
{
    type Request = Request;
    type Response = Response;
}

impl<Def, Request, Response, Streams> SerializableMap
    for BlockingMapOnceDef<Def, Request, Response, Streams>
where
    Request: Serializable,
    Response: Serializable,
{
    type Request = Request;
    type Response = Response;
}

impl<Def, Request, Task, Streams> SerializableMap for AsyncMapDef<Def, Request, Task, Streams>
where
    Request: Serializable,
    Task: Future,
    Task::Output: Serializable,
{
    type Request = Request;
    type Response = Task::Output;
}

impl<Def, Request, Task, Streams> SerializableMap for AsyncMapOnceDef<Def, Request, Task, Streams>
where
    Request: Serializable,
    Task: Future,
    Task::Output: Serializable,
{
    type Request = Request;
    type Response = Task::Output;
}

struct OpaqueMapMarker;

impl<Def, Request, Response, Streams> IntoOpaqueExt<Request, Response, OpaqueMapMarker>
    for BlockingMapDef<Def, Request, Response, Streams>
{
}

impl<Request, Response> SerializableMap for OpaqueProvider<Request, Response, OpaqueMapMarker>
where
    Request: Serializable,
    Response: Serializable,
{
    type Request = Request;
    type Response = Response;
}

pub trait RegisterMapExt {
    fn register_map<T>(&mut self, map: &T, id: &'static str, name: &'static str) -> &mut Self
    where
        T: SerializableMap;

    fn map_registration(&self, id: &'static str) -> Option<&ProviderRegistration>;
}

impl RegisterMapExt for App {
    fn register_map<T>(&mut self, _map: &T, id: &'static str, name: &'static str) -> &mut Self
    where
        T: SerializableMap,
    {
        self.init_non_send_resource::<ProviderRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.non_send_resource_mut::<ProviderRegistry>();
        T::insert_into_registry(&mut registry, id, name);
        self
    }

    fn map_registration(&self, id: &'static str) -> Option<&ProviderRegistration> {
        match self.world.get_non_send_resource::<ProviderRegistry>() {
            Some(registry) => registry.maps.get(id),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use bevy_impulse::{AsMap, AsMapOnce, AsyncMap, BlockingMap};
    use schemars::JsonSchema;
    use serde::Serialize;

    use super::*;

    #[test]
    fn test_register_map() {
        let mut app = App::new();

        #[derive(JsonSchema, Serialize)]
        struct TestRequest {}

        let map_with_req = |_: BlockingMap<TestRequest, ()>| {};
        app.register_map(&map_with_req.as_map(), "map_with_req", "map_with_req_name");
        let registration = app.map_registration("map_with_req").unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "map_with_req_name");

        app.register_map(
            &map_with_req.as_map_once(),
            "map_with_req_once",
            "map_with_req_once_name",
        );
        let registration = app.map_registration("map_with_req_once").unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "map_with_req_once_name");

        async fn async_map(_: AsyncMap<TestRequest, ()>) {}
        app.register_map(&async_map.as_map(), "async_map", "async_map_name");
        let registration = app.map_registration("async_map").unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "async_map_name");

        app.register_map(
            &async_map.as_map_once(),
            "async_map_once",
            "async_map_once_name",
        );
        let registration = app.map_registration("async_map_once").unwrap();
        assert!(registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "async_map_once_name");
    }

    #[test]
    fn test_register_opaque_map() {
        let mut app = App::new();

        struct TestRequest {}
        let opaque_req_map = |_: BlockingMap<TestRequest, ()>| {};

        app.register_map(
            &opaque_req_map.as_map().into_opaque_request(),
            "opaque_req_map",
            "opaque_req_map_name",
        );
        let registration = app.map_registration("opaque_req_map").unwrap();
        assert!(!registration.request.serializable);
        assert!(registration.response.serializable);
        assert!(registration.name == "opaque_req_map_name");

        let opaque_req_resp_map = |_: BlockingMap<TestRequest, ()>| TestRequest {};
        app.register_map(
            &opaque_req_resp_map.as_map().into_opaque(),
            "opaque_req_resp_map",
            "opaque_req_resp_map_name",
        );
        let registration = app.map_registration("opaque_req_resp_map").unwrap();
        assert!(!registration.request.serializable);
        assert!(!registration.response.serializable);
        assert!(registration.name == "opaque_req_resp_map_name");
    }
}
