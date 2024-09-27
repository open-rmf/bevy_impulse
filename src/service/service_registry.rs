use std::{
    any::{Any, TypeId},
    hash::{Hash, Hasher},
};

use bevy_app::App;
use bevy_ecs::system::Resource;
use bevy_utils::HashMap;

use super::IntoBlockingService;

#[derive(Eq, Debug)]
pub struct ServiceRegistration {
    id: &'static str,
    // TODO(koonpeng): Add request and response
    // request_schema: serde_json::Value
    // response_schema: serde_json::Value
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
#[derive(Resource, Default)]
pub struct ServiceRegistry {
    registrations: HashMap<&'static str, ServiceRegistration>,
}

pub trait RegisterServiceExt {
    fn register_blocking_service<T, M>(&mut self, service: T) -> &mut Self
    where
        T: IntoBlockingService<M> + Any;

    fn service_registration<T>(&self, service: T) -> Option<&ServiceRegistration>
    where
        T: Any;
}

impl RegisterServiceExt for App {
    fn register_blocking_service<T, M>(&mut self, _service: T) -> &mut Self
    where
        T: IntoBlockingService<M> + Any,
    {
        self.init_resource::<ServiceRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.resource_mut::<ServiceRegistry>();
        let id = std::any::type_name::<T>();
        registry
            .registrations
            .insert(id, ServiceRegistration { id });
        self
    }

    fn service_registration<T>(&self, _service: T) -> Option<&ServiceRegistration>
    where
        T: Any,
    {
        match self.world.get_resource::<ServiceRegistry>() {
            Some(registry) => registry.registrations.get(std::any::type_name::<T>()),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use bevy_app::App;

    use super::*;

    fn noop() {}

    #[test]
    fn test_register_service() {
        let mut app = App::new();
        assert!(app.service_registration(noop).is_none());
        app.register_blocking_service(noop);
        assert!(app.service_registration(noop).is_some());
    }
}
