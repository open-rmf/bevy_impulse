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
    registrations: HashMap<TypeId, ServiceRegistration>,
}

pub trait RegisterServiceExt<M> {
    fn register_blocking_service<T>(&mut self, service: T) -> &mut Self
    where
        T: IntoBlockingService<M> + Any;

    fn service_registration<T>(&self, _: T) -> Option<&ServiceRegistration>
    where
        T: IntoBlockingService<M> + Any;
}

impl<M> RegisterServiceExt<M> for App {
    fn register_blocking_service<T>(&mut self, service: T) -> &mut Self
    where
        T: IntoBlockingService<M> + Any,
    {
        self.init_resource::<ServiceRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.world.resource_mut::<ServiceRegistry>();
        let id = std::any::type_name::<T>();
        registry
            .registrations
            .insert(service.type_id(), ServiceRegistration { id });
        self
    }

    fn service_registration<T>(&self, service: T) -> Option<&ServiceRegistration>
    where
        T: IntoBlockingService<M> + Any,
    {
        match self.world.get_resource::<ServiceRegistry>() {
            Some(registry) => registry.registrations.get(&service.type_id()),
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
