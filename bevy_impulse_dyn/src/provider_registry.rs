use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use bevy_app::App;
use schemars::gen::{SchemaGenerator, SchemaSettings};
use serde::Serialize;

use crate::{MessageMetadata, NonSerializableMessage};

/// Provider id must be fixed and (most likely) known at compile time.
pub type ProviderId = &'static str;

#[derive(Debug, Serialize)]
pub struct ProviderRegistration {
    pub id: ProviderId,
    /// Friendly name for the provider, may not be unique.
    pub name: &'static str,
    pub request: MessageMetadata,
    pub response: MessageMetadata,
}

impl PartialEq for ProviderRegistration {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for ProviderRegistration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Serialize)]
pub struct ServiceRegistration {
    pub id: ProviderId,
    pub name: &'static str,
    pub request: MessageMetadata,
    pub response: MessageMetadata,
    /// type name of the `configure` type
    pub configure: Option<String>,
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

pub struct OpaqueProvider<Request, Response, M> {
    _unused: PhantomData<(Request, Response, M)>,
}

pub type FullOpaqueProvider<Request, Response, M> =
    OpaqueProvider<NonSerializableMessage<Request>, NonSerializableMessage<Response>, M>;

pub type OpaqueRequestProvider<Request, Response, M> =
    OpaqueProvider<NonSerializableMessage<Request>, Response, M>;

pub type OpaqueResponseProvider<Request, Response, M> =
    OpaqueProvider<Request, NonSerializableMessage<Response>, M>;

pub trait IntoOpaqueExt<Request, Response, M> {
    /// Mark this provider as fully opaque, this means that both the request and response cannot
    /// be serialized. Opaque services can still be registered into the service registry but
    /// their request and response types are undefined and cannot be transformed.
    fn into_opaque(&self) -> FullOpaqueProvider<Request, Response, M> {
        FullOpaqueProvider::<Request, Response, M> {
            _unused: Default::default(),
        }
    }

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the request as opaque.
    fn into_opaque_request(&self) -> OpaqueRequestProvider<Request, Response, M> {
        OpaqueRequestProvider::<Request, Response, M> {
            _unused: PhantomData,
        }
    }

    /// Similar to [`OpaqueRequestExt::into_opaque`] but only mark the response as opaque.
    fn into_opaque_response(&self) -> OpaqueResponseProvider<Request, Response, M> {
        OpaqueResponseProvider::<Request, Response, M> {
            _unused: PhantomData,
        }
    }
}

fn serialize_provider_registry_types<S>(gen: &SchemaGenerator, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    gen.definitions().serialize(s)
}

#[derive(Serialize)]
pub struct ProviderRegistry {
    /// List of services registered.
    pub services: HashMap<ProviderId, ServiceRegistration>,

    /// List of callbacks registered.
    pub callbacks: HashMap<ProviderId, ProviderRegistration>,

    /// List of maps registered.
    pub maps: HashMap<ProviderId, ProviderRegistration>,

    /// List of all request and response types used in all registered services, this only
    /// contains serializable types, non serializable types are opaque and is only compatible
    /// with itself.
    #[serde(rename = "types", serialize_with = "serialize_provider_registry_types")]
    pub(crate) gen: SchemaGenerator,
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/types/".to_string();
        ProviderRegistry {
            services: HashMap::<ProviderId, ServiceRegistration>::default(),
            callbacks: HashMap::<ProviderId, ProviderRegistration>::default(),
            maps: HashMap::<ProviderId, ProviderRegistration>::default(),
            gen: SchemaGenerator::new(settings),
        }
    }
}

pub trait ProviderRegistryExt {
    fn provider_registry(&mut self) -> &ProviderRegistry;
}

impl ProviderRegistryExt for App {
    fn provider_registry(&mut self) -> &ProviderRegistry {
        self.init_non_send_resource::<ProviderRegistry>(); // nothing happens if the resource already exist
        self.world.non_send_resource::<ProviderRegistry>()
    }
}
