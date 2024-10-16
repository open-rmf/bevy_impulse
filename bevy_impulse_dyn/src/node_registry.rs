use std::collections::HashMap;

use bevy_ecs::{entity::Entity, world::World};
use bevy_impulse::{Builder, Node, StreamPack};
use schemars::gen::{SchemaGenerator, SchemaSettings};
use serde::Serialize;

use crate::{MessageMetadata, Serializable};

pub struct DynInputSlot {
    scope: Entity,
    source: Entity,
}

pub struct DynOutput {
    scope: Entity,
    target: Entity,
}

/// A type erased [`bevy_impulse::Node`]
pub struct DynNode {
    pub input: DynInputSlot,
    pub output: DynOutput,
}

impl<Request, Response, Streams> From<Node<Request, Response, Streams>> for DynNode
where
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn from(value: Node<Request, Response, Streams>) -> Self {
        DynNode {
            input: DynInputSlot {
                scope: value.input.scope(),
                source: value.input.id(),
            },
            output: DynOutput {
                scope: value.output.scope(),
                target: value.output.id(),
            },
        }
    }
}

#[derive(Serialize)]
pub struct NodeRegistration {
    pub id: &'static str,
    pub name: &'static str,
    pub request: MessageMetadata,
    pub response: MessageMetadata,

    #[serde(skip)]
    create_node: Box<dyn Fn(&mut Builder) -> DynNode>,
}

fn serialize_provider_registry_types<S>(gen: &SchemaGenerator, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    gen.definitions().serialize(s)
}

#[derive(Serialize)]
pub struct NodeRegistry {
    nodes: HashMap<&'static str, NodeRegistration>,

    /// List of all request and response types used in all registered nodes, this only
    /// contains serializable types, non serializable types are opaque and is only compatible
    /// with itself.
    #[serde(rename = "types", serialize_with = "serialize_provider_registry_types")]
    gen: SchemaGenerator,
}

impl Default for NodeRegistry {
    fn default() -> Self {
        let mut settings = SchemaSettings::default();
        settings.definitions_path = "#/types/".to_string();
        NodeRegistry {
            nodes: HashMap::<&'static str, NodeRegistration>::default(),
            gen: SchemaGenerator::new(settings),
        }
    }
}

pub trait RegisterNodeExt {
    fn register_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: fn(&mut Builder) -> Node<Request, Response, Streams>,
    ) -> &mut Self
    where
        Request: Serializable + 'static,
        Response: Serializable + 'static + Send + Sync,
        Streams: StreamPack;
}

impl RegisterNodeExt for World {
    fn register_node<Request, Response, Streams>(
        &mut self,
        id: &'static str,
        name: &'static str,
        f: fn(&mut Builder) -> Node<Request, Response, Streams>,
    ) -> &mut Self
    where
        Request: Serializable + 'static,
        Response: Serializable + 'static + Send + Sync,
        Streams: StreamPack,
    {
        self.init_non_send_resource::<NodeRegistry>(); // nothing happens if the resource already exist
        let mut registry = self.non_send_resource_mut::<NodeRegistry>();
        let request = Request::insert_json_schema(&mut registry.gen);
        let response = Response::insert_json_schema(&mut registry.gen);
        let create_node = Box::new(move |builder: &mut Builder| f(builder).into());
        registry.nodes.insert(
            id,
            NodeRegistration {
                id,
                name,
                request,
                response,
                create_node,
            },
        );
        self
    }
}

#[cfg(test)]
mod tests {
    use bevy_app::App;
    use bevy_impulse::BlockingMap;

    use super::*;

    #[test]
    fn test_register_workflow() {
        let mut app = App::new();

        fn test_map(_: BlockingMap<()>) {}

        app.world
            .register_node("test_node", "Test Name", |builder| {
                builder.create_map(test_map)
            });
    }
}
