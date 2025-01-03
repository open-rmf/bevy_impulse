use std::any::TypeId;

use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{de::DeserializeOwned, Serialize};
use tracing::debug;

use super::NodeRegistry;

#[derive(thiserror::Error, Debug)]
pub enum SerializationError {
    #[error("not supported")]
    NotSupported,

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

pub trait DynType {
    /// Returns the type name of the request. Note that the type name must be unique.
    fn type_name() -> String;

    fn json_schema(gen: &mut SchemaGenerator) -> schemars::schema::Schema;
}

impl<T> DynType for T
where
    T: JsonSchema,
{
    fn type_name() -> String {
        <T>::schema_name()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> schemars::schema::Schema {
        gen.subschema_for::<T>()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RequestMetadata {
    /// The JSON Schema of the request.
    pub(super) schema: Schema,

    /// Indicates if the request is deserializable.
    pub(super) deserializable: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResponseMetadata {
    /// The JSON Schema of the response.
    pub(super) schema: Schema,

    /// Indicates if the response is serializable.
    pub(super) serializable: bool,

    /// Indicates if the response is cloneable, a node must have a cloneable response
    /// in order to connect it to a "fork clone" operation.
    pub(super) cloneable: bool,

    /// The number of unzip slots that a response have, a value of 0 means that the response
    /// cannot be unzipped. This should be > 0 only if the response is a tuple.
    pub(super) unzip_slots: usize,

    /// Indicates if the response can fork result
    pub(super) fork_result: bool,

    /// Indiciates if the response can be split
    pub(super) splittable: bool,
}

pub trait SerializeMessage<T> {
    fn type_name() -> String;

    fn json_schema(gen: &mut SchemaGenerator) -> Option<Schema>;

    fn to_json(v: &T) -> Result<serde_json::Value, SerializationError>;

    fn serializable() -> bool;
}

#[derive(Default)]
pub struct DefaultSerializer;

impl<T> SerializeMessage<T> for DefaultSerializer
where
    T: Serialize + DynType,
{
    fn type_name() -> String {
        T::type_name()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Option<Schema> {
        Some(T::json_schema(gen))
    }

    fn to_json(v: &T) -> Result<serde_json::Value, SerializationError> {
        serde_json::to_value(v).map_err(|err| SerializationError::from(err))
    }

    fn serializable() -> bool {
        true
    }
}

pub trait DeserializeMessage<T> {
    fn type_name() -> String;

    fn json_schema(gen: &mut SchemaGenerator) -> Option<Schema>;

    fn from_json(json: serde_json::Value) -> Result<T, SerializationError>;

    fn deserializable() -> bool;
}

#[derive(Default)]
pub struct DefaultDeserializer;

impl<T> DeserializeMessage<T> for DefaultDeserializer
where
    T: DeserializeOwned + DynType,
{
    fn type_name() -> String {
        T::type_name()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Option<Schema> {
        Some(T::json_schema(gen))
    }

    fn from_json(json: serde_json::Value) -> Result<T, SerializationError> {
        serde_json::from_value::<T>(json).map_err(|err| SerializationError::from(err))
    }

    fn deserializable() -> bool {
        true
    }
}

#[derive(Default)]
pub struct OpaqueMessageSerializer;

impl<T> SerializeMessage<T> for OpaqueMessageSerializer {
    fn type_name() -> String {
        std::any::type_name::<T>().to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Option<Schema> {
        None
    }

    fn to_json(_v: &T) -> Result<serde_json::Value, SerializationError> {
        Err(SerializationError::NotSupported)
    }

    fn serializable() -> bool {
        false
    }
}

#[derive(Default)]
pub struct OpaqueMessageDeserializer;

impl<T> DeserializeMessage<T> for OpaqueMessageDeserializer {
    fn type_name() -> String {
        std::any::type_name::<T>().to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Option<Schema> {
        None
    }

    fn from_json(_json: serde_json::Value) -> Result<T, SerializationError> {
        Err(SerializationError::NotSupported)
    }

    fn deserializable() -> bool {
        false
    }
}

pub(super) fn register_deserialize<T, Deserializer>(registry: &mut NodeRegistry)
where
    Deserializer: DeserializeMessage<T>,
    T: Send + Sync + 'static,
{
    if registry.deserialize_impls.contains_key(&TypeId::of::<T>())
        || !Deserializer::deserializable()
    {
        return;
    }

    debug!(
        "register deserialize for type: {}, with deserializer: {}",
        std::any::type_name::<T>(),
        std::any::type_name::<Deserializer>()
    );
    registry.deserialize_impls.insert(
        TypeId::of::<T>(),
        Box::new(|builder, output| {
            debug!("deserialize output: {:?}", output);
            let receiver =
                builder.create_map_block(|json: serde_json::Value| Deserializer::from_json(json));
            builder.connect(output, receiver.input);
            let deserialized_output = receiver
                .output
                .chain(builder)
                .cancel_on_err()
                .output()
                .into();
            debug!("deserialized output: {:?}", deserialized_output);
            deserialized_output
        }),
    );
}

pub(super) fn register_serialize<T, Serializer>(registry: &mut NodeRegistry)
where
    Serializer: SerializeMessage<T>,
    T: Send + Sync + 'static,
{
    if registry.serialize_impls.contains_key(&TypeId::of::<T>()) || !Serializer::serializable() {
        return;
    }

    debug!(
        "register serialize for type: {}, with serializer: {}",
        std::any::type_name::<T>(),
        std::any::type_name::<Serializer>()
    );
    registry.serialize_impls.insert(
        TypeId::of::<T>(),
        Box::new(|builder, output| {
            debug!("serialize output: {:?}", output);
            let n = builder.create_map_block(|resp: T| Serializer::to_json(&resp));
            builder.connect(output.into_output(), n.input);
            let serialized_output = n.output.chain(builder).cancel_on_err().output();
            debug!("serialized output: {:?}", serialized_output);
            serialized_output
        }),
    );
}
