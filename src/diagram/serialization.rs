use std::{error::Error, fmt::Display};

use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Debug)]
pub enum SerializationError {
    NotSupported,
    JsonError(serde_json::Error),
}

impl Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSupported => f.write_str("not supported"),
            Self::JsonError(err) => err.fmt(f),
        }
    }
}

impl Error for SerializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NotSupported => None,
            Self::JsonError(err) => Some(err),
        }
    }
}

impl From<serde_json::Error> for SerializationError {
    fn from(value: serde_json::Error) -> Self {
        Self::JsonError(value)
    }
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
        gen.subschema_for::<()>()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RequestMetadata {
    /// The type name of the request.
    pub(super) r#type: String,

    /// Indicates if the request is deserializable.
    pub(super) deserializable: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResponseMetadata {
    /// The type name of the response.
    pub(super) r#type: String,

    /// Indicates if the response is serializable.
    pub(super) serializable: bool,

    /// Indicates if the response is cloneable, a node must have a cloneable response
    /// in order to connect it to a "fork clone" operation.
    pub(super) cloneable: bool,

    /// The number of unzip slots that a response have, a value of 0 means that the response
    /// cannot be unzipped. This should be > 0 only if the response is a tuple.
    pub(super) unzip_slots: usize,
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
