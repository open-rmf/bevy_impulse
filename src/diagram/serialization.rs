use std::{error::Error, fmt::Display};

use schemars::{gen::SchemaGenerator, JsonSchema};
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

#[derive(Debug, Serialize)]
pub struct MessageMetadata {
    /// The type of the message, if the message is serializable, this will be the json schema
    /// type, if it is not serializable, it will be the rust type.
    pub(crate) r#type: String,

    /// Indicates if the message is serializable.
    pub(crate) serializable: bool,
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

pub trait SerializeMessage<T> {
    fn to_json(v: &T) -> Result<serde_json::Value, SerializationError>;

    fn message_metadata(gen: &mut SchemaGenerator) -> MessageMetadata;
}

#[derive(Default)]
pub struct DefaultSerializer;

impl<T> SerializeMessage<T> for DefaultSerializer
where
    T: Serialize + DynType,
{
    fn to_json(v: &T) -> Result<serde_json::Value, SerializationError> {
        serde_json::to_value(v).map_err(|err| SerializationError::from(err))
    }

    fn message_metadata(gen: &mut SchemaGenerator) -> MessageMetadata {
        T::json_schema(gen);
        MessageMetadata {
            r#type: T::type_name(),
            serializable: true,
        }
    }
}

pub trait DeserializeMessage<T> {
    fn from_json(json: serde_json::Value) -> Result<T, SerializationError>;

    fn message_metadata(gen: &mut SchemaGenerator) -> MessageMetadata;
}

#[derive(Default)]
pub struct DefaultDeserializer;

impl<T> DeserializeMessage<T> for DefaultDeserializer
where
    T: DeserializeOwned + DynType,
{
    fn from_json(json: serde_json::Value) -> Result<T, SerializationError> {
        serde_json::from_value::<T>(json).map_err(|err| SerializationError::from(err))
    }

    fn message_metadata(gen: &mut SchemaGenerator) -> MessageMetadata {
        T::json_schema(gen);
        MessageMetadata {
            r#type: T::type_name(),
            serializable: true,
        }
    }
}

#[derive(Default)]
pub struct OpaqueMessageSerializer;

impl<T> SerializeMessage<T> for OpaqueMessageSerializer {
    fn to_json(_v: &T) -> Result<serde_json::Value, SerializationError> {
        Err(SerializationError::NotSupported)
    }

    fn message_metadata(_gen: &mut SchemaGenerator) -> MessageMetadata {
        MessageMetadata {
            r#type: std::any::type_name::<T>().to_string(),
            serializable: false,
        }
    }
}

#[derive(Default)]
pub struct OpaqueMessageDeserializer;

impl<T> DeserializeMessage<T> for OpaqueMessageDeserializer {
    fn from_json(_json: serde_json::Value) -> Result<T, SerializationError> {
        Err(SerializationError::NotSupported)
    }

    fn message_metadata(_gen: &mut SchemaGenerator) -> MessageMetadata {
        MessageMetadata {
            r#type: std::any::type_name::<T>().to_string(),
            serializable: false,
        }
    }
}
