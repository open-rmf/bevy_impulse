use std::marker::PhantomData;

use schemars::{gen::SchemaGenerator, JsonSchema};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct MessageMetadata {
    /// The type of the message, if the message is serializable, this will be the json schema
    /// type, if it is not serializable, it will be the rust type.
    pub r#type: String,

    /// Indicates if the message is serializable.
    pub serializable: bool,
}

pub trait DynType {
    /// Returns the type name of the request. Note that the type name must be unique.
    fn type_name() -> String;

    /// Insert the request type into the schema generator and returns the request metadata.
    fn insert_json_schema(gen: &mut SchemaGenerator) -> MessageMetadata;
}

impl<T> DynType for T
where
    T: JsonSchema,
{
    fn type_name() -> String {
        <()>::schema_name()
    }

    fn insert_json_schema(gen: &mut SchemaGenerator) -> MessageMetadata {
        gen.subschema_for::<()>();
        MessageMetadata {
            r#type: Self::type_name(),
            serializable: true,
        }
    }
}

pub struct OpaqueMessage<T> {
    _unused: PhantomData<T>,
}

impl<T> DynType for OpaqueMessage<T> {
    fn type_name() -> String {
        std::any::type_name::<T>().to_string()
    }

    fn insert_json_schema(_gen: &mut SchemaGenerator) -> MessageMetadata {
        MessageMetadata {
            r#type: Self::type_name(),
            serializable: false,
        }
    }
}

/// Helper trait to unwrap the request type of a wrapped request like `BlockingMap<Request, _>`.
pub trait InferDynRequest<Request>
where
    Request: DynType,
{
}

impl<Request> InferDynRequest<Request> for Request where Request: DynType {}
