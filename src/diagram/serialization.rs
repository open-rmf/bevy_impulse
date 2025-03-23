/*
 * Copyright (C) 2025 Open Source Robotics Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

use std::collections::HashMap;

use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{de::DeserializeOwned, Serialize};

use super::{type_info::TypeInfo, MessageRegistration};
use crate::JsonBuffer;

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
    T: Serialize + DynType + Send + Sync + 'static,
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
        // Register the ability to cast this message's buffer type
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

pub(super) fn register_serialize<T, Serializer>(
    messages: &mut HashMap<TypeInfo, MessageRegistration>,
    schema_generator: &mut SchemaGenerator,
) -> bool
where
    T: Send + Sync + 'static,
    Serializer: SerializeMessage<T>,
{
    let reg = &mut messages
        .entry(TypeInfo::of::<T>())
        .or_insert(MessageRegistration::new::<T>());
    let ops = &mut reg.operations;
    if !Serializer::serializable() || ops.serialize_impl.is_some() {
        return false;
    }

    ops.serialize_impl = Some(|builder, output| {
        let n = builder.create_map_block(|resp: T| Serializer::to_json(&resp));
        builder.connect(output.into_output()?, n.input);
        let serialized_output = n.output.chain(builder).cancel_on_err().output();
        Ok(serialized_output)
    });

    reg.schema = Serializer::json_schema(schema_generator);

    true
}

pub trait RegisterJson<T> {
    fn register_json();
}

pub struct JsonRegistration<Serializer, Deserializer> {
    _ignore: std::marker::PhantomData<fn(Serializer, Deserializer)>,
}

impl<T> RegisterJson<T> for JsonRegistration<DefaultSerializer, DefaultDeserializer>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    fn register_json() {
        JsonBuffer::register_for::<T>();
    }
}

impl<T> RegisterJson<T> for JsonRegistration<DefaultSerializer, OpaqueMessageDeserializer> {
    fn register_json() {
        // Do nothing
    }
}

impl<T> RegisterJson<T> for JsonRegistration<OpaqueMessageSerializer, DefaultDeserializer> {
    fn register_json() {
        // Do nothing
    }
}

impl<T> RegisterJson<T> for JsonRegistration<OpaqueMessageSerializer, OpaqueMessageDeserializer> {
    fn register_json() {
        // Do nothing
    }
}

pub(super) fn register_json<T, Serializer, Deserializer>()
where
    JsonRegistration<Serializer, Deserializer>: RegisterJson<T>,
{
    JsonRegistration::<Serializer, Deserializer>::register_json();
}
