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

use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{de::DeserializeOwned, Serialize};

use super::{
    supported::*, DiagramContext, DiagramErrorCode, DynForkResult, DynInputSlot, DynOutput,
    JsonMessage, MessageRegistration, MessageRegistry, TypeInfo, TypeMismatch,
};
use crate::{Builder, JsonBuffer};

pub trait DynType {
    /// Returns the type name of the request. Note that the type name must be unique.
    fn type_name() -> Cow<'static, str>;

    fn json_schema(gen: &mut SchemaGenerator) -> Schema;
}

impl<T> DynType for T
where
    T: JsonSchema,
{
    fn type_name() -> Cow<'static, str> {
        <T>::schema_name()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        gen.subschema_for::<T>()
    }
}

pub trait SerializeMessage<T> {
    fn register_serialize(
        messages: &mut HashMap<TypeInfo, MessageRegistration>,
        schema_generator: &mut SchemaGenerator,
    );
}

impl<T> SerializeMessage<T> for Supported
where
    T: Serialize + DynType + Send + Sync + 'static,
{
    fn register_serialize(
        messages: &mut HashMap<TypeInfo, MessageRegistration>,
        schema_generator: &mut SchemaGenerator,
    ) {
        let reg = &mut messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>());

        reg.operations.serialize_impl = Some(|builder| {
            let serialize = builder.create_map_block(|message: T| {
                serde_json::to_value(message).map_err(|err| err.to_string())
            });

            let (ok, err) = serialize
                .output
                .chain(builder)
                .fork_result(|ok| ok.output(), |err| err.output());

            Ok(DynForkResult {
                input: serialize.input.into(),
                ok: ok.into(),
                err: err.into(),
            })
        });

        // Serialize and deserialize both generate the schema, so check before
        // generating it.
        if reg.schema.is_none() {
            reg.schema = Some(T::json_schema(schema_generator));
        }
    }
}

pub trait DeserializeMessage<T> {
    fn register_deserialize(
        messages: &mut HashMap<TypeInfo, MessageRegistration>,
        schema_generator: &mut SchemaGenerator,
    );
}

impl<T> DeserializeMessage<T> for Supported
where
    T: 'static + Send + Sync + DeserializeOwned + DynType,
{
    fn register_deserialize(
        messages: &mut HashMap<TypeInfo, MessageRegistration>,
        schema_generator: &mut SchemaGenerator,
    ) {
        let reg = &mut messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>());

        reg.operations.deserialize_impl = Some(|builder| {
            let deserialize = builder.create_map_block(|message: JsonMessage| {
                serde_json::from_value::<T>(message).map_err(|err| err.to_string())
            });

            let (ok, err) = deserialize
                .output
                .chain(builder)
                .fork_result(|ok| ok.output(), |err| err.output());

            Ok(DynForkResult {
                input: deserialize.input.into(),
                ok: ok.into(),
                err: err.into(),
            })
        });

        // Serialize and deserialize both generate the schema, so check before
        // generating it.
        if reg.schema.is_none() {
            reg.schema = Some(T::json_schema(schema_generator));
        }
    }
}

impl<T> SerializeMessage<T> for NotSupported {
    fn register_serialize(_: &mut HashMap<TypeInfo, MessageRegistration>, _: &mut SchemaGenerator) {
        // Do nothing
    }
}

impl<T> DeserializeMessage<T> for NotSupported {
    fn register_deserialize(
        _: &mut HashMap<TypeInfo, MessageRegistration>,
        _: &mut SchemaGenerator,
    ) {
        // Do nothing
    }
}

pub trait RegisterJson<T> {
    fn register_json();
}

pub struct JsonRegistration<Serializer, Deserializer> {
    _ignore: std::marker::PhantomData<fn(Serializer, Deserializer)>,
}

impl<T> RegisterJson<T> for JsonRegistration<Supported, Supported>
where
    T: 'static + Send + Sync + Serialize + DeserializeOwned,
{
    fn register_json() {
        JsonBuffer::register_for::<T>();
    }
}

impl<T> RegisterJson<T> for JsonRegistration<Supported, NotSupported> {
    fn register_json() {
        // Do nothing
    }
}

impl<T> RegisterJson<T> for JsonRegistration<NotSupported, Supported> {
    fn register_json() {
        // Do nothing
    }
}

impl<T> RegisterJson<T> for JsonRegistration<NotSupported, NotSupported> {
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

pub struct ImplicitSerialization {
    incoming_types: HashMap<TypeInfo, DynInputSlot>,
    serialized_input: Arc<DynInputSlot>,
}

impl ImplicitSerialization {
    pub fn new(serialized_input: DynInputSlot) -> Result<Self, DiagramErrorCode> {
        if serialized_input.message_info() != &TypeInfo::of::<JsonMessage>() {
            return Err(TypeMismatch {
                source_type: TypeInfo::of::<JsonMessage>(),
                target_type: *serialized_input.message_info(),
            }
            .into());
        }

        Ok(Self {
            serialized_input: Arc::new(serialized_input),
            incoming_types: Default::default(),
        })
    }

    /// Attempt to implicitly serialize an output before passing it into the
    /// input slot that this implicit serialization targets.
    ///
    /// If the incoming type cannot be serialized then it will be returned
    /// unchanged as the inner [`Err`].
    pub fn try_implicit_serialize(
        &mut self,
        incoming: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<Result<(), DynOutput>, DiagramErrorCode> {
        if incoming.message_info() == &TypeInfo::of::<JsonMessage>() {
            incoming.connect_to(&self.serialized_input, builder)?;
            return Ok(Ok(()));
        }

        let input = match self.incoming_types.entry(*incoming.message_info()) {
            Entry::Occupied(input_slot) => input_slot.get().clone(),
            Entry::Vacant(vacant) => {
                let Some(serialize) = ctx
                    .registry
                    .messages
                    .try_serialize(incoming.message_info(), builder)?
                else {
                    // We are unable to serialize this type.
                    return Ok(Err(incoming));
                };

                serialize.ok.connect_to(&self.serialized_input, builder)?;

                let error_target = ctx.get_implicit_error_target();
                ctx.add_output_into_target(error_target, serialize.err);

                vacant.insert(serialize.input).clone()
            }
        };

        incoming.connect_to(&input, builder)?;

        Ok(Ok(()))
    }

    /// Implicitly serialize an output. If the incoming message cannot be
    /// serialized then treat it is a diagram error.
    pub fn implicit_serialize(
        &mut self,
        incoming: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        self.try_implicit_serialize(incoming, builder, ctx)?
            .map_err(|incoming| DiagramErrorCode::NotSerializable(*incoming.message_info()))
    }

    pub fn serialized_input_slot(&self) -> &Arc<DynInputSlot> {
        &self.serialized_input
    }
}

pub struct ImplicitDeserialization {
    deserialized_input: Arc<DynInputSlot>,
    // The serialized input will only be created if a JsonMessage output
    // attempts to connect to this operation. Otherwise there is no need to
    // create it.
    serialized_input: Option<DynInputSlot>,
}

impl ImplicitDeserialization {
    pub fn try_new(
        deserialized_input: DynInputSlot,
        registration: &MessageRegistry,
    ) -> Result<Option<Self>, DiagramErrorCode> {
        if registration
            .messages
            .get(&deserialized_input.message_info())
            .and_then(|reg| reg.operations.deserialize_impl.as_ref())
            .is_some()
        {
            return Ok(Some(Self {
                deserialized_input: Arc::new(deserialized_input),
                serialized_input: None,
            }));
        }

        return Ok(None);
    }

    pub fn implicit_deserialize(
        &mut self,
        incoming: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        if incoming.message_info() == self.deserialized_input.message_info() {
            // Connect them directly because they match
            return incoming
                .connect_to(&self.deserialized_input, builder)
                .map_err(Into::into);
        }

        if incoming.message_info() == &TypeInfo::of::<JsonMessage>() {
            // Connect to the input for serialized messages
            let serialized_input = match self.serialized_input {
                Some(serialized_input) => serialized_input,
                None => {
                    let deserialize = ctx
                        .registry
                        .messages
                        .deserialize(self.deserialized_input.message_info(), builder)?;

                    deserialize
                        .ok
                        .connect_to(&self.deserialized_input, builder)?;

                    let error_target = ctx.get_implicit_error_target();
                    ctx.add_output_into_target(error_target, deserialize.err);

                    self.serialized_input = Some(deserialize.input);
                    deserialize.input
                }
            };

            return incoming
                .connect_to(&serialized_input, builder)
                .map_err(Into::into);
        }

        Err(TypeMismatch {
            source_type: *incoming.message_info(),
            target_type: *self.deserialized_input.message_info(),
        }
        .into())
    }

    pub fn deserialized_input_slot(&self) -> &Arc<DynInputSlot> {
        &self.deserialized_input
    }
}

pub struct ImplicitStringify {
    incoming_types: HashMap<TypeInfo, DynInputSlot>,
    string_input: DynInputSlot,
}

impl ImplicitStringify {
    pub fn new(string_input: DynInputSlot) -> Result<Self, DiagramErrorCode> {
        if string_input.message_info() != &TypeInfo::of::<String>() {
            return Err(TypeMismatch {
                source_type: TypeInfo::of::<String>(),
                target_type: *string_input.message_info(),
            }
            .into());
        }

        Ok(Self {
            string_input,
            incoming_types: Default::default(),
        })
    }

    pub fn try_implicit_stringify(
        &mut self,
        incoming: DynOutput,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<Result<(), DynOutput>, DiagramErrorCode> {
        if incoming.message_info() == &TypeInfo::of::<String>() {
            incoming.connect_to(&self.string_input, builder)?;
            return Ok(Ok(()));
        }

        let input = match self.incoming_types.entry(*incoming.message_info()) {
            Entry::Occupied(input_slot) => input_slot.get().clone(),
            Entry::Vacant(vacant) => {
                let Some(stringify) = ctx
                    .registry
                    .messages
                    .try_to_string(incoming.message_info(), builder)?
                else {
                    // We are unable to stringify this type.
                    return Ok(Err(incoming));
                };

                stringify.output.connect_to(&self.string_input, builder)?;
                vacant.insert(stringify.input).clone()
            }
        };

        incoming.connect_to(&input, builder)?;

        Ok(Ok(()))
    }
}
