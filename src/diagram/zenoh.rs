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

use super::*;
use crate::prelude::*;

use ::zenoh::{
    bytes::{Encoding, ZBytes},
    qos::{CongestionControl, Priority},
    sample::{Locality, Sample},
    Session,
};
use bevy_derive::{Deref, DerefMut};
use bevy_ecs::{
    prelude::{Resource, World},
    system::Command,
};
use futures::future::Shared;
use prost_reflect::{
    prost::Message, DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions,
};
use std::{error::Error, future::Future, sync::Mutex};
use thiserror::Error as ThisError;
use tokio::sync::mpsc::UnboundedSender;
use zenoh_ext::z_deserialize;

mod register_zenoh_publisher;
mod register_zenoh_querier;
mod register_zenoh_subscription;

impl DiagramElementRegistry {
    /// Add nodes to the registry that allow you to interact with the
    /// [zenoh](https://zenoh.io/) middleware. This includes:
    /// - `zenoh_subscription`` - begin subscribing to a key when the node is triggered,
    ///   and stream out incoming messages until cancelled.
    /// - `zenoh_publisher` - advertise a publisher when the workflow is created,
    ///   and then publish each message that gets passed into the node
    /// - `zenoh_querier` - query
    pub fn enable_zenoh(&mut self, zenoh_session_config: ::zenoh::Config) {
        let ensure_session = EnsureZenohSession::new(zenoh_session_config);
        self.register_zenoh_subscription(ensure_session.clone());
        self.register_zenoh_publisher(ensure_session.clone());
        self.register_zenoh_querier(ensure_session);

        // TODO(@mxgrey): Support dynamic connections whose configurations are
        // decided within the workflow and passed into the node as input.
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohCongestionControlConfig {
    #[default]
    Drop,
    Block,
}

impl From<ZenohCongestionControlConfig> for CongestionControl {
    fn from(value: ZenohCongestionControlConfig) -> Self {
        match value {
            ZenohCongestionControlConfig::Block => Self::Block,
            ZenohCongestionControlConfig::Drop => Self::Drop,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohPriorityConfig {
    RealTime,
    InteractiveHigh,
    InteractiveLow,
    DataHigh,
    #[default]
    Data,
    DataLow,
    Background,
}

impl From<ZenohPriorityConfig> for Priority {
    fn from(value: ZenohPriorityConfig) -> Self {
        match value {
            ZenohPriorityConfig::RealTime => Self::RealTime,
            ZenohPriorityConfig::InteractiveHigh => Self::InteractiveHigh,
            ZenohPriorityConfig::InteractiveLow => Self::InteractiveLow,
            ZenohPriorityConfig::DataHigh => Self::DataHigh,
            ZenohPriorityConfig::Data => Self::Data,
            ZenohPriorityConfig::DataLow => Self::DataLow,
            ZenohPriorityConfig::Background => Self::Background,
        }
    }
}

/// We need the JsonSchema trait for the config struct, so we need to redefine
/// this enum from zenoh.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohLocalityConfig {
    SessionLocal,
    Remote,
    #[default]
    Any,
}

impl From<ZenohLocalityConfig> for Locality {
    fn from(value: ZenohLocalityConfig) -> Self {
        match value {
            ZenohLocalityConfig::SessionLocal => Locality::SessionLocal,
            ZenohLocalityConfig::Remote => Locality::Remote,
            ZenohLocalityConfig::Any => Locality::Any,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohEncodingConfig {
    /// Interpret the payload as a JSON value, serialized as a string
    Json,
    /// Interpret the payload as the serialized bytes of the specified type of protobuf message
    Protobuf(Arc<str>),
    /// Interpret the payload as the raw bytes of a BSON value
    Bson,
}

#[derive(StreamPack)]
pub struct ZenohNodeStreams {
    /// Messages that come out of the subscription or query
    pub out: JsonMessage,
    /// Error messages that are produced if an error occurs while decoding a
    /// payload
    pub out_error: String,
    /// A way to cancel the subscription or query
    pub canceller: UnboundedSender<JsonMessage>,
}

#[derive(ThisError, Debug)]
pub enum ZenohBuildError {
    #[error("cannot find protobuf message descriptor for [{}]", .0)]
    MissingMessageDescriptor(Arc<str>),
}

#[derive(ThisError, Debug, Clone, Deref, DerefMut)]
#[error(transparent)]
pub struct ArcError(#[from] pub Arc<dyn Error + Send + Sync + 'static>);

impl ArcError {
    pub fn new<T: Into<Arc<dyn Error + Send + Sync + 'static>>>(err: T) -> Self {
        Self(err.into())
    }
}

impl Serialize for ArcError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self.0))
    }
}

struct ArcErrorVisitor;

impl<'de> Visitor<'de> for ArcErrorVisitor {
    type Value = ArcError;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string describing an error")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(ArcError(Arc::new(DeserializedError(v.to_string()))))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(ArcError(Arc::new(DeserializedError(v))))
    }
}

impl<'de> Deserialize<'de> for ArcError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(ArcErrorVisitor)
    }
}

impl JsonSchema for ArcError {
    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "title": "ArcError",
            "description": "An error message",
            "type": "string"
        })
    }

    fn schema_name() -> Cow<'static, str> {
        "ArcError".into()
    }
}

#[derive(Resource)]
pub struct ZenohSession {
    pub promise: Shared<Promise<Result<Session, ArcError>>>,
}

/// Used to perform lazy evaluation in creating a zenoh session. As soon as any
/// zenoh-related node is created, this command will begin spinning up the zenoh
/// session, and only create one session for the World.
#[derive(Clone)]
struct EnsureZenohSession(Arc<Mutex<Option<::zenoh::Config>>>);

impl EnsureZenohSession {
    fn new(config: ::zenoh::Config) -> Self {
        Self(Arc::new(Mutex::new(Some(config))))
    }
}

impl Command for EnsureZenohSession {
    fn apply(self, world: &mut World) {
        let Some(config) = self.0.lock().unwrap().take() else {
            return;
        };

        let promise = world
            .command(|commands| {
                commands
                    .serve(async { ::zenoh::open(config).await.map_err(ArcError::new) })
                    .take_response()
            })
            .shared();

        world.insert_resource(ZenohSession { promise });
    }
}

async fn receive_cancel<E>(
    receiver: impl Future<Output = Option<JsonMessage>>,
) -> Result<JsonMessage, E> {
    match receiver.await {
        Some(msg) => Ok(msg),
        None => {
            // The sender for the cancellation signal was dropped so we must
            // never let this future finish.
            NeverFinish.await;
            unreachable!("this future will never finish")
        }
    }
}

#[derive(Debug, Clone)]
enum Codec {
    Protobuf(MessageDescriptor),
    Json,
    Bson,
}

impl Codec {
    fn encode(&self, message: JsonMessage) -> Result<ZBytes, String> {
        match self {
            Codec::Protobuf(descriptor) => {
                let msg = DynamicMessage::deserialize(descriptor.clone(), message)
                    .map_err(|err| format!("{err}"))?;

                Ok(ZBytes::from(msg.encode_to_vec()))
            }
            Codec::Json => {
                let msg_as_string: String =
                    serde_json::from_value(message).map_err(|err| format!("{err}"))?;

                Ok(ZBytes::from(msg_as_string))
            }
            Codec::Bson => {
                let bytes = bson::serialize_to_vec(&message).map_err(|err| format!("{err}"))?;
                Ok(ZBytes::from(bytes))
            }
        }
    }

    fn decode(&self, sample: &Sample) -> Result<JsonMessage, String> {
        match self {
            Codec::Protobuf(descriptor) => {
                let msg = DynamicMessage::decode(
                    descriptor.clone(),
                    sample.payload().to_bytes().as_ref(),
                )
                .map_err(|err| decode_error_msg(err, &sample))?;

                // TODO(@mxgrey): Refactor this to share an implementation with
                // the decoder in the grpc feature
                let value = msg.serialize_with_options(
                    serde_json::value::Serializer,
                    &SerializeOptions::new()
                        .stringify_64_bit_integers(false)
                        .use_proto_field_name(true)
                        .skip_default_fields(false),
                );

                value.map_err(|err| decode_error_msg(err, &sample))
            }
            Codec::Json => {
                let payload: String = match z_deserialize(sample.payload()) {
                    Ok(payload) => payload,
                    Err(err) => {
                        return Err(decode_error_msg(err, &sample));
                    }
                };

                serde_json::from_str::<JsonMessage>(&payload)
                    .map_err(|err| decode_error_msg(err, &sample))
            }
            Codec::Bson => {
                bson::deserialize_from_slice::<JsonMessage>(sample.payload().to_bytes().as_ref())
                    .map_err(|err| decode_error_msg(err, &sample))
            }
        }
    }

    fn encoding(&self) -> Encoding {
        match self {
            Codec::Protobuf(descriptor) => {
                Encoding::APPLICATION_PROTOBUF.with_schema(descriptor.full_name())
            }
            Codec::Json => Encoding::TEXT_JSON,
            Codec::Bson => Encoding::from("application/bson"),
        }
    }
}

fn decode_error_msg<T: Error>(msg: T, sample: &Sample) -> String {
    format!(
        "failed to decode sample with encoding [{}]: {}",
        sample.encoding(),
        msg,
    )
}

impl TryFrom<&'_ ZenohEncodingConfig> for Codec {
    type Error = ZenohBuildError;
    fn try_from(value: &ZenohEncodingConfig) -> Result<Self, Self::Error> {
        match value {
            ZenohEncodingConfig::Protobuf(message_type) => {
                let descriptors = DescriptorPool::global();
                let Some(msg) = descriptors.get_message_by_name(&message_type) else {
                    return Err(ZenohBuildError::MissingMessageDescriptor(Arc::clone(
                        &message_type,
                    ))
                    .into());
                };

                Ok(Codec::Protobuf(msg))
            }
            ZenohEncodingConfig::Json => Ok(Codec::Json),
            ZenohEncodingConfig::Bson => Ok(Codec::Bson),
        }
    }
}

#[derive(ThisError, Debug)]
#[error("{}", .0)]
struct DeserializedError(String);
