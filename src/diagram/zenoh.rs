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

        // Make sure this is registered since it gets used by canceller streams
        self.opt_out()
            .no_serializing()
            .no_deserializing()
            .register_message::<UnboundedSender<JsonMessage>>();

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
}

impl Codec {
    fn encode(&self, message: JsonMessage) -> Result<ZBytes, String> {
        match self {
            Codec::Protobuf(descriptor) => {
                let msg = DynamicMessage::deserialize(descriptor.clone(), &message)
                    .map_err(|err| format!("{err}"))?;

                Ok(ZBytes::from(msg.encode_to_vec()))
            }
            Codec::Json => {
                let msg_as_string = serde_json::to_string(&message)
                    .map_err(|err| format!("{err}"))?;

                Ok(ZBytes::from(msg_as_string))
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
                let payload = sample.payload().try_to_string()
                    .map_err(|err| decode_error_msg(err, &sample))?;

                serde_json::from_str::<JsonMessage>(&payload)
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
        }
    }
}

#[derive(ThisError, Debug)]
#[error("{}", .0)]
struct DeserializedError(String);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{diagram::testing::*, testing::*};
    use async_std::future::timeout as until_timeout;
    use std::time::Instant;
    use serde_json::json;

    #[test]
    fn test_zenoh_pub_sub() {
        impl_test_zenoh_pub_sub(
            vec![
                json!({
                    "hello": "world",
                    "number": 10.0,
                }),
                json!(5.0),
                json!("some string"),
                json!([
                    "string_value",
                    13,
                    {
                        "object": "value",
                        "with": "fields"
                    }
                ])
            ],
            "json",
        );
    }

    fn impl_test_zenoh_pub_sub(
        input_messages: Vec<JsonMessage>,
        codec: &str,
    ) {
        let mut fixture = DiagramTestFixture::new();
        fixture.registry.enable_zenoh(Default::default());

        fixture.registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("wait_for_matching"),
                |builder, _config: ()| {
                    builder.create_node(wait_for_matching.into_blocking_callback())
                }
            )
            .with_listen()
            .with_fork_result();

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("slow_spread"),
            |builder, _config: ()| {
                builder.create_map(
                    |input: AsyncMap<Vec<JsonMessage>, SlowSpreadStreams>| {
                        async move {
                            for value in input.request {
                                let _ = until_timeout(Duration::from_millis(1), NeverFinish).await;
                                input.streams.out.send(value);
                            }
                        }
                    }
                )
            }
        );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "initialize",
            "ops": {
                "initialize": {
                    "type": "fork_clone",
                    "next": [
                        "slow_spread",
                        "trigger_sub",
                        "expectation"
                    ]
                },
                "slow_spread": {
                    "type": "node",
                    "builder": "slow_spread",
                    "next": { "builtin": "dispose" },
                    "stream_out": { "out": "pub" }
                },
                "pub": {
                    "type": "node",
                    "builder": "zenoh_publisher",
                    "config": {
                        "key": format!("test_zenoh_pub_sub_key_{}", codec),
                        "encoder": codec,
                        "locality": "session_local"
                    },
                    "next": { "builtin": "dispose" }
                },
                "trigger_sub": {
                    "type": "transform",
                    "cel": "null",
                    "next": "sub"
                },
                "sub": {
                    "type": "node",
                    "builder": "zenoh_subscription",
                    "config": {
                        "key": format!("test_zenoh_pub_sub_key_{}", codec),
                        "decoder": codec,
                        "locality": "session_local"
                    },
                    "next": { "builtin": "dispose" },
                    "stream_out": { "out": "actual" }
                },
                "expectation": { "type": "buffer" },
                "actual": {
                    "type": "buffer",
                    "settings": { "retention": "keep_all" }
                },
                "listen_for_wait": {
                    "type": "listen",
                    "buffers": {
                        "expectation": "expectation",
                        "actual": "actual"
                    },
                    "next": "wait"
                },
                "wait": {
                    "type": "node",
                    "builder": "wait_for_matching",
                    "next": "filter"
                },
                "filter": {
                    "type": "fork_result",
                    "ok": { "builtin": "terminate" },
                    "err": { "builtin" : "dispose" }
                },
            }
        }))
        .unwrap();

        let result: Result<(), String> = fixture.spawn_and_run(&diagram, input_messages).unwrap();
        result.unwrap();
    }

    #[derive(StreamPack)]
    struct SlowSpreadStreams {
        out: JsonMessage
    }

    #[derive(Accessor, Clone)]
    struct PubSubTestKeys {
        expectation: BufferKey<Vec<JsonMessage>>,
        actual: BufferKey<JsonMessage>,
    }

    fn wait_for_matching(
        In(keys): In<PubSubTestKeys>,
        access_expectation: BufferAccess<Vec<JsonMessage>>,
        access_actual: BufferAccess<JsonMessage>,
        mut timer: Local<Option<Instant>>,
    ) -> Result<Result<(), String>, ()> {
        let expectation = access_expectation.get_newest(&keys.expectation).unwrap();
        let actual = access_actual.get(&keys.actual).unwrap();

        if actual.len() < expectation.len() {
            // Still waiting for publications to arrive. Check the timer.
            if let Some(start) = &*timer {
                if start.elapsed() > Duration::from_secs(2) {
                    // Have the test fail because it's taking too long
                    return Ok(Err(String::from("timeout")));
                }

                // Keep waiting for more messages
                return Err(());
            } else {
                // This is the first run of the system, so insert the current time
                *timer = Some(Instant::now());
                return Err(());
            }
        }

        if actual.len() > expectation.len() {
            let actual_values: Vec<_> = actual.iter().collect();
            return Ok(Err(format!(
                "Too many messages appeared. Expected {}, received {}: {:?}",
                expectation.len(),
                actual.len(),
                actual_values,
            )));
        }

        for expected_value in expectation {
            // NOTE: zenoh seems willing to send messages out of their original
            // order, and there doesn't seem to be any way to enforce that messages
            // arrive in order, so we can't make any assumptions about ordering
            // when we check for the expected messages.
            if actual.iter().find(|actual_value| *actual_value == expected_value).is_some() {
                continue;
            }

            return Ok(Err(format!("Expected message missing: {expected_value:?}")));
        }

        Ok(Ok(()))
    }
}
