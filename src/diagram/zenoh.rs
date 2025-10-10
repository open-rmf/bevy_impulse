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
use crate::{prelude::*, utils::*};

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
    Json,
    Protobuf(MessageDescriptor),
}

impl Codec {
    fn encode(&self, message: &JsonMessage) -> Result<ZBytes, String> {
        match self {
            Codec::Json => {
                let msg_as_string =
                    serde_json::to_string(message).map_err(|err| format!("{err}"))?;

                Ok(ZBytes::from(msg_as_string))
            }
            Codec::Protobuf(descriptor) => {
                let msg = DynamicMessage::deserialize(descriptor.clone(), message)
                    .map_err(|err| format!("{err}"))?;

                Ok(ZBytes::from(msg.encode_to_vec()))
            }
        }
    }

    fn decode(&self, sample: &Sample) -> Result<JsonMessage, String> {
        self.decode_payload(sample.payload())
    }

    fn decode_payload(&self, payload: &ZBytes) -> Result<JsonMessage, String> {
        match self {
            Codec::Protobuf(descriptor) => {
                let msg = DynamicMessage::decode(descriptor.clone(), payload.to_bytes().as_ref())
                    .map_err(error_to_string)?;

                // TODO(@mxgrey): Refactor this to share an implementation with
                // the decoder in the grpc feature
                let value = msg.serialize_with_options(
                    serde_json::value::Serializer,
                    &SerializeOptions::new()
                        .stringify_64_bit_integers(false)
                        .use_proto_field_name(true)
                        .skip_default_fields(false),
                );

                value.map_err(error_to_string)
            }
            Codec::Json => {
                let payload = payload.try_to_string().map_err(error_to_string)?;

                serde_json::from_str::<JsonMessage>(&payload).map_err(error_to_string)
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

fn error_to_string<T: Display>(msg: T) -> String {
    format!("{msg}")
}

impl TryFrom<&'_ ZenohEncodingConfig> for Codec {
    type Error = ZenohBuildError;
    fn try_from(value: &ZenohEncodingConfig) -> Result<Self, Self::Error> {
        match value {
            ZenohEncodingConfig::Json => Ok(Codec::Json),
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
    use serde_json::json;
    use std::time::Instant;

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
                ]),
            ],
            json!("json"),
        );

        let descriptor_set_bytes =
            include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        DescriptorPool::decode_global_file_descriptor_set(&descriptor_set_bytes[..]).unwrap();
        impl_test_zenoh_pub_sub(
            vec![
                json!({
                    "x": 1.0,
                    "y": 2.0,
                    "yaw": 1.0
                }),
                json!({
                    "x": 10.0,
                    "y": 20.0,
                    "yaw": -1.0
                }),
                json!({
                    "x": -5.0,
                    "y": -10.0,
                    "yaw": -0.5
                }),
                json!({
                    "x": 100.0,
                    "y": -50.0,
                    "yaw": 3.0
                }),
            ],
            json!({
                "protobuf": "example_protos.navigation.NavigationUpdate"
            }),
        )
    }

    fn impl_test_zenoh_pub_sub(input_messages: Vec<JsonMessage>, codec: JsonMessage) {
        let mut fixture = DiagramTestFixture::new();
        fixture.registry.enable_zenoh(Default::default());

        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("wait_for_matching"),
                |builder, _config: ()| {
                    builder.create_node(wait_for_matching.into_blocking_callback())
                },
            )
            .with_listen()
            .with_fork_result();

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("slow_spread"),
            |builder, _config: ()| {
                builder.create_map(
                    |input: AsyncMap<Vec<JsonMessage>, SlowSpreadStreams>| async move {
                        for value in input.request {
                            let _ = until_timeout(Duration::from_micros(100), NeverFinish).await;
                            input.streams.out.send(value);
                        }
                    },
                )
            },
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
                        "key": "test_zenoh_pub_sub_key",
                        "encoder": codec,
                        "locality": "session_local",
                        "max_samples": 100,
                        "heartbeat": {
                            "period": 0.01,
                            "sporadic": false
                        },
                        "ordering": "strict"
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
                        "key": "test_zenoh_pub_sub_key",
                        "decoder": codec,
                        "history": {
                            "detect_late_publishers": true,
                            "max_samples": 100
                        },
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

        let result: Result<(), String> = fixture
            .spawn_and_run_with_conditions(&diagram, input_messages, Duration::from_secs(2))
            .unwrap();
        result.unwrap();
    }

    #[derive(StreamPack)]
    struct SlowSpreadStreams {
        out: JsonMessage,
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

        for (i, (expected_value, actual_value)) in expectation.iter().zip(actual.iter()).enumerate()
        {
            if expected_value != actual_value {
                return Ok(Err(format!(
                    "Incorrect message at index {i}:\
                    \n - expected: {expected_value:?} \
                    \n - received: {actual_value:?}"
                )));
            }
        }

        Ok(Ok(()))
    }

    #[test]
    fn test_zenoh_querier() {
        let descriptor_set_bytes =
            include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        DescriptorPool::decode_global_file_descriptor_set(&descriptor_set_bytes[..]).unwrap();

        let mut fixture = DiagramTestFixture::new();
        fixture.registry.enable_zenoh(::zenoh::Config::default());
        fixture.registry.register_message::<[f64; 2]>();

        const ADD_KEY: &'static str = "test_zenoh_querier_add";
        const NAV_KEY: &'static str = "test_zenoh_querier_nav";
        const NAV_GOAL_MSG: &'static str = "example_protos.navigation.NavigationGoal";
        const NAV_UPDATE_MSG: &'static str = "example_protos.navigation.NavigationUpdate";

        fixture.context.command(|commands| {
            commands
                .serve(async {
                    let session = ::zenoh::open(::zenoh::Config::default()).await.unwrap();
                    let queryable = session.declare_queryable(ADD_KEY).await.unwrap();
                    let codec = Codec::Json;
                    while let Ok(query) = queryable.recv_async().await {
                        let payload = codec.decode_payload(query.payload().unwrap()).unwrap();
                        let reply = payload[0].as_f64().unwrap() + payload[1].as_f64().unwrap();
                        query
                            .reply(ADD_KEY, codec.encode(&json!(reply)).unwrap())
                            .await
                            .unwrap();
                    }
                })
                .detach();

            commands
                .serve(async {
                    let session = ::zenoh::open(::zenoh::Config::default()).await.unwrap();

                    let queryable = session.declare_queryable(NAV_KEY).await.unwrap();

                    let decoder: Codec = (&ZenohEncodingConfig::Protobuf(NAV_GOAL_MSG.into()))
                        .try_into()
                        .unwrap();

                    let encoder: Codec = (&ZenohEncodingConfig::Protobuf(NAV_UPDATE_MSG.into()))
                        .try_into()
                        .unwrap();

                    while let Ok(query) = queryable.recv_async().await {
                        let payload = decoder.decode_payload(query.payload().unwrap()).unwrap();
                        let goal_x = payload["x"].as_f64().unwrap() as f32;
                        let goal_y = payload["y"].as_f64().unwrap() as f32;

                        let _ = until_timeout(Duration::from_millis(1), NeverFinish).await;
                        let msg = json!({
                            "x": goal_x,
                            "y": goal_y,
                            "yaw": 0.0,
                        });
                        let reply = encoder.encode(&msg).unwrap();
                        query.reply(NAV_KEY, reply).await.unwrap();
                    }
                })
                .detach();
        });

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "add",
            "default_trace": "on",
            "ops": {
                "add": {
                    "type": "node",
                    "builder": "zenoh_querier",
                    "config": {
                        "key": ADD_KEY,
                        "decoder": "json",
                        "encoder": "json"
                    },
                    "stream_out": {
                        "out": "to_nav_cmd"
                    },
                    "next": { "builtin" : "dispose" }
                },
                "to_nav_cmd": {
                    "type": "transform",
                    "cel": "{ \"x\": request, \"y\": request }",
                    "next": "nav"
                },
                "nav": {
                    "type": "node",
                    "builder": "zenoh_querier",
                    "config": {
                        "key": NAV_KEY,
                        "encoder": { "protobuf": NAV_GOAL_MSG },
                        "decoder": { "protobuf": NAV_UPDATE_MSG }
                    },
                    "stream_out": { "out": "position" },
                    "next": "nav_done"
                },
                "position": { "type": "buffer" },
                "nav_done": { "type": "buffer" },
                "finished": {
                    "type": "join",
                    "buffers": {
                        "position": "position",
                        "done": "nav_done"
                    },
                    "next": "just_position"
                },
                "just_position": {
                    "type": "transform",
                    "cel": "request.position",
                    "next": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run_with_conditions(&diagram, [2.0, 2.0], Duration::from_secs(2))
            .unwrap();

        assert_eq!(result["x"].as_f64().unwrap(), 4.0);
        assert_eq!(result["y"].as_f64().unwrap(), 4.0);
        assert_eq!(result["yaw"].as_f64().unwrap(), 0.0);
    }
}
