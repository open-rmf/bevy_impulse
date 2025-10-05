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

use bevy_ecs::prelude::{In, Resource, Res, World};
use bevy_derive::{Deref, DerefMut};
use ::zenoh::{
    sample::Locality,
    Session, sample::Sample,
};
use zenoh_ext::{
    AdvancedPublisher, AdvancedPublisherBuilderExt, AdvancedSubscriberBuilderExt, CacheConfig,
    HistoryConfig, RecoveryConfig,
    z_deserialize,
};
use std::{
    error::Error,
    time::Duration,
    future::Future,
};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use futures::future::Shared;
use futures_lite::future::race;
use thiserror::Error as ThisError;
use prost_reflect::{DynamicMessage, MessageDescriptor, DescriptorPool, SerializeOptions};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ZenohSubscriptionConfig {
    pub key: Arc<str>,
    #[serde(default, skip_serializing_if = "ZenohSubscriptionHistoryConfig::is_default")]
    pub history: ZenohSubscriptionHistoryConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub recovery: ZenohSubscriptionRecoveryConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub locality: LocalityConfig,
    #[serde(default, skip_serializing_if = "PayloadConfig::is_json")]
    pub payload: PayloadConfig,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ZenohSubscriptionHistoryConfig {
    /// Enable detection of late joiner publishers and query for their historical data.
    #[serde(default, skip_serializing_if = "is_default")]
    pub detect_late_publishers: bool,
    /// Specify how many samples to query for each resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_samples: Option<usize>,
    /// Specify the maximum age (in seconds) of samples to query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<f64>,
}

impl From<ZenohSubscriptionHistoryConfig> for HistoryConfig {
    fn from(value: ZenohSubscriptionHistoryConfig) -> Self {
        let mut config = HistoryConfig::default();
        if value.detect_late_publishers {
            config = config.detect_late_publishers();
        }

        if let Some(depth) = value.max_samples {
            config = config.max_samples(depth);
        }

        if let Some(seconds) = value.max_age {
            config = config.max_age(seconds);
        }

        config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohSubscriptionRecoveryConfig {
    /// Do not use any recovery method
    None,
    /// Enable periodic querying for samples that have not been received yet,
    /// and specify the period of that querying.
    PeriodicQueries(Duration),
    /// Subscribe to the heartbeats of advanced publishers so the subscription
    /// can check if any messages were missed.
    Heartbeat,
}

impl Default for ZenohSubscriptionRecoveryConfig {
    fn default() -> Self {
        ZenohSubscriptionRecoveryConfig::Heartbeat
    }
}

/// We need the JsonSchema trait for the config struct, so we need to redefine
/// this enum from zenoh.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LocalityConfig {
    SessionLocal,
    Remote,
    #[default]
    Any,
}

impl From<LocalityConfig> for Locality {
    fn from(value: LocalityConfig) -> Self {
        match value {
            LocalityConfig::SessionLocal => Locality::SessionLocal,
            LocalityConfig::Remote => Locality::Remote,
            LocalityConfig::Any => Locality::Any,
        }
    }
}

impl ZenohSubscriptionHistoryConfig {
    pub fn is_default(&self) -> bool {
        !self.detect_late_publishers
        && self.max_samples.is_none()
        && self.max_age.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PayloadConfig {
    /// Interpret the payload as a JSON value, serialized as a string
    Json,
    /// Interpret the payload as the serialized bytes of the specified type of protobuf message
    Protobuf(Arc<str>),
    /// Interpret the payload as the raw bytes of a BSON value
    Bson,
}

impl PayloadConfig {
    pub fn is_json(&self) -> bool {
        matches!(self, Self::Json)
    }
}

impl Default for PayloadConfig {
    fn default() -> Self {
        Self::Json
    }
}

#[derive(StreamPack)]
pub struct ZenohSubscriptionStreams {
    /// Messages that come out of the subscription
    pub out: JsonMessage,
    /// Error message that will be produced if an error occurs while decoding a
    /// payload
    pub out_error: String,
    /// A way to cancel the subscription
    pub canceller: UnboundedSender<JsonMessage>,
}

#[derive(ThisError, Debug, Clone, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ZenohSubscriptionError {
    #[error("the subscription was already active")]
    AlreadyActive,
    #[error("the active buffer tracker was despawned")]
    BufferDespawned,
    #[error("the zenoh session was removed from its resource")]
    SessionRemoved,
    #[error("{}", .0)]
    ZenohError(#[from] ArcError),
}

#[derive(ThisError, Debug)]
pub enum ZenohSubscriptionBuildError {
    #[error("cannot find protobuf message descriptor for [{}]", .0)]
    MissingMessageDescriptor(Arc<str>),
}

impl DiagramElementRegistry {
    pub fn enable_zenoh(&mut self) {
        self
            .opt_out()
            .no_deserializing()
            .register_node_builder_fallible(
                NodeBuilderOptions::new("zenoh_subscription")
                    .with_default_display_text("Zenoh Subscription"),
                |builder, config: ZenohSubscriptionConfig| {
                    let active_buffer = builder.create_buffer::<()>(BufferSettings::default());
                    let access = builder.create_buffer_access::<(), _>(active_buffer);

                    let decoder = match &config.payload {
                        PayloadConfig::Protobuf(message_type) => {
                            let descriptors = DescriptorPool::global();
                            let Some(msg) = descriptors.get_message_by_name(&message_type) else {
                                return Err(ZenohSubscriptionBuildError::MissingMessageDescriptor(Arc::clone(&message_type)).into());
                            };

                            Decoder::Protobuf(msg)
                        }
                        PayloadConfig::Json => Decoder::Json,
                        PayloadConfig::Bson => Decoder::Bson,
                    };

                    let callback = move |
                        In(input): AsyncCallbackInput<((), BufferKey<()>), ZenohSubscriptionStreams>,
                        mut active: BufferAccessMut<()>,
                        session: Res<ZenohSession>,
                    | {
                        // First make sure an instance of this node isn't already active
                        let already_active = active.get_mut(&input.request.1)
                            .map_err(|_| ZenohSubscriptionError::BufferDespawned)
                            .and_then(|mut active_buffer| {
                                if active_buffer.is_empty() {
                                    active_buffer.push(());
                                    return Ok(());
                                } else {
                                    return Err(ZenohSubscriptionError::AlreadyActive);
                                }
                            });

                        let session = session.promise.clone();

                        let (sender, mut receiver) = unbounded_channel();
                        input.streams.canceller.send(sender);

                        let config = config.clone();
                        let decoder = decoder.clone();
                        async move {
                            // Return right away if the subscription is already active
                            already_active?;

                            let cancel = receiver.recv().shared();

                            let active_key = input.request.1;
                            let r = async move {
                                let subscription_future = async move {
                                    let session = session
                                        .await
                                        .available()
                                        .map(|r| r.map_err(ZenohSubscriptionError::ZenohError))
                                        .unwrap_or_else(|| Err(ZenohSubscriptionError::SessionRemoved))?;

                                    let subscription_builder = session
                                        .declare_subscriber(config.key.as_ref())
                                        .allowed_origin(config.locality.into())
                                        .history(config.history.into());

                                    let subscription = match config.recovery {
                                        ZenohSubscriptionRecoveryConfig::None => { subscription_builder }
                                        ZenohSubscriptionRecoveryConfig::PeriodicQueries(period) => {
                                            subscription_builder.recovery(
                                                RecoveryConfig::default()
                                                .periodic_queries(period)
                                            )
                                        }
                                        ZenohSubscriptionRecoveryConfig::Heartbeat => {
                                            subscription_builder.recovery(
                                                RecoveryConfig::default()
                                                .heartbeat()
                                            )
                                        }
                                    }
                                    .await
                                    .map_err(ArcError::new)?;

                                    Ok::<_, ZenohSubscriptionError>(Ok::<_, JsonMessage>(subscription))
                                };

                                // Await the subscription connection in parallel
                                // with watching for a cancellation
                                let subscription = match race(subscription_future, receive_cancel(cancel.clone())).await? {
                                    Ok(subscription) => subscription,
                                    Err(cancel_msg) => {
                                        return Ok(cancel_msg);
                                    }
                                };

                                loop {
                                    let next_sample = subscription
                                        .recv_async()
                                        .map(|r|
                                            r
                                            .map(|sample| Ok::<_, JsonMessage>(sample))
                                            .map_err(|err| ZenohSubscriptionError::ZenohError(ArcError::new(err)))
                                        );

                                    match race(next_sample, receive_cancel(cancel.clone())).await? {
                                        Ok(sample) => {
                                            match decoder.decode(sample) {
                                                Ok(msg) => {
                                                    input.streams.out.send(msg);
                                                }
                                                Err(msg) => {
                                                    input.streams.out_error.send(msg);
                                                }
                                            }
                                        }
                                        Err(cancel_msg) => {
                                            // The user has asked to cancel this node
                                            return Ok::<_, ZenohSubscriptionError>(cancel_msg);
                                        }
                                    }
                                }
                            }.await;

                            // Clear the buffer to indicate that the subscription
                            // could be restarted.
                            input.channel.command(move |commands| {
                                commands.add(move |world: &mut World| {
                                    let _ = world.buffer_mut(&active_key, |mut active_buffer| {
                                        active_buffer.drain(..);
                                    });
                                });
                            })
                            .await;

                            // Return the result from earlier
                            r
                        }
                    };

                    let node = builder.create_node(callback.as_callback());
                    builder.connect(access.output, node.input);

                    Ok(Node::<_, _, ZenohSubscriptionStreams> {
                        input: access.input,
                        output: node.output,
                        streams: node.streams,
                    })
                }
            )
            .with_fork_result();

        // TODO(@mxgrey): Support dynamic connections whose configurations are
        // decided within the workflow and passed into the node as input.
    }
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
        S: Serializer
    {
        serializer.serialize_str(&format!("{}", self.0))
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

async fn receive_cancel<T>(
    receiver: impl Future<Output = Option<JsonMessage>>,
) -> Result<Result<T, JsonMessage>, ZenohSubscriptionError> {
    match receiver.await {
        Some(msg) => {
            Ok(Err(msg))
        }
        None => {
            // The sender for the cancellation signal was dropped so we must
            // never let this future finish.
            NeverFinish.await;
            unreachable!("this future will never finish")
        }
    }
}

#[derive(Debug, Clone)]
enum Decoder {
    Protobuf(MessageDescriptor),
    Json,
    Bson,
}

impl Decoder {
    fn decode(&self, sample: Sample) -> Result<JsonMessage, String> {
        match self {
            Decoder::Protobuf(descriptor) => {
                let msg = match DynamicMessage::decode(descriptor.clone(), sample.payload().to_bytes().as_ref()) {
                    Ok(msg) => msg,
                    Err(err) => {
                        return Err(format!("{err}"));
                    }
                };

                // TODO(@mxgrey): Refactor this to share an implementation with
                // the decoder in the grpc feature
                let value = msg
                    .serialize_with_options(
                        serde_json::value::Serializer,
                        &SerializeOptions::new()
                            .stringify_64_bit_integers(false)
                            .use_proto_field_name(true)
                            .skip_default_fields(false),
                    );

                match value {
                    Ok(msg) => {
                        return Ok(msg);
                    }
                    Err(err) => {
                        return Err(format!("{err}"));
                    }
                }
            }
            Decoder::Json => {
                let payload: String = match z_deserialize(sample.payload()) {
                    Ok(payload) => payload,
                    Err(err) => {
                        return Err(format!("{err}"));
                    }
                };

                match serde_json::from_str::<JsonMessage>(&payload) {
                    Ok(msg) => {
                        return Ok(msg);
                    }
                    Err(err) => {
                        return Err(format!("{err}"));
                    }
                };
            }
            Decoder::Bson => {
                match bson::deserialize_from_slice::<JsonMessage>(sample.payload().to_bytes().as_ref()) {
                    Ok(msg) => {
                        return Ok(msg);
                    }
                    Err(err) => {
                        return Err(format!("{err}"));
                    }
                }
            }
        }
    }
}
