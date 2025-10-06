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
    bytes::ZBytes,
    qos::{CongestionControl, Priority},
    sample::{Locality, Sample},
    Session,
};
use bevy_derive::{Deref, DerefMut};
use bevy_ecs::{
    prelude::{In, Res, Resource, World},
    system::Command,
};
use futures::future::Shared;
use futures_lite::future::race;
use prost_reflect::{
    prost::Message, DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions,
};
use std::{error::Error, future::Future, sync::Mutex, time::Duration};
use thiserror::Error as ThisError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use zenoh_ext::{
    z_deserialize, AdvancedPublisherBuilderExt, AdvancedSubscriberBuilderExt, CacheConfig,
    HistoryConfig, MissDetectionConfig, RecoveryConfig, RepliesConfig,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ZenohSubscriptionConfig {
    /// The key that this subscription will use to connect to publishers.
    pub key: Arc<str>,
    #[serde(
        default,
        skip_serializing_if = "ZenohSubscriptionHistoryConfig::is_default"
    )]
    pub history: ZenohSubscriptionHistoryConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub recovery: ZenohSubscriptionRecoveryConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub locality: LocalityConfig,
    #[serde(default, skip_serializing_if = "EncodingConfig::is_json")]
    pub encoding: EncodingConfig,
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
    /// Specify the maximum age of samples to query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<Duration>,
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

        if let Some(duration) = value.max_age {
            config = config.max_age(duration.as_secs_f64());
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ZenohPublisherConfig {
    /// The key that this publisher will use to advertise itself.
    pub key: Arc<str>,
    /// Maximum number of samples that will be kept for late joiners or
    /// subscriptions that missed a sample. If unset it will use Zenoh's default
    /// which is 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_samples: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub heartbeat: ZenohPublisherHeartbeatOption,
    #[serde(default, skip_serializing_if = "is_default")]
    pub priority: ZenohPublisherPriority,
    #[serde(default, skip_serializing_if = "is_default")]
    pub congestion_control: ZenohPublisherCongestionControl,
    /// When express is set to true, messages will not be batched.
    /// This usually has a positive impact on latency but negative impact on throughput.
    #[serde(default, skip_serializing_if = "is_default")]
    pub express: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub reliability: ZenohPublisherReliability,
    #[serde(default, skip_serializing_if = "is_default")]
    pub locality: LocalityConfig,
    #[serde(default, skip_serializing_if = "EncodingConfig::is_json")]
    pub encoding: EncodingConfig,
}

impl ZenohPublisherConfig {
    pub fn cache_config(&self) -> CacheConfig {
        let mut cache = CacheConfig::default().replies_config(self.replies_config());

        if let Some(depth) = self.max_samples {
            cache = cache.max_samples(depth);
        }

        cache
    }

    pub fn replies_config(&self) -> RepliesConfig {
        RepliesConfig::default()
            .congestion_control(self.congestion_control.into())
            .priority(self.priority.into())
            .express(self.express)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case", untagged)]
pub enum ZenohPublisherHeartbeatOption {
    /// Disable the heartbeat functionality
    Disable,
    /// Enable the heartbeat
    Enable(ZenohPublisherHeartbeatConfig),
}

impl Default for ZenohPublisherHeartbeatOption {
    fn default() -> Self {
        Self::Enable(Default::default())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ZenohPublisherHeartbeatConfig {
    /// How frequently should the heartbeat be published?
    pub period: Duration,
    /// Is a sporadic heartbeat allowed? A sporadic heartbeat means the
    /// heartbeat will only be published if the publisher's sequence number has
    /// increased within the latest heartbeat period. This means subscriptions
    /// that missed a message might not know until the next time a message gets
    /// published.
    pub sporadic: bool,
}

impl Default for ZenohPublisherHeartbeatConfig {
    fn default() -> Self {
        Self {
            period: Duration::from_secs(1),
            sporadic: false,
        }
    }
}

impl From<ZenohPublisherHeartbeatConfig> for MissDetectionConfig {
    fn from(value: ZenohPublisherHeartbeatConfig) -> Self {
        if value.sporadic {
            MissDetectionConfig::default().heartbeat(value.period)
        } else {
            MissDetectionConfig::default().sporadic_heartbeat(value.period)
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohPublisherPriority {
    RealTime,
    InteractiveHigh,
    InteractiveLow,
    DataHigh,
    #[default]
    Data,
    DataLow,
    Background,
}

impl From<ZenohPublisherPriority> for Priority {
    fn from(value: ZenohPublisherPriority) -> Self {
        match value {
            ZenohPublisherPriority::RealTime => Self::RealTime,
            ZenohPublisherPriority::InteractiveHigh => Self::InteractiveHigh,
            ZenohPublisherPriority::InteractiveLow => Self::InteractiveLow,
            ZenohPublisherPriority::DataHigh => Self::DataHigh,
            ZenohPublisherPriority::Data => Self::Data,
            ZenohPublisherPriority::DataLow => Self::DataLow,
            ZenohPublisherPriority::Background => Self::Background,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohPublisherCongestionControl {
    #[default]
    Drop,
    Block,
}

impl From<ZenohPublisherCongestionControl> for CongestionControl {
    fn from(value: ZenohPublisherCongestionControl) -> Self {
        match value {
            ZenohPublisherCongestionControl::Block => Self::Block,
            ZenohPublisherCongestionControl::Drop => Self::Drop,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohPublisherReliability {
    BestEffort,
    #[default]
    Reliable,
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
        !self.detect_late_publishers && self.max_samples.is_none() && self.max_age.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EncodingConfig {
    /// Interpret the payload as a JSON value, serialized as a string
    Json,
    /// Interpret the payload as the serialized bytes of the specified type of protobuf message
    Protobuf(Arc<str>),
    /// Interpret the payload as the raw bytes of a BSON value
    Bson,
}

impl EncodingConfig {
    pub fn is_json(&self) -> bool {
        matches!(self, Self::Json)
    }
}

impl Default for EncodingConfig {
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

#[derive(ThisError, Debug, Clone, Serialize, Deserialize, JsonSchema)]
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

#[derive(ThisError, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ZenohPublisherError {
    #[error("the zenoh session was removed from its resource")]
    SessionRemoved,
    #[error("error while encoding message: {}", .0)]
    EncodingError(String),
    #[error("{}", .0)]
    ZenohError(#[from] ArcError),
}

#[derive(ThisError, Debug)]
pub enum ZenohBuildError {
    #[error("cannot find protobuf message descriptor for [{}]", .0)]
    MissingMessageDescriptor(Arc<str>),
}

impl DiagramElementRegistry {
    pub fn enable_zenoh(&mut self, zenoh_session_config: ::zenoh::Config) {
        let ensure_session = EnsureZenohSession::new(zenoh_session_config);

        let ensure_session_for_subscription = ensure_session.clone();
        self.register_node_builder_fallible(
            NodeBuilderOptions::new("zenoh_subscription")
                .with_default_display_text("Zenoh Subscription"),
            move |builder, config: ZenohSubscriptionConfig| {
                builder
                    .commands()
                    .add(ensure_session_for_subscription.clone());

                let active_buffer = builder.create_buffer::<()>(BufferSettings::default());
                let access = builder.create_buffer_access::<(), _>(active_buffer);

                let decoder: Codec = (&config.encoding).try_into()?;

                let callback = move |In(input): AsyncCallbackInput<
                    ((), BufferKey<()>),
                    ZenohSubscriptionStreams,
                >,
                                     mut active: BufferAccessMut<()>,
                                     session: Res<ZenohSession>| {
                    // First make sure an instance of this node isn't already active
                    let already_active = active
                        .get_mut(&input.request.1)
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
                                    .unwrap_or_else(|| {
                                        Err(ZenohSubscriptionError::SessionRemoved)
                                    })?;

                                let subscription_builder = session
                                    .declare_subscriber(config.key.as_ref())
                                    .allowed_origin(config.locality.into())
                                    .history(config.history.into());

                                let subscription = match config.recovery {
                                    ZenohSubscriptionRecoveryConfig::None => subscription_builder,
                                    ZenohSubscriptionRecoveryConfig::PeriodicQueries(period) => {
                                        subscription_builder.recovery(
                                            RecoveryConfig::default().periodic_queries(period),
                                        )
                                    }
                                    ZenohSubscriptionRecoveryConfig::Heartbeat => {
                                        subscription_builder
                                            .recovery(RecoveryConfig::default().heartbeat())
                                    }
                                }
                                .await
                                .map_err(ArcError::new)?;

                                Ok::<_, ZenohSubscriptionError>(Ok::<_, JsonMessage>(subscription))
                            };

                            // Await the subscription connection in parallel
                            // with watching for a cancellation
                            let subscription =
                                match race(subscription_future, receive_cancel(cancel.clone()))
                                    .await?
                                {
                                    Ok(subscription) => subscription,
                                    Err(cancel_msg) => {
                                        return Ok(cancel_msg);
                                    }
                                };

                            loop {
                                let next_sample = subscription.recv_async().map(|r| {
                                    r.map(|sample| Ok::<_, JsonMessage>(sample)).map_err(|err| {
                                        ZenohSubscriptionError::ZenohError(ArcError::new(err))
                                    })
                                });

                                match race(next_sample, receive_cancel(cancel.clone())).await? {
                                    Ok(sample) => match decoder.decode(sample) {
                                        Ok(msg) => {
                                            input.streams.out.send(msg);
                                        }
                                        Err(msg) => {
                                            input.streams.out_error.send(msg);
                                        }
                                    },
                                    Err(cancel_msg) => {
                                        // The user has asked to cancel this node
                                        return Ok::<_, ZenohSubscriptionError>(cancel_msg);
                                    }
                                }
                            }
                        }
                        .await;

                        // Clear the buffer to indicate that the subscription
                        // could be restarted.
                        input
                            .channel
                            .command(move |commands| {
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
            },
        )
        .with_fork_result();

        let create_publisher = |In(config): In<ZenohPublisherConfig>,
                                session: Res<ZenohSession>| {
            let session_promise = session.promise.clone();
            async move {
                let session = session_promise
                    .await
                    .available()
                    .map(|r| r.map_err(ZenohPublisherError::ZenohError))
                    .unwrap_or(Err(ZenohPublisherError::SessionRemoved))?;

                let publisher = session
                    .declare_publisher(config.key.to_string())
                    .cache(config.cache_config())
                    .publisher_detection();

                let publisher = match config.heartbeat {
                    ZenohPublisherHeartbeatOption::Disable => publisher,
                    ZenohPublisherHeartbeatOption::Enable(heartbeat) => {
                        publisher.sample_miss_detection(heartbeat.into())
                    }
                };

                let publisher = publisher.await.map_err(ArcError::new)?;

                Ok::<_, ZenohPublisherError>(Arc::new(publisher))
            }
        };
        let create_publisher = create_publisher.into_async_callback();

        self.register_node_builder_fallible(
            NodeBuilderOptions::new("zenoh_publisher").with_default_display_text("Zenoh Publisher"),
            move |builder, config: ZenohPublisherConfig| {
                builder.commands().add(ensure_session.clone());

                let encoder: Codec = (&config.encoding).try_into()?;
                let publisher = builder
                    .commands()
                    .request(config, create_publisher.clone())
                    .take_response();
                let publisher = publisher.shared();

                let callback = move |message: JsonMessage| {
                    let publisher = publisher.clone();
                    let encoder = encoder.clone();
                    async move {
                        let payload = encoder
                            .encode(message)
                            .map_err(ZenohPublisherError::EncodingError)?;

                        // SAFETY: There is no mechanism for the publisher to
                        // be taken out of the promise, so it should always be
                        // available after being awaited.
                        let publisher = publisher.await.available().unwrap()?;
                        publisher.put(payload).await.map_err(ArcError::new)?;
                        Ok::<_, ZenohPublisherError>(())
                    }
                };

                Ok(builder.create_map_async(callback))
            },
        );

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

async fn receive_cancel<T>(
    receiver: impl Future<Output = Option<JsonMessage>>,
) -> Result<Result<T, JsonMessage>, ZenohSubscriptionError> {
    match receiver.await {
        Some(msg) => Ok(Err(msg)),
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

    fn decode(&self, sample: Sample) -> Result<JsonMessage, String> {
        match self {
            Codec::Protobuf(descriptor) => {
                let msg = DynamicMessage::decode(
                    descriptor.clone(),
                    sample.payload().to_bytes().as_ref(),
                )
                .map_err(|err| format!("{err}"))?;

                // TODO(@mxgrey): Refactor this to share an implementation with
                // the decoder in the grpc feature
                let value = msg.serialize_with_options(
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
            Codec::Json => {
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
            Codec::Bson => {
                match bson::deserialize_from_slice::<JsonMessage>(
                    sample.payload().to_bytes().as_ref(),
                ) {
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

impl TryFrom<&'_ EncodingConfig> for Codec {
    type Error = ZenohBuildError;
    fn try_from(value: &EncodingConfig) -> Result<Self, Self::Error> {
        match value {
            EncodingConfig::Protobuf(message_type) => {
                let descriptors = DescriptorPool::global();
                let Some(msg) = descriptors.get_message_by_name(&message_type) else {
                    return Err(ZenohBuildError::MissingMessageDescriptor(Arc::clone(
                        &message_type,
                    ))
                    .into());
                };

                Ok(Codec::Protobuf(msg))
            }
            EncodingConfig::Json => Ok(Codec::Json),
            EncodingConfig::Bson => Ok(Codec::Bson),
        }
    }
}

#[derive(ThisError, Debug)]
#[error("{}", .0)]
struct DeserializedError(String);
