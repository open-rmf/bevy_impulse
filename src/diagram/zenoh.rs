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
    query::{QueryConsolidation, ConsolidationMode, QueryTarget, Parameters},
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
    /// The encoding of incoming messages.
    pub decoder: ZenohEncodingConfig,
    #[serde(
        default,
        skip_serializing_if = "ZenohSubscriptionHistoryConfig::is_default"
    )]
    pub history: ZenohSubscriptionHistoryConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub recovery: ZenohSubscriptionRecoveryConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub locality: ZenohLocalityConfig,
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
    /// How outgoing messages will be encoded.
    pub encoder: ZenohEncodingConfig,
    /// Maximum number of samples that will be kept for late joiners or
    /// subscriptions that missed a sample. If unset it will use Zenoh's default
    /// which is 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_samples: Option<usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub heartbeat: ZenohPublisherHeartbeatOption,
    #[serde(default, skip_serializing_if = "is_default")]
    pub priority: ZenohPriorityConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub congestion_control: ZenohCongestionControlConfig,
    /// When express is set to true, messages will not be batched.
    /// This usually has a positive impact on latency but negative impact on throughput.
    #[serde(default, skip_serializing_if = "is_default")]
    pub express: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub reliability: ZenohPublisherReliability,
    #[serde(default, skip_serializing_if = "is_default")]
    pub locality: ZenohLocalityConfig,
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

#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohPublisherReliability {
    BestEffort,
    #[default]
    Reliable,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ZenohQuerierConfig {
    /// The key of the query
    pub key: Arc<str>,
    /// How outgoing queries will be encoded.
    pub encoder: ZenohEncodingConfig,
    /// How incoming responses will be decoded.
    pub decoder: ZenohEncodingConfig,
    /// Key/value parameters for the
    #[serde(default, skip_serializing_if = "is_default")]
    pub parameters: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub congestion_control: ZenohCongestionControlConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub priority: ZenohPriorityConfig,
    /// When express is set to true, messages will not be batched.
    /// This usually has a positive impact on latency but negative impact on throughput.
    #[serde(default, skip_serializing_if = "is_default")]
    pub express: bool,
    #[serde(default, skip_serializing_if = "is_default")]
    pub target: ZenohQueryTargetConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub consolidation: ZenohQueryConsolidationModeConfig,
    #[serde(default, skip_serializing_if = "is_default")]
    pub locality: ZenohLocalityConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Duration>,
    /// If true, the replies that are sent through the out-stream will be a
    /// JSON Object with two fields: `replier_id` and `payload`.
    ///
    /// If false (default), the data sent through the out-stream will only
    /// contain the payload.
    #[serde(default, skip_serializing_if = "is_default")]
    pub include_replier_id: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohQueryConsolidationModeConfig {
    /// Apply automatic consolidation based on queryable's preferences
    #[default]
    Auto,
    /// No consolidation applied: multiple samples may be received for the same key-timestamp.
    None,
    /// Monotonic consolidation immediately forwards samples, except if one with an equal or more recent timestamp
    /// has already been sent with the same key.
    ///
    /// This optimizes latency while potentially reducing bandwidth.
    ///
    /// Note that this doesn't cause re-ordering, but drops the samples for which a more recent timestamp has already
    /// been observed with the same key.
    Monotonic,
    /// Holds back samples to only send the set of samples that had the highest timestamp for their key.
    Latest,
}

impl From<ZenohQueryConsolidationModeConfig> for QueryConsolidation {
    fn from(value: ZenohQueryConsolidationModeConfig) -> Self {
        let mode = match value {
            ZenohQueryConsolidationModeConfig::Auto => ConsolidationMode::Auto,
            ZenohQueryConsolidationModeConfig::None => ConsolidationMode::None,
            ZenohQueryConsolidationModeConfig::Monotonic => ConsolidationMode::Monotonic,
            ZenohQueryConsolidationModeConfig::Latest => ConsolidationMode::Latest,
        };
        mode.into()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ZenohQueryTargetConfig {
    /// Let Zenoh find the BestMatching queryable capabale of serving the query.
    #[default]
    BestMatching,
    /// Deliver the query to all queryables matching the query's key expression.
    All,
    /// Deliver the query to all queryables matching the query's key expression that are declared as complete.
    AllComplete,
}

impl From<ZenohQueryTargetConfig> for QueryTarget {
    fn from(value: ZenohQueryTargetConfig) -> Self {
        match value {
            ZenohQueryTargetConfig::BestMatching => QueryTarget::BestMatching,
            ZenohQueryTargetConfig::All => QueryTarget::All,
            ZenohQueryTargetConfig::AllComplete => QueryTarget::AllComplete,
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

impl ZenohSubscriptionHistoryConfig {
    pub fn is_default(&self) -> bool {
        !self.detect_late_publishers && self.max_samples.is_none() && self.max_age.is_none()
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

#[derive(ThisError, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ZenohQuerierError {
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

                let decoder: Codec = (&config.decoder).try_into()?;

                let callback = move |In(input): AsyncCallbackInput<
                    ((), BufferKey<()>),
                    ZenohNodeStreams,
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

                        let cancel = receiver.recv();

                        let active_key = input.request.1;

                        // Capture all the activity of the subscribing and
                        // cancelling inside this block so we have a convenient
                        // way to catch errors. We want to make sure we clean up
                        // the buffer before fully exiting the node implementation,
                        // and we don't want any errors to short-circuit that cleanup.
                        let r = async move {
                            // Capture all activity related to subscribing in
                            // this future so we can race it head-to-head with
                            // the cancellation signal.
                            let subscribing = async move {
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

                                loop {
                                    let next_sample = match subscription.recv_async().await {
                                        Ok(sample) => sample,
                                        Err(err) => {
                                            input.streams.out_error.send(format!("{err}"));
                                            continue;
                                        }
                                    };

                                    match decoder.decode(&next_sample) {
                                        Ok(msg) => {
                                            input.streams.out.send(msg);
                                        }
                                        Err(msg) => {
                                            input.streams.out_error.send(msg);
                                        }
                                    }
                                }
                            };

                            race(subscribing, receive_cancel::<ZenohSubscriptionError>(cancel)).await
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

                Ok(Node::<_, _, ZenohNodeStreams> {
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

        let ensure_session_for_publisher = ensure_session.clone();
        self.register_node_builder_fallible(
            NodeBuilderOptions::new("zenoh_publisher").with_default_display_text("Zenoh Publisher"),
            move |builder, config: ZenohPublisherConfig| {
                builder.commands().add(ensure_session_for_publisher.clone());

                let encoder: Codec = (&config.encoder).try_into()?;
                let publisher = builder
                    .commands()
                    .request(config, create_publisher.clone())
                    .take_response()
                    .shared();

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
                        publisher
                            .put(payload)
                            .encoding(encoder.encoding())
                            .await
                            .map_err(ArcError::new)?;
                        Ok::<_, ZenohPublisherError>(())
                    }
                };

                Ok(builder.create_map_async(callback))
            },
        );

        let create_querier = |In(config): In<ZenohQuerierConfig>,
                              session: Res<ZenohSession>| {
            let session_promise = session.promise.clone();
            async move {
                let session = session_promise
                    .await
                    .available()
                    .map(|r| r.map_err(ZenohQuerierError::ZenohError))
                    .unwrap_or(Err(ZenohQuerierError::SessionRemoved))?;

                let querier = session
                    .declare_querier(config.key.to_string())
                    .congestion_control(config.congestion_control.into())
                    .priority(config.priority.into())
                    .express(config.express)
                    .target(config.target.into())
                    .consolidation(config.consolidation)
                    .allowed_destination(config.locality.into());

                let querier = if let Some(timeout) = config.timeout {
                    querier.timeout(timeout)
                } else {
                    querier
                };

                let querier = querier.await.map_err(ArcError::new)?;
                Ok::<_, ZenohQuerierError>(Arc::new(querier))
            }
        };
        let create_querier = create_querier.into_async_callback();

        self.register_node_builder_fallible(
            NodeBuilderOptions::new("zenoh_querier").with_default_display_text("Zenoh Querier"),
            move |builder, mut config: ZenohQuerierConfig| {
                builder.commands().add(ensure_session.clone());

                let encoder: Codec = (&config.encoder).try_into()?;
                let decoder: Codec = (&config.decoder).try_into()?;
                let parameters = std::mem::replace(&mut config.parameters, Default::default());
                let parameters: Arc<Parameters> = Arc::new(parameters.into());

                let querier = builder
                    .commands()
                    .request(config, create_querier.clone())
                    .take_response()
                    .shared();

                let node = builder.create_map(move |input: AsyncMap<JsonMessage, ZenohNodeStreams>| {
                    let querier = querier.clone();
                    let parameters = Arc::clone(&parameters);
                    let encoder = encoder.clone();
                    let decoder = decoder.clone();
                    let (sender, mut cancellation_receiver) = unbounded_channel();
                    input.streams.canceller.send(sender);

                    async move {
                        let querying = async move {
                            let payload = encoder
                                .encode(input.request)
                                .map_err(ZenohQuerierError::EncodingError)?;

                            // SAFETY: There is no mechanism for the querier to be
                            // taken out of the promise, so it should always be
                            // available after being awaited.
                            let querier = querier.await.available().unwrap()?;
                            let replies = querier
                                .get()
                                .parameters(parameters.as_ref().clone())
                                .encoding(encoder.encoding())
                                .payload(payload)
                                .await
                                .map_err(ArcError::new)?;

                            while let Ok(reply) = replies.recv_async().await {
                                let next_sample = match reply.result() {
                                    Ok(sample) => sample,
                                    Err(err) => {
                                        input.streams.out_error.send(format!("{err}"));
                                        continue;
                                    }
                                };

                                match decoder.decode(next_sample) {
                                    Ok(msg) => {
                                        input.streams.out.send(msg);
                                    }
                                    Err(msg) => {
                                        input.streams.out_error.send(msg);
                                    }
                                }
                            }

                            Ok::<_, ZenohQuerierError>(JsonMessage::default())
                        };
                        let cancel = cancellation_receiver.recv();
                        race(querying, receive_cancel(cancel)).await
                    }
                });

                Ok(node)
            }
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
                bson::deserialize_from_slice::<JsonMessage>(
                    sample.payload().to_bytes().as_ref()
                )
                    .map_err(|err| decode_error_msg(err, &sample))
            }
        }
    }

    fn encoding(&self) -> Encoding {
        match self {
            Codec::Protobuf(descriptor) => {
                Encoding::APPLICATION_PROTOBUF
                    .with_schema(descriptor.full_name())
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
