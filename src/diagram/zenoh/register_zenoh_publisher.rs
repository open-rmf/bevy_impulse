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

use bevy_ecs::prelude::{In, Res};
use futures::channel::oneshot::{self, Receiver as OneShotReceiver, Sender as OneShotSender};
use std::time::Duration;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use zenoh_ext::{AdvancedPublisherBuilderExt, CacheConfig, MissDetectionConfig, RepliesConfig};

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
    #[serde(default, skip_serializing_if = "is_default")]
    pub ordering: OrderingConfig,
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

/// How strict should the publishing order be for messages sent into a publisher?
/// This ordering also applies for independent sessions of the same workflow since
/// all sessions of the same workflow will use the same publisher. You can prevent
/// this interdependence if you spawn a separate workflow for each session.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OrderingConfig {
    /// Guarantee that messages get published in the order that they arrive at
    /// the publisher node. This means encoding and publishing will both be done
    /// serially, so encoding large messages will be a bottleneck for all other
    /// messages.
    #[default]
    Strict,
    /// Message ordering is not considered as important, so messages will be
    /// encoded in parallel and sent for publication as soon as they are ready,
    /// no matter what order they arrived at the publisher node. All messages
    /// will be published, but they might arrive at the subscription out of order.
    Loose,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ZenohPublisherHeartbeatConfig {
    /// How frequently should the heartbeat be published?
    pub period: f64,
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
            period: 1.0,
            sporadic: false,
        }
    }
}

impl From<ZenohPublisherHeartbeatConfig> for MissDetectionConfig {
    fn from(value: ZenohPublisherHeartbeatConfig) -> Self {
        let period = Duration::from_secs_f64(value.period);
        if value.sporadic {
            MissDetectionConfig::default().heartbeat(period)
        } else {
            MissDetectionConfig::default().sporadic_heartbeat(period)
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

#[derive(ThisError, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ZenohPublisherError {
    #[error("the zenoh session was removed from its resource")]
    SessionRemoved,
    #[error("error while encoding message: {}", .0)]
    EncodingError(String),
    #[error("the publisher was dropped while still in use")]
    PublisherDropped,
    #[error("{}", .0)]
    ZenohError(#[from] ArcError),
}

type PublishingResult = Result<(), ZenohPublisherError>;

struct PublisherSetup {
    receiver: UnboundedReceiver<(Payload, OneShotSender<PublishingResult>)>,
    config: ZenohPublisherConfig,
    encoder: Codec,
}

enum Payload {
    Encoded(ZBytes),
    Unencoded(JsonMessage),
}

impl Payload {
    fn encode(self, encoder: &Codec) -> Result<ZBytes, ZenohPublisherError> {
        match self {
            Self::Encoded(bytes) => Ok(bytes),
            Self::Unencoded(msg) => encoder
                .encode(&msg)
                .map_err(ZenohPublisherError::EncodingError),
        }
    }
}

impl DiagramElementRegistry {
    pub(super) fn register_zenoh_publisher(&mut self, ensure_session: EnsureZenohSession) {
        // We run the publisher as its own async job because we noticed an
        // undesirable behavior in zenoh where multiple simultaneous attempts to
        // publish a message can cause one of them to be dropped from the history
        // and not received by a subscription if it starts before another message
        // and ends after.
        //
        // Funneling all the publishing activity into one async job ensures that
        // multiple uses of the same publisher will never overlap with each other.
        //
        // We could examine what effect the congestion control setting may have
        // on this behavior. Perhaps we don't to funnel like this if congestion
        // control is set to Block.
        let run_publisher = |In(PublisherSetup {
                                 mut receiver,
                                 config,
                                 encoder,
                             }): In<PublisherSetup>,
                             session: Res<ZenohSession>| {
            let session_promise = session.promise.clone();
            async move {
                let publisher = async move {
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
                    Ok::<_, ZenohPublisherError>(publisher)
                }
                .await;

                while let Some((payload, responder)) = receiver.recv().await {
                    // If an error happened while creating the publisher, just
                    // report that back immediately.
                    let publisher = match &publisher {
                        Ok(publisher) => publisher,
                        Err(err) => {
                            let _ = responder.send(Err(err.clone()));
                            continue;
                        }
                    };

                    let r = async {
                        let payload = payload.encode(&encoder)?;

                        publisher
                            .put(payload)
                            .encoding(encoder.encoding())
                            .await
                            .map_err(ArcError::new)?;

                        Ok::<_, ZenohPublisherError>(())
                    }
                    .await;

                    let _ = responder.send(r);
                }
            }
        };
        let run_publisher = run_publisher.into_async_callback();

        self.register_node_builder_fallible(
            NodeBuilderOptions::new("zenoh_publisher").with_default_display_text("Zenoh Publisher"),
            move |builder, config: ZenohPublisherConfig| {
                builder.commands().queue(ensure_session.clone());

                let encoder: Codec = (&config.encoder).try_into()?;
                let ordering = config.ordering;
                let (publishing_sender, receiver) = unbounded_channel();
                let setup = PublisherSetup {
                    receiver,
                    config,
                    encoder: encoder.clone(),
                };

                builder
                    .commands()
                    .request(setup, run_publisher.clone())
                    .detach();

                let callback = move |message: JsonMessage| {
                    let publishing_sender = publishing_sender.clone();
                    let encoder = encoder.clone();

                    let work = match ordering {
                        OrderingConfig::Strict => {
                            let (sender, receiver) = oneshot::channel();
                            let receiver = publishing_sender
                                .send((Payload::Unencoded(message), sender))
                                .map(|_| receiver)
                                .map_err(|_| ZenohPublisherError::PublisherDropped);

                            PublisherWork::Strict(receiver)
                        }
                        OrderingConfig::Loose => PublisherWork::Loose(message),
                    };

                    async move {
                        let receiver = match work {
                            PublisherWork::Strict(receiver) => receiver?,
                            PublisherWork::Loose(message) => {
                                let payload = encoder
                                    .encode(&message)
                                    .map_err(ZenohPublisherError::EncodingError)?;

                                let (sender, receiver) = oneshot::channel();
                                publishing_sender
                                    .send((Payload::Encoded(payload), sender))
                                    .map_err(|_| ZenohPublisherError::PublisherDropped)?;

                                receiver
                            }
                        };

                        receiver
                            .await
                            .or(Err(ZenohPublisherError::PublisherDropped))
                            .flatten()
                    }
                };

                Ok(builder.create_map_async(callback))
            },
        );
    }
}

/// This enum allows the user to choose whether message encoding should be done
/// serially (allowing bottlenecks) or in parallel (allowing messages to arrive
/// out of order). Rust's async syntax forces one final async block to be returned
/// by the node callback, so this enum allows us to have that one block behave in
/// two different ways depending on what the user selected.
enum PublisherWork {
    Strict(Result<OneShotReceiver<Result<(), ZenohPublisherError>>, ZenohPublisherError>),
    Loose(JsonMessage),
}
