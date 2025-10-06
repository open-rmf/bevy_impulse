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

use bevy_ecs::{
    prelude::{In, Res, World},
};
use futures_lite::future::race;
use std::time::Duration;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::unbounded_channel;
use zenoh_ext::{
    AdvancedSubscriberBuilderExt,
    HistoryConfig, RecoveryConfig,
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

impl ZenohSubscriptionHistoryConfig {
    pub fn is_default(&self) -> bool {
        !self.detect_late_publishers && self.max_samples.is_none() && self.max_age.is_none()
    }
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

impl DiagramElementRegistry {
    pub(super) fn register_zenoh_subscription(&mut self, ensure_session: EnsureZenohSession) {
        self.register_node_builder_fallible(
            NodeBuilderOptions::new("zenoh_subscription")
                .with_default_display_text("Zenoh Subscription"),
            move |builder, config: ZenohSubscriptionConfig| {
                builder.commands().add(ensure_session.clone());

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
    }
}
