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

use ::zenoh::query::{ConsolidationMode, Parameters, Querier, QueryConsolidation, QueryTarget};
use bevy_ecs::prelude::{In, Res};
use futures_lite::future::race;
use std::time::Duration;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::unbounded_channel;

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
    #[serde(default, skip_serializing_if = "is_default")]
    pub wait_for_matching: WaitForMatching,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WaitForMatching {
    /// Never wait for a matching queryable before sending off a query. This has
    /// a risk of having your querier immediately yield nothing and quitting when
    /// it gets triggered.
    Never,
    /// Wait for a matching queryable right after constructing the querier. This
    /// will make sure a matching queryable exists before the first time that
    /// you send out a query, but it will never check again after that, meaning
    /// your queries could start getting dropped if the queryables ever close.
    Once,
    /// Wait for a matching queryable every time you attempt a query. This is
    /// the safest way to ensure that your query arrives somewhere, but it has
    /// the penalty of doing extra work for each query that you send.
    #[default]
    Always,
}

impl WaitForMatching {
    pub fn once(&self) -> bool {
        matches!(self, Self::Once)
    }

    pub fn always(&self) -> bool {
        matches!(self, Self::Always)
    }
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

impl DiagramElementRegistry {
    pub(super) fn register_zenoh_querier(&mut self, ensure_session: EnsureZenohSession) {
        let create_querier = |In(config): In<ZenohQuerierConfig>, session: Res<ZenohSession>| {
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

                if config.wait_for_matching.once() {
                    wait_for_matching(&querier).await?;
                }

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
                let wait_choice = config.wait_for_matching;

                let querier = builder
                    .commands()
                    .request(config, create_querier.clone())
                    .take_response()
                    .shared();

                let node =
                    builder.create_map(move |input: AsyncMap<JsonMessage, ZenohNodeStreams>| {
                        let querier = querier.clone();
                        let parameters = Arc::clone(&parameters);
                        let encoder = encoder.clone();
                        let decoder = decoder.clone();
                        let (sender, mut cancellation_receiver) = unbounded_channel();
                        input.streams.canceller.send(sender);

                        async move {
                            let querying = async move {
                                let payload = encoder
                                    .encode(&input.request)
                                    .map_err(ZenohQuerierError::EncodingError)?;

                                // SAFETY: There is no mechanism for the querier to be
                                // taken out of the promise, so it should always be
                                // available after being awaited.
                                let querier = querier.await.available().unwrap()?;
                                if wait_choice.always() {
                                    wait_for_matching(&querier).await?;
                                }

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
                                            println!(" received ===> {msg}");
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
            },
        )
        .with_fork_result();
    }
}

async fn wait_for_matching(querier: &Querier<'_>) -> Result<(), ZenohQuerierError> {
    let listener = querier.matching_listener().await.map_err(ArcError::new)?;
    loop {
        let matching = listener
            .recv_async()
            .await
            .map_err(ArcError::new)?
            .matching();

        if matching {
            return Ok(());
        }
    }
}
