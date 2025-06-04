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

use bevy_app::{App, Plugin};
use bevy_ecs::prelude::*;
use bevy_impulse::prelude::*;
use futures::future::Shared;
use prost::Message;
pub mod protos;
use schemars::JsonSchema;
use serde::{Serialize, Deserialize};
use std::{
    error::Error,
    sync::Arc,
    future::Future,
};
use zenoh::Session;
use zenoh_ext::{AdvancedPublisher, AdvancedPublisherBuilderExt, AdvancedSubscriberBuilderExt, CacheConfig, HistoryConfig, RecoveryConfig};

pub type ArcError = Arc<dyn Error + Send + Sync + 'static>;

#[derive(Default)]
pub struct ZenohImpulsePlugin {}

impl Plugin for ZenohImpulsePlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<ZenohSession>();
    }
}

#[derive(Resource)]
pub struct ZenohSession {
    pub promise: Shared<Promise<Result<Session, ArcError>>>,
}

impl FromWorld for ZenohSession {
    fn from_world(world: &mut World) -> Self {
        let promise = world.command(|commands| {
            commands
                .serve(async {
                    zenoh::open(zenoh::Config::default())
                        .await
                        .map_err(Arc::from)
                })
                .take_response()
        })
        .shared();

        Self { promise }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ZenohSubscriptionConfig {
    pub topic_name: Arc<str>,
}

#[derive(StreamPack)]
pub struct ZenohSubscriptionStream<T: 'static + Send + Sync> {
    pub sample: T,
}

pub fn zenoh_subscription_node<T: 'static + Send + Sync + Message + Default>(
    topic_name: Arc<str>,
    builder: &mut Builder,
) -> Node<(), Result<(), ArcError>, ZenohSubscriptionStream<T>> {
    let callback = move |
        In(input): AsyncCallbackInput<(), ZenohSubscriptionStream<T>>,
        session: Res<ZenohSession>,
    | {
        let session = session.promise.clone();
        let topic_name = topic_name.clone();
        async move {
            let session = session.await.available().unwrap()?;

            let subscriber = session
                .declare_subscriber(topic_name.as_ref())
                .history(HistoryConfig::default().detect_late_publishers())
                .recovery(RecoveryConfig::default())
                .await?;

            loop {
                let sample = subscriber.recv_async().await?;
                match T::decode(&*sample.payload().to_bytes()) {
                    Ok(msg) => {
                        input.streams.sample.send(msg);
                    }
                    Err(err) => {
                        println!("Error decoding incoming sample on topic [{topic_name}]: {err}");
                    }
                }
            }
        }
    };

    builder.create_node(callback.as_callback())
}

pub fn zenoh_publisher_node<T: 'static + Send + Sync + Message>(
    topic_name: Arc<str>,
    builder: &mut Builder,
) -> Node<T, Result<(), ArcError>> {
    let publisher = builder.commands().request(
        topic_name,
        get_zenoh_publisher.into_async_callback(),
    ).take_response();
    let publisher = publisher.shared();

    let callback = move |message: T| {
        let publisher = publisher.clone();
        async move {
            let publisher = publisher.await.available().unwrap()?;

            publisher.put(zenoh::bytes::ZBytes::from(message.encode_to_vec())).await?;
            Ok(())
        }
    };

    builder.create_map_async(callback)
}

fn get_zenoh_publisher(
    In(topic_name): In<Arc<str>>,
    session: Res<ZenohSession>,
) -> impl Future<Output = Result<Arc<AdvancedPublisher<'static>>, ArcError>> {
    let session_promise = session.promise.clone();

    async move {
        let session = session_promise.await.available().unwrap()?;
        let publisher = session
            .declare_publisher(topic_name.to_string())
            .cache(CacheConfig::default().max_samples(1))
            .sample_miss_detection(Default::default())
            .publisher_detection()
            .await?;

        Ok(Arc::new(publisher))
    }
}
