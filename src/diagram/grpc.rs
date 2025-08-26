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

use crate::AsyncMap;
use super::*;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::anyhow;

use tonic::{
    client::Grpc as Client,
    transport::Channel,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    Status, Code, Request,
};
use prost_reflect::{DescriptorPool, MethodDescriptor, MessageDescriptor, DynamicMessage};
use prost::Message;
use http::uri::PathAndQuery;

use serde::{Serialize, Deserialize};
use schemars::JsonSchema;

use futures::{
    FutureExt, Stream as FutureStream,
    channel::{
        oneshot::{self, Sender as OneShotSender},
        mpsc::UnboundedReceiver,
    },
    never::Never,
    stream::once,
};

use futures_lite::future::race;

use async_std::future::timeout as until_timeout;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct GrpcConfig {
    pub service: Arc<str>,
    pub method: Option<NameOrIndex>,
    pub uri: Box<[u8]>,
    pub timeout: Option<Duration>,
}

#[derive(StreamPack)]
pub struct GrpcStreams {
    out: JsonMessage,
    canceller: OneShotSender<Option<String>>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub enum NameOrIndex {
    Name(Arc<str>),
    Index(usize),
}

impl DiagramElementRegistry {
    pub fn enable_grpc(&mut self) {
        self.register_node_builder_fallible(
            NodeBuilderOptions::new("grpc_request")
                .with_default_display_text("gRPC Request"),
            move |builder, config: GrpcConfig| {
                let GrpcDescriptions { method, codec, path } = get_descriptions(&config)?;

                let client = make_client(config.uri.clone()).shared();
                let is_server_streaming = method.is_server_streaming();
                let path = PathAndQuery::from_maybe_shared(path.clone())?;

                let node = builder.create_map(move |input: AsyncMap<JsonMessage, GrpcStreams>| {
                    let client = client.clone();
                    let codec = codec.clone();
                    let path = path.clone();
                    async move {
                        let client = client.await?;

                        // Convert the request message into a stream of a single dynamic message
                        let request = input.request;
                        let request = Request::new(once(async move { request }));

                        execute(request, client, codec, path, config.timeout, input.streams, is_server_streaming).await
                    }
                });

                Ok(node)
            }
        );

        self
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_node_builder_fallible(
                NodeBuilderOptions::new("grpc_client_stream_out")
                    .with_default_display_text("grpc Stream Out"),
                move |builder, config: GrpcConfig| {
                    let GrpcDescriptions { method, codec, path } = get_descriptions(&config)?;

                    let client = make_client(config.uri.clone()).shared();
                    let is_server_streaming = method.is_server_streaming();
                    let path = PathAndQuery::from_maybe_shared(path.clone())?;

                    let node = builder.create_map(move |input: AsyncMap<UnboundedReceiver<JsonMessage>, GrpcStreams>| {
                        let client = client.clone();
                        let codec = codec.clone();
                        let path = path.clone();
                        async move {
                            let client = client.await?;

                            let request = Request::new(input.request);
                            execute(request, client, codec, path, config.timeout, input.streams, is_server_streaming).await
                        }
                    });

                    Ok(node)
                }
            );
    }
}

async fn receive_cancel<T, E>(
    receiver: impl Future<Output = Result<Option<String>, E>>,
) -> Result<T, Status> {
    match receiver.await {
        Ok(msg) => {
            // The user sent a signal to cancel this request
            let msg = if let Some(msg) = msg {
                format!("cancelled: {msg}")
            } else {
                format!("cancelled")
            };

            return Err::<T, _>(Status::new(Code::Cancelled, msg));
        }
        Err(_) => {
            // The sender for the cancellation signal was dropped
            // so we must never let this future finish.
            NeverFinish.await;
            unreachable!("this future will never finish");
        }
    }
}

struct GrpcDescriptions {
    method: MethodDescriptor,
    codec: DynamicServiceCodec,
    path: String,
}

async fn make_client(uri: Box<[u8]>) -> Result<Client<Channel>, String> {
    let channel = Channel::from_shared(uri)
        .map_err(|e| format!("invalid uri for service: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("failed to connect: {e}"))?;
    Ok::<_, String>(Client::new(channel))
}

fn get_descriptions(config: &GrpcConfig) -> Result<GrpcDescriptions, Anyhow> {
    let descriptors = DescriptorPool::global();
    let service_name = &config.service;
    let service = descriptors.get_service_by_name(&service_name)
        .ok_or_else(|| anyhow!("could not find service name [{}]", config.service))?;

    let method = match config.method.as_ref().unwrap_or(&NameOrIndex::Index(0)) {
        NameOrIndex::Index(index) => {
            service
                .methods()
                .skip(*index)
                .next()
                .ok_or_else(|| anyhow!("service [{service_name}] does not have a method with index [{index}]"))?
        }
        NameOrIndex::Name(name) => {
            service
                .methods()
                .find(|m| m.name() == &**name)
                .ok_or_else(|| anyhow!("service [{service_name}] does not have a method with name [{name}]"))?
        }
    };

    let codec = DynamicServiceCodec {
        input: method.input(),
        output: method.output(),
    };

    let path = format!(
        "/{}.{}/{}",
        service.package_name(),
        service.name(),
        method.name(),
    );

    Ok(GrpcDescriptions { method, codec, path })
}

async fn execute<S>(
    request: Request<S>,
    mut client: Client<Channel>,
    codec: DynamicServiceCodec,
    path: PathAndQuery,
    timeout: Option<Duration>,
    output_streams: <GrpcStreams as StreamPack>::StreamChannels,
    is_server_streaming: bool,
) -> Result<(), String>
where
    S: FutureStream<Item = JsonMessage> + Send + 'static,
{
    // Wait for client to be ready
    client.ready().await.map_err(|e| format!("{e}"))?;
    if let Some(t) = timeout {
        until_timeout(t, client.ready())
            .await
            .map_err(|_| "timeout waiting for server to be ready".to_owned())?
            .map_err(|e| format!("server failed to be ready: {e}"))?;
    } else {
        client.ready().await.map_err(|e| format!("server failed to be ready: {e}"))?;
    }

    // Set up cancellation channel
    let (sender, receiver) = oneshot::channel();
    output_streams.canceller.send(sender);

    let cancel = receiver.shared();

    let session = client.streaming(request, path, codec);
    let cancellable_session = race(session, receive_cancel(cancel.clone()));

    let r = if let Some(t) = timeout {
        until_timeout(t, cancellable_session).await.map_err(|e| format!("{e}"))?
    } else {
        cancellable_session.await
    };

    let mut streaming = r.map_err(|e| format!("{e}"))?.into_inner();
    loop {
        let r = if let Some(t) = timeout {
            until_timeout(t, race(streaming.message(), receive_cancel(cancel.clone())))
            .await
            .map_err(|_| "timeout waiting for new stream message".to_owned())?
        } else {
            race(streaming.message(), receive_cancel(cancel.clone())).await
        }
        .map_err(|e| format!("{e}"))?;

        let Some(response) = r else {
            return Ok::<_, String>(());
        };

        let value = serde_json::to_value(response)
            .map_err(|e| format!("failed to convert to json: {e}"))?;
        output_streams.out.send(value);

        if !is_server_streaming {
            return Ok(());
        }
    }
}

#[derive(Clone)]
struct DynamicServiceCodec {
    input: MessageDescriptor,
    output: MessageDescriptor,
}

impl Codec for DynamicServiceCodec {
    type Encode = JsonMessage;
    type Decode = JsonMessage;
    type Encoder = DynamicMessageCodec;
    type Decoder = DynamicMessageCodec;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicMessageCodec { descriptor: self.input.clone() }
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicMessageCodec { descriptor: self.output.clone() }
    }
}

struct DynamicMessageCodec {
    descriptor: MessageDescriptor,
}

impl Encoder for DynamicMessageCodec {
    type Item = JsonMessage;
    type Error = Status;
    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        let msg = DynamicMessage::deserialize(self.descriptor.clone(), item)
            .map_err(|e| Status::new(Code::InvalidArgument, format!("{e}")))?;

        msg
        .encode(dst)
        .map_err(|_|
            Status::new(
                Code::ResourceExhausted,
                "unable to encode message because of insufficient buffer",
            )
        )
    }
}

impl Decoder for DynamicMessageCodec {
    type Item = JsonMessage;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let msg = DynamicMessage::decode(self.descriptor.clone(), src)
            .map_err(|e| {
                Status::new(
                    Code::DataLoss,
                    format!("failed to decode message: {e}"),
                )
            })?;

        let value = serde_json::to_value(msg)
            .map_err(|e| {
                Status::new(
                    Code::DataLoss,
                    format!("failed to convert to json: {e}"),
                )
            })?;

        Ok(Some(value))
    }
}

/// This is used to block a future from ever returning. This should only be used
/// in a race to force one of the contesting futures to lose. Make sure that at
/// least one contesting future will finish or else this will lead to a deadlock.
struct NeverFinish;

impl Future for NeverFinish {
    type Output = Never;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*, diagram::testing::*};
    use super::*;
    use prost_reflect::prost_types::FileDescriptorSet;

    #[test]
    fn test_simple_grpc_request() {
        let descriptor_set_bytes = include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        DescriptorPool::decode_global_file_descriptor_set(&descriptor_set_bytes[..]).unwrap();
    }
}
