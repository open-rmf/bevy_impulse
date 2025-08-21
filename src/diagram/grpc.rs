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

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use crate::AsyncMap;
use super::*;

use tonic::{
    client::Grpc as Client,
    transport::Channel,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    Status, Code, Request,
};
use prost_reflect::{DescriptorPool, MessageDescriptor, DynamicMessage};
use prost::Message;
use http::uri::PathAndQuery;

use serde::{Serialize, Deserialize};
use schemars::JsonSchema;

use futures::{
    FutureExt,
    channel::{
        oneshot::{self, Sender as OneShotSender},
        mpsc::{unbounded, UnboundedSender},
    },
    never::Never,
};

use futures_lite::future::race;

use async_std::future::timeout;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct GrpcConfig {
    pub service: Arc<str>,
    pub method: Option<NameOrIndex>,
    pub uri: Box<[u8]>,
    pub timeout: Option<Duration>,
}

#[derive(StreamPack)]
struct GrpcStream {
    out: JsonMessage,
    stream_status: Status,
    canceller: OneShotSender<Option<String>>,
    input: ClientStreamInput,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub enum NameOrIndex {
    Name(Arc<str>),
    Index(usize),
}

impl DiagramElementRegistry {
    pub fn enable_grpc(&mut self) {
        self.register_node_builder(
            NodeBuilderOptions::new("grpc_client").with_default_display_text("gRPC"),
            move |builder, config: GrpcConfig| {
                let descriptors = DescriptorPool::global();
                let service = descriptors.get_service_by_name(&config.service).unwrap();
                let method = match config.method.as_ref().unwrap_or(&NameOrIndex::Index(0)) {
                    NameOrIndex::Index(index) => {
                        service.methods().skip(*index).next().unwrap()
                    }
                    NameOrIndex::Name(name) => {
                        service.methods().find(|m| m.name() == &**name).unwrap()
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

                let uri = config.uri.clone();
                let client = async move {
                    let channel = Channel::from_shared(uri).unwrap().connect().await.unwrap();
                    Client::new(channel)
                }.shared();

                let is_client_streaming = method.is_client_streaming();
                let is_server_streaming = method.is_server_streaming();
                let path = PathAndQuery::from_maybe_shared(path.clone()).unwrap();

                builder.create_map(move |input: AsyncMap<JsonMessage, GrpcStream>| {
                    let client = client.clone();
                    let codec = codec.clone();
                    let path = path.clone();
                    async move {
                        let mut client = client.await;

                        // Wait for client to be ready
                        client.ready().await.map_err(|e| format!("{e}"))?;
                        if let Some(t) = config.timeout {
                            timeout(t, client.ready())
                                .await
                                .map_err(|_| "timeout waiting for server to be ready".to_owned())?
                                .map_err(|e| format!("server failed to be ready: {e}"))?;
                        } else {
                            client.ready().await.map_err(|e| format!("server failed to be ready: {e}"))?;
                        }

                        // Set up cancellation channel
                        let (sender, receiver) = oneshot::channel();
                        input.streams.canceller.send(sender);

                        // Convert the request message into the appropriate type
                        let request = DynamicMessage::deserialize(codec.input.clone(), input.request)
                            .map_err(|e| format!("{e}"))?;
                        let request = Request::new(request);

                        if !is_client_streaming && !is_server_streaming {
                            let session = client.unary(request, path, codec);
                            let cancellable_session = race(session, receive_cancel(receiver));

                            let r = if let Some(t) = config.timeout {
                                timeout(t, cancellable_session).await.map_err(|e| format!("{e}"))?
                            } else {
                                cancellable_session.await
                            };

                            let response = r.map_err(|e| format!("{e}"))?.into_inner();
                            let value = serde_json::to_value(response)
                                .map_err(|e| format!("failed to convert to json: {e}"))?;
                            input.streams.out.send(value);

                        } else if !is_client_streaming && is_server_streaming {
                            let cancel = receiver.shared();

                            let session = client.server_streaming(request, path, codec);
                            let cancellable_session = race(session, receive_cancel(cancel.clone()));

                            let r = if let Some(t) = config.timeout {
                                timeout(t, cancellable_session).await.map_err(|e| format!("{e}"))?
                            } else {
                                cancellable_session.await
                            };

                            let mut streaming = r.map_err(|e| format!("{e}"))?.into_inner();
                            loop {
                                let r = if let Some(t) = config.timeout {
                                    timeout(t, race(streaming.message(), receive_cancel(cancel.clone())))
                                    .await
                                    .map_err(|_| "timeout waiting for new stream message".to_owned())?
                                } else {
                                    race(streaming.message(), receive_cancel(cancel.clone())).await
                                }
                                .map_err(|e| format!("{e}"))?;

                                let Some(response) = r else {
                                    return Ok(());
                                };

                                let value = serde_json::to_value(response)
                                    .map_err(|e| format!("failed to convert to json: {e}"))?;
                                input.streams.out.send(value);
                            }
                        } else if is_client_streaming && !is_server_streaming {
                            let (sender, receiver) = unbounded();

                            let session = client.client_streaming(request, path, codec).await;

                        }

                        Ok::<_, String>(())
                    }
                })
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

#[derive(Clone)]
pub struct ClientStreamInput {
    sender: UnboundedSender<DynamicMessage>,
    descriptor: MessageDescriptor,
}

#[derive(Clone)]
struct DynamicServiceCodec {
    input: MessageDescriptor,
    output: MessageDescriptor,
}

impl Codec for DynamicServiceCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
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
    type Item = DynamicMessage;
    type Error = Status;
    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item
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
    type Item = DynamicMessage;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let msg = DynamicMessage::decode(self.descriptor.clone(), src)
            .map_err(|e| {
                Status::new(
                    Code::DataLoss,
                    format!("failed to decode message: {e}"),
                )
            })?;

        Ok(Some(msg))
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
