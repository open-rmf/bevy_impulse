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

use std::sync::Arc;
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

use futures::FutureExt;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct GrpcConfig {
    pub service: Arc<str>,
    pub method: Option<NameOrIndex>,
    pub uri: Box<[u8]>,
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

                builder.create_map(move |input: AsyncMap<JsonMessage, GrpcStream>| {
                    let client = client.clone();
                    let codec = codec.clone();
                    let path = path.clone();
                    async move {
                        let mut client = client.await;
                        client.ready().await.map_err(|e| format!("{e}"))?;
                        let request = DynamicMessage::deserialize(codec.input.clone(), input.request)
                            .map_err(|e| format!("{e}"))?;

                        let (_, r, _) = client
                            .unary(
                                Request::new(request),
                                PathAndQuery::from_maybe_shared(path.clone()).map_err(|e| format!("{e}"))?,
                                codec.clone(),
                            )
                            .await
                            .map_err(|e| format!("{e}"))?
                            .into_parts();

                        serde_json::to_value(r)
                        .map_err(|e| format!("{e}"))
                    }
                })
            }
        );

    }
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

#[derive(StreamPack)]
struct GrpcStream {
    out: JsonMessage,
}
