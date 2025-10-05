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
use crate::AsyncMap;

use std::{
    future::Future,
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;

use http::uri::PathAndQuery;
use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, MethodDescriptor, SerializeOptions,
};
use tonic::{
    client::Grpc as Client,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    codegen::tokio_stream::wrappers::UnboundedReceiverStream,
    transport::Channel,
    Code, Request, Status,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use tokio::runtime::Runtime;

use futures::{
    stream::once,
    FutureExt, Stream as FutureStream,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use futures_lite::future::race;

use async_std::future::timeout as until_timeout;

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct GrpcConfig {
    pub service: Arc<str>,
    pub method: Option<NameOrIndex>,
    pub uri: Arc<str>,
    pub timeout: Option<Duration>,
}

#[derive(StreamPack)]
pub struct GrpcStreams {
    out: JsonMessage,
    canceller: UnboundedSender<Option<String>>,
}

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum NameOrIndex {
    Name(Arc<str>),
    Index(usize),
}

impl DiagramElementRegistry {
    pub fn enable_grpc(&mut self, runtime: Arc<Runtime>) {
        let rt = Arc::clone(&runtime);
        self.register_node_builder_fallible(
            NodeBuilderOptions::new("grpc_request").with_default_display_text("gRPC Request"),
            move |builder, config: GrpcConfig| {
                let GrpcDescriptions {
                    method,
                    codec,
                    path,
                } = get_descriptions(&config)?;

                let uri: Box<[u8]> = config.uri.as_bytes().into();
                let client = make_client(uri).shared();
                let is_server_streaming = method.is_server_streaming();
                let path = PathAndQuery::from_maybe_shared(path.clone())?;

                let rt = Arc::clone(&rt);
                let node = builder.create_map(move |input: AsyncMap<JsonMessage, GrpcStreams>| {
                    let client = client.clone();
                    let codec = codec.clone();
                    let path = path.clone();

                    // The tonic gRPC client needs to be run inside a tokio
                    // async runtime, so we spawn a tokio task here and use the
                    // JoinHandle to pass its result through the workflow.
                    let task = rt.spawn(async move {
                        let client = client.await?;

                        // Convert the request message into a stream of a single dynamic message
                        let request = input.request;
                        let request = Request::new(once(async move { request }));
                        execute(
                            request,
                            client,
                            codec,
                            path,
                            config.timeout,
                            input.streams,
                            is_server_streaming,
                        )
                        .await
                    });

                    async move {
                        let r = task.await.map_err(|e| format!("{e}")).flatten();

                        r
                    }
                });

                Ok(node)
            },
        )
        .with_fork_result();

        let rt = runtime;
        self.opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_node_builder_fallible(
                NodeBuilderOptions::new("grpc_client_stream_out")
                    .with_default_display_text("grpc Stream Out"),
                move |builder, config: GrpcConfig| {
                    let GrpcDescriptions {
                        method,
                        codec,
                        path,
                    } = get_descriptions(&config)?;

                    let uri: Box<[u8]> = config.uri.as_bytes().into();
                    let client = make_client(uri).shared();
                    let is_server_streaming = method.is_server_streaming();
                    let path = PathAndQuery::from_maybe_shared(path.clone())?;

                    let rt = Arc::clone(&rt);
                    let node = builder.create_map(
                        move |input: AsyncMap<UnboundedReceiver<JsonMessage>, GrpcStreams>| {
                            let client = client.clone();
                            let codec = codec.clone();
                            let path = path.clone();

                            // The tonic gRPC client needs to be run inside a tokio
                            // async runtime, so we spawn a tokio task here and use the
                            // JoinHandle to pass its result through the workflow.
                            let task = rt.spawn(async move {
                                let client = client.await?;

                                let request =
                                    Request::new(UnboundedReceiverStream::new(input.request));
                                execute(
                                    request,
                                    client,
                                    codec,
                                    path,
                                    config.timeout,
                                    input.streams,
                                    is_server_streaming,
                                )
                                .await
                            });

                            async move { task.await.map_err(|e| format!("{e}")).flatten() }
                        },
                    );

                    Ok(node)
                },
            )
            .with_fork_result();

        // TODO(@mxgrey): Support dynamic gRPC requests whose configurations are
        // decided within the workflow and passed into the node as input.
    }
}

async fn receive_cancel<T>(
    receiver: impl Future<Output = Option<Option<String>>>,
) -> Result<T, Status> {
    match receiver.await {
        Some(msg) => {
            // The user sent a signal to cancel this request
            let msg = if let Some(msg) = msg {
                format!("cancelled: {msg}")
            } else {
                format!("cancelled")
            };

            return Err::<T, _>(Status::new(Code::Cancelled, msg));
        }
        None => {
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
    let service = descriptors
        .get_service_by_name(&service_name)
        .ok_or_else(|| anyhow!("could not find service name [{}]", config.service))?;

    let method = match config.method.as_ref().unwrap_or(&NameOrIndex::Index(0)) {
        NameOrIndex::Index(index) => service.methods().skip(*index).next().ok_or_else(|| {
            anyhow!("service [{service_name}] does not have a method with index [{index}]")
        })?,
        NameOrIndex::Name(name) => {
            service
                .methods()
                .find(|m| m.name() == &**name)
                .ok_or_else(|| {
                    anyhow!("service [{service_name}] does not have a method with name [{name}]")
                })?
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

    Ok(GrpcDescriptions {
        method,
        codec,
        path,
    })
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
        client
            .ready()
            .await
            .map_err(|e| format!("server failed to be ready: {e}"))?;
    }

    // Set up cancellation channel
    let (sender, mut receiver) = unbounded_channel();
    output_streams.canceller.send(sender);

    let cancel = receiver.recv().shared();

    let session = client.streaming(request, path, codec);
    let cancellable_session = race(session, receive_cancel(cancel.clone()));

    let r = if let Some(t) = timeout {
        until_timeout(t, cancellable_session)
            .await
            .map_err(|e| format!("{e}"))?
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
        DynamicMessageCodec {
            descriptor: self.input.clone(),
        }
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicMessageCodec {
            descriptor: self.output.clone(),
        }
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

        msg.encode(dst).map_err(|_| {
            Status::new(
                Code::ResourceExhausted,
                "unable to encode message because of insufficient buffer",
            )
        })
    }
}

impl Decoder for DynamicMessageCodec {
    type Item = JsonMessage;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let msg = DynamicMessage::decode(self.descriptor.clone(), src)
            .map_err(|e| Status::new(Code::DataLoss, format!("failed to decode message: {e}")))?;

        let value = msg
            .serialize_with_options(
                serde_json::value::Serializer,
                &SerializeOptions::new()
                    .stringify_64_bit_integers(false)
                    .use_proto_field_name(true)
                    .skip_default_fields(false),
            )
            .map_err(|e| Status::new(Code::DataLoss, format!("failed to convert to json: {e}")))?;

        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{diagram::testing::*, prelude::*, testing::*};
    use prost_reflect::Kind;
    use protos::{
        fibonacci_server::{Fibonacci, FibonacciServer},
        navigation_server::{Navigation, NavigationServer},
        FibonacciReply, FibonacciRequest, NavigationGoal, NavigationUpdate,
    };
    use futures::channel::oneshot::{self, Sender as OneShotSender};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::{
        runtime::Runtime,
        sync::mpsc::{error::TryRecvError, unbounded_channel, UnboundedReceiver, UnboundedSender},
    };
    use tonic::{
        codegen::tokio_stream::wrappers::UnboundedReceiverStream, transport::Server, Request,
        Response, Status, Streaming,
    };

    #[test]
    fn test_file_descriptor_loading() {
        let descriptor_set_bytes =
            include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        DescriptorPool::decode_global_file_descriptor_set(&descriptor_set_bytes[..]).unwrap();
        let fibonacci_service = DescriptorPool::global()
            .get_service_by_name("example_protos.fibonacci.Fibonacci")
            .unwrap();
        let final_number = fibonacci_service
            .methods()
            .find(|m| m.name() == "FinalNumber")
            .unwrap();
        let sequence_stream = fibonacci_service
            .methods()
            .find(|m| m.name() == "SequenceStream")
            .unwrap();

        assert_eq!(final_number.input().name(), "FibonacciRequest");
        assert_eq!(final_number.is_client_streaming(), false);
        assert_eq!(final_number.output().name(), "FibonacciReply");
        assert_eq!(final_number.is_server_streaming(), false);

        assert_eq!(sequence_stream.input().name(), "FibonacciRequest");
        assert_eq!(sequence_stream.is_client_streaming(), false);
        assert_eq!(sequence_stream.output().name(), "FibonacciReply");
        assert!(sequence_stream.is_server_streaming());

        let order = final_number
            .input()
            .fields()
            .find(|f| f.name() == "order")
            .unwrap();
        assert_eq!(order.kind(), Kind::Uint64);
        let value = final_number
            .output()
            .fields()
            .find(|f| f.name() == "value")
            .unwrap();
        assert_eq!(value.kind(), Kind::Uint64);
    }

    #[test]
    fn test_grcp_unary_request() {
        let descriptor_set_bytes =
            include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        DescriptorPool::decode_global_file_descriptor_set(&descriptor_set_bytes[..]).unwrap();

        let mut fixture = DiagramTestFixture::new();
        let port = 50000 + line!();
        let addr = format!("[::1]:{port}").parse().unwrap();

        let rt = Arc::new(Runtime::new().unwrap());
        rt.spawn(async move {
            Server::builder()
                .add_service(FibonacciServer::new(GenerateFibonacci))
                .serve(addr)
                .await
                .unwrap();
        });
        fixture.registry.enable_grpc(Arc::clone(&rt));

        let (exit_sender, exit_receiver) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let _ = rt.block_on(exit_receiver);
        });

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fibonacci",
            "ops": {
                "fibonacci": {
                    "type": "node",
                    "builder": "grpc_request",
                    "config": {
                        "service": "example_protos.fibonacci.Fibonacci",
                        "method": "FinalNumber",
                        "uri": format!("http://localhost:{port}"),
                    },
                    "stream_out": {
                        "out": { "builtin": "terminate" }
                    },
                    "next": { "builtin": "terminate" }
                }
            }
        }))
        .unwrap();

        let request = json!({
            "order": 10
        });

        let result: JsonMessage = fixture.spawn_and_run(&diagram, request).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        let value = result["value"].as_number().unwrap().as_u64().unwrap();
        assert_eq!(value, 55);

        let _ = exit_sender.send(());
    }

    #[test]
    fn test_grpc_service_streaming() {
        let descriptor_set_bytes =
            include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        DescriptorPool::decode_global_file_descriptor_set(&descriptor_set_bytes[..]).unwrap();

        let mut fixture = DiagramTestFixture::new();
        let port = 50000 + line!();
        let addr = format!("[::1]:{port}").parse().unwrap();

        let rt = Arc::new(Runtime::new().unwrap());
        rt.spawn(async move {
            Server::builder()
                .add_service(FibonacciServer::new(GenerateFibonacci))
                .serve(addr)
                .await
                .unwrap();
        });
        fixture.registry.enable_grpc(Arc::clone(&rt));

        let (exit_sender, exit_receiver) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let _ = rt.block_on(exit_receiver);
        });

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "fibonacci",
            "ops": {
                "fibonacci": {
                    "type": "node",
                    "builder": "grpc_request",
                    "config": {
                        "service": "example_protos.fibonacci.Fibonacci",
                        "method": "SequenceStream",
                        "uri": format!("http://localhost:{port}"),
                    },
                    "stream_out": {
                        "out": "get_value"
                    },
                    "next": { "builtin" : "dispose" }
                },
                "get_value": {
                    "type": "transform",
                    "cel": "request.value",
                    "next": "evaluate"
                },
                "evaluate": {
                    "type": "node",
                    "builder": "less_than",
                    "config": 10,
                    "next": "fork_result"
                },
                "fork_result": {
                    "type": "fork_result",
                    "ok": { "builtin" : "dispose" },
                    "err": { "builtin" : "terminate" }
                }
            }
        }))
        .unwrap();

        let request = json!({
            "order": 10
        });

        let result: u64 = fixture.spawn_and_run(&diagram, request).unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 13);

        let _ = exit_sender.send(());
    }

    #[test]
    fn test_grpc_bidirectional_streaming() {
        let descriptor_set_bytes =
            include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        DescriptorPool::decode_global_file_descriptor_set(&descriptor_set_bytes[..]).unwrap();

        let mut fixture = DiagramTestFixture::new();
        fixture
            .registry
            .register_node_builder(NodeBuilderOptions::new("guide"), create_guide_to_goal);
        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .no_cloning()
            .register_message::<GoalTracker>();

        let reached_listener = fixture
            .context
            .app
            .spawn_service(check_if_reached.into_blocking_service());
        fixture
            .registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_node_builder(
                NodeBuilderOptions::new("reached_listener"),
                move |builder, _: ()| builder.create_node(reached_listener),
            )
            .with_listen();

        let port = 50000 + line!();
        let addr = format!("[::1]:{port}").parse().unwrap();

        let rt = Arc::new(Runtime::new().unwrap());
        let navigation = TestNavigation::new(Arc::clone(&rt));
        rt.spawn(async move {
            Server::builder()
                .add_service(NavigationServer::new(navigation))
                .serve(addr)
                .await
                .unwrap();
        });
        fixture.registry.enable_grpc(Arc::clone(&rt));

        let (exit_sender, exit_receiver) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            let _ = rt.block_on(exit_receiver);
        });

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "guider",
            "ops": {
                "guider": {
                    "type": "node",
                    "builder": "guide",
                    "config": {
                        "goals": [
                            [10.0, 10.0],
                            [5.0, 5.0],
                            [-5.0, 0.0]
                        ]
                    },
                    "stream_out": {
                        "goal_receiver": "guided",
                        "goal_tracker": "tracker",
                    },
                    "next": { "builtin" : "terminate" }
                },
                "guided": {
                    "type": "node",
                    "builder": "grpc_client_stream_out",
                    "config": {
                        "service": "example_protos.navigation.Navigation",
                        "method": "Guide",
                        "uri": format!("http://localhost:{port}"),
                    },
                    "stream_out": {
                        "out": "update_buffer"
                    },
                    "next": { "builtin": "dispose" }
                },
                "update_buffer": { "type": "buffer" },
                "tracker": { "type": "buffer" },
                "listen": {
                    "type": "listen",
                    "buffers": {
                        "navigation_update": "update_buffer",
                        "goal_tracker": "tracker"
                    },
                    "next": "reached_listener"
                },
                "reached_listener": {
                    "type": "node",
                    "builder": "reached_listener",
                    "next": { "builtin" : "dispose" }
                }
            }
        }))
        .unwrap();

        let _: () = fixture.spawn_and_run(&diagram, ()).unwrap();

        let _ = exit_sender.send(());
    }

    struct GenerateFibonacci;

    #[tonic::async_trait]
    impl Fibonacci for GenerateFibonacci {
        async fn final_number(
            &self,
            request: Request<FibonacciRequest>,
        ) -> Result<Response<FibonacciReply>, Status> {
            let reply = calculate_fibonacci(request.into_inner().order, None);
            Ok(Response::new(reply))
        }

        type SequenceStreamStream = UnboundedReceiverStream<Result<FibonacciReply, Status>>;

        async fn sequence_stream(
            &self,
            request: Request<FibonacciRequest>,
        ) -> Result<Response<Self::SequenceStreamStream>, Status> {
            let (sender, receiver) = unbounded_channel();
            std::thread::spawn(move || {
                calculate_fibonacci(request.into_inner().order, Some(sender));
            });

            Ok(Response::new(receiver.into()))
        }
    }

    fn calculate_fibonacci(
        order: u64,
        sender: Option<UnboundedSender<Result<FibonacciReply, Status>>>,
    ) -> FibonacciReply {
        let mut current = 0;
        let mut next = 1;
        for _ in 0..order {
            if let Some(sender) = &sender {
                let _ = sender.send(Ok(FibonacciReply { value: current }));
            }

            let sum = current + next;
            current = next;
            next = sum;
        }

        if let Some(sender) = &sender {
            let _ = sender.send(Ok(FibonacciReply { value: current }));
        }

        FibonacciReply { value: current }
    }

    #[derive(Serialize, Deserialize, JsonSchema)]
    struct GuideThroughGoals {
        goals: Vec<[f32; 2]>,
    }

    #[derive(StreamPack)]
    struct GuideStreams {
        goal_receiver: UnboundedReceiver<JsonMessage>,
        goal_tracker: GoalTracker,
    }

    fn create_guide_to_goal(
        builder: &mut Builder,
        config: GuideThroughGoals,
    ) -> Node<(), (), GuideStreams> {
        builder.create_map(move |input: AsyncMap<(), GuideStreams>| {
            let goals = config.goals.clone();
            async move {
                let (goal_sender, goal_receiver) = unbounded_channel::<JsonMessage>();
                input.streams.goal_receiver.send(goal_receiver);

                for goal in goals {
                    let goal_msg = json!({
                        "x": goal[0],
                        "y": goal[1],
                        "has_yaw_target": false,
                    });

                    if goal_sender.send(goal_msg).is_err() {
                        return;
                    }

                    let (done_sender, done_receiver) = oneshot::channel();
                    input.streams.goal_tracker.send(GoalTracker {
                        goal,
                        done: Some(done_sender),
                    });

                    done_receiver.await.unwrap();
                }
            }
        })
    }

    #[derive(Clone, Accessor)]
    struct ReachedKeys {
        navigation_update: BufferKey<JsonMessage>,
        goal_tracker: BufferKey<GoalTracker>,
    }

    struct GoalTracker {
        goal: [f32; 2],
        done: Option<OneShotSender<()>>,
    }

    fn check_if_reached(
        In(keys): In<ReachedKeys>,
        update_buffer: BufferAccess<JsonMessage>,
        mut goal_buffer: BufferAccessMut<GoalTracker>,
    ) {
        let Some(update) = update_buffer.get_newest(&keys.navigation_update) else {
            return;
        };

        let mut tracker_buffer = goal_buffer.get_mut(&keys.goal_tracker).unwrap();
        let Some(tracker) = tracker_buffer.newest_mut() else {
            return;
        };
        let goal = tracker.goal;

        let x_close = f32::abs(update["x"].as_f64().unwrap() as f32 - goal[0]) < 1e-3;
        let y_close = f32::abs(update["y"].as_f64().unwrap() as f32 - goal[1]) < 1e-3;
        let arrived = x_close && y_close;

        if arrived {
            if let Some(done) = tracker.done.take() {
                let _ = done.send(());
            }
        }
    }

    struct TestNavigation {
        goal_sender: UnboundedSender<NavigationGoalInfo>,
        stream_sender: UnboundedSender<NavigationUpdateSender>,
        runtime: Arc<Runtime>,
    }

    struct NavigationGoalInfo {
        goal: Option<NavigationGoal>,
        goal_streaming: bool,
    }

    impl TestNavigation {
        fn new(runtime: Arc<Runtime>) -> Self {
            let (goal_sender, mut receive_goal) = unbounded_channel::<NavigationGoalInfo>();
            let (stream_sender, mut receive_stream) = unbounded_channel();

            std::thread::spawn(move || {
                let mut current_stream: Option<NavigationUpdateSender> = None;
                let mut current_position = NavigationUpdate::default();
                let mut current_goal = None;
                let mut goal_streaming = false;
                let linear_speed = 1.0;
                let angular_speed = 0.1;

                loop {
                    std::thread::sleep(Duration::from_micros(100));
                    match receive_stream.try_recv() {
                        Ok(new_stream) => {
                            current_stream = Some(new_stream);
                            current_goal = None;
                        }
                        Err(TryRecvError::Disconnected) => {
                            break;
                        }
                        Err(TryRecvError::Empty) => {}
                    }

                    match receive_goal.try_recv() {
                        Ok(NavigationGoalInfo {
                            goal,
                            goal_streaming: streaming,
                        }) => {
                            current_goal = goal;
                            goal_streaming = streaming;
                        }
                        Err(TryRecvError::Disconnected) => {
                            break;
                        }
                        Err(TryRecvError::Empty) => {}
                    }

                    if let Some(goal) = current_goal {
                        let delta_x = clamp(goal.x - current_position.x, linear_speed);
                        current_position.x += delta_x;

                        let delta_y = clamp(goal.y - current_position.y, linear_speed);
                        current_position.y += delta_y;

                        let delta_yaw = if goal.has_yaw_target {
                            clamp(goal.yaw - current_position.yaw, angular_speed)
                        } else {
                            0.0
                        };
                        current_position.yaw += delta_yaw;

                        if let Some(stream) = &mut current_stream {
                            let arrived = f32::abs(delta_x) < 1e-3
                                && f32::abs(delta_y) < 1e-3
                                && f32::abs(delta_yaw) < 1e-6;

                            if stream.send(Ok(current_position)).is_err() {
                                // If the stream has been dropped then we assume the
                                // goal is cancelled
                                current_goal = None;
                            }

                            if arrived {
                                if !goal_streaming {
                                    // This is not a client-streaming request, so
                                    // we should close these down to indicate that
                                    // the goal has been reached.
                                    current_goal = None;
                                    current_stream = None;
                                }
                            }
                        } else {
                            current_goal = None;
                        }
                    }
                }
            });

            Self {
                goal_sender,
                stream_sender,
                runtime,
            }
        }
    }

    fn clamp(val: f32, limit: f32) -> f32 {
        if f32::abs(val) > limit {
            return f32::signum(val) * limit;
        }

        val
    }

    type NavigationUpdateSender = UnboundedSender<Result<NavigationUpdate, Status>>;
    type NavigationUpdateReceiver = UnboundedReceiverStream<Result<NavigationUpdate, Status>>;

    #[tonic::async_trait]
    impl Navigation for TestNavigation {
        type GuideStream = NavigationUpdateReceiver;

        async fn guide(
            &self,
            request: Request<Streaming<NavigationGoal>>,
        ) -> Result<Response<Self::GuideStream>, Status> {
            let _ = self.goal_sender.send(NavigationGoalInfo {
                goal: None,
                goal_streaming: true,
            });

            let (update_sender, update_receiver) = unbounded_channel();
            let connection = update_sender.downgrade();
            let _ = self.stream_sender.send(update_sender);

            let goal_sender = self.goal_sender.clone();
            let mut goal_stream = request.into_inner();
            self.runtime.spawn(async move {
                loop {
                    match goal_stream.message().await {
                        Ok(Some(goal)) => {
                            if connection.strong_count() < 1 {
                                // A new request has taken over
                                return;
                            }
                            let r = goal_sender.send(NavigationGoalInfo {
                                goal: Some(goal),
                                goal_streaming: true,
                            });

                            if r.is_err() {
                                return;
                            }
                        }
                        Ok(None) => {
                            // The request has been cancelled
                            return;
                        }
                        Err(err) => {
                            println!("error while receiving navigation goal: {err}");
                        }
                    }
                }
            });

            Ok(Response::new(update_receiver.into()))
        }

        type GoToPlaceStream = NavigationUpdateReceiver;

        async fn go_to_place(
            &self,
            request: Request<NavigationGoal>,
        ) -> Result<Response<Self::GoToPlaceStream>, Status> {
            let (update_sender, update_receiver) = unbounded_channel();
            let _ = self.stream_sender.send(update_sender);
            let _ = self.goal_sender.send(NavigationGoalInfo {
                goal: Some(request.into_inner()),
                goal_streaming: false,
            });

            Ok(Response::new(update_receiver.into()))
        }
    }

    mod protos {
        include!(concat!(env!("OUT_DIR"), "/example_protos.fibonacci.rs"));
        include!(concat!(env!("OUT_DIR"), "/example_protos.navigation.rs"));
    }
}
