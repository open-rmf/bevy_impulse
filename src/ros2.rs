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

use crate::{Builder, Node, StreamPack, AsyncMap, NeverFinish};
use rclrs::{
    Node as Ros2Node, IntoPrimitiveOptions, SubscriptionOptions, PublisherOptions,
    GoalEvent, GoalStatus, GoalStatusCode, CancelResponse,
    RclrsError, MessageIDL, ClientOptions, ActionClientOptions, ServiceIDL, ActionIDL,
};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use futures_lite::future::race;
use futures::{StreamExt, future::{select, Either}};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use std::{
    pin::pin,
    sync::Arc,
};
use schemars::JsonSchema;

#[derive(StreamPack)]
pub struct SubscriptionStreams<T: 'static + Send + Sync, CancelSignal: 'static + Send + Sync> {
    pub out: T,
    pub canceller: UnboundedSender<CancelSignal>,
}

#[derive(StreamPack)]
pub struct ServiceClientStreams<CancelSignal: 'static + Send + Sync> {
    pub canceller: UnboundedSender<CancelSignal>,
}

pub type ServiceClientNode<S, CancelSignal> = Node<
    <S as ServiceIDL>::Request,
    Result<<S as ServiceIDL>::Response, Result<CancelSignal, String>>,
    ServiceClientStreams<CancelSignal>,
>;

#[derive(StreamPack)]
pub struct ActionClientStreams<A: ActionIDL, CancelSignal: 'static + Send + Sync> {
    pub feedback: A::Feedback,
    pub canceller: UnboundedSender<CancelSignal>,
    pub status: GoalStatus,
    pub cancellation_response: ActionCancellation<CancelSignal>,
}

pub type ActionClientNode<A, CancelSignal> = Node<
    <A as ActionIDL>::Goal,
    Result<ActionResult<<A as ActionIDL>::Result>, Result<CancelSignal, String>>,
    ActionClientStreams<A, CancelSignal>,
>;

/// This is the final result returned by a ROS 2 action if it ended under normal
/// circumstances.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ActionResult<R> {
    pub status: GoalStatusCode,
    pub result: R,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ActionCancellation<CancelSignal> {
    pub signal: CancelSignal,
    pub response: CancelResponse,
}

pub trait BuildRos2 {
    fn create_ros2_subscription<'o, T: MessageIDL, CancelSignal>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<SubscriptionOptions<'o>>,
    ) -> Node<(), Result<CancelSignal, String>, SubscriptionStreams<T, CancelSignal>>
    where
        CancelSignal: 'static + Send + Sync;

    fn create_ros2_publisher<'o, T: MessageIDL>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<PublisherOptions<'o>>,
    ) -> Result<Node<T, Result<(), String>>, RclrsError>;

    fn create_ros2_service_client<'o, S: ServiceIDL, CancelSignal>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<ClientOptions<'o>>,
    ) -> Result<ServiceClientNode<S, CancelSignal>, RclrsError>
    where
        CancelSignal: 'static + Send + Sync;

    fn create_ros2_action_client<'o, A: ActionIDL, CancelSignal>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<ActionClientOptions<'o>>,
    ) -> Result<ActionClientNode<A, CancelSignal>, RclrsError>
    where
        CancelSignal: 'static + Send + Sync,
        A::Result: Serialize + DeserializeOwned + JsonSchema;
}

impl<'w, 's, 'a> BuildRos2 for Builder<'w, 's, 'a> {
    fn create_ros2_subscription<'o, T: MessageIDL, CancelSignal>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<SubscriptionOptions<'o>>,
    ) -> Node<(), Result<CancelSignal, String>, SubscriptionStreams<T, CancelSignal>>
    where
        CancelSignal: 'static + Send + Sync
    {
        let SubscriptionOptions { topic, qos, .. } = options.into();
        let topic: Arc<str> = topic.into();
        self.create_map(move |input: AsyncMap<(), SubscriptionStreams<T, CancelSignal>>| {
            let topic = topic.clone();
            let qos = qos.clone();
            let ros2_node = ros2_node.clone();
            let (cancel_sender, mut cancel_receiver) = unbounded_channel();
            input.streams.canceller.send(cancel_sender);

            let subscribing = async move {
                let mut receiver = ros2_node.create_subscription_receiver::<T>(
                    (&topic).qos(qos)
                )
                .map_err(|err| err.to_string())?;

                while let Some(msg) = receiver.recv().await {
                    input.streams.out.send(msg);
                }

                unreachable!(
                    "The channel of a SubscriptionReceiver can never close \
                    because it keeps ownership of both the sender and receiver."
                );
            };

            let cancellation = async move {
                let Some(msg) = cancel_receiver.recv().await else {
                    // The canceller was dropped, meaning the user will never
                    // cancel this node, so it should just keep running forever
                    // (it will be forcibly stopped during the workflow cleanup)
                    NeverFinish.await;
                    unreachable!("this future will never finish");
                };

                Ok(msg)
            };

            race(subscribing, cancellation)
        })
    }

    fn create_ros2_publisher<'o, T: MessageIDL>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<PublisherOptions<'o>>,
    ) -> Result<Node<T, Result<(), String>>, RclrsError> {
        let publisher = ros2_node.create_publisher(options)?;
        let node = self.create_map_block(move |message: T| {
            publisher
                .publish(message)
                .map_err(|err| err.to_string())
        });
        Ok(node)
    }

    fn create_ros2_service_client<'o, S: ServiceIDL, CancelSignal>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<ClientOptions<'o>>,
    ) -> Result<ServiceClientNode<S, CancelSignal>, RclrsError>
    where
        CancelSignal: 'static + Send + Sync,
    {
        let client = ros2_node.create_client::<S>(options)?;
        let node = self.create_map(move |input: AsyncMap<S::Request, ServiceClientStreams<CancelSignal>>| {
            let AsyncMap { request, streams, .. } = input;
            let client = Arc::clone(&client);
            let (cancel_sender, mut cancel_receiver) = unbounded_channel();
            streams.canceller.send(cancel_sender);

            let receive = async move {
                client.notify_on_service_ready()
                    .await
                    .map_err(|_| Err(String::from("failed to wait for action server")))?;

                let receiver = client.call(request);
                let response: Result<S::Response, _> = match receiver {
                    Ok(receiver) => receiver.await,
                    Err(err) => return Err(Err(err.to_string())),
                };

                let Ok(response) = response else {
                    return Err(Err(String::from("response was cancelled by the executor")));
                };

                Ok(response)
            };

            let cancellation = async move {
                let Some(msg) = cancel_receiver.recv().await else {
                    // The canceller was dropped, meaning the user will never
                    // cancel this node, so it should just keep running forever
                    // (it will be forcibly stopped during the workflow cleanup)
                    NeverFinish.await;
                    unreachable!("this future will never finish");
                };

                Err(Ok(msg))
            };

            race(receive, cancellation)
        });

        Ok(node)
    }

    fn create_ros2_action_client<'o, A: ActionIDL, CancelSignal>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<ActionClientOptions<'o>>,
    ) -> Result<ActionClientNode<A, CancelSignal>, RclrsError>
    where
        CancelSignal: 'static + Send + Sync,
        A::Result: Serialize + DeserializeOwned + JsonSchema,
    {
        let client = ros2_node.create_action_client::<A>(options)?;
        let node = self.create_map(move |input: AsyncMap<A::Goal, ActionClientStreams<A, CancelSignal>>| {
            let AsyncMap { request, streams, .. } = input;
            let client = Arc::clone(&client);
            let (cancel_sender, mut cancel_receiver) = unbounded_channel();
            streams.canceller.send(cancel_sender);

            async move {
                client.notify_on_action_ready()
                    .await
                    .map_err(|_| Err(String::from("failed to wait for action server")))?;

                let goal_requested = client.request_goal(request);
                let Some(goal_client) = goal_requested.await else {
                    return Err(Err(String::from("goal was rejected")));
                };

                let canceller = goal_client.cancellation.clone();
                let mut goal_client_stream = goal_client.stream();

                let receiving = async move {
                    while let Some(event) = goal_client_stream.next().await {
                        match event {
                            GoalEvent::Feedback(feedback) => {
                                streams.feedback.send(feedback);
                            }
                            GoalEvent::Status(status) => {
                                streams.status.send(status);
                            }
                            GoalEvent::Result((status, result)) => {
                                return Ok(ActionResult { status, result });
                            }
                        }
                    }

                    Err(Err(String::from("goal stream closed without receiving result")))
                };

                let cancellation = async move {
                    loop {
                        let Some(signal) = cancel_receiver.recv().await else {
                            // The canceller was dropped, meaning the user will never
                            // cancel this node, so it should just keep running forever
                            // (it will be forcibly stopped during the node cleanup)
                            NeverFinish.await;
                            unreachable!("this future will never finish");
                        };

                        let response = canceller.cancel().await;
                        streams.cancellation_response.send(ActionCancellation {
                            signal,
                            response,
                        });
                    }
                };

                match select(pin!(receiving), pin!(cancellation)).await {
                    Either::Left((received, _)) => {
                        return received;
                    }
                    Either::Right(_) => {
                        unreachable!("the cancellation future will never finish");
                    }
                }
            }
        });

        Ok(node)
    }
}
