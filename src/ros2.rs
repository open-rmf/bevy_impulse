/*
 * Copyright (C) 2024 Open Source Robotics Foundation
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
use rclrs::{Node as Ros2Node, IntoPrimitiveOptions, SubscriptionOptions, RclrsError, MessageIDL};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use futures_lite::future::race;

#[derive(StreamPack)]
pub struct SubscriptionStreams<T: 'static + Send + Sync, Cancel: 'static + Send + Sync> {
    pub out: T,
    pub canceller: UnboundedSender<Cancel>,
}

pub trait BuildRos2 {
    fn create_ros2_subscription<'o, T: MessageIDL, Cancel>(
        &mut self,
        executor: Ros2Node,
        options: impl Into<SubscriptionOptions<'o>>,
    ) -> Node<(), Result<Cancel, RclrsError>, SubscriptionStreams<T, Cancel>>
    where
        Cancel: 'static + Send + Sync;
}

impl<'w, 's, 'a> BuildRos2 for Builder<'w, 's, 'a> {
    fn create_ros2_subscription<'o, T: MessageIDL, Cancel>(
        &mut self,
        ros2_node: Ros2Node,
        options: impl Into<SubscriptionOptions<'o>>,
    ) -> Node<(), Result<Cancel, RclrsError>, SubscriptionStreams<T, Cancel>>
    where
        Cancel: 'static + Send + Sync
    {
        let SubscriptionOptions { topic, qos, .. } = options.into();
        let topic = topic.to_owned();
        self.create_map(move |input: AsyncMap<(), SubscriptionStreams<T, Cancel>>| {
            let topic = topic.clone();
            let qos = qos.clone();
            let ros2_node = ros2_node.clone();
            let (cancel_sender, mut cancel_receiver) = unbounded_channel();
            input.streams.canceller.send(cancel_sender);

            let subscribing = async move {
                let mut receiver = ros2_node.create_subscription_receiver::<T>(
                    (&topic).qos(qos)
                )?;

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
}
