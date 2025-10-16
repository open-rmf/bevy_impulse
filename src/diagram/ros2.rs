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
use crate::BuildRos2;
use rclrs::{
    ActionClientOptions, ActionIDL, GoalStatus, GoalStatusCode, MessageIDL, Node as Ros2Node,
    PrimitiveOptions, QosOptions, ServiceIDL,
};
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::UnboundedSender;

impl DiagramElementRegistry {
    /// Use this to register workflow node builders that will produce subscriptions,
    /// publishers, service clients, and action clients belong to a specific ROS 2
    /// node.
    ///
    /// This will return a [`Ros2Registry`] which you can use to register messages
    /// (for subscriptions and publishers), services, and actions.
    ///
    /// The node builders that get registered will be named
    /// `{node_name}_{type_name}_{subscription|publisher|service_client|action_client}`
    #[must_use = "no node builders will be created unless you register the specific messages, services, or actions that you need"]
    pub fn enable_ros2(&mut self, ros2_node: Ros2Node) -> Ros2Registry<'_> {
        Ros2Registry {
            ros2_node,
            registry: self,
        }
    }
}

/// Use this to register message, service, and action bindings so that the
/// registry can build workflow nodes for ROS 2 subscriptions, publishers,
/// service clients, and action clients.
pub struct Ros2Registry<'a> {
    ros2_node: Ros2Node,
    registry: &'a mut DiagramElementRegistry,
}

impl<'a> Ros2Registry<'a> {
    /// Register a message definition to obtain node builders for publishers and
    /// subscriptions for this message type.
    pub fn register_ros2_message<T: MessageIDL + Serialize + DeserializeOwned + JsonSchema>(
        &mut self,
    ) -> &'_ mut Self {
        let node_name_snake = self.ros2_node.name().replace("/", "_");
        let message_name_minimal = T::TYPE_NAME.split("/").last().unwrap_or("<unnamed>");
        let message_name_snake = T::TYPE_NAME.replace("/", "_");

        let ros2_node = self.ros2_node.clone();
        self.registry
            .register_node_builder(
                NodeBuilderOptions::new(format!(
                    "{node_name_snake}__{message_name_snake}__subscription"
                ))
                .with_default_display_text(format!("{message_name_minimal} Subscription")),
                move |builder, config: PrimitiveOptions| {
                    builder.create_ros2_subscription::<T, JsonMessage>(ros2_node.clone(), config)
                },
            )
            .with_result();

        // For cancel sender
        self.registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_message::<UnboundedSender<JsonMessage>>();

        let ros2_node = self.ros2_node.clone();
        self.registry
            .register_node_builder_fallible(
                NodeBuilderOptions::new(format!(
                    "{node_name_snake}__{message_name_snake}__publisher"
                ))
                .with_default_display_text(format!("{message_name_minimal} Publisher",)),
                move |builder, config: PrimitiveOptions| {
                    let node = builder.create_ros2_publisher::<T>(ros2_node.clone(), config)?;
                    Ok(node)
                },
            )
            .with_result();

        self
    }

    /// Register a service definition to obtain a node builder for a service
    /// client that can use this service definition.
    pub fn register_ros2_service<S: ServiceIDL>(&mut self) -> &'_ mut Self
    where
        S::Request: Serialize + DeserializeOwned + JsonSchema,
        S::Response: Serialize + DeserializeOwned + JsonSchema,
    {
        let node_name_snake = self.ros2_node.name().replace("/", "_");
        let service_name_minimal = S::TYPE_NAME.split("/").last().unwrap_or("<unnamed>");
        let service_name_snake = S::TYPE_NAME.replace("/", "_");

        let ros2_node = self.ros2_node.clone();
        self.registry
            .register_node_builder_fallible(
                NodeBuilderOptions::new(format!("{node_name_snake}__{service_name_snake}__client"))
                    .with_default_display_text(format!("{service_name_minimal} Client")),
                move |builder, config: PrimitiveOptions| {
                    let node = builder
                        .create_ros2_service_client::<S, JsonMessage>(ros2_node.clone(), config)?;
                    Ok(node)
                },
            )
            .with_result();

        self.registry
            .register_message::<Result<JsonMessage, String>>()
            .with_result();

        // For cancel sender
        self.registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_message::<UnboundedSender<JsonMessage>>();

        self
    }

    /// Register an action definition to obtain a node builder for an action
    /// client that can use this action definition.
    pub fn register_ros2_action<A: ActionIDL>(&mut self) -> &'_ mut Self
    where
        A::Goal: Serialize + DeserializeOwned + JsonSchema,
        A::Result: Serialize + DeserializeOwned + JsonSchema,
        A::Feedback: Serialize + DeserializeOwned + JsonSchema,
    {
        let node_name_snake = self.ros2_node.name().replace("/", "_");
        let action_name_minimal = A::TYPE_NAME.split("/").last().unwrap_or("<unnamed>");
        let action_name_snake = A::TYPE_NAME.replace("/", "_");

        let ros2_node = self.ros2_node.clone();
        self.registry
            .register_node_builder_fallible(
                NodeBuilderOptions::new(format!("{node_name_snake}__{action_name_snake}__client"))
                    .with_default_display_text(format!("{action_name_minimal} Client")),
                move |builder, config: ActionClientConfig| {
                    let node = builder
                        .create_ros2_action_client::<A, JsonMessage>(ros2_node.clone(), &config)?;
                    Ok(node)
                },
            )
            .with_result();

        self.registry
            .register_message::<Result<JsonMessage, String>>()
            .with_result();

        self.registry.register_message::<A::Feedback>();
        self.registry.register_message::<GoalStatus>();
        self.registry.register_message::<GoalStatusCode>();

        // For cancel sender
        self.registry
            .opt_out()
            .no_serializing()
            .no_deserializing()
            .register_message::<UnboundedSender<JsonMessage>>();

        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ActionClientConfig {
    /// The name of the action that this client will send requests to
    pub action: Arc<str>,
    /// The quality of service profile for the goal service
    pub goal_service_qos: QosOptions,
    /// The quality of service profile for the result service
    pub result_service_qos: QosOptions,
    /// The quality of service profile for the cancel service
    pub cancel_service_qos: QosOptions,
    /// The quality of service profile for the feedback topic
    pub feedback_topic_qos: QosOptions,
    /// The quality of service profile for the status topic
    pub status_topic_qos: QosOptions,
}

impl<'a> From<&'a ActionClientConfig> for ActionClientOptions<'a> {
    fn from(value: &'a ActionClientConfig) -> Self {
        let mut options = ActionClientOptions::new(&value.action);
        value
            .goal_service_qos
            .apply_to(&mut options.goal_service_qos);
        value
            .result_service_qos
            .apply_to(&mut options.result_service_qos);
        value
            .cancel_service_qos
            .apply_to(&mut options.cancel_service_qos);
        value
            .feedback_topic_qos
            .apply_to(&mut options.feedback_topic_qos);
        value
            .status_topic_qos
            .apply_to(&mut options.status_topic_qos);
        options
    }
}
