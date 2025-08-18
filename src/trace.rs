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

use crate::{JsonMessage, OperationRef, TraceToggle, TypeInfo};

use bevy_ecs::prelude::{Component, Entity, Event};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{any::Any, borrow::Cow, sync::Arc};
use thiserror::Error as ThisError;

/// A component attached to workflow operation entities in order to trace their
/// activities.
#[derive(Component)]
pub struct Trace {
    toggle: TraceToggle,
    info: Arc<OperationInfo>,
    serialize_value: Option<fn(&dyn Any) -> Result<JsonMessage, GetValueError>>,
}

impl Trace {
    /// Create trace information for an entity. By default this will not serialize
    /// the messages passing through.
    pub fn new(toggle: TraceToggle, info: Arc<OperationInfo>) -> Self {
        Trace {
            toggle,
            info,
            serialize_value: None,
        }
    }

    /// Enable the trace for this operation to also send out the data of the
    /// messages that are passing through.
    pub fn enable_value_serialization<T: Any + Serialize>(&mut self) {
        self.serialize_value = Some(get_serialize_value::<T>);
    }

    pub fn set_toggle(&mut self, toggle: TraceToggle) {
        self.toggle = toggle;
    }

    pub fn toggle(&self) -> TraceToggle {
        self.toggle
    }

    /// Get the information for this workflow operation.
    pub fn info(&self) -> &Arc<OperationInfo> {
        &self.info
    }

    /// Attempt to serialize the value. This will return a None if the trace is
    /// not set up to serialize the values.
    pub fn serialize_value(&self, value: &dyn Any) -> Option<Result<JsonMessage, GetValueError>> {
        self.serialize_value.map(|f| f(value))
    }
}

fn get_serialize_value<T: Any + Serialize>(value: &dyn Any) -> Result<JsonMessage, GetValueError> {
    let Some(value_ref) = value.downcast_ref::<T>() else {
        return Err(GetValueError::FailedDowncast {
            expected: TypeInfo::of::<T>(),
            received: std::any::TypeId::of::<T>(),
        });
    };

    serde_json::to_value(value_ref).map_err(GetValueError::FailedSerialization)
}

#[derive(ThisError, Debug)]
pub enum GetValueError {
    #[error("The downcast was incompatible. Expected {expected:?}, received {received:?}")]
    FailedDowncast {
        expected: TypeInfo,
        received: std::any::TypeId,
    },
    #[error("The serialization into json failed: {0}")]
    FailedSerialization(serde_json::Error),
}

/// An event that gets transmitted whenever an operation receives an input or is
/// activated some other way while tracing is on (meaning the operation entity
/// has a [`Trace`] component).
#[derive(Event, Clone, Debug)]
pub struct OperationStarted {
    /// The entity of the operation that was triggered.
    pub operation: Entity,
    /// The stack of session IDs that triggered the operation. The first entry
    /// is the root session. Each subsequent entry is a child session of the
    /// previous. There are two common ways to get a child session:
    /// * In an impulse chain, earlier sessions in the chain are children of later
    ///   sessions in the chain, so the last session of the chain is the root of
    ///   the entire chain.
    /// * When a scope operation is triggered, a new session is created. Its parent
    ///   is the session of the message that triggered the scope operation. Every
    ///   time a workflow is triggered it creates a new scope, and therefore also
    ///   creates a child session.
    pub session_stack: SmallVec<[Entity; 8]>,
    /// Information about the operation that was triggered.
    pub info: Arc<OperationInfo>,
    /// The message that triggered the operation, if serialization is enabled
    /// for it.
    pub message: Option<JsonMessage>,
}

/// Information about an operation.
#[derive(Serialize, Deserialize, JsonSchema, Debug)]
pub struct OperationInfo {
    /// The identifier of this operation which is unique within the workflow
    id: Option<OperationRef>,
    /// The input message type
    message_type: Option<Cow<'static, str>>,
    /// Information about how the operation was constructed. For operations
    /// built by the diagram workflow builder, this will be the contents of the
    /// operation definition.
    ///
    /// For manually inserted traces, the content of this is user-defined.
    construction: Option<Arc<JsonMessage>>,
}

impl OperationInfo {
    pub fn new(
        id: Option<OperationRef>,
        message_type: Option<Cow<'static, str>>,
        construction: Option<Arc<JsonMessage>>,
    ) -> Self {
        Self {
            id,
            message_type,
            construction,
        }
    }

    /// The unique identifier for this operation within the workflow.
    pub fn id(&self) -> &Option<OperationRef> {
        &self.id
    }

    /// Get the message type that this operation uses, if one is available.
    pub fn message_type(&self) -> &Option<Cow<'static, str>> {
        &self.message_type
    }

    /// If this operation was created by a builder, this is the ID of that
    /// builder
    pub fn construction(&self) -> &Option<Arc<JsonMessage>> {
        &self.construction
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        diagram::{testing::*, *},
        prelude::*,
        trace::OperationStarted,
    };
    use bevy_app::{App, PostUpdate};
    use bevy_ecs::prelude::{Entity, EventReader, ResMut, Resource};
    use serde_json::json;
    use std::{sync::Arc, time::Duration};

    #[derive(Clone, Resource, Default, Debug)]
    struct TraceRecorder {
        record: Vec<OperationStarted>,
    }

    fn record_traces(
        mut trace_reader: EventReader<OperationStarted>,
        mut recorder: ResMut<TraceRecorder>,
    ) {
        for trace in trace_reader.read() {
            recorder.record.push(trace.clone());
        }
    }

    fn enable_trace_recording(app: &mut App) {
        app.init_resource::<TraceRecorder>()
            .add_systems(PostUpdate, record_traces);
    }

    #[test]
    fn test_tracing_pachinko() {
        let mut fixture = DiagramTestFixture::new();
        enable_trace_recording(&mut fixture.context.app);

        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("less_than"),
                |builder, config: i64| {
                    builder.create_map_block(move |value: i64| {
                        if value < config {
                            Ok(value)
                        } else {
                            Err(value)
                        }
                    })
                },
            )
            .with_fork_result();

        fixture
            .registry
            .register_node_builder(NodeBuilderOptions::new("noop"), |builder, _config: ()| {
                builder.create_map_block(|value: i64| value)
            });

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "default_trace": "messages",
            "start": "less_than_60",
            "ops": {
                "less_than_60": {
                    "type": "node",
                    "builder": "less_than",
                    "config": 60,
                    "next": "fork_60",
                    "display_text": "Evaluate 60",
                },
                "fork_60": {
                    "type": "fork_result",
                    "ok": "less_than_30",
                    "err": "less_than_90",
                    "display_text": "Fork 60",
                },
                "less_than_30": {
                    "type": "node",
                    "builder": "less_than",
                    "config": 30,
                    "next": "fork_30",
                    "display_text": "Evaluate 30",
                },
                "fork_30": {
                    "type": "fork_result",
                    "ok": "towards_15",
                    "err": "towards_45",
                    "display_text": "Fork 30",
                },
                "towards_15": {
                    "type": "node",
                    "builder": "noop",
                    "next": { "builtin": "terminate" },
                    "display_text": "Towards 15",
                },
                "towards_45": {
                    "type": "node",
                    "builder": "noop",
                    "next": { "builtin": "terminate" },
                    "display_text": "Towards 45",
                },
                "less_than_90": {
                    "type": "node",
                    "builder": "less_than",
                    "config": 90,
                    "next": "fork_90",
                    "display_text": "Evaluate 90",
                },
                "fork_90": {
                    "type": "fork_result",
                    "ok": "towards_75",
                    "err": "towards_105",
                    "display_text": "Fork 90",
                },
                "towards_75": {
                    "type": "node",
                    "builder": "noop",
                    "next": { "builtin": "terminate" },
                    "display_text": "Towards 75",
                },
                "towards_105": {
                    "type": "node",
                    "builder": "noop",
                    "next": { "builtin": "terminate" },
                    "display_text": "Towards 105",
                },
            }
        }))
        .unwrap();

        let panchinko = fixture.spawn_io_workflow::<i64, i64>(&diagram).unwrap();

        confirm_panchinko_route(
            10,
            panchinko,
            &mut fixture,
            &[
                "less_than_60",
                "fork_60",
                "less_than_30",
                "fork_30",
                "towards_15",
            ],
        );

        confirm_panchinko_route(
            70,
            panchinko,
            &mut fixture,
            &[
                "less_than_60",
                "fork_60",
                "less_than_90",
                "fork_90",
                "towards_75",
            ],
        );

        confirm_panchinko_route(
            50,
            panchinko,
            &mut fixture,
            &[
                "less_than_60",
                "fork_60",
                "less_than_30",
                "fork_30",
                "towards_45",
            ],
        );
    }

    fn confirm_panchinko_route(
        value: i64,
        panchinko: Service<i64, i64>,
        fixture: &mut DiagramTestFixture,
        route: &[&str],
    ) {
        let Recipient {
            response: mut promise,
            session,
            ..
        } = fixture
            .context
            .command(|commands| commands.request(value, panchinko).take());

        fixture
            .context
            .run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(fixture.context.no_unhandled_errors());
        let result = promise.take().available().unwrap();
        assert_eq!(value, result);

        let recorder = fixture
            .context
            .app
            .world
            .resource_mut::<TraceRecorder>()
            .clone();
        confirm_trace(&recorder, route, session);

        // Clear the record so these results do not interfere with the next test
        fixture
            .context
            .app
            .world
            .resource_mut::<TraceRecorder>()
            .record
            .clear();
    }

    fn confirm_trace(
        recorder: &TraceRecorder,
        expectation: &[&str],
        expected_root_session: Entity,
    ) {
        let mut actual = recorder.record.clone();
        for next_op_name in expectation {
            let name: Arc<str> = (*next_op_name).into();
            let expected_op = OperationRef::Named((&name).into());
            let next_actual = actual.first().unwrap();
            let actual_op = next_actual.info.id.as_ref().unwrap();
            assert_eq!(expected_op, *actual_op);

            let actual_root_session = *next_actual.session_stack.first().unwrap();
            assert_eq!(expected_root_session, actual_root_session);

            actual.remove(0);
        }

        let expected_op = OperationRef::Terminate(NamespaceList::default());
        let next_actual = actual.first().unwrap();
        let actual_op = next_actual.info.id.as_ref().unwrap();
        assert_eq!(expected_op, *actual_op);

        let actual_root_session = *next_actual.session_stack.first().unwrap();
        assert_eq!(expected_root_session, actual_root_session);

        actual.remove(0);
        assert!(actual.is_empty());
    }
}
