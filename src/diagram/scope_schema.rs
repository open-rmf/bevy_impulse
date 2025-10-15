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

use bevy_ecs::prelude::Entity;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    standard_input_connection, BuildDiagramOperation, BuildStatus, ConnectIntoTarget,
    DiagramContext, DiagramErrorCode, DynOutput, IncrementalScopeBuilder, IncrementalScopeRequest,
    IncrementalScopeResponse, InferMessageType, NamespaceList, NextOperation, OperationName,
    OperationRef, Operations, ScopeSettings, StreamOutRef,
};

/// Create a scope which will function like its own encapsulated workflow
/// within the paren workflow. Each message that enters a scope will trigger
/// a new independent session for that scope to begin running with the incoming
/// message itself being the input message of the scope. When multiple sessions
/// for the same scope are running, they cannot see or interfere with each other.
///
/// Once a session terminates, the scope will send the terminating message as
/// its output. Scopes can use the `stream_out` operation to stream messages out
/// to the parent workflow while running.
///
/// Scopes have two common uses:
/// * isolate - Prevent simultaneous runs of the same workflow components
///   (especially buffers) from interfering with each other.
/// * race - Run multiple branches simultaneously inside the scope and race
///   them against each ohter. The first branch that reaches the scope's
///   terminate operation "wins" the race, and only its output will continue
///   on in the parent workflow. All other branches will be disposed.
///
/// # Examples
/// ```
/// # crossflow::Diagram::from_json_str(r#"
/// {
///     "version": "0.1.0",
///     "start": "approach_door",
///     "ops": {
///         "approach_door": {
///             "type": "scope",
///             "start": "begin",
///             "ops": {
///                 "begin": {
///                     "type": "fork_clone",
///                     "next": [
///                         "move_to_door",
///                         "detect_door_proximity"
///                     ]
///                 },
///                 "move_to_door": {
///                     "type": "node",
///                     "builder": "move",
///                     "config": {
///                         "place": "L1_north_lobby_outside"
///                     },
///                     "next": { "builtin" : "terminate" }
///                 },
///                 "detect_proximity": {
///                     "type": "node",
///                     "builder": "detect_proximity",
///                     "config": {
///                         "type": "door",
///                         "name": "L1_north_lobby"
///                     },
///                     "next": { "builtin" : "terminate" }
///                 }
///             },
///             "next": { "builtin" : "try_open_door" }
///         }
///     }
/// }
/// # "#)?;
/// # Ok::<_, serde_json::Error>(())
/// ```
//
// TODO(@mxgrey): Add an example of streaming out of a scope
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ScopeSchema {
    /// Indicates which node inside the scope should receive the input into the
    /// scope.
    pub start: NextOperation,

    /// To simplify diagram definitions, the diagram workflow builder will
    /// sometimes insert implicit operations into the workflow, such as implicit
    /// serializing and deserializing. These implicit operations may be fallible.
    ///
    /// This field indicates how a failed implicit operation should be handled.
    /// If left unspecified, an implicit error will cause the entire workflow to
    /// be cancelled.
    #[serde(default)]
    pub on_implicit_error: Option<NextOperation>,

    /// Operations that exist inside this scope.
    pub ops: Operations,

    /// Where to connect streams that are coming out of this scope.
    #[serde(default)]
    pub stream_out: HashMap<OperationName, NextOperation>,

    /// Where to connect the output of this scope.
    pub next: NextOperation,

    /// Settings specific to the scope, e.g. whether it is interruptible.
    #[serde(default)]
    pub settings: ScopeSettings,
}

impl BuildDiagramOperation for ScopeSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let scope = IncrementalScopeBuilder::begin(self.settings.clone(), ctx.builder);

        for (stream_in_id, stream_out_target) in &self.stream_out {
            ctx.set_connect_into_target(
                StreamOutRef::new_for_scope(id.clone(), stream_in_id.clone()),
                ConnectScopeStream {
                    scope_id: scope.builder_scope_context().scope,
                    parent_scope_id: ctx.builder.scope(),
                    stream_out_target: ctx.into_operation_ref(stream_out_target),
                    connection: None,
                },
            )?;
        }

        for (child_id, op) in self.ops.iter() {
            ctx.add_child_operation(
                id,
                child_id,
                op,
                self.ops.clone(),
                Some(scope.builder_scope_context()),
            );
        }

        ctx.set_connect_into_target(
            id,
            ConnectScopeRequest {
                scope: scope.clone(),
                start: ctx.into_child_operation_ref(id, &self.start),
                connection: None,
            },
        )?;

        ctx.set_connect_into_target(
            OperationRef::Terminate(NamespaceList::for_child_of(Arc::clone(id))),
            ConnectScopeResponse {
                scope,
                next: ctx.into_operation_ref(&self.next),
                connection: None,
            },
        )?;

        Ok(BuildStatus::Finished)
    }
}

struct ConnectScopeRequest {
    scope: IncrementalScopeBuilder,
    start: OperationRef,
    connection: Option<Box<dyn ConnectIntoTarget + 'static>>,
}

impl ConnectIntoTarget for ConnectScopeRequest {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        if let Some(connection) = &mut self.connection {
            return connection.connect_into_target(output, ctx);
        } else {
            let IncrementalScopeRequest {
                external_input,
                begin_scope,
            } = ctx.registry.messages.set_scope_request(
                output.message_info(),
                &mut self.scope,
                ctx.builder.commands(),
            )?;

            if let Some(begin_scope) = begin_scope {
                ctx.add_output_into_target(self.start.clone(), begin_scope);
            }

            let mut connection = standard_input_connection(external_input, &ctx.registry)?;
            connection.connect_into_target(output, ctx)?;
            self.connection = Some(connection);
        }

        Ok(())
    }

    fn infer_input_type(
        &self,
        ctx: &DiagramContext,
        visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        if let Some(connection) = &self.connection {
            connection.infer_input_type(ctx, visited)
        } else {
            ctx.redirect_infer_input_type(&self.start, visited)
        }
    }

    fn is_finished(&self) -> Result<(), DiagramErrorCode> {
        self.scope.is_finished().map_err(Into::into)
    }
}

struct ConnectScopeResponse {
    scope: IncrementalScopeBuilder,
    next: OperationRef,
    connection: Option<Box<dyn ConnectIntoTarget + 'static>>,
}

impl ConnectIntoTarget for ConnectScopeResponse {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        if let Some(connection) = &mut self.connection {
            return connection.connect_into_target(output, ctx);
        } else {
            let IncrementalScopeResponse {
                terminate,
                external_output,
            } = ctx.registry.messages.set_scope_response(
                output.message_info(),
                &mut self.scope,
                ctx.builder.commands(),
            )?;

            if let Some(external_output) = external_output {
                ctx.add_output_into_target(self.next.clone(), external_output);
            }

            let mut connection = standard_input_connection(terminate, ctx.registry)?;
            connection.connect_into_target(output, ctx)?;
            self.connection = Some(connection);
        }

        Ok(())
    }

    fn infer_input_type(
        &self,
        ctx: &DiagramContext,
        visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        if let Some(connection) = &self.connection {
            connection.infer_input_type(ctx, visited)
        } else {
            ctx.redirect_infer_input_type(&self.next, visited)
        }
    }

    // We do not implement is_finished because just having it for
    // ConnectScopeRequest is sufficient, since it will report that the scope
    // isn't finished whether the request or the response didn't get a connection.
}

struct ConnectScopeStream {
    scope_id: Entity,
    parent_scope_id: Entity,
    stream_out_target: OperationRef,
    connection: Option<Box<dyn ConnectIntoTarget + 'static>>,
}

impl ConnectIntoTarget for ConnectScopeStream {
    fn connect_into_target(
        &mut self,
        output: DynOutput,
        ctx: &mut DiagramContext,
    ) -> Result<(), DiagramErrorCode> {
        if let Some(connection) = &mut self.connection {
            return connection.connect_into_target(output, ctx);
        } else {
            let (stream_input, stream_output) = ctx.registry.messages.spawn_basic_scope_stream(
                output.message_info(),
                self.scope_id,
                self.parent_scope_id,
                ctx.builder.commands(),
            )?;

            ctx.add_output_into_target(self.stream_out_target.clone(), stream_output);
            let mut connection = standard_input_connection(stream_input, ctx.registry)?;
            connection.connect_into_target(output, ctx)?;
            self.connection = Some(connection);
        }

        Ok(())
    }

    fn infer_input_type(
        &self,
        ctx: &DiagramContext,
        visited: &mut HashSet<OperationRef>,
    ) -> Result<Option<Arc<dyn InferMessageType>>, DiagramErrorCode> {
        if let Some(connection) = &self.connection {
            connection.infer_input_type(ctx, visited)
        } else {
            ctx.redirect_infer_input_type(&self.stream_out_target, visited)
        }
    }

    // We do not implement is_finished because there is no negative impact on
    // the soundness of the workflow if the user hasn't connected any output to
    // a stream, as long as the rest of the workflow can build. The worst case
    // scenario is an operation downstream of the stream output won't be able to
    // infer its message type, but that will show up as a diagram error elsewhere.
}

#[cfg(test)]
mod tests {
    use crate::{
        diagram::{testing::*, *},
        prelude::*,
        stream::tests::*,
        testing::*,
    };
    use serde_json::json;

    #[test]
    fn test_simple_diagram_scope() {
        let mut fixture = DiagramTestFixture::new();
        fixture.context.set_flush_loop_limit(Some(10));

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "scope",
            "ops": {
                "scope": {
                    "type": "scope",
                    "start": "multiply",
                    "ops": {
                        "multiply": {
                            "type": "node",
                            "builder": "multiply3",
                            "next": { "builtin" : "terminate" },
                        }
                    },
                    "next": { "builtin" : "terminate" },
                }
            }
        }))
        .unwrap();

        let result: i64 = fixture.spawn_and_run(&diagram, 4_i64).unwrap();
        assert_eq!(result, 12);
    }

    #[derive(Serialize, Deserialize, JsonSchema)]
    #[serde(rename_all = "snake_case")]
    enum DelayDuration {
        Short,
        Long,
    }

    #[test]
    fn test_diagram_scope_race() {
        let mut fixture = DiagramTestFixture::new();

        let short_delay = fixture
            .context
            .spawn_delay::<()>(Duration::from_secs_f32(0.01));
        let long_delay = fixture
            .context
            .spawn_delay::<()>(Duration::from_secs_f64(10.0));

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("delay"),
            move |builder, config: DelayDuration| {
                let provider = match config {
                    DelayDuration::Short => short_delay,
                    DelayDuration::Long => long_delay,
                };

                builder.create_node(provider)
            },
        );

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("text"),
            move |builder, config: String| {
                builder.create_map_block(move |_: JsonMessage| config.clone())
            },
        );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "scope",
            "ops": {
                "scope": {
                    "type": "scope",
                    "start": "fork",
                    "ops": {
                        "fork": {
                            "type": "fork_clone",
                            "next": ["long_delay", "short_delay"]
                        },
                        "long_delay": {
                            "type": "node",
                            "builder": "delay",
                            "config": "long",
                            "next": "print_slow"
                        },
                        "print_slow": {
                            "type": "node",
                            "builder": "text",
                            "config": "slow",
                            "next": { "builtin" : "terminate" }
                        },
                        "short_delay": {
                            "type": "node",
                            "builder": "delay",
                            "config": "short",
                            "next": "print_fast"
                        },
                        "print_fast": {
                            "type": "node",
                            "builder": "text",
                            "config": "fast",
                            "next": { "builtin" : "terminate" }
                        }
                    },
                    "next": { "builtin" : "terminate" }
                }
            }
        }))
        .unwrap();

        let result: String = fixture.spawn_and_run(&diagram, ()).unwrap();
        assert_eq!(result, "fast");
    }

    #[test]
    fn test_streams_in_diagram_scope() {
        let mut fixture = DiagramTestFixture::new();

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("streaming_node"),
            |builder, _config: ()| {
                builder.create_map(|input: BlockingMap<Vec<String>, TestStreamPack>| {
                    for r in input.request {
                        if let Ok(value) = r.parse::<u32>() {
                            input.streams.stream_u32.send(value);
                        }

                        if let Ok(value) = r.parse::<i32>() {
                            input.streams.stream_i32.send(value);
                        }

                        input.streams.stream_string.send(r);
                    }
                })
            },
        );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "scope",
            "ops": {
                "scope": {
                    "type": "scope",
                    "start": "test",
                    "ops": {
                        "test": {
                            "type": "node",
                            "builder": "streaming_node",
                            "next": { "builtin": "terminate" },
                            "stream_out": {
                                "stream_u32": "stream_u32_out",
                                "stream_i32": "stream_i32_out",
                                "stream_string": "stream_string_out"
                            }
                        },
                        "stream_u32_out": {
                            "type": "stream_out",
                            "name": "stream_u32"
                        },
                        "stream_i32_out": {
                            "type": "stream_out",
                            "name": "stream_i32"
                        },
                        "stream_string_out": {
                            "type": "stream_out",
                            "name": "stream_string"
                        }
                    },
                    "stream_out": {
                        "stream_u32": "stream_u32_out",
                        "stream_i32": "stream_i32_out",
                        "stream_string": "stream_string_out"
                    },
                    "next": { "builtin": "terminate" }
                },
                "stream_u32_out": {
                    "type": "stream_out",
                    "name": "stream_u32"
                },
                "stream_i32_out": {
                    "type": "stream_out",
                    "name": "stream_i32"
                },
                "stream_string_out": {
                    "type": "stream_out",
                    "name": "stream_string"
                }
            }
        }))
        .unwrap();

        let request = vec![
            "5".to_owned(),
            "10".to_owned(),
            "-3".to_owned(),
            "-27".to_owned(),
            "hello".to_owned(),
        ];

        let (_, receivers) = fixture
            .spawn_and_run_with_streams::<_, (), TestStreamPack>(
                &diagram,
                request,
                FlushConditions::default(),
            )
            .unwrap();

        let outcome_stream_u32 = collect_received_values(receivers.stream_u32);
        let outcome_stream_i32 = collect_received_values(receivers.stream_i32);
        let outcome_stream_string = collect_received_values(receivers.stream_string);

        assert_eq!(outcome_stream_u32, [5, 10]);
        assert_eq!(outcome_stream_i32, [5, 10, -3, -27]);
        assert_eq!(outcome_stream_string, ["5", "10", "-3", "-27", "hello"]);
    }

    // TODO(@mxgrey): Add an interruptibility test
}
