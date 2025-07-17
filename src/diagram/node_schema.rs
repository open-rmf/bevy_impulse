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

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

use super::{
    is_default, BuildDiagramOperation, BuildStatus, BuilderId, ConstructionInfo, DiagramContext,
    DiagramErrorCode, JsonMessage, MissingStream, NextOperation, OperationName, TraceInfo,
    TraceSettings,
};

/// Create an operation that that takes an input message and produces an
/// output message.
///
/// The behavior is determined by the choice of node `builder` and
/// optioanlly the `config` that you provide. Each type of node builder has
/// its own schema for the config.
///
/// The output message will be sent to the operation specified by `next`.
///
/// TODO(@mxgrey): [Support stream outputs](https://github.com/open-rmf/bevy_impulse/issues/43)
///
/// # Examples
/// ```
/// # bevy_impulse::Diagram::from_json_str(r#"
/// {
///     "version": "0.1.0",
///     "start": "cutting_board",
///     "ops": {
///         "cutting_board": {
///             "type": "node",
///             "builder": "chop",
///             "config": "diced",
///             "next": "bowl"
///         },
///         "bowl": {
///             "type": "node",
///             "builder": "stir",
///             "next": "oven"
///         },
///         "oven": {
///             "type": "node",
///             "builder": "bake",
///             "config": {
///                 "temperature": 200,
///                 "duration": 120
///             },
///             "next": { "builtin": "terminate" }
///         }
///     }
/// }
/// # "#)?;
/// # Ok::<_, serde_json::Error>(())
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NodeSchema {
    pub builder: BuilderId,
    #[serde(default, skip_serializing_if = "is_default")]
    pub config: Arc<JsonMessage>,
    pub next: NextOperation,
    #[serde(default, skip_serializing_if = "is_default")]
    pub stream_out: HashMap<OperationName, NextOperation>,
    #[serde(flatten)]
    pub trace_settings: TraceSettings,
}

impl BuildDiagramOperation for NodeSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let node_registration = ctx.registry.get_node_registration(&self.builder)?;
        let mut node = node_registration.create_node(ctx.builder, (*self.config).clone())?;

        let display_text = self
            .trace_settings
            .display_text
            .as_ref()
            .unwrap_or(&node_registration.default_display_text);

        let trace = TraceInfo::new(
            ConstructionInfo::for_node(&self.builder, &self.config, display_text),
            self.trace_settings.trace,
        );

        ctx.set_input_for_target(id, node.input.into(), trace)?;
        ctx.add_output_into_target(&self.next, node.output);

        let available_names = node
            .streams
            .available_names()
            .map(|n| n.clone().into())
            .collect();

        for (name, target) in &self.stream_out {
            let Some(output) = node.streams.take_named(&name) else {
                return Err(DiagramErrorCode::MissingStream(MissingStream {
                    missing_name: Arc::clone(name),
                    available_names,
                }));
            };

            ctx.add_output_into_target(target, output);
        }

        Ok(BuildStatus::Finished)
    }
}

#[cfg(test)]
mod tests {
    // TODO(@mxgrey): Stop depending on stream::tests::* when we have the proc
    // macro finished.
    use crate::{
        diagram::{testing::*, *},
        prelude::*,
        stream::tests::*,
    };
    use serde_json::json;

    #[test]
    fn test_streams_in_diagram() {
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
            .spawn_and_run_with_streams::<_, (), TestStreamPack>(&diagram, request)
            .unwrap();

        let outcome_stream_u32 = collect_received_values(receivers.stream_u32);
        let outcome_stream_i32 = collect_received_values(receivers.stream_i32);
        let outcome_stream_string = collect_received_values(receivers.stream_string);

        assert_eq!(outcome_stream_u32, [5, 10]);
        assert_eq!(outcome_stream_i32, [5, 10, -3, -27]);
        assert_eq!(outcome_stream_string, ["5", "10", "-3", "-27", "hello"]);
    }
}
