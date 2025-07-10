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

use std::{collections::HashMap, usize};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Builder, ForRemaining, FromSequential, FromSpecific, ListSplitKey, MapSplitKey,
    OperationResult, SplitDispatcher, Splittable,
};

use super::{
    supported::*, BuildDiagramOperation, BuildStatus, DiagramContext, DiagramErrorCode,
    DynInputSlot, DynOutput, MessageRegistration, MessageRegistry, NextOperation, OperationName,
    PerformForkClone, SerializeMessage, TraceInfo, TraceSettings, TypeInfo,
};

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SplitSchema {
    #[serde(default)]
    pub sequential: Vec<NextOperation>,
    #[serde(default)]
    pub keyed: HashMap<String, NextOperation>,
    pub remaining: Option<NextOperation>,
    #[serde(flatten)]
    pub trace_settings: TraceSettings,
}

impl BuildDiagramOperation for SplitSchema {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        let Some(sample_input) = ctx.infer_input_type_into_target(id)? else {
            // There are no outputs ready for this target, so we can't do
            // anything yet. The builder should try again later.
            return Ok(BuildStatus::defer("waiting for an input"));
        };

        let split = ctx.registry.messages.split(&sample_input, self, ctx.builder)?;
        let trace = TraceInfo::for_basic_op("split", &self.trace_settings);
        ctx.set_input_for_target(id, split.input, trace)?;
        for (target, output) in split.outputs {
            ctx.add_output_into_target(&target, output);
        }
        Ok(BuildStatus::Finished)
    }
}

impl Splittable for Value {
    type Key = MapSplitKey<String>;
    type Item = (JsonPosition, Value);

    fn validate(_: &Self::Key) -> bool {
        true
    }

    fn next(key: &Option<Self::Key>) -> Option<Self::Key> {
        MapSplitKey::next(key)
    }

    fn split(self, mut dispatcher: SplitDispatcher<'_, Self::Key, Self::Item>) -> OperationResult {
        match self {
            Value::Array(array) => {
                for (index, value) in array.into_iter().enumerate() {
                    let position = JsonPosition::ArrayElement(index);
                    match dispatcher.outputs_for(&MapSplitKey::Sequential(index)) {
                        Some(outputs) => {
                            outputs.push((position, value));
                        }
                        None => {
                            if let Some(outputs) = dispatcher.outputs_for(&MapSplitKey::Remaining) {
                                outputs.push((position, value));
                            }
                        }
                    }
                }
            }
            Value::Object(map) => {
                let mut next_seq = 0;
                for (name, value) in map.into_iter() {
                    let key = MapSplitKey::Specific(name);
                    match dispatcher.outputs_for(&key) {
                        Some(outputs) => {
                            // SAFETY: This key was initialized as MapSplitKey::Specific earlier
                            // in the function and is immutable, so this method is guaranteed to
                            // return `Some`
                            let position = JsonPosition::ObjectField(key.specific().unwrap());
                            outputs.push((position, value));
                        }
                        None => {
                            // No connection to the specific field name, so let's
                            // check for a sequential connection.
                            let seq = MapSplitKey::Sequential(next_seq);
                            next_seq += 1;

                            // SAFETY: This key was initialized as MapSplitKey::Specific earlier
                            // in the function and is immutable, so this method is guaranteed to
                            // return `Some`
                            let position = JsonPosition::ObjectField(key.specific().unwrap());
                            match dispatcher.outputs_for(&seq) {
                                Some(outputs) => outputs.push((position, value)),
                                None => {
                                    // No connection to this point in the sequence
                                    // so let's send it to any remaining connection.
                                    let remaining = MapSplitKey::Remaining;
                                    if let Some(outputs) = dispatcher.outputs_for(&remaining) {
                                        outputs.push((position, value));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            singular => {
                // This is a singular value, so it cannot be split. We will
                // send it to the first sequential connection or else to the
                // remaining connection.
                let position = JsonPosition::Singular;
                match dispatcher.outputs_for(&MapSplitKey::Sequential(0)) {
                    Some(outputs) => outputs.push((position, singular)),
                    None => {
                        let remaining = MapSplitKey::Remaining;
                        if let Some(outputs) = dispatcher.outputs_for(&remaining) {
                            outputs.push((position, singular));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Where was this positioned within the JSON structure.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub enum JsonPosition {
    /// This was the only item, e.g. the [`Value`] was a [`Null`][Value::Null],
    /// [`Bool`][Value::Bool], [`Number`][Value::Number], or [`String`][Value::String].
    Singular,
    /// The item came from an array.
    ArrayElement(usize),
    /// The item was a field of an object.
    ObjectField(String),
}

impl FromSpecific for ListSplitKey {
    type SpecificKey = String;

    fn from_specific(specific: Self::SpecificKey) -> Self {
        match specific.parse::<usize>() {
            Ok(seq) => Self::Sequential(seq),
            Err(_) => Self::Remaining,
        }
    }
}

#[derive(Debug)]
pub struct DynSplit {
    pub(super) input: DynInputSlot,
    pub(super) outputs: Vec<(NextOperation, DynOutput)>,
}

pub trait RegisterSplit {
    fn perform_split(
        split_op: &SplitSchema,
        builder: &mut Builder,
    ) -> Result<DynSplit, DiagramErrorCode>;

    fn on_register(registry: &mut MessageRegistry);
}

impl<T, Serializer, Cloneable> RegisterSplit for Supported<(T, Serializer, Cloneable)>
where
    T: Send + Sync + 'static + Splittable,
    T::Key: FromSequential + FromSpecific<SpecificKey = String> + ForRemaining,
    Serializer: SerializeMessage<T::Item> + SerializeMessage<Vec<T::Item>>,
    Cloneable: PerformForkClone<T::Item> + PerformForkClone<Vec<T::Item>>,
{
    fn perform_split(
        split_op: &SplitSchema,
        builder: &mut Builder,
    ) -> Result<DynSplit, DiagramErrorCode> {
        let (input, split) = builder.create_split::<T>();
        let mut outputs = Vec::new();
        let mut split = split.build(builder);
        for (key, target) in &split_op.keyed {
            outputs.push((target.clone(), split.specific_output(key.clone())?.into()));
        }

        for (i, target) in split_op.sequential.iter().enumerate() {
            outputs.push((target.clone(), split.sequential_output(i)?.into()))
        }

        if let Some(remaining_target) = &split_op.remaining {
            outputs.push((remaining_target.clone(), split.remaining_output()?.into()));
        }

        Ok(DynSplit {
            input: input.into(),
            outputs,
        })
    }

    fn on_register(registry: &mut MessageRegistry) {
        let ops = &mut registry
            .messages
            .entry(TypeInfo::of::<T>())
            .or_insert(MessageRegistration::new::<T>())
            .operations;

        ops.split_impl = Some(Self::perform_split);

        registry.register_serialize::<T::Item, Serializer>();
        registry.register_fork_clone::<T::Item, Cloneable>();
        registry.register_serialize::<Vec<T::Item>, Serializer>();
        registry.register_fork_clone::<Vec<T::Item>, Cloneable>();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{testing::*, *};
    use diagram::testing::DiagramTestFixture;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use test_log::test;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct Person {
        name: String,
        age: i8,
    }

    impl Person {
        fn new(name: impl Into<String>, age: i8) -> Self {
            Self {
                name: name.into(),
                age,
            }
        }
    }

    #[test]
    fn test_json_value_split() {
        let mut context = TestingContext::minimal_plugins();

        let value = json!(
            {
                "foo": 10,
                "bar": "hello",
                "jobs": {
                    "engineer": {
                        "name": "Alice",
                        "age": 28,
                    },
                    "designer": {
                        "name": "Bob",
                        "age": 67,
                    }
                }
            }
        );

        // Test multiple layers of splitting
        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder).split(|split| {
                split
                    // Get only the jobs data from the json
                    .specific_branch("jobs".to_owned(), |chain| {
                        chain.value().split(|jobs| {
                            jobs
                                // Grab the "first" job in the list, which should be
                                // alphabetical by default, so we should get the
                                // "designer" job.
                                .next_branch(|_, person| {
                                    person
                                        .value()
                                        .map_block(serde_json::from_value)
                                        .cancel_on_err()
                                        .connect(scope.terminate);
                                })
                                .unwrap()
                                .unused();
                        });
                    })
                    .unwrap()
                    .unused();
            });
        });

        let mut promise =
            context.command(|commands| commands.request(value, workflow).take_response());

        context.run_with_conditions(&mut promise, 1);
        assert!(context.no_unhandled_errors());

        let result: Person = promise.take().available().unwrap();
        assert_eq!(result, Person::new("Bob", 67));

        // Test serializing and splitting a tuple, then deserializing the split item
        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope
                .input
                .chain(builder)
                .map_block(serde_json::to_value)
                .cancel_on_err()
                .split(|split| {
                    split
                        // The second branch of our test input should have
                        // seralized Person data
                        .sequential_branch(1, |chain| {
                            chain
                                .value()
                                .map_block(serde_json::from_value)
                                .cancel_on_err()
                                .connect(scope.terminate);
                        })
                        .unwrap()
                        .unused();
                });
        });

        let mut promise = context.command(|commands| {
            commands
                .request((3.14159, Person::new("Charlie", 42)), workflow)
                .take_response()
        });

        context.run_with_conditions(&mut promise, 1);
        assert!(context.no_unhandled_errors());

        let result: Person = promise.take().available().unwrap();
        assert_eq!(result, Person::new("Charlie", 42));
    }

    #[test]
    fn test_split_list() {
        let mut fixture = DiagramTestFixture::new();

        fn split_list(_: i64) -> Vec<i64> {
            vec![1, 2, 3]
        }

        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("split_list".to_string()),
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            )
            .with_split();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "split_list",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "sequential": [{ "builtin": "terminate" }],
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result[1], 1);
    }

    #[test]
    fn test_split_list_with_key() {
        let mut fixture = DiagramTestFixture::new();

        fn split_list(_: i64) -> Vec<i64> {
            vec![1, 2, 3]
        }

        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("split_list".to_string()),
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            )
            .with_split();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "split_list",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "keyed": { "1": { "builtin": "terminate" } },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result[1], 2);
    }

    #[test]
    fn test_split_map() {
        let mut fixture = DiagramTestFixture::new();

        fn split_map(_: i64) -> HashMap<String, i64> {
            HashMap::from([
                ("a".to_string(), 1),
                ("b".to_string(), 2),
                ("c".to_string(), 3),
            ])
        }

        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("split_map".to_string()),
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_map),
            )
            .with_split();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "split_map",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "keyed": { "b": { "builtin": "terminate" } },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result[1], 2);
    }

    #[test]
    fn test_split_dual_key_seq() {
        let mut fixture = DiagramTestFixture::new();

        fn split_map(_: i64) -> HashMap<String, i64> {
            HashMap::from([("a".to_string(), 1), ("b".to_string(), 2)])
        }

        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("split_map".to_string()),
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_map),
            )
            .with_split();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "split_map",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "keyed": { "a": { "builtin": "dispose" } },
                    "sequential": [{ "builtin": "terminate" }],
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        // "a" is "eaten" up by the keyed path, so we should be the result of "b".
        assert_eq!(result[1], 2);
    }

    #[test]
    fn test_split_remaining() {
        let mut fixture = DiagramTestFixture::new();

        fn split_list(_: i64) -> Vec<i64> {
            vec![1, 2, 3]
        }

        fixture
            .registry
            .register_node_builder(
                NodeBuilderOptions::new("split_list".to_string()),
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            )
            .with_split();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "split_list",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "sequential": [{ "builtin": "dispose" }],
                    "remaining": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(&diagram, JsonMessage::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result[1], 2);
    }

    #[test]
    fn test_split_start() {
        let mut fixture = DiagramTestFixture::new();

        fn get_split_value(pair: (JsonPosition, serde_json::Value)) -> serde_json::Value {
            pair.1
        }

        fixture.registry.register_node_builder(
            NodeBuilderOptions::new("get_split_value".to_string()),
            |builder, _config: ()| builder.create_map_block(get_split_value),
        );

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "split",
            "ops": {
                "split": {
                    "type": "split",
                    "sequential": ["getSplitValue"],
                },
                "getSplitValue": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result: JsonMessage = fixture
            .spawn_and_run(
                &diagram,
                serde_json::to_value(HashMap::from([("test".to_string(), 1)])).unwrap(),
            )
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 1);
    }
}
