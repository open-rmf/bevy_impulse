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

use std::{any::TypeId, collections::HashMap, usize};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::debug;

use crate::{
    Builder, Chain, ForRemaining, FromSequential, FromSpecific, ListSplitKey, MapSplitKey,
    OperationResult, SplitDispatcher, Splittable,
};

use super::{
    impls::{DefaultImpl, NotSupported},
    join::register_join_impl,
    register_serialize, DiagramError, DynOutput, NodeRegistry, OperationId, SerializeMessage,
};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SplitOp {
    #[serde(flatten)]
    pub(super) params: SplitOpParams,

    pub(super) remaining: Option<OperationId>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SplitOpParams {
    Index(Vec<OperationId>),
    Key(HashMap<String, OperationId>),
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
                            let position = JsonPosition::ObjectField(key.specific().unwrap());
                            outputs.push((position, value));
                        }
                        None => {
                            // No connection to the specific field name, so let's
                            // check for a sequential connection.
                            let seq = MapSplitKey::Sequential(next_seq);
                            next_seq += 1;

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
pub struct DynSplitOutputs<'a> {
    pub(super) outputs: HashMap<&'a OperationId, DynOutput>,
    pub(super) remaining: DynOutput,
}

pub(super) fn split_chain<'a, T>(
    chain: Chain<T>,
    split_op: &'a SplitOp,
) -> Result<DynSplitOutputs<'a>, DiagramError>
where
    T: Send + Sync + 'static + Splittable,
    T::Key: FromSequential + FromSpecific<SpecificKey = String> + ForRemaining,
{
    debug!(
        "split chain of type: {:?}, op: {:?}",
        TypeId::of::<T>(),
        split_op
    );
    chain.split(|mut sb| -> Result<DynSplitOutputs, DiagramError> {
        let outputs = match &split_op.params {
            SplitOpParams::Index(v) => {
                let outputs: HashMap<_, _> = v
                    .into_iter()
                    .enumerate()
                    .map(|(i, op_id)| -> Result<(_, DynOutput), DiagramError> {
                        Ok((op_id, sb.sequential_output(i)?.into()))
                    })
                    .collect::<Result<_, _>>()?;
                outputs
            }
            SplitOpParams::Key(v) => {
                let outputs: HashMap<_, _> = v
                    .into_iter()
                    .map(|(k, op_id)| -> Result<(_, DynOutput), DiagramError> {
                        Ok((op_id, sb.specific_output(k.clone())?.into()))
                    })
                    .collect::<Result<_, _>>()?;
                outputs
            }
        };
        let split_outputs = DynSplitOutputs {
            outputs,
            remaining: sb.remaining_output()?.into(),
        };
        debug!("splitted outputs: {:?}", split_outputs);
        Ok(split_outputs)
    })
}

pub trait DynSplit<T, Serializer> {
    const SUPPORTED: bool;

    fn dyn_split<'a>(
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramError>;

    fn on_register(registry: &mut NodeRegistry);
}

impl<T, Serializer> DynSplit<T, Serializer> for NotSupported {
    const SUPPORTED: bool = false;

    fn dyn_split<'a>(
        _builder: &mut Builder,
        _output: DynOutput,
        _split_op: &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramError> {
        Err(DiagramError::NotSplittable)
    }

    fn on_register(_registry: &mut NodeRegistry) {}
}

impl<T, Serializer> DynSplit<T, Serializer> for DefaultImpl
where
    T: Send + Sync + 'static + Splittable,
    T::Key: FromSequential + FromSpecific<SpecificKey = String> + ForRemaining,
    Serializer: SerializeMessage<T::Item> + SerializeMessage<Vec<T::Item>>,
{
    const SUPPORTED: bool = true;

    fn dyn_split<'a>(
        builder: &mut Builder,
        output: DynOutput,
        split_op: &'a SplitOp,
    ) -> Result<DynSplitOutputs<'a>, DiagramError> {
        let chain = output.into_output::<T>().chain(builder);
        split_chain(chain, split_op)
    }

    fn on_register(registry: &mut NodeRegistry) {
        register_serialize::<T::Item, Serializer>(registry);
        register_join_impl::<T::Item, Serializer>(registry);
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
            .registration_builder()
            .with_splittable()
            .register_node_builder(
                "split_list",
                "split_list",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            );

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "split_list",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "index": ["terminate"],
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
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
            .registration_builder()
            .with_splittable()
            .register_node_builder(
                "split_list",
                "split_list",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            );

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "split_list",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "key": {"1": "terminate"},
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
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
            .registration_builder()
            .with_splittable()
            .register_node_builder(
                "split_map",
                "split_map",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_map),
            );

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "split_map",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "key": {"b": "terminate"},
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
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
            .registration_builder()
            .with_splittable()
            .register_node_builder(
                "split_list",
                "split_list",
                |builder: &mut Builder, _config: ()| builder.create_map_block(&split_list),
            );

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "op1",
                },
                "op1": {
                    "type": "node",
                    "builder": "split_list",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "index": ["dispose"],
                    "remaining": "terminate",
                },
                "dispose": {
                    "type": "dispose",
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert_eq!(result[1], 2);
    }

    #[test]
    fn test_split_start() {
        let mut fixture = DiagramTestFixture::new();

        fn get_split_value(pair: (JsonPosition, serde_json::Value)) -> serde_json::Value {
            pair.1
        }

        fixture.registry.register_node_builder(
            "get_split_value",
            "get_split_value",
            |builder, _config: ()| builder.create_map_block(get_split_value),
        );

        let diagram = Diagram::from_json(json!({
            "ops": {
                "start": {
                    "type": "start",
                    "next": "split",
                },
                "split": {
                    "type": "split",
                    "index": ["getSplitValue"],
                },
                "getSplitValue": {
                    "type": "node",
                    "builder": "get_split_value",
                    "next": "terminate",
                },
                "terminate": {
                    "type": "terminate",
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(
                &diagram,
                serde_json::to_value(HashMap::from([("test".to_string(), 1)])).unwrap(),
            )
            .unwrap();
        assert_eq!(result, 1);
    }
}
