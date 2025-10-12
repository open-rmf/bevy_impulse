use std::fmt::Write;

use bevy_impulse::{
    AsyncMap, ConfigExample, DiagramElementRegistry, JsonMessage, NodeBuilderOptions, StreamPack,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Number, Value};

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum ComparisonConfig {
    None,
    OrEqual(OrEqualTag),
    ComparedTo(f64),
    Settings(ComparisonSettings),
}

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum OrEqualTag {
    OrEqual,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
pub struct ComparisonSettings {
    #[serde(default)]
    compared_to: Option<f64>,
    #[serde(default)]
    or_equal: bool,
}

impl From<ComparisonConfig> for ComparisonSettings {
    fn from(config: ComparisonConfig) -> Self {
        let mut settings = ComparisonSettings::default();
        match config {
            ComparisonConfig::None => {}
            ComparisonConfig::ComparedTo(value) => {
                settings.compared_to = Some(value);
            }
            ComparisonConfig::OrEqual(_) => {
                settings.or_equal = true;
            }
            ComparisonConfig::Settings(s) => {
                settings = s;
            }
        }

        settings
    }
}

fn compare(
    settings: ComparisonSettings,
    request: JsonMessage,
    comparison: fn(f64, f64) -> bool,
) -> Result<JsonMessage, JsonMessage> {
    let check = |lhs: f64, rhs: f64| -> bool {
        if comparison(lhs, rhs) {
            return true;
        }

        settings.or_equal && (lhs == rhs)
    };

    match &request {
        JsonMessage::Array(array) => {
            let mut at_least_one_comparison = false;

            if let Some(compared_to) = settings.compared_to {
                // Check that every item in the array compares favorably against
                // the fixed value.
                for value in array.iter() {
                    let Some(value) = value.as_number().and_then(Number::as_f64) else {
                        return Err(request);
                    };

                    if !check(value, compared_to) {
                        return Err(request);
                    }
                }
            } else {
                // No fixed value to compare against, so check that the array is
                // in the appropriate order.
                let mut iter = array.iter();
                let Some(mut previous) = iter
                    .next()
                    .and_then(Value::as_number)
                    .and_then(Number::as_f64)
                else {
                    return Err(request);
                };

                for next in iter {
                    at_least_one_comparison = true;
                    let Some(next) = next.as_number().and_then(Number::as_f64) else {
                        return Err(request);
                    };

                    if !check(previous, next) {
                        return Err(request);
                    }

                    previous = next;
                }
            }

            if !at_least_one_comparison {
                return Err(request);
            }
            return Ok(request);
        }
        JsonMessage::Number(number) => {
            let Some(compared_to) = settings.compared_to else {
                return Err(request);
            };

            let Some(value) = number.as_f64() else {
                return Err(request);
            };

            if !check(value, compared_to) {
                return Err(request);
            }

            return Ok(request);
        }
        _ => {
            return Err(request);
        }
    }
}

#[derive(StreamPack)]
pub struct FibonacciStream {
    sequence: u64,
}

pub fn register(registry: &mut DiagramElementRegistry) {
    let add_description = "Add together any set of numbers passed as input and \
        then add the value in the config. If only one number is passed in as \
        input, it will be added to the value set in the config.";
    let add_examples = [
        ConfigExample::new(
            "Simply sum the set of numbers passed as input.",
            json!(null),
        ),
        ConfigExample::new(
            "Sum the set of numbers passed as input, and then add 5.",
            json!(5.0),
        ),
    ];

    registry.register_node_builder(
        NodeBuilderOptions::new("add")
            .with_default_display_text("Add")
            .with_description(add_description)
            .with_examples_configs(add_examples),
        |builder, config: Option<f64>| {
            builder.create_map_block(move |req: JsonMessage| {
                let input = match req {
                    JsonMessage::Array(array) => {
                        let mut sum: f64 = 0.0;
                        for item in array
                            .iter()
                            .filter_map(Value::as_number)
                            .filter_map(Number::as_f64)
                        {
                            sum += item;
                        }
                        sum
                    }
                    JsonMessage::Number(number) => number.as_f64().unwrap_or(0.0),
                    _ => 0.0,
                };

                input + config.unwrap_or(0.0)
            })
        },
    );

    let sub_description = "Subtract some numbers. If an array of numbers is \
        passed as input then the first number will be subtracted by every \
        subsequent number. If a number is set in the config, that will also be \
        subtracted from the output.";
    let sub_examples = [
        ConfigExample::new(
            "Simply subtract the first array element by all subsequent elements.",
            json!(null),
        ),
        ConfigExample::new("Additionally subtract 5 from the output.", json!(5.0)),
    ];

    registry.register_node_builder(
        NodeBuilderOptions::new("sub")
            .with_default_display_text("Subtract")
            .with_description(sub_description)
            .with_examples_configs(sub_examples),
        |builder, config: Option<f64>| {
            builder.create_map_block(move |req: JsonMessage| {
                let input = match req {
                    JsonMessage::Array(array) => {
                        let mut iter = array
                            .iter()
                            .filter_map(Value::as_number)
                            .filter_map(Number::as_f64);
                        let mut input = iter.next().unwrap_or(0.0);
                        for item in iter {
                            input -= item;
                        }
                        input
                    }
                    JsonMessage::Number(number) => number.as_f64().unwrap_or(0.0),
                    _ => 0.0,
                };

                input - config.unwrap_or(0.0)
            })
        },
    );

    let mul_description = "Multiply some numbers. If an array of numbers is \
        passed as input then all the numbers will be multiplied together. If \
        a number is set in the config, that will also be multipled into the \
        output.";
    let mul_examples = [
        ConfigExample::new("Simply multiply the input numbers together.", json!(null)),
        ConfigExample::new("Additionally multiple the output by 5.", json!(5.0)),
    ];

    registry.register_node_builder(
        NodeBuilderOptions::new("mul")
            .with_default_display_text("Multiply")
            .with_description(mul_description)
            .with_examples_configs(mul_examples),
        |builder, config: Option<f64>| {
            builder.create_map_block(move |req: JsonMessage| {
                let input = match req {
                    JsonMessage::Array(array) => {
                        let mut iter = array
                            .iter()
                            .filter_map(Value::as_number)
                            .filter_map(Number::as_f64);
                        let mut input = iter.next().unwrap_or(0.0);
                        for item in iter {
                            input *= item;
                        }
                        input
                    }
                    JsonMessage::Number(number) => number.as_f64().unwrap_or(0.0),
                    _ => 0.0,
                };

                input * config.unwrap_or(1.0)
            })
        },
    );

    let div_description = "Divide some numbers. If an array of numbers is \
        passed as input then the first number will be divided by all \
        subsequent numbers. If a number is set in the config, the final output \
        will also be divided by that value.";
    let div_examples = [
        ConfigExample::new(
            "Simply divide the first array element by all subsequent elements.",
            json!(null),
        ),
        ConfigExample::new("Additionally divide the output by 2.", json!(2.0)),
    ];

    registry.register_node_builder(
        NodeBuilderOptions::new("div")
            .with_default_display_text("Divide")
            .with_description(div_description)
            .with_examples_configs(div_examples),
        |builder, config: Option<f64>| {
            builder.create_map_block(move |req: JsonMessage| {
                let input = match req {
                    JsonMessage::Array(array) => {
                        let mut iter = array
                            .iter()
                            .filter_map(Value::as_number)
                            .filter_map(Number::as_f64);
                        let mut input = iter.next().unwrap_or(0.0);
                        for item in iter {
                            input /= item;
                        }
                        input
                    }
                    JsonMessage::Number(number) => number.as_f64().unwrap_or(0.0),
                    _ => 0.0,
                };

                input / config.unwrap_or(1.0)
            })
        },
    );

    let fibonacci_description = "Stream out a Fibonacci sequence. If a number \
        is given in the config, that will be used as the order of the \
        sequence. If no config is given then the input value will be \
        interpreted as a number and used as the order of the sequence. If no \
        suitable number can be found for the order then this will return an Err
        containing the input message.";
    let fibonacci_examples = [
        ConfigExample::new(
            "Generate a Fibonacci sequence whose order is the input value. If \
            the input message cannot be interpreted as a number then this node \
            will return an Err.",
            json!(null),
        ),
        ConfigExample::new("Generate a Fibonacci sequence of order 10.", json!(10.0)),
    ];

    registry.register_node_builder(
        NodeBuilderOptions::new("fibonacci")
            .with_default_display_text("Fibonacci")
            .with_description(fibonacci_description)
            .with_examples_configs(fibonacci_examples),
        |builder, config: Option<u64>| {
            builder.create_map(
                move |input: AsyncMap<JsonMessage, FibonacciStream>| async move {
                    let order = if let Some(order) = config {
                        order
                    } else if let JsonMessage::Number(number) = &input.request {
                        number.as_u64().ok_or(input.request)?
                    } else {
                        return Err(input.request);
                    };

                    let mut current = 0;
                    let mut next = 1;
                    for _ in 0..=order {
                        input.streams.sequence.send(current);

                        let sum = current + next;
                        current = next;
                        next = sum;
                    }

                    Ok(())
                },
            )
        },
    );

    let print_description = "Prints the input to stdout. An optional string \
        can be provided in the config to label the output.";

    let print_examples = [
        ConfigExample::new("Print the input as-is", json!(null)),
        ConfigExample::new(
            "Add \"printed from node: \" to the printed message",
            json!("printed from node"),
        ),
    ];

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("print")
                .with_default_display_text("Print")
                .with_description(print_description)
                .with_examples_configs(print_examples),
            |builder, config: Option<String>| {
                let header = config.clone();
                builder.create_map_block(move |request: JsonMessage| {
                    let mut msg = String::new();
                    if let Some(header) = &header {
                        write!(&mut msg, "{header}: ")?;
                    }

                    write!(&mut msg, "{request}")?;
                    println!("{msg}");
                    Ok::<(), std::fmt::Error>(())
                })
            },
        )
        .with_deserialize_request();

    let less_than_description = "Compares for a less-than relationship, \
        returning a Result<Msg> based on the evaluation. Inputs can be an \
        array of numbers or a single number value. The exact behavior will \
        depend on the config (see examples).";

    let less_than_examples = [
        ConfigExample::new(
            "Verify that every element in the input array is less than the next one.",
            ComparisonConfig::None,
        ),
        ConfigExample::new(
            "Verify that every element in the input array is less than OR EQUAL to the next one.",
            ComparisonConfig::OrEqual(OrEqualTag::OrEqual),
        ),
        ConfigExample::new(
            "Verify that every element in the input array is less than 10.",
            ComparisonConfig::ComparedTo(10.0),
        ),
        ConfigExample::new(
            "Verify that every element in the input array is less than or \
            equal to 10.",
            ComparisonConfig::Settings(ComparisonSettings {
                compared_to: Some(10.0),
                or_equal: true,
            }),
        ),
    ];

    registry
        .register_node_builder(
            NodeBuilderOptions::new("less_than")
                .with_default_display_text("Less Than")
                .with_description(less_than_description)
                .with_examples_configs(less_than_examples),
            |builder, config: ComparisonConfig| {
                let settings: ComparisonSettings = config.into();
                builder.create_map_block(move |request: JsonMessage| {
                    compare(settings, request, |a: f64, b: f64| a < b)
                })
            },
        )
        .with_fork_result();

    let greater_than_description = "Compares for a greater-than relationship, \
        returning a Result<Msg> based on the evaluation. Inputs can be an \
        array of numbers or a single number value. The exact behavior will \
        depend on the config (see examples).";

    let greater_than_examples = [
        ConfigExample::new(
            "Verify that every element in the input array is greater than the next one.",
            ComparisonConfig::None,
        ),
        ConfigExample::new(
            "Verify that every element in the input array is greater than OR EQUAL to the next one.",
            ComparisonConfig::OrEqual(OrEqualTag::OrEqual),
        ),
        ConfigExample::new(
            "Verify that every element in the input array is greater than 10.",
            ComparisonConfig::ComparedTo(10.0),
        ),
        ConfigExample::new(
            "Verify that every element in the input array is greater than or \
            equal to 10.",
            ComparisonConfig::Settings(ComparisonSettings {
                compared_to: Some(10.0),
                or_equal: true,
            }),
        ),
    ];

    registry
        .register_node_builder(
            NodeBuilderOptions::new("greater_than")
                .with_default_display_text("Greater Than")
                .with_description(greater_than_description)
                .with_examples_configs(greater_than_examples),
            |builder, config: ComparisonConfig| {
                let settings: ComparisonSettings = config.into();
                builder.create_map_block(move |request: JsonMessage| {
                    compare(settings, request, |a: f64, b: f64| a > b)
                })
            },
        )
        .with_fork_result();
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_impulse::prelude::*;
    use serde_json::json;

    #[test]
    fn test_split() {
        let diagram = Diagram::from_json(json!(
            {
                "$schema": "https://raw.githubusercontent.com/open-rmf/bevy_impulse/refs/heads/main/diagram.schema.json",
                "version": "0.1.0",
                "templates": {},
                "start": "62746cc5-19e4-456f-a94f-d49619ccd2c0",
                "ops": {
                    "1283dab4-aec4-41d9-8dd3-ee04b2dc4c2b": {
                        "type": "node",
                        "builder": "mul",
                        "next": "49939e88-eefb-4dcc-8185-c880abe43cc6",
                        "config": 10
                    },
                    "2115dca9-fa22-406a-b4d1-6755732f9e4d": {
                        "type": "node",
                        "builder": "mul",
                        "next": "aa4fdf7b-a554-4683-82de-0bfbf3752d1d",
                        "config": 100
                    },
                    "62746cc5-19e4-456f-a94f-d49619ccd2c0": {
                        "type": "split",
                        "sequential": [
                            "1283dab4-aec4-41d9-8dd3-ee04b2dc4c2b",
                            "2115dca9-fa22-406a-b4d1-6755732f9e4d"
                        ]
                    },
                    "49939e88-eefb-4dcc-8185-c880abe43cc6": {
                        "type": "buffer"
                    },
                    "aa4fdf7b-a554-4683-82de-0bfbf3752d1d": {
                        "type": "buffer"
                    },
                    "f26502a3-84ec-4c4d-9367-15c099248640": {
                        "type": "node",
                        "builder": "add",
                        "next": {
                            "builtin": "terminate"
                        }
                    },
                    "c3f84b5f-5f09-4dc7-b937-bd1cdf504bf9": {
                        "type": "join",
                        "buffers": [
                            "49939e88-eefb-4dcc-8185-c880abe43cc6",
                            "aa4fdf7b-a554-4683-82de-0bfbf3752d1d"
                        ],
                        "next": "f26502a3-84ec-4c4d-9367-15c099248640"
                    }
                }
            }
        ))
        .unwrap();

        let request = json!([1, 2]);

        let mut app = bevy_app::App::new();
        app.add_plugins(ImpulseAppPlugin::default());
        let mut registry = DiagramElementRegistry::new();
        register(&mut registry);

        let mut promise = app
            .world_mut()
            .command(|cmds| -> Result<Promise<JsonMessage>, DiagramError> {
                let workflow = diagram.spawn_io_workflow(cmds, &registry)?;
                Ok(cmds.request(request, workflow).take_response())
            })
            .unwrap();

        while promise.peek().is_pending() {
            app.update();
        }

        let result = promise.take().available().unwrap().as_f64().unwrap();
        assert_eq!(result, 210.0);
    }
}
