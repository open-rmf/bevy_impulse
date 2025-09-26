use std::fmt::Write;

use bevy_impulse::{AsyncMap, DiagramElementRegistry, JsonMessage, NodeBuilderOptions, StreamPack};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

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
    registry.register_node_builder(
        NodeBuilderOptions::new("add").with_default_display_text("Add"),
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

    registry.register_node_builder(
        NodeBuilderOptions::new("sub").with_default_display_text("Subtract"),
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

    registry.register_node_builder(
        NodeBuilderOptions::new("mul").with_default_display_text("Multiply"),
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

    registry.register_node_builder(
        NodeBuilderOptions::new("div").with_default_display_text("Divide"),
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

    registry.register_node_builder(
        NodeBuilderOptions::new("fibonacci").with_default_display_text("Fibonacci"),
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

    registry
        .opt_out()
        .no_serializing()
        .no_deserializing()
        .register_node_builder(
            NodeBuilderOptions::new("print").with_default_display_text("Print"),
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

    registry
        .register_node_builder(
            NodeBuilderOptions::new("less_than").with_default_display_text("Less Than"),
            |builder, config: ComparisonConfig| {
                let settings: ComparisonSettings = config.into();
                builder.create_map_block(move |request: JsonMessage| {
                    compare(settings, request, |a: f64, b: f64| a < b)
                })
            },
        )
        .with_fork_result();

    registry
        .register_node_builder(
            NodeBuilderOptions::new("greater_than").with_default_display_text("Greater Than"),
            |builder, config: ComparisonConfig| {
                let settings: ComparisonSettings = config.into();
                builder.create_map_block(move |request: JsonMessage| {
                    compare(settings, request, |a: f64, b: f64| a > b)
                })
            },
        )
        .with_fork_result();
}
