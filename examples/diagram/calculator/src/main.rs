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

use bevy_app;
use bevy_impulse::{
    AsyncMap, Diagram, DiagramElementRegistry, DiagramError, ImpulseAppPlugin, JsonMessage,
    NodeBuilderOptions, Promise, RequestExt, RunCommandsOnWorldExt, StreamPack,
};
use bevy_impulse_diagram_editor::{new_router, ServerOptions};
use clap::Parser;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::{error::Error, fmt::Write, fs::File, str::FromStr, sync::Arc, thread};
use prost_reflect::DescriptorPool;

#[derive(Parser, Debug)]
#[clap(
    name = "calculator",
    version = "0.1.0",
    about = "Example calculator app using diagrams."
)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Runs a diagram with the given request.
    Run(RunArgs),

    /// Starts a server to edit and run diagrams.
    Serve(ServeArgs),
}

#[derive(Parser, Debug)]
struct RunArgs {
    #[arg(help = "path to the diagram to run")]
    diagram: String,

    #[arg(help = "json containing the request to the diagram")]
    request: String,
}

#[derive(Parser, Debug)]
struct ServeArgs {
    #[arg(short, long, default_value_t = 3000)]
    port: u16,
}

fn run(args: RunArgs, registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());
    let file = File::open(args.diagram).unwrap();
    let diagram = Diagram::from_reader(file)?;

    let request = serde_json::Value::from_str(&args.request)?;
    let mut promise =
        app.world
            .command(|cmds| -> Result<Promise<serde_json::Value>, DiagramError> {
                let workflow = diagram.spawn_io_workflow(cmds, &registry)?;
                Ok(cmds.request(request, workflow).take_response())
            })?;

    while promise.peek().is_pending() {
        app.update();
    }

    println!("{}", promise.take().available().unwrap());
    Ok(())
}

async fn serve(args: ServeArgs, registry: DiagramElementRegistry) -> Result<(), Box<dyn Error>> {
    println!("Serving diagram editor at http://localhost:{}", args.port);

    let mut app = bevy_app::App::new();
    app.add_plugins(ImpulseAppPlugin::default());
    let router = new_router(&mut app, registry, ServerOptions::default());
    thread::spawn(move || app.run());
    let listener = tokio::net::TcpListener::bind(("localhost", args.port))
        .await
        .unwrap();
    axum::serve(listener, router).await?;
    Ok(())
}

fn create_registry(rt: Option<Arc<tokio::runtime::Runtime>>) -> DiagramElementRegistry {
    if rt.is_some() {
        let descriptor_set_bytes = include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        DescriptorPool::decode_global_file_descriptor_set(&descriptor_set_bytes[..]).unwrap();
    }

    let mut registry = DiagramElementRegistry::new();
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

    if let Some(rt) = rt {
        registry.enable_grpc(rt);
    }

    registry
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let registry = create_registry(Some(rt.clone()));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = rt.block_on(receiver);
    });

    let r = match cli.command {
        Commands::Run(args) => run(args, registry),
        Commands::Serve(args) => serve(args, registry).await,
    };

    let _ = sender.send(());
    r
}

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
enum ComparisonConfig {
    None,
    OrEqual(OrEqualTag),
    ComparedTo(f64),
    Settings(ComparisonSettings),
}

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
enum OrEqualTag {
    OrEqual,
}

#[derive(Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
struct ComparisonSettings {
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
struct FibonacciStream {
    sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let registry = create_registry(None);

        let mut promise = app
            .world
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
