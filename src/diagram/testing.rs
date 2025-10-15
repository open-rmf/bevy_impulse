use std::error::Error;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, Number, json};

use crate::{
    testing::TestingContext, Builder, JsonMessage, RequestExt, RunCommandsOnWorldExt, Service,
    StreamPack, ConfigExample,
};

pub use crate::testing::FlushConditions;

use super::{Diagram, DiagramElementRegistry, DiagramError, NodeBuilderOptions};

pub(crate) struct DiagramTestFixture {
    pub(crate) context: TestingContext,
    pub(crate) registry: DiagramElementRegistry,
}

impl DiagramTestFixture {
    pub(crate) fn new() -> Self {
        Self {
            context: TestingContext::minimal_plugins(),
            registry: new_registry_with_basic_nodes(),
        }
    }

    pub(crate) fn spawn_json_io_workflow(
        &mut self,
        diagram: &Diagram,
    ) -> Result<Service<JsonMessage, JsonMessage>, DiagramError> {
        self.spawn_io_workflow::<JsonMessage, JsonMessage>(diagram)
    }

    /// Equivalent to `self.spawn_workflow::<JsonMessage, JsonMessage>(diagram)`
    pub(crate) fn spawn_io_workflow<Request, Response>(
        &mut self,
        diagram: &Diagram,
    ) -> Result<Service<Request, Response, ()>, DiagramError>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        self.spawn_workflow(diagram)
    }

    pub fn spawn_workflow<Request, Response, Streams>(
        &mut self,
        diagram: &Diagram,
    ) -> Result<Service<Request, Response, Streams>, DiagramError>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        self.context
            .app
            .world_mut()
            .command(|cmds| diagram.spawn_workflow(cmds, &self.registry))
    }

    /// Spawns a workflow from a diagram then run the workflow until completion.
    /// Returns the result of the workflow.
    pub fn spawn_and_run<Request, Response>(
        &mut self,
        diagram: &Diagram,
        request: Request,
    ) -> Result<Response, Box<dyn Error>>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        self.spawn_and_run_with_conditions(diagram, request, FlushConditions::default())
    }

    pub fn spawn_and_run_with_conditions<Request, Response>(
        &mut self,
        diagram: &Diagram,
        request: Request,
        conditions: impl Into<FlushConditions>,
    ) -> Result<Response, Box<dyn Error>>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        self.spawn_and_run_with_streams::<_, _, ()>(diagram, request, conditions)
            .map(|(response, _)| response)
    }

    pub fn spawn_and_run_with_streams<Request, Response, Streams>(
        &mut self,
        diagram: &Diagram,
        request: Request,
        conditions: impl Into<FlushConditions>,
    ) -> Result<(Response, Streams::StreamReceivers), Box<dyn Error>>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let workflow = self.spawn_workflow::<_, _, Streams>(diagram)?;
        let mut recipient = self
            .context
            .command(|cmds| cmds.request(request, workflow).take());
        self.context
            .run_with_conditions(&mut recipient.response, conditions);

        // Some workflows have callbacks with lifelong state that needs to be
        // cleaned up. In the case of zenoh, it's important to get that state
        // cleaned up before the tokio runtime starts to wind down. It seems we
        // can encounter a race condition with that which can lead to a panic.
        // Despawning the entity will signal the task pool that the task can be
        // dropped, and then running one cycle of the app will allow the task
        // pool to process this and clean up the callback appropriately.
        self.context.app.world_mut().despawn(workflow.provider());
        self.context.run(1);

        self.context.assert_no_errors();

        let taken = recipient.response.take();
        if taken.is_available() {
            Ok((taken.available().unwrap(), recipient.streams))
        } else if taken.is_cancelled() {
            let cancellation = taken.cancellation().unwrap();
            Err(cancellation.clone().into())
        } else {
            Err(String::from("promise is in invalid state").into())
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
struct Uncloneable<T>(T);

fn multiply3(i: i64) -> i64 {
    i * 3
}

fn multiply3_uncloneable(i: Uncloneable<i64>) -> Uncloneable<i64> {
    Uncloneable(i.0 * 3)
}

fn multiply3_5(x: i64) -> (i64, i64) {
    (x * 3, x * 5)
}

struct Unserializable;

fn opaque(_: Unserializable) -> Unserializable {
    Unserializable {}
}

fn opaque_request(_: Unserializable) {}

fn opaque_response(_: i64) -> Unserializable {
    Unserializable {}
}

/// create a new node registry with some basic nodes registered
fn new_registry_with_basic_nodes() -> DiagramElementRegistry {
    let mut registry = DiagramElementRegistry::new();
    registry.opt_out().no_cloning().register_node_builder(
        NodeBuilderOptions::new("multiply3_uncloneable"),
        |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3_uncloneable),
    );
    registry.register_node_builder(
        NodeBuilderOptions::new("multiply3"),
        |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3),
    );
    registry
        .register_node_builder(
            NodeBuilderOptions::new("multiply3_5"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(multiply3_5),
        )
        .with_unzip();

    registry.register_node_builder(
        NodeBuilderOptions::new("multiply_by"),
        |builder: &mut Builder, config: i64| builder.create_map_block(move |a: i64| a * config),
    );

    registry.register_node_builder(NodeBuilderOptions::new("add_to"), |builder, config: i64| {
        builder.create_map_block(move |a: i64| a + config)
    });

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
                    dbg!(&request);
                    compare(settings, request, |a: f64, b: f64| a < b)
                })
            },
        )
        .with_result();

    registry
        .opt_out()
        .no_deserializing()
        .no_serializing()
        .no_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("opaque"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque),
        );
    registry
        .opt_out()
        .no_deserializing()
        .no_serializing()
        .no_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("opaque_request"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque_request),
        );
    registry
        .opt_out()
        .no_deserializing()
        .no_serializing()
        .no_cloning()
        .register_node_builder(
            NodeBuilderOptions::new("opaque_response"),
            |builder: &mut Builder, _config: ()| builder.create_map_block(opaque_response),
        );

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
                dbg!((&req, config));
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
        .with_result();

    registry
}

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
