mod buffer_schema;
mod fork_clone_schema;
mod fork_result_schema;
mod impls;
mod join_schema;
mod node_schema;
mod registration;
mod serialization;
mod split_schema;
mod transform_schema;
mod type_info;
mod unzip_schema;
mod workflow_builder;

use bevy_ecs::system::Commands;
use buffer_schema::{BufferAccessSchema, BufferSchema, ListenSchema};
use fork_clone_schema::ForkCloneSchema;
use fork_result_schema::ForkResultSchema;
pub use join_schema::JoinOutput;
use join_schema::{JoinSchema, SerializedJoinSchema};
pub use node_schema::NodeSchema;
pub use registration::*;
pub use serialization::*;
pub use split_schema::*;
use tracing::debug;
use transform_schema::{TransformError, TransformSchema};
use type_info::TypeInfo;
use unzip_schema::UnzipSchema;
use workflow_builder::create_workflow;

// ----------

use std::{collections::HashMap, fmt::Display, io::Read};

use crate::{
    Builder, IncompatibleLayout, Scope, Service, SpawnWorkflowExt, SplitConnectionError, StreamPack,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const SUPPORTED_DIAGRAM_VERSION: &str = ">=0.1.0, <0.2.0";

pub type BuilderId = String;
pub type OperationId = String;

#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(untagged, rename_all = "snake_case")]
pub enum NextOperation {
    Target(OperationId),
    Builtin { builtin: BuiltinTarget },
}

impl Display for NextOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Target(operation_id) => f.write_str(operation_id),
            Self::Builtin { builtin } => write!(f, "builtin:{}", builtin),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    JsonSchema,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BuiltinTarget {
    /// Use the output to terminate the workflow. This will be the return value
    /// of the workflow.
    Terminate,

    /// Dispose of the output.
    Dispose,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(untagged, rename_all = "snake_case")]
pub enum SourceOperation {
    Source(OperationId),
    Builtin { builtin: BuiltinSource },
}

impl From<OperationId> for SourceOperation {
    fn from(value: OperationId) -> Self {
        SourceOperation::Source(value)
    }
}

impl Display for SourceOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(operation_id) => f.write_str(operation_id),
            Self::Builtin { builtin } => write!(f, "builtin:{}", builtin),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    JsonSchema,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BuiltinSource {
    Start,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct TerminateSchema {}

#[derive(Clone, strum::Display, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
#[strum(serialize_all = "snake_case")]
pub enum DiagramOperation {
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
    Node(NodeSchema),

    /// If the request is cloneable, clone it into multiple responses that can
    /// each be sent to a different operation. The `next` property is an array.
    ///
    /// This creates multiple simultaneous branches of execution within the
    /// workflow. Usually when you have multiple branches you will either
    /// * race - connect all branches to `terminate` and the first branch to
    ///   finish "wins" the race and gets to the be output
    /// * join - connect each branch into a buffer and then use the `join`
    ///   operation to reunite them
    /// * collect - TODO(@mxgrey): [add the collect operation](https://github.com/open-rmf/bevy_impulse/issues/59)
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "begin_race",
    ///     "ops": {
    ///         "begin_race": {
    ///             "type": "fork_clone",
    ///             "next": [
    ///                 "ferrari",
    ///                 "mustang"
    ///             ]
    ///         },
    ///         "ferrari": {
    ///             "type": "node",
    ///             "builder": "drive",
    ///             "config": "ferrari",
    ///             "next": { "builtin": "terminate" }
    ///         },
    ///         "mustang": {
    ///             "type": "node",
    ///             "builder": "drive",
    ///             "config": "mustang",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    ForkClone(ForkCloneSchema),

    /// If the input message is a tuple of (T1, T2, T3, ...), unzip it into
    /// multiple output messages of T1, T2, T3, ...
    ///
    /// Each output message may have a different type and can be sent to a
    /// different operation. This creates multiple simultaneous branches of
    /// execution within the workflow. See [`DiagramOperation::ForkClone`] for
    /// more information on parallel branches.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "name_phone_address",
    ///     "ops": {
    ///         "name_phone_address": {
    ///             "type": "unzip",
    ///             "next": [
    ///                 "process_name",
    ///                 "process_phone_number",
    ///                 "process_address"
    ///             ]
    ///         },
    ///         "process_name": {
    ///             "type": "node",
    ///             "builder": "process_name",
    ///             "next": "name_processed"
    ///         },
    ///         "process_phone_number": {
    ///             "type": "node",
    ///             "builder": "process_phone_number",
    ///             "next": "phone_number_processed"
    ///         },
    ///         "process_address": {
    ///             "type": "node",
    ///             "builder": "process_address",
    ///             "next": "address_processed"
    ///         },
    ///         "name_processed": { "type": "buffer" },
    ///         "phone_number_processed": { "type": "buffer" },
    ///         "address_processed": { "type": "buffer" },
    ///         "finished": {
    ///             "type": "join",
    ///             "buffers": [
    ///                 "name_processed",
    ///                 "phone_number_processed",
    ///                 "address_processed"
    ///             ],
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    Unzip(UnzipSchema),

    /// If the request is a [`Result<T, E>`], send the output message down an
    /// `ok` branch or down an `err` branch depending on whether the result has
    /// an [`Ok`] or [`Err`] value. The `ok` branch will receive a `T` while the
    /// `err` branch will receive an `E`.
    ///
    /// Only one branch will be activated by each input message that enters the
    /// operation.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_result",
    ///     "ops": {
    ///         "fork_result": {
    ///             "type": "fork_result",
    ///             "ok": { "builtin": "terminate" },
    ///             "err": { "builtin": "dispose" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    ForkResult(ForkResultSchema),

    /// If the input message is a list-like or map-like object, split it into
    /// multiple output messages.
    ///
    /// Note that the type of output message from the split depends on how the
    /// input message implements the [`Splittable`][1] trait. In many cases this
    /// will be a tuple of `(key, value)`.
    ///
    /// There are three ways to specify where the split output messages should
    /// go, and all can be used at the same time:
    /// * `sequential` - For array-like collections, send the "first" element of
    ///   the collection to the first operation listed in the `sequential` array.
    ///   The "second" element of the collection goes to the second operation
    ///   listed in the `sequential` array. And so on for all elements in the
    ///   collection. If one of the elements in the collection is mentioned in
    ///   the `keyed` set, then the sequence will pass over it as if the element
    ///   does not exist at all.
    /// * `keyed` - For map-like collections, send the split element associated
    ///   with the specified key to its associated output.
    /// * `remaining` - Any elements that are were not captured by `sequential`
    ///   or by `keyed` will be sent to this.
    ///
    /// [1]: crate::Splittable
    ///
    /// # Examples
    ///
    /// Suppose I am an animal rescuer sorting through a new collection of
    /// animals that need recuing. My home has space for three exotic animals
    /// plus any number of dogs and cats.
    ///
    /// I have a custom `SpeciesCollection` data structure that implements
    /// [`Splittable`][1] by allowing you to key on the type of animal.
    ///
    /// In the workflow below, we send all cats and dogs to `home`, and we also
    /// send the first three non-dog and non-cat species to `home`. All
    /// remaining animals go to the zoo.
    ///
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "select_animals",
    ///     "ops": {
    ///         "select_animals": {
    ///             "type": "split",
    ///             "sequential": [
    ///                 "home",
    ///                 "home",
    ///                 "home"
    ///             ],
    ///             "keyed": {
    ///                 "cat": "home",
    ///                 "dog": "home"
    ///             },
    ///             "remaining": "zoo"
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    ///
    /// If we input `["frog", "cat", "bear", "beaver", "dog", "rabbit", "dog", "monkey"]`
    /// then `frog`, `bear`, and `beaver` will be sent to `home` since those are
    /// the first three animals that are not `dog` or `cat`, and we will also
    /// send one `cat` and two `dog` home. `rabbit` and `monkey` will be sent to the zoo.
    Split(SplitSchema),

    /// Wait for exactly one item to be available in each buffer listed in
    /// `buffers`, then join each of those items into a single output message
    /// that gets sent to `next`.
    ///
    /// If the `next` operation is not a `node` type (e.g. `fork_clone`) then
    /// you must specify a `target_node` so that the diagram knows what data
    /// structure to join the values into.
    ///
    /// The output message type must be registered as joinable at compile time.
    /// If you want to join into a dynamic data structure then you should use
    /// [`DiagramOperation::SerializedJoin`] instead.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_measuring",
    ///     "ops": {
    ///         "fork_measuring": {
    ///             "type": "fork_clone",
    ///             "next": ["localize", "imu"]
    ///         },
    ///         "localize": {
    ///             "type": "node",
    ///             "builder": "localize",
    ///             "next": "estimated_position"
    ///         },
    ///         "imu": {
    ///             "type": "node",
    ///             "builder": "imu",
    ///             "config": "velocity",
    ///             "next": "estimated_velocity"
    ///         },
    ///         "estimated_position": { "type": "buffer" },
    ///         "estimated_velocity": { "type": "buffer" },
    ///         "gather_state": {
    ///             "type": "join",
    ///             "buffers": {
    ///                 "position": "estimate_position",
    ///                 "velocity": "estimate_velocity"
    ///             },
    ///             "next": "report_state"
    ///         },
    ///         "report_state": {
    ///             "type": "node",
    ///             "builder": "publish_state",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Join(JoinSchema),

    /// Same as [`DiagramOperation::Join`] but all input messages must be
    /// serializable, and the output message will always be [`serde_json::Value`].
    ///
    /// If you use an array for `buffers` then the output message will be a
    /// [`serde_json::Value::Array`]. If you use a map for `buffers` then the
    /// output message will be a [`serde_json::Value::Object`].
    ///
    /// Unlike [`DiagramOperation::Join`], the `target_node` property does not
    /// exist for this schema.
    SerializedJoin(SerializedJoinSchema),

    /// If the request is serializable, transform it by running it through a [CEL](https://cel.dev/) program.
    /// The context includes a "request" variable which contains the input message.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "transform",
    ///     "ops": {
    ///         "transform": {
    ///             "type": "transform",
    ///             "cel": "request.name",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    ///
    /// Note that due to how `serde_json` performs serialization, positive integers are always
    /// serialized as unsigned. In CEL, You can't do an operation between unsigned and signed so
    /// it is recommended to always perform explicit casts.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "transform",
    ///     "ops": {
    ///         "transform": {
    ///             "type": "transform",
    ///             "cel": "int(request.score) * 3",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Transform(TransformSchema),

    /// Create a [`Buffer`][1] which can be used to store and pull data within
    /// a scope.
    ///
    /// By default the [`BufferSettings`][2] will keep the single last message
    /// pushed to the buffer. You can change that with the optional `settings`
    /// property.
    ///
    /// Use the `"serialize": true` option to serialize the messages into
    /// [`JsonMessage`][3] before they are inserted into the buffer. This
    /// allows any serializable message type to be pushed into the buffer. If
    /// left unspecified, the buffer will store the specific data type that gets
    /// pushed into it. If the buffer inputs are not being serialized, then all
    /// incoming messages being pushed into the buffer must have the same type.
    ///
    /// [1]: crate::Buffer
    /// [2]: crate::BufferSettings
    /// [3]: crate::JsonMessage
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_clone",
    ///     "ops": {
    ///         "fork_clone": {
    ///             "type": "fork_clone",
    ///             "next": ["num_output", "string_output", "all_num_buffer", "serialized_num_buffer"]
    ///         },
    ///         "num_output": {
    ///             "type": "node",
    ///             "builder": "num_output",
    ///             "next": "buffer_access"
    ///         },
    ///         "string_output": {
    ///             "type": "node",
    ///             "builder": "string_output",
    ///             "next": "string_buffer"
    ///         },
    ///         "string_buffer": {
    ///             "type": "buffer",
    ///             "settings": {
    ///                 "retention": { "keep_last": 10 }
    ///             }
    ///         },
    ///         "all_num_buffer": {
    ///             "type": "buffer",
    ///             "settings": {
    ///                 "retention": "keep_all"
    ///             }
    ///         },
    ///         "serialized_num_buffer": {
    ///             "type": "buffer",
    ///             "serialize": true
    ///         },
    ///         "buffer_access": {
    ///             "type": "buffer_access",
    ///             "buffers": ["string_buffer"],
    ///             "target_node": "with_buffer_access",
    ///             "next": "with_buffer_access"
    ///         },
    ///         "with_buffer_access": {
    ///             "type": "node",
    ///             "builder": "with_buffer_access",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Buffer(BufferSchema),

    /// Zip a message together with access to one or more buffers.
    ///
    /// The receiving node must have an input type of `(Message, Keys)`
    /// where `Keys` implements the [`Accessor`][1] trait.
    ///
    /// [1]: crate::Accessor
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "fork_clone",
    ///     "ops": {
    ///         "fork_clone": {
    ///             "type": "fork_clone",
    ///             "next": ["num_output", "string_output"]
    ///         },
    ///         "num_output": {
    ///             "type": "node",
    ///             "builder": "num_output",
    ///             "next": "buffer_access"
    ///         },
    ///         "string_output": {
    ///             "type": "node",
    ///             "builder": "string_output",
    ///             "next": "string_buffer"
    ///         },
    ///         "string_buffer": {
    ///             "type": "buffer"
    ///         },
    ///         "buffer_access": {
    ///             "type": "buffer_access",
    ///             "buffers": ["string_buffer"],
    ///             "target_node": "with_buffer_access",
    ///             "next": "with_buffer_access"
    ///         },
    ///         "with_buffer_access": {
    ///             "type": "node",
    ///             "builder": "with_buffer_access",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    BufferAccess(BufferAccessSchema),

    /// Listen on a buffer.
    ///
    /// # Examples
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "num_output",
    ///     "ops": {
    ///         "buffer": {
    ///             "type": "buffer"
    ///         },
    ///         "num_output": {
    ///             "type": "node",
    ///             "builder": "num_output",
    ///             "next": "buffer"
    ///         },
    ///         "listen": {
    ///             "type": "listen",
    ///             "buffers": ["buffer"],
    ///             "target_node": "listen_buffer",
    ///             "next": "listen_buffer"
    ///         },
    ///         "listen_buffer": {
    ///             "type": "node",
    ///             "builder": "listen_buffer",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    Listen(ListenSchema),
}

type DiagramStart = serde_json::Value;
type DiagramTerminate = serde_json::Value;
type DiagramScope<Streams = ()> = Scope<DiagramStart, DiagramTerminate, Streams>;

/// Returns the schema for [`String`]
fn schema_with_string(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    gen.subschema_for::<String>()
}

/// deserialize semver and validate that it has a supported version
fn deserialize_semver<'de, D>(de: D) -> Result<semver::Version, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(de)?;
    // SAFETY: `SUPPORTED_DIAGRAM_VERSION` is a const, this will never fail.
    let ver_req = semver::VersionReq::parse(SUPPORTED_DIAGRAM_VERSION).unwrap();
    let ver = semver::Version::parse(&s).map_err(|_| {
        serde::de::Error::invalid_value(serde::de::Unexpected::Str(&s), &SUPPORTED_DIAGRAM_VERSION)
    })?;
    if !ver_req.matches(&ver) {
        return Err(serde::de::Error::invalid_value(
            serde::de::Unexpected::Str(&s),
            &SUPPORTED_DIAGRAM_VERSION,
        ));
    }
    Ok(ver)
}

/// serialize semver as a string
fn serialize_semver<S>(o: &semver::Version, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::ser::Serializer,
{
    o.to_string().serialize(ser)
}

#[derive(JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Diagram {
    /// Version of the diagram, should always be `0.1.0`.
    #[serde(
        deserialize_with = "deserialize_semver",
        serialize_with = "serialize_semver"
    )]
    #[schemars(schema_with = "schema_with_string")]
    version: semver::Version,

    /// Signifies the start of a workflow.
    start: NextOperation,

    ops: HashMap<OperationId, DiagramOperation>,
}

impl Diagram {
    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use bevy_impulse::{Diagram, DiagramError, NodeBuilderOptions, DiagramElementRegistry, RunCommandsOnWorldExt};
    ///
    /// let mut app = bevy_app::App::new();
    /// let mut registry = DiagramElementRegistry::new();
    /// registry.register_node_builder(NodeBuilderOptions::new("echo".to_string()), |builder, _config: ()| {
    ///     builder.create_map_block(|msg: String| msg)
    /// });
    ///
    /// let json_str = r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "echo",
    ///     "ops": {
    ///         "echo": {
    ///             "type": "node",
    ///             "builder": "echo",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// "#;
    ///
    /// let diagram = Diagram::from_json_str(json_str)?;
    /// let workflow = app.world.command(|cmds| diagram.spawn_io_workflow(cmds, &registry))?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    // TODO(koonpeng): Support streams other than `()` #43.
    /* pub */
    fn spawn_workflow<Streams>(
        &self,
        cmds: &mut Commands,
        registry: &DiagramElementRegistry,
    ) -> Result<Service<DiagramStart, DiagramTerminate, Streams>, DiagramError>
    where
        Streams: StreamPack,
    {
        let mut err: Option<DiagramError> = None;

        macro_rules! unwrap_or_return {
            ($v:expr) => {
                match $v {
                    Ok(v) => v,
                    Err(e) => {
                        err = Some(e);
                        return;
                    }
                }
            };
        }

        let w = cmds.spawn_workflow(|scope: DiagramScope<Streams>, builder: &mut Builder| {
            debug!(
                "spawn workflow, scope input: {:?}, terminate: {:?}",
                scope.input.id(),
                scope.terminate.id()
            );

            unwrap_or_return!(create_workflow(scope, builder, registry, self));
        });

        if let Some(err) = err {
            return Err(err);
        }

        Ok(w)
    }

    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use bevy_impulse::{Diagram, DiagramError, NodeBuilderOptions, DiagramElementRegistry, RunCommandsOnWorldExt};
    ///
    /// let mut app = bevy_app::App::new();
    /// let mut registry = DiagramElementRegistry::new();
    /// registry.register_node_builder(NodeBuilderOptions::new("echo".to_string()), |builder, _config: ()| {
    ///     builder.create_map_block(|msg: String| msg)
    /// });
    ///
    /// let json_str = r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "echo",
    ///     "ops": {
    ///         "echo": {
    ///             "type": "node",
    ///             "builder": "echo",
    ///             "next": { "builtin": "terminate" }
    ///         }
    ///     }
    /// }
    /// "#;
    ///
    /// let diagram = Diagram::from_json_str(json_str)?;
    /// let workflow = app.world.command(|cmds| diagram.spawn_io_workflow(cmds, &registry))?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn spawn_io_workflow(
        &self,
        cmds: &mut Commands,
        registry: &DiagramElementRegistry,
    ) -> Result<Service<DiagramStart, DiagramTerminate, ()>, DiagramError> {
        self.spawn_workflow::<()>(cmds, registry)
    }

    pub fn from_json(value: serde_json::Value) -> Result<Self, serde_json::Error> {
        serde_json::from_value(value)
    }

    pub fn from_json_str(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }

    pub fn from_reader<R>(r: R) -> Result<Self, serde_json::Error>
    where
        R: Read,
    {
        serde_json::from_reader(r)
    }

    fn get_op(&self, op_id: &OperationId) -> Result<&DiagramOperation, DiagramErrorCode> {
        self.ops
            .get(op_id)
            .ok_or_else(|| DiagramErrorCode::OperationNotFound(op_id.clone()))
    }
}

#[derive(Debug)]
pub struct DiagramErrorContext {
    op_id: Option<OperationId>,
}

impl Display for DiagramErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(op_id) = &self.op_id {
            write!(f, "in operation [{}],", op_id)?;
        }
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
#[error("{context} {code}")]
pub struct DiagramError {
    pub context: DiagramErrorContext,

    #[source]
    pub code: DiagramErrorCode,
}

#[derive(thiserror::Error, Debug)]
pub enum DiagramErrorCode {
    #[error("node builder [{0}] is not registered")]
    BuilderNotFound(BuilderId),

    #[error("operation [{0}] not found")]
    OperationNotFound(OperationId),

    #[error("type mismatch, source {source_type}, target {target_type}")]
    TypeMismatch {
        source_type: TypeInfo,
        target_type: TypeInfo,
    },

    #[error("Missing a connection to start or terminate. A workflow cannot run with a valid connection to each.")]
    MissingStartOrTerminate,

    #[error("Serialization was disabled for the target message type.")]
    NotSerializable,

    #[error("Cloning was disabled for the target message type.")]
    NotCloneable,

    #[error("The number of unzip slots in response does not match the number of inputs.")]
    NotUnzippable,

    #[error("Call .with_fork_result() on your node to be able to fork its Result-type output.")]
    CannotForkResult,

    #[error("Response cannot be split. Make sure to use .with_split() when building the node.")]
    NotSplittable,

    #[error(
        "Message cannot be joined. Make sure to use .with_join() when building the target node."
    )]
    NotJoinable,

    #[error("Empty join is not allowed.")]
    EmptyJoin,

    #[error("Target type cannot be determined from [next] and [target_node] is not provided.")]
    UnknownTarget,

    #[error(transparent)]
    CannotTransform(#[from] TransformError),

    #[error("box/unbox operation for the message is not registered")]
    CannotBoxOrUnbox,

    #[error("Buffer access was not enabled for a node connected to a buffer access operation. Make sure to use .with_buffer_access() when building the node.")]
    CannotBufferAccess,

    #[error("cannot listen on these buffers to produce a request of [{0}]")]
    CannotListen(TypeInfo),

    #[error(transparent)]
    IncompatibleBuffers(#[from] IncompatibleLayout),

    #[error("one or more operation is missing inputs")]
    IncompleteDiagram,

    #[error("operation type only accept single input")]
    OnlySingleInput,

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    ConnectionError(#[from] SplitConnectionError),

    /// Use this only for errors that *should* never happen because of some preconditions.
    /// If this error ever comes up, then it likely means that there is some logical flaws
    /// in the algorithm.
    #[error("an unknown error occurred while building the diagram, {0}")]
    UnknownError(String),
}

#[macro_export]
macro_rules! unknown_diagram_error {
    () => {
        DiagramErrorCode::UnknownError(format!("{}:{}", file!(), line!()))
    };
}

#[cfg(test)]
mod testing;

#[cfg(test)]
mod tests {
    use crate::{Cancellation, CancellationCause};
    use serde_json::json;
    use test_log::test;
    use testing::DiagramTestFixture;

    use super::*;

    #[test]
    fn test_no_terminate() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "dispose" },
                },
            },
        }))
        .unwrap();

        let err = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap_err();
        assert!(fixture.context.no_unhandled_errors());
        assert!(matches!(
            *err.downcast_ref::<Cancellation>().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_unserializable_start() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "opaque_request",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotSerializable),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_unserializable_terminate() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "opaque_response",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotSerializable),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_mismatch_types() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "opaque_request",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let err = fixture.spawn_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(
                err.code,
                DiagramErrorCode::TypeMismatch {
                    target_type: _,
                    source_type: _
                }
            ),
            "{:?}",
            err
        );
    }

    #[test]
    fn test_disconnected() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "op2",
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "op1",
                },
            },
        }))
        .unwrap();

        let err = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap_err();
        assert!(fixture.context.no_unhandled_errors());
        assert!(matches!(
            *err.downcast_ref::<Cancellation>().unwrap().cause,
            CancellationCause::Unreachable(_)
        ));
    }

    #[test]
    fn test_looping_diagram() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": "fork_clone",
                },
                "fork_clone": {
                    "type": "fork_clone",
                    "next": ["op1", "op2"],
                },
                "op2": {
                    "type": "node",
                    "builder": "multiply3",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 36);
    }

    #[test]
    fn test_noop_diagram() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": { "builtin": "terminate" },
            "ops": {},
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 4);
    }

    #[test]
    fn test_serialized_diagram() {
        let mut fixture = DiagramTestFixture::new();

        let json_str = r#"
        {
            "version": "0.1.0",
            "start": "multiply3",
            "ops": {
                "multiply3": {
                    "type": "node",
                    "builder": "multiply_by",
                    "config": 7,
                    "next": { "builtin": "terminate" }
                }
            }
        }
        "#;

        let result = fixture
            .spawn_and_run(
                &Diagram::from_json_str(json_str).unwrap(),
                serde_json::Value::from(4),
            )
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 28);
    }

    /// Test that we can transform on a slot of a unzipped response. Operations which changes
    /// the output type has extra serialization logic.
    #[test]
    fn test_transform_unzip() {
        let mut fixture = DiagramTestFixture::new();

        let diagram = Diagram::from_json(json!({
            "version": "0.1.0",
            "start": "op1",
            "ops": {
                "op1": {
                    "type": "node",
                    "builder": "multiply3_5",
                    "next": "unzip",
                },
                "unzip": {
                    "type": "unzip",
                    "next": ["transform"],
                },
                "transform": {
                    "type": "transform",
                    "cel": "777",
                    "next": { "builtin": "terminate" },
                },
            },
        }))
        .unwrap();

        let result = fixture
            .spawn_and_run(&diagram, serde_json::Value::from(4))
            .unwrap();
        assert!(fixture.context.no_unhandled_errors());
        assert_eq!(result, 777);
    }
}
