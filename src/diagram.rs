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

mod buffer_schema;
mod fork_clone_schema;
mod fork_result_schema;
mod join_schema;
mod node_schema;
mod registration;
mod section;
mod serialization;
mod split_schema;
mod supported;
mod transform_schema;
mod type_info;
mod unzip_schema;
mod workflow_builder;

use bevy_derive::{Deref, DerefMut};
use bevy_ecs::system::Commands;
use buffer_schema::{BufferAccessSchema, BufferSchema, ListenSchema};
use fork_clone_schema::{DynForkClone, ForkCloneSchema, PerformForkClone};
use fork_result_schema::{DynForkResult, ForkResultSchema};
pub use join_schema::JoinOutput;
use join_schema::{JoinSchema, SerializedJoinSchema};
pub use node_schema::NodeSchema;
pub use registration::*;
pub use section::*;
pub use serialization::*;
pub use split_schema::*;
use tracing::debug;
use transform_schema::{TransformError, TransformSchema};
use type_info::TypeInfo;
use unzip_schema::UnzipSchema;
use workflow_builder::{
    create_workflow, BuildDiagramOperation, BuildStatus, DiagramContext, OperationRef,
};

// ----------

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Display,
    io::Read,
    sync::Arc,
};

use crate::{
    Builder, IncompatibleLayout, JsonMessage, Scope, Service, SpawnWorkflowExt,
    SplitConnectionError, StreamPack,
};
use schemars::{
    JsonSchema, r#gen::SchemaGenerator,
    schema::{Schema, SchemaObject, InstanceType, ObjectValidation, SingleOrVec, Metadata},
};
use serde::{Deserialize, Serialize, Serializer, Deserializer, ser::SerializeMap, de::{Visitor, Error}};

const CURRENT_DIAGRAM_VERSION: &str = "0.1.0";
const SUPPORTED_DIAGRAM_VERSION: &str = ">=0.1.0, <0.2.0";
const RESERVED_OPERATION_NAMES: [&'static str; 2] = ["", "builtin"];

pub type BuilderId = Arc<str>;
pub type OperationName = Arc<str>;

#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
#[serde(untagged, rename_all = "snake_case")]
pub enum NextOperation {
    Name(OperationName),
    Builtin { builtin: BuiltinTarget },
    /// Refer to an "inner" operation of one of the sibling operations in a
    /// diagram. This can be used to target section inputs.
    Namespace(NamespacedOperation),
}

impl Display for NextOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(operation_id) => f.write_str(operation_id),
            Self::Namespace(NamespacedOperation { namespace, operation }) => write!(f, "{namespace}:{operation}"),
            Self::Builtin { builtin } => write!(f, "builtin:{builtin}"),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// This describes an operation that exists inside of some namespace, such as a
/// [`Section`]. This will serialize as a map with a single entry of
/// `{ "<namespace>": "<operation>" }`.
pub struct NamespacedOperation {
    pub namespace: OperationName,
    pub operation: OperationName,
}

impl Serialize for NamespacedOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.namespace, &self.operation)?;
        map.end()
    }
}

struct NamespacedOperationVisitor;

impl<'de> Visitor<'de> for NamespacedOperationVisitor {
    type Value = NamespacedOperation;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(
            "a map with exactly one entry of { \"<namespace>\" : \"<operation>\" } \
            whose key is the namespace string and whose value is the operation string"
        )
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let (key, value) = map.next_entry::<String, String>()?.ok_or_else(
            || A::Error::custom("namespaced operation must be a map from the namespace to the operation name")
        )?;

        if !map.next_key::<String>()?.is_none() {
            return Err(A::Error::custom("namespaced operation must contain exactly one entry"));
        }

        Ok(NamespacedOperation {
            namespace: key.into(),
            operation: value.into(),
        })
    }
}

impl<'de> Deserialize<'de> for NamespacedOperation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>
    {
        deserializer.deserialize_map(NamespacedOperationVisitor)
    }
}

impl JsonSchema for NamespacedOperation {
    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let mut schema = SchemaObject::new_ref(Self::schema_name());
        schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Object)));
        schema.object = Some(Box::new(ObjectValidation {
            max_properties: Some(1),
            min_properties: Some(1),
            required: Default::default(),
            properties: Default::default(),
            pattern_properties: Default::default(),
            property_names: Default::default(),
            additional_properties: Some(Box::new(generator.subschema_for::<String>())),
        }));
        schema.metadata = Some(Box::new(Metadata {
            title: Some("NamespacedOperation".to_string()),
            description: Some("Refer to an operation inside of a namespace, e.g. { \"<namespace>\": \"<operation>\"".to_string()),
            ..Default::default()
        }));

        Schema::Object(schema)
    }

    fn schema_name() -> String {
        "NamespacedOperation".to_string()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", untagged)]
pub enum BufferInputs {
    Single(NextOperation),
    Dict(HashMap<String, NextOperation>),
    Array(Vec<NextOperation>),
}

impl BufferInputs {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Single(_) => false,
            Self::Dict(d) => d.is_empty(),
            Self::Array(a) => a.is_empty(),
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
    /// Use the output to terminate the current scope. The value passed into
    /// this operation will be the return value of the scope.
    Terminate,

    /// Dispose of the output.
    Dispose,

    /// When triggered, cancel the current scope. If this is an inner scope of a
    /// workflow then the parent scope will see a disposal happen. If this is
    /// the root scope of a workflow then the whole workflow will cancel.
    Cancel,
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

    /// Connect the request to a registered section.
    ///
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "start": "section_op",
    ///     "ops": {
    ///         "section_op": {
    ///             "type": "section",
    ///             "builder": "my_section_builder",
    ///             "connect": {
    ///                 "my_section_output": { "builtin": "terminate" }
    ///             }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    ///
    /// Custom sections can also be created via templates
    /// ```
    /// # bevy_impulse::Diagram::from_json_str(r#"
    /// {
    ///     "version": "0.1.0",
    ///     "templates": {
    ///         "my_template": {
    ///             "inputs": ["section_input"],
    ///             "outputs": ["section_output"],
    ///             "buffers": [],
    ///             "ops": {
    ///                 "section_input": {
    ///                     "type": "node",
    ///                     "builder": "my_node",
    ///                     "next": "section_output"
    ///                 }
    ///             }
    ///         }
    ///     },
    ///     "start": "section_op",
    ///     "ops": {
    ///         "section_op": {
    ///             "type": "section",
    ///             "template": "my_template",
    ///             "connect": {
    ///                 "section_output": { "builtin": "terminate" }
    ///             }
    ///         }
    ///     }
    /// }
    /// # "#)?;
    /// # Ok::<_, serde_json::Error>(())
    /// ```
    Section(SectionSchema),

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
    /// ```
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
    /// ```
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
    ///     "start": "begin_measuring",
    ///     "ops": {
    ///         "begin_measuring": {
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
    /// [`JsonMessage`] before they are inserted into the buffer. This
    /// allows any serializable message type to be pushed into the buffer. If
    /// left unspecified, the buffer will store the specific data type that gets
    /// pushed into it. If the buffer inputs are not being serialized, then all
    /// incoming messages being pushed into the buffer must have the same type.
    ///
    /// [1]: crate::Buffer
    /// [2]: crate::BufferSettings
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
    /// ```
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

impl BuildDiagramOperation for DiagramOperation {
    fn build_diagram_operation(
        &self,
        id: &OperationName,
        builder: &mut Builder,
        ctx: &mut DiagramContext,
    ) -> Result<BuildStatus, DiagramErrorCode> {
        match self {
            Self::Buffer(op) => op.build_diagram_operation(id, builder, ctx),
            Self::BufferAccess(op) => op.build_diagram_operation(id, builder, ctx),
            Self::ForkClone(op) => op.build_diagram_operation(id, builder, ctx),
            Self::ForkResult(op) => op.build_diagram_operation(id, builder, ctx),
            Self::Join(op) => op.build_diagram_operation(id, builder, ctx),
            Self::Listen(op) => op.build_diagram_operation(id, builder, ctx),
            Self::Node(op) => op.build_diagram_operation(id, builder, ctx),
            Self::Section(op) => op.build_diagram_operation(id, builder, ctx),
            Self::SerializedJoin(op) => op.build_diagram_operation(id, builder, ctx),
            Self::Split(op) => op.build_diagram_operation(id, builder, ctx),
            Self::Transform(op) => op.build_diagram_operation(id, builder, ctx),
            Self::Unzip(op) => op.build_diagram_operation(id, builder, ctx),
        }
    }
}

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

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Diagram {
    /// Version of the diagram, should always be `0.1.0`.
    #[serde(
        deserialize_with = "deserialize_semver",
        serialize_with = "serialize_semver"
    )]
    #[schemars(schema_with = "schema_with_string")]
    version: semver::Version,

    #[serde(default)]
    pub templates: Templates,

    /// Indicates where the workflow should start running.
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

    /// Operations that define the workflow
    pub ops: Operations,
}

impl Diagram {
    /// Begin creating a new diagram
    pub fn new(start: NextOperation) -> Self {
        Self {
            version: semver::Version::parse(CURRENT_DIAGRAM_VERSION).unwrap(),
            start,
            templates: Default::default(),
            on_implicit_error: Default::default(),
            ops: Default::default(),
        }
    }

    /// Spawns a workflow from this diagram.
    ///
    /// # Examples
    ///
    /// ```
    /// use bevy_impulse::*;
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
    /// let workflow = app.world.command(|cmds| diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry))?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    // TODO(koonpeng): Support streams other than `()` #43.
    /* pub */
    fn spawn_workflow<Request, Response, Streams>(
        &self,
        cmds: &mut Commands,
        registry: &DiagramElementRegistry,
    ) -> Result<Service<Request, Response, Streams>, DiagramError>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
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

        let w = cmds.spawn_workflow(
            |scope: Scope<Request, Response, Streams>, builder: &mut Builder| {
                debug!(
                    "spawn workflow, scope input: {:?}, terminate: {:?}",
                    scope.input.id(),
                    scope.terminate.id()
                );

                unwrap_or_return!(create_workflow(scope, builder, registry, self));
            },
        );

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
    /// use bevy_impulse::*;
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
    /// let workflow = app.world.command(|cmds| diagram.spawn_io_workflow::<JsonMessage, JsonMessage>(cmds, &registry))?;
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn spawn_io_workflow<Request, Response>(
        &self,
        cmds: &mut Commands,
        registry: &DiagramElementRegistry,
    ) -> Result<Service<Request, Response, ()>, DiagramError>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        self.spawn_workflow::<Request, Response, ()>(cmds, registry)
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


    pub fn validate_operation_names(&self) -> Result<(), DiagramErrorCode> {
        self.ops.validate_operation_names()?;
        self.templates.validate_operation_names()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, JsonSchema, Serialize, Deserialize, Deref, DerefMut)]
#[serde(transparent, rename_all = "snake_case")]
pub struct Operations(HashMap<OperationName, DiagramOperation>);

impl Operations {
    /// Get an operation from this map, or a diagram error code if the operation
    /// is not available.
    pub fn get_op(&self, op_id: &Arc<str>) -> Result<&DiagramOperation, DiagramErrorCode> {
        self.get(op_id)
            .ok_or_else(|| DiagramErrorCode::operation_name_not_found(op_id.clone()))
    }

    pub fn validate_operation_names(&self) -> Result<(), DiagramErrorCode> {
        validate_operation_names(&self.0)
    }
}

#[derive(Debug, Clone, Default, JsonSchema, Serialize, Deserialize, Deref, DerefMut)]
pub struct Templates(HashMap<OperationName, SectionTemplate>);

impl Templates {
    /// Get a template from this map, or a diagram error code if the template is
    /// not available.
    pub fn get_template(&self, template_id: &OperationName) -> Result<&SectionTemplate, DiagramErrorCode> {
        self.get(template_id)
            .ok_or_else(|| DiagramErrorCode::TemplateNotFound(template_id.clone()))
    }

    pub fn validate_operation_names(&self) -> Result<(), DiagramErrorCode> {
        for (name, template) in &self.0 {
            validate_operation_name(name)?;
            validate_operation_names(&template.ops)?;
            // TODO(@mxgrey): Validate correctness of input, output, and buffer mapping
        }

        Ok(())
    }
}

fn validate_operation_names(ops: &HashMap<OperationName, DiagramOperation>) -> Result<(), DiagramErrorCode> {
    for name in ops.keys() {
        validate_operation_name(name)?;
    }

    Ok(())
}

fn validate_operation_name(name: &str) -> Result<(), DiagramErrorCode> {
    for reserved in &RESERVED_OPERATION_NAMES {
        if name == *reserved {
            return Err(DiagramErrorCode::InvalidUseOfReservedName(*reserved))
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
#[error("{context} {code}")]
pub struct DiagramError {
    pub context: DiagramErrorContext,

    #[source]
    pub code: DiagramErrorCode,
}

impl DiagramError {
    pub fn in_operation(op_id: OperationRef<'static>, code: DiagramErrorCode) -> Self {
        Self {
            context: DiagramErrorContext { op_id: Some(op_id) },
            code,
        }
    }
}

#[derive(Debug)]
pub struct DiagramErrorContext {
    op_id: Option<OperationRef<'static>>,
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
pub enum DiagramErrorCode {
    #[error("node builder [{0}] is not registered")]
    BuilderNotFound(BuilderId),

    #[error("operation [{0}] not found")]
    OperationNotFound(NextOperation),

    #[error("section template [{0}] does not exist")]
    TemplateNotFound(OperationName),

    #[error("type mismatch, source {source_type}, target {target_type}")]
    TypeMismatch {
        source_type: TypeInfo,
        target_type: TypeInfo,
    },

    #[error("Operation [{0}] attempted to instantiate multiple inputs.")]
    MultipleInputsCreated(OperationRef<'static>),

    #[error("Operation [{0}] attempted to instantiate multiple buffers.")]
    MultipleBuffersCreated(OperationRef<'static>),

    #[error("Missing a connection to start or terminate. A workflow cannot run with a valid connection to each.")]
    MissingStartOrTerminate,

    #[error("Serialization was disabled for the target message type.")]
    NotSerializable(TypeInfo),

    #[error("Deserialization was disabled for the target message type.")]
    NotDeserializable(TypeInfo),

    #[error("Cloning was disabled for the target message type.")]
    NotCloneable,

    #[error("The target message type does not support unzipping.")]
    NotUnzippable,

    #[error("The number of elements in the unzip expected by the diagram [{expected}] is different from the real number [{actual}]")]
    UnzipMismatch {
        expected: usize,
        actual: usize,
        elements: Vec<TypeInfo>,
    },

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

    #[error("Target type cannot be determined from [next] and [target_node] is not provided or cannot be inferred from.")]
    UnknownTarget,

    #[error("There was an attempt to access an unknown operation: [{0}]")]
    UnknownOperation(OperationRef),

    #[error("There was an attempt to use an operation in an invalid way: [{0}]")]
    InvalidOperation(OperationRef),

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

    #[error(transparent)]
    SectionError(#[from] SectionError),

    #[error("one or more operation is missing inputs")]
    IncompleteDiagram,

    #[error("operation type only accept single input")]
    OnlySingleInput,

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    ConnectionError(#[from] SplitConnectionError),

    #[error("a type being used in the diagram was not registered {0}")]
    UnregisteredType(TypeInfo),

    #[error("The build of the workflow came to a halt, reasons:\n{reasons:?}")]
    BuildHalted {
        /// Reasons that operations were unable to make progress building
        reasons: HashMap<OperationRef, Cow<'static, str>>,
    },

    #[error("The workflow building process has had an excessive number of iterations. This may indicate an implementation bug or an extraordinarily complex diagram.")]
    ExcessiveIterations,

    #[error("An operation was given a reserved name [{0}]")]
    InvalidUseOfReservedName(&'static str),

    #[error("an error happened while building a nested diagram: {0}")]
    NestedError(Box<DiagramError>),
}

impl From<DiagramErrorCode> for DiagramError {
    fn from(code: DiagramErrorCode) -> Self {
        DiagramError {
            context: DiagramErrorContext { op_id: None },
            code,
        }
    }
}

impl DiagramErrorCode {
    pub fn operation_name_not_found(name: OperationName) -> Self {
        DiagramErrorCode::OperationNotFound(NextOperation::Name(name))
    }
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

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::TypeMismatch { .. }),
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

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
        assert!(
            matches!(err.code, DiagramErrorCode::NotSerializable(_)),
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

        let err = fixture.spawn_json_io_workflow(&diagram).unwrap_err();
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
            CancellationCause::Unreachable(_),
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
                    "next": [
                        "transform",
                        { "builtin": "dispose" },
                    ],
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
