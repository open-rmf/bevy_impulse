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
    any::Any,
    borrow::Cow,
    collections::{hash_map::Keys as HashMapKeys, HashMap},
};
use thiserror::Error as ThisError;

use crate::{
    type_info::TypeInfo, AnyBuffer, Builder, Connect, InputSlot, Node, Output, StreamPack,
};

/// A type erased [`Node`]
pub struct DynNode {
    pub input: DynInputSlot,
    pub output: DynOutput,
    pub streams: DynStreamOutputPack,
}

impl<Request, Response, Streams> From<Node<Request, Response, Streams>> for DynNode
where
    Request: 'static,
    Response: Send + Sync + 'static,
    Streams: StreamPack,
{
    fn from(node: Node<Request, Response, Streams>) -> Self {
        let mut streams = DynStreamOutputPack::default();
        Streams::into_dyn_stream_output_pack(&mut streams, node.streams);

        Self {
            input: node.input.into(),
            output: node.output.into(),
            streams,
        }
    }
}

/// A type erased [`InputSlot`]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct DynInputSlot {
    scope: Entity,
    source: Entity,
    type_info: TypeInfo,
}

impl DynInputSlot {
    pub fn scope(&self) -> Entity {
        self.scope
    }

    pub fn id(&self) -> Entity {
        self.source
    }

    pub fn message_info(&self) -> &TypeInfo {
        &self.type_info
    }

    pub(crate) fn new(
        scope: Entity,
        source: Entity,
        type_info: TypeInfo,
    ) -> Self {
        Self { scope, source, type_info }
    }
}

impl<T: Any> From<InputSlot<T>> for DynInputSlot {
    fn from(input: InputSlot<T>) -> Self {
        Self {
            scope: input.scope(),
            source: input.id(),
            type_info: TypeInfo::of::<T>(),
        }
    }
}

impl From<AnyBuffer> for DynInputSlot {
    fn from(buffer: AnyBuffer) -> Self {
        let any_interface = buffer.get_interface();
        Self {
            scope: buffer.scope(),
            source: buffer.id(),
            type_info: TypeInfo {
                type_id: any_interface.message_type_id(),
                type_name: any_interface.message_type_name(),
            },
        }
    }
}

/// A type erased [`crate::Output`]
#[derive(Debug)]
pub struct DynOutput {
    scope: Entity,
    target: Entity,
    message_info: TypeInfo,
}

impl DynOutput {
    pub fn message_info(&self) -> &TypeInfo {
        &self.message_info
    }

    pub fn into_output<T>(self) -> Result<Output<T>, TypeMismatch>
    where
        T: Send + Sync + 'static + Any,
    {
        if self.message_info != TypeInfo::of::<T>() {
            Err(TypeMismatch {
                source_type: self.message_info,
                target_type: TypeInfo::of::<T>(),
            })
        } else {
            Ok(Output::<T>::new(self.scope, self.target))
        }
    }

    pub fn scope(&self) -> Entity {
        self.scope
    }

    pub fn id(&self) -> Entity {
        self.target
    }

    /// Connect a [`DynOutput`] to a [`DynInputSlot`].
    pub fn connect_to(
        self,
        input: &DynInputSlot,
        builder: &mut Builder,
    ) -> Result<(), TypeMismatch> {
        if self.message_info() != input.message_info() {
            return Err(TypeMismatch {
                source_type: *self.message_info(),
                target_type: *input.message_info(),
            });
        }

        builder.commands().add(Connect {
            original_target: self.id(),
            new_target: input.id(),
        });

        Ok(())
    }

    pub(crate) fn new(scope: Entity, target: Entity, message_info: TypeInfo) -> Self {
        Self {
            scope,
            target,
            message_info,
        }
    }
}

impl<T> From<Output<T>> for DynOutput
where
    T: Send + Sync + 'static + Any,
{
    fn from(output: Output<T>) -> Self {
        Self {
            scope: output.scope(),
            target: output.id(),
            message_info: TypeInfo::of::<T>(),
        }
    }
}

// pub struct DynStreamInputSlots

/// Error type that happens when you try to convert a [`DynOutput`] to an
/// <code>[Output]&lt;T&gt;</code> for the wrong `T`.
#[derive(ThisError, Debug, Clone)]
#[error("type mismatch: source {source_type}, target {target_type}")]
pub struct TypeMismatch {
    /// What type of message is the [`DynOutput`] able to provide.
    pub source_type: TypeInfo,
    /// What type of message did you ask it provide.
    pub target_type: TypeInfo,
}

/// This is a pack of stream inputs whose message types are determined at runtime.
/// This can be created using the [`crate::StreamPack`] trait.
#[derive(Default)]
pub struct DynStreamInputPack {
    pub named: HashMap<Cow<'static, str>, DynInputSlot>,
    pub anonymous: HashMap<TypeInfo, DynInputSlot>,
}

impl DynStreamInputPack {
    /// Add a named stream input to this pack.
    pub fn add_named(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        input: impl Into<DynInputSlot>,
    ) {
        self.named.insert(name.into(), input.into());
    }

    /// Access a named stream input from this pack.
    pub fn get_named(&self, name: &str) -> Option<&DynInputSlot> {
        self.named.get(name)
    }

    /// Add an anonymous stream input to this pack.
    pub fn add_anonymous(&mut self, input: impl Into<DynInputSlot>) {
        let input: DynInputSlot = input.into();
        self.anonymous.insert(*input.message_info(), input);
    }

    /// Get an anonymous stream input from this pack.
    pub fn get_anonymous(&self, type_info: &TypeInfo) -> Option<&DynInputSlot> {
        self.anonymous.get(type_info)
    }
}

/// This is a pack of streams outputs whose message types are determined at runtime.
/// This can be created using the [`crate::StreamPack`].
#[derive(Default)]
pub struct DynStreamOutputPack {
    pub named: HashMap<Cow<'static, str>, DynOutput>,
    pub anonymous: HashMap<TypeInfo, DynOutput>,
}

impl DynStreamOutputPack {
    /// Add a named stream output to this pack.
    pub fn add_named(&mut self, name: impl Into<Cow<'static, str>>, output: impl Into<DynOutput>) {
        self.named.insert(name.into(), output.into());
    }

    /// Take a named stream output from this pack. The output needs to be taken
    /// because it will get consumed when it is connected to an input slot.
    pub fn take_named(&mut self, name: &str) -> Option<DynOutput> {
        self.named.remove(name)
    }

    /// Add an anonymous stream output to this pack.
    pub fn add_anonymous(&mut self, output: impl Into<DynOutput>) {
        let output: DynOutput = output.into();
        self.anonymous.insert(*output.message_info(), output);
    }

    /// Take an anonymous stream output from this pack. The output needs to be
    /// taken because it will get consumed when it is connected to an input slot.
    pub fn take_anonymous(&mut self, type_info: &TypeInfo) -> Option<DynOutput> {
        self.anonymous.remove(type_info)
    }

    /// Get the names that are available in this stream pack. This may provide
    /// a different result after you have called [`Self::take_named`].
    pub fn available_names(&self) -> HashMapKeys<'_, Cow<'static, str>, DynOutput> {
        self.named.keys()
    }
}
