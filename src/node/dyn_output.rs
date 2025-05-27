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
use thiserror::Error as ThisError;
use std::any::Any;

use crate::{
    type_info::TypeInfo,
    dyn_input_slot::DynInputSlot,
    Connect, Output, Builder,
};

/// A type erased [`crate::Output`]
#[derive(Debug)]
pub struct DynOutput {
    scope: Entity,
    target: Entity,
    message_info: TypeInfo,
}

impl DynOutput {
    pub fn new(scope: Entity, target: Entity, message_info: TypeInfo) -> Self {
        Self {
            scope,
            target,
            message_info,
        }
    }

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

/// Error type that happens when you try to convert a [`DynOutput`] to an
/// <code>[Output]<T></code> for the wrong `T`.
#[derive(ThisError, Debug)]
#[error("type mismatch: source {source_type}, target {target_type}")]
pub struct TypeMismatch {
    /// What type of message is the [`DynOutput`] able to provide.
    pub source_type: TypeInfo,
    /// What type of message did you ask it provide.
    pub target_type: TypeInfo,
}
