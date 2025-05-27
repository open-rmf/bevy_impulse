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
use std::any::Any;

use crate::{
    type_info::TypeInfo,
    AnyBuffer, InputSlot,
};

/// A type erased [`InputSlot`]
#[derive(Copy, Clone, Debug)]
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
