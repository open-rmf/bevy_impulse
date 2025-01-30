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

use std::{
    any::TypeId,
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use bevy_ecs::prelude::Entity;

use schemars::{
    gen::SchemaGenerator,
    schema::Schema,
    JsonSchema,
};

use serde::{de::DeserializeOwned, Serialize};

#[derive(Clone, Copy, Debug)]
pub struct JsonBuffer {
    pub(crate) scope: Entity,
    pub(crate) source: Entity,
    pub(crate) interface: &'static (dyn JsonBufferAccessInterface + Send + Sync),
}

pub(crate) trait JsonBufferAccessInterface {
    fn message_type_name(&self) -> &str;
    fn message_type_id(&self) -> TypeId;
    fn schema(&self, generator: &mut SchemaGenerator) -> &'static Schema;
}

impl<'a> std::fmt::Debug for &'a (dyn JsonBufferAccessInterface + Send + Sync) {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("Message Properties")
            .field("type", &self.message_type_name())
            .finish()
    }
}

struct JsonBufferAccessImpl<T>(std::marker::PhantomData<T>);

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned + JsonSchema> JsonBufferAccessImpl<T> {

}

impl<T: 'static + Send + Sync + Serialize + DeserializeOwned + JsonSchema> JsonBufferAccessInterface for JsonBufferAccessImpl<T> {
    fn message_type_name(&self) -> &str {
        std::any::type_name::<T>()
    }

    fn message_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    fn schema(&self, generator: &mut SchemaGenerator) -> &'static Schema {
        // We use a const map so that we only need to generate the schema once
        // per message type.
        const SCHEMA_MAP: OnceLock<Mutex<HashMap<TypeId, &'static Schema>>> = OnceLock::new();
        let binding = SCHEMA_MAP;

        let schemas = binding.get_or_init(|| Mutex::default());

        let mut schemas_mut = schemas.lock().unwrap();
        *schemas_mut.entry(TypeId::of::<T>()).or_insert_with(|| {
            Box::leak(Box::new(generator.subschema_for::<T>()))
        })
    }
}
