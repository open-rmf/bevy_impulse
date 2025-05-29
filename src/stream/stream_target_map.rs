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

use bevy_ecs::prelude::{Commands, Component, Entity, World};

use std::{
    any::TypeId,
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use crate::{DuplicateStream, NamedTarget, NamedValue, UnhandledErrors};

/// The actual entity target of the stream is held in this component which does
/// not have any generic parameters. This means it is possible to lookup the
/// targets of the streams coming out of the node without knowing the concrete
/// type of the streams. This is crucial for being able to redirect the stream
/// targets.
#[derive(Component, Default, Clone, Debug)]
pub struct StreamTargetMap {
    pub(crate) anonymous: HashMap<TypeId, Entity>,
    pub(crate) named: HashMap<Cow<'static, str>, (TypeId, Entity)>,
}

impl StreamTargetMap {
    pub fn add_anonymous<T: 'static + Send + Sync>(
        &mut self,
        target: Entity,
        commands: &mut Commands,
    ) {
        match self.anonymous.entry(TypeId::of::<T>()) {
            Entry::Vacant(vacant) => {
                vacant.insert(target);
            }
            Entry::Occupied(_) => {
                commands.add(move |world: &mut World| {
                    world
                        .get_resource_or_insert_with(|| UnhandledErrors::default())
                        .duplicate_streams
                        .push(DuplicateStream {
                            target,
                            type_name: std::any::type_name::<T>(),
                            stream_name: None,
                        })
                });
            }
        }
    }

    pub fn add_named<T: 'static + Send + Sync>(
        &mut self,
        name: Cow<'static, str>,
        target: Entity,
        commands: &mut Commands,
    ) {
        match self.named.entry(name.clone()) {
            Entry::Vacant(vacant) => {
                vacant.insert((TypeId::of::<T>(), target));
            }
            Entry::Occupied(_) => {
                commands.add(move |world: &mut World| {
                    world
                        .get_resource_or_insert_with(|| UnhandledErrors::default())
                        .duplicate_streams
                        .push(DuplicateStream {
                            target,
                            type_name: std::any::type_name::<T>(),
                            stream_name: Some(name.clone()),
                        });
                });
            }
        }
    }

    pub fn anonymous(&self) -> &HashMap<TypeId, Entity> {
        &self.anonymous
    }

    pub fn get_anonymous<T: 'static + Send + Sync>(&self) -> Option<Entity> {
        self.anonymous.get(&TypeId::of::<T>()).copied()
    }

    pub fn get_named<T: 'static + Send + Sync>(&self, name: &str) -> Option<Entity> {
        let target_type = TypeId::of::<T>();
        self.named
            .get(name)
            .filter(|(ty, _)| *ty == target_type)
            .map(|(_, target)| *target)
    }

    pub fn get_named_or_anonymous<T: 'static + Send + Sync>(
        &self,
        name: &str,
    ) -> Option<NamedTarget> {
        self.get_named::<T>(name)
            .map(NamedTarget::Value)
            .or_else(|| {
                self.get_anonymous::<NamedValue<T>>()
                    .map(NamedTarget::NamedValue)
            })
            .or_else(|| self.get_anonymous::<T>().map(NamedTarget::Value))
    }
}
