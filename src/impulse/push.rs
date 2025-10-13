/*
 * Copyright (C) 2024 Open Source Robotics Foundation
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

use std::borrow::Cow;

use bevy_ecs::prelude::{Component, Entity};

use crate::{
    add_lifecycle_dependency, Collection, Impulsive, Input, InputBundle, ManageInput, NamedValue,
    OperationRequest, OperationResult, OperationSetup, OrBroken, Storage,
};

pub(crate) struct Push<T> {
    settings: PushSettings,
    _ignore: std::marker::PhantomData<fn(T)>,
}

#[derive(Component, Clone)]
struct PushSettings {
    target: Entity,
    is_stream: bool,
    name: Option<Cow<'static, str>>,
}

impl<T> Push<T> {
    pub(crate) fn new(target: Entity, is_stream: bool) -> Self {
        Self {
            settings: PushSettings {
                target,
                is_stream,
                name: None,
            },
            _ignore: Default::default(),
        }
    }

    pub(crate) fn with_name(mut self, name: Cow<'static, str>) -> Self {
        self.settings.name = Some(name);
        self
    }
}

impl<T: 'static + Send + Sync> Impulsive for Push<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        if !self.settings.is_stream {
            add_lifecycle_dependency(source, self.settings.target, world);
        }
        world
            .entity_mut(source)
            .insert((InputBundle::<T>::new(), self.settings));
        Ok(())
    }

    fn execute(OperationRequest { source, world, .. }: OperationRequest) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<T>()?;
        let PushSettings {
            target,
            is_stream,
            name,
            ..
        } = source_mut.get::<PushSettings>().or_broken()?.clone();
        let mut target_mut = world.get_entity_mut(target).or_broken()?;

        if let Some(name) = name {
            let data = NamedValue { name, value: data };
            if let Some(mut collection) = target_mut.get_mut::<Collection<NamedValue<T>>>() {
                collection.items.push(Storage { session, data });
            } else {
                let mut collection = Collection::default();
                collection.items.push(Storage { session, data });
                target_mut.insert(collection);
            }
        } else {
            if let Some(mut collection) = target_mut.get_mut::<Collection<T>>() {
                collection.items.push(Storage { session, data });
            } else {
                let mut collection = Collection::default();
                collection.items.push(Storage { session, data });
                target_mut.insert(collection);
            }
        }

        if is_stream {
            world.entity_mut(source).despawn();
        }
        Ok(())
    }
}
