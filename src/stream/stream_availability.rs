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

use bevy_ecs::prelude::Component;

use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap, HashSet},
};

use crate::{MissingStreamsError, NamedValue, TypeInfo};

/// [`StreamAvailability`] is a component that indicates what streams are offered by
/// a service.
#[derive(Component, Default)]
pub struct StreamAvailability {
    anonymous: HashSet<TypeInfo>,
    named: HashMap<Cow<'static, str>, NamedAvailability>,
}

impl StreamAvailability {
    pub fn add_anonymous<T: 'static + Send + Sync>(&mut self) {
        self.anonymous.insert(TypeInfo::of::<T>());
    }

    pub fn add_named<T: 'static + Send + Sync>(
        &mut self,
        name: impl Into<Cow<'static, str>>,
    ) -> Result<(), TypeInfo> {
        match self.named.entry(name.into()) {
            Entry::Vacant(vacant) => {
                vacant.insert(NamedAvailability::new::<T>());
                Ok(())
            }
            Entry::Occupied(occupied) => Err(occupied.get().value),
        }
    }

    pub fn has_anonymous<T: 'static + Send + Sync>(&self) -> bool {
        self.dyn_has_anonymous(&TypeInfo::of::<T>())
    }

    pub fn dyn_has_anonymous(&self, target_type: &TypeInfo) -> bool {
        self.anonymous.contains(target_type)
    }

    pub fn has_named<T: 'static + Send + Sync>(&self, name: &str) -> bool {
        self.dyn_has_named(name, &TypeInfo::of::<T>())
    }

    pub fn dyn_has_named(&self, name: &str, target_type: &TypeInfo) -> bool {
        self.named
            .get(name)
            .is_some_and(|ty| ty.value == *target_type)
    }

    pub fn can_cast_to(&self, target: &Self) -> Result<(), MissingStreamsError> {
        let mut missing = MissingStreamsError::default();
        for anon in &self.anonymous {
            if !target.anonymous.contains(anon) {
                missing.anonymous.insert(*anon);
            }
        }

        for (name, avail) in &self.named {
            if let Some(target_avail) = target.named.get(name) {
                if avail.value != target_avail.value {
                    missing.named.insert(name.clone(), avail.value);
                }
            } else if !target.anonymous.contains(&avail.named_value) {
                missing.named.insert(name.clone(), avail.value);
            }
        }

        missing.into_result()
    }

    pub fn named_streams(&self) -> HashMap<Cow<'static, str>, TypeInfo> {
        self.named.iter().map(|(k, v)| (k.clone(), v.value)).collect()
    }
}

#[derive(Clone, Copy)]
struct NamedAvailability {
    value: TypeInfo,
    named_value: TypeInfo,
}

impl NamedAvailability {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self {
            value: TypeInfo::of::<T>(),
            named_value: TypeInfo::of::<NamedValue<T>>(),
        }
    }
}
