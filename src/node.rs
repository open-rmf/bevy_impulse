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

use bevy::prelude::Entity;

pub struct InputSlot<Request> {
    scope: Entity,
    source: Entity,
    _ignore: std::marker::PhantomData<Request>,
}

impl<Request> InputSlot<Request> {
    pub(crate) fn new(scope: Entity, source: Entity) -> Self {
        Self { scope, source, _ignore: Default::default() }
    }
}

pub struct Output<Response> {
    scope: Entity,
    target: Entity,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response> Output<Response> {
    pub(crate) fn new(scope: Entity, target: Entity) -> Self {
        Self { scope, target, _ignore: Default::default() }
    }
}
