/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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

use crate::{GenericAssistant, Req, Resp, Job};
use bevy::{
    prelude::{Entity, Component},
    ecs::system::BoxedSystem,
};

pub(crate) type BoxedJob<Response> = Job<Box<dyn FnOnce(GenericAssistant) -> Option<Response>>>;

/// A service is a type of system that takes in a request and produces a
/// response, optionally emitting events from its streams using the provided
/// assistant.
#[derive(Component)]
pub(crate) enum Service<Request, Response> {
    /// The service takes in the request and blocks execution until the response
    /// is produced.
    Blocking(BoxedSystem<(Entity, Req<Request>), Resp<Response>>),
    /// The service produces a task that runs asynchronously in the bevy thread
    /// pool.
    Async(BoxedSystem<(Entity, Req<Request>), BoxedJob<Response>>),
}
