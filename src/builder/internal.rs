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

use bevy::{
    prelude::Entity,
    ecs::system::Command,
};

use crate::{SingleTargetStorage, ForkTargetStorage};

/// If two nodes have been created, they will each have a unique source and a
/// target entity allocated to them. If we want to connect them, then we want
/// the target of one to no longer be unique - we instead want it to be the
/// source entity of the other. This [`Command`] redirects the target information
/// of the sending node to target the source entity of the receiving node.
struct RedirectConnection {
    original_target: Entity,
    new_target: Entity,
}

