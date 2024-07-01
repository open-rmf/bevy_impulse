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

use bevy::prelude::{Entity, Commands};

/// The `Provider` trait encapsulates the idea of providing a service. There are
/// three different service types with different advantages and disadvantages:
/// - [`Service`](crate::Service)
/// - [`Handler`](crate::Handler)
/// - [`AsyncMap`](crate::AsyncMap)
/// - [`BlockingMap`](crate::BlockingMap)
pub trait Provider {
    type Request;
    type Response;
    type Streams;

    /// Take a request from a source (stream target information will also be
    /// extracted from the source) and connect it to a target.
    fn connect(self, source: Entity, target: Entity, commands: &mut Commands);
}
