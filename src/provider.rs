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

/// The `Provider` trait encapsulates the idea of providing a response to a
/// request. There are three different provider types with different advantages:
/// - [`Service`](crate::Service)
/// - [`Callback`](crate::Callback)
/// - [`AsyncMap`](crate::AsyncMap)
/// - [`BlockingMap`](crate::BlockingMap)
///
/// Types that implement this trait can be used to create nodes in a workflow or
/// impulses in an impulse chain.
pub trait Provider: ProvideOnce {}

/// Similar to [`Provider`] but can be used for functions that are only able to
/// run once, e.g. that use [`FnOnce`]. Because of this, [`ProvideOnce`] is
/// suitable for impulses, but not for workflows.
pub trait ProvideOnce {
    type Request;
    type Response;
    type Streams;

    /// Take a request from a source (stream target information will also be
    /// extracted from the source) and connect it to a target.
    fn connect(self, scope: Option<Entity>, source: Entity, target: Entity, commands: &mut Commands);
}
