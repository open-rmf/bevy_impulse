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

use bevy::prelude::{Entity, Commands};

use crate::{
    Provider, UnusedTarget, StreamPack, Node, InputSlot, Output, StreamTargetMap,
};

pub(crate) mod internal;
pub(crate) use internal::*;

/// Device used for building a workflow. Simply pass a mutable borrow of this
/// into any functions which ask for it.
///
/// Note that each scope has its own [`Builder`], and a panic will occur if a
/// [`Builder`] gets used in the wrong scope. As of right now there is no known
/// way to trick the compiler into using a [`Builder`] in the wrong scope, but
/// please open an issue with a minimal reproducible example if you find a way
/// to make it panic.
pub struct Builder<'w, 's, 'a> {
    pub(crate) scope: Entity,
    pub(crate) commands: &'a mut Commands<'w, 's>,
}

impl<'w, 's, 'a> Builder<'w, 's, 'a> {
    pub fn create_node<P: Provider>(
        &mut self,
        provider: P,
    ) -> Node<P::Request, P::Response, P::Streams>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: StreamPack,
    {
        let source = self.commands.spawn(()).id();
        let target = self.commands.spawn(UnusedTarget).id();
        provider.connect(source, target, self.commands);

        let mut map = StreamTargetMap::default();
        let (bundle, streams) = <P::Streams as StreamPack>::spawn_node_streams(
            &mut map, self,
        );
        self.commands.entity(source).insert(bundle);
        Node {
            input: InputSlot::new(self.scope, source),
            output: Output::new(self.scope, target),
            streams,
        }
    }

    /// Get the scope that this builder is building for
    pub fn scope(&self) -> Entity {
        self.scope
    }

    /// Borrow the commands for the builder
    pub fn commands(&'a mut self) -> &'a mut Commands<'w, 's> {
        &mut self.commands
    }
}
