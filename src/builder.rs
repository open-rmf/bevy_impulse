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
    Buffer, BufferSettings, AddOperation, OperateBuffer,
};

pub(crate) mod connect;
pub(crate) use connect::*;

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
    /// Create a node for a provider. This will give access to an input slot, an
    /// output slots, and a pack of stream outputs which can all be connected to
    /// other nodes.
    #[must_use]
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

    /// Connect the output of one into the input slot of another node.
    pub fn connect<T: 'static + Send + Sync>(
        &mut self,
        output: Output<T>,
        input: InputSlot<T>,
    ) {
        assert_eq!(output.scope(), input.scope());
        self.commands.add(Connect {
            original_target: output.id(),
            new_target: input.id(),
        });
    }

    /// Create a [`Buffer`] which can be used to store and pull data within
    /// a scope. This is often used along with joining to synchronize multiple
    /// branches.
    pub fn create_buffer<T: 'static + Send + Sync>(
        &mut self,
        settings: BufferSettings,
    ) -> Buffer<T> {
        let source = self.commands.spawn(()).id();
        self.commands.add(AddOperation::new(
            source,
            OperateBuffer::<T>::new(settings),
        ));

        Buffer { scope: self.scope, source, _ignore: Default::default() }
    }

    /// Create a [`Buffer`] which can be used to store and pull data within a
    /// scope. Unlike the buffer created by [`Self::create_buffer`], this will
    /// give a clone of the latest items to nodes that pull from it instead of
    /// the items being consumed. That means the items are reused by default,
    /// which may be useful if you want some data to persist across multiple
    /// uses.
    pub fn create_cloning_buffer<T: 'static + Send + Sync + Clone>(
        &mut self,
        settings: BufferSettings,
    ) -> Buffer<T> {
        let source = self.commands.spawn(()).id();
        self.commands.add(AddOperation::new(
            source,
            OperateBuffer::<T>::new_cloning(settings),
        ));

        Buffer { scope: self.scope, source, _ignore: Default::default() }
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
