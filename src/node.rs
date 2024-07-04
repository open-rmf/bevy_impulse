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

use crate::{StreamPack, Chain};

/// A collection of all the inputs and outputs for a node within a workflow.
pub struct Node<Request, Response, Streams: StreamPack> {
    /// The input slot for the node. Connect outputs into this slot to trigger
    /// the node.
    pub input: InputSlot<Request>,
    /// The final output of the node. Build off of this to handle the response
    /// that comes out of the node.
    pub output: Output<Response>,
    /// The streams that come out of the node. A stream may fire off data any
    /// number of times while a node is active. Each stream can fire off data
    /// independently. Once the final output of the node is sent, no more
    /// stream data will come out.
    pub streams: Streams::StreamOutputPack,
}

/// The slot that receives input for a node. When building a workflow, you can
/// connect the output of a node to this, as long as the types match.
pub struct InputSlot<Request> {
    scope: Entity,
    source: Entity,
    _ignore: std::marker::PhantomData<Request>,
}

impl<Request> InputSlot<Request> {
    pub fn id(&self) -> Entity {
        self.source
    }
    pub fn scope(&self) -> Entity {
        self.scope
    }
    pub(crate) fn new(scope: Entity, source: Entity) -> Self {
        Self { scope, source, _ignore: Default::default() }
    }
}

/// The output of a node. This can only be fed to one input slot before being
/// consumed. If the `Response` parameter can be cloned then you can feed this
/// into a [`CloneForkOutput`] to feed the output into any number of input slots.
pub struct Output<Response> {
    scope: Entity,
    target: Entity,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response> Output<Response> {
    /// Create a chain that builds off of this response.
    pub fn chain<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>,
    ) -> Chain<'w, 's, 'a, Response>
    where
        Response: 'static + Send + Sync,
    {
        Chain::new(self.scope, self.target, commands)
    }

    /// Get the entity that this output will be sent to.
    pub fn id(&self) -> Entity {
        self.target
    }

    /// Get the scope that this output exists inside of.
    pub fn scope(&self) -> Entity {
        self.scope
    }

    pub(crate) fn new(scope: Entity, target: Entity) -> Self {
        Self { scope, target, _ignore: Default::default() }
    }
}

/// The output of a cloning fork node. This output can be fed into any number of
/// input slots.
pub struct CloneForkOutput<Response> {
    scope: Entity,
    target: Entity,
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response> CloneForkOutput<Response> {
    pub fn id(&self) -> Entity {
        self.target
    }
    pub fn scope(&self) -> Entity {
        self.scope
    }
    pub(crate) fn new(scope: Entity, target: Entity) -> Self {
        Self { scope, target, _ignore: Default::default() }
    }
}
