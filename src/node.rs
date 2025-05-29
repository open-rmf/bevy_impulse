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

use bevy_ecs::prelude::Entity;

use crate::{
    AddBranchToForkClone, AddOperation, Builder, Chain, ForkClone, ForkTargetStorage,
    SingleInputStorage, StreamPack, UnusedTarget,
};

pub mod dyn_node;

/// A collection of all the inputs and outputs for a node within a workflow.
#[derive(Debug)]
#[must_use]
pub struct Node<Request, Response, Streams: StreamPack = ()> {
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
///
/// Any number of node outputs can be connected to one input slot.
#[must_use]
pub struct InputSlot<Request> {
    scope: Entity,
    source: Entity,
    _ignore: std::marker::PhantomData<fn(Request)>,
}

impl<T> Clone for InputSlot<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for InputSlot<T> {}

impl<Request> std::fmt::Debug for InputSlot<Request> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(format!("Input<{}>", std::any::type_name::<Request>()).as_str())
            .field("scope", &self.scope)
            .field("source", &self.source)
            .finish()
    }
}

impl<Request> InputSlot<Request> {
    pub fn id(&self) -> Entity {
        self.source
    }
    pub fn scope(&self) -> Entity {
        self.scope
    }
    pub(crate) fn new(scope: Entity, source: Entity) -> Self {
        Self {
            scope,
            source,
            _ignore: Default::default(),
        }
    }
}

/// The output of a node. This can only be connected to one input slot. If the
/// `Response` parameter can be cloned then you can call [`Self::fork_clone`] to
/// transform this into a [`ForkCloneOutput`] and then connect the output into
/// any number of input slots.
///
/// `Output` intentionally does not implement copy or clone because it must only
/// be consumed exactly once.
#[must_use]
pub struct Output<Response> {
    scope: Entity,
    target: Entity,
    _ignore: std::marker::PhantomData<fn(Response)>,
}

impl<Response> std::fmt::Debug for Output<Response> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(format!("Output<{}>", std::any::type_name::<Response>()).as_str())
            .field("scope", &self.scope)
            .field("target", &self.target)
            .finish()
    }
}

impl<Response: 'static + Send + Sync> Output<Response> {
    /// Create a chain that builds off of this response.
    pub fn chain<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Response>
    where
        Response: 'static + Send + Sync,
    {
        assert_eq!(self.scope, builder.scope());
        Chain::new(self.target, builder)
    }

    /// Create a node that will fork the output along multiple branches, giving
    /// a clone of the output to each branch.
    pub fn fork_clone(self, builder: &mut Builder) -> ForkCloneOutput<Response>
    where
        Response: Clone,
    {
        assert_eq!(self.scope, builder.scope());
        builder.commands.add(AddOperation::new(
            Some(self.scope),
            self.target,
            ForkClone::<Response>::new(ForkTargetStorage::new()),
        ));
        ForkCloneOutput::new(self.scope, self.target)
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
        Self {
            scope,
            target,
            _ignore: Default::default(),
        }
    }
}

/// The output of a cloning fork node. Use [`Self::clone_output`] to create a
/// cloned output that you can connect to an input slot.
#[must_use]
pub struct ForkCloneOutput<Response> {
    scope: Entity,
    source: Entity,
    _ignore: std::marker::PhantomData<fn(Response)>,
}

impl<Response: 'static + Send + Sync> ForkCloneOutput<Response> {
    pub fn clone_output(&self, builder: &mut Builder) -> Output<Response> {
        assert_eq!(self.scope, builder.scope());
        let target = builder
            .commands
            .spawn((SingleInputStorage::new(self.id()), UnusedTarget))
            .id();
        builder.commands.add(AddBranchToForkClone {
            source: self.source,
            target,
        });

        Output::new(self.scope, target)
    }

    pub fn clone_chain<'w, 's, 'a, 'b>(
        &self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Response> {
        let output = self.clone_output(builder);
        output.chain(builder)
    }

    pub fn id(&self) -> Entity {
        self.source
    }

    pub fn scope(&self) -> Entity {
        self.scope
    }

    pub(crate) fn new(scope: Entity, source: Entity) -> Self {
        Self {
            scope,
            source,
            _ignore: Default::default(),
        }
    }
}

/// The output of a fork result operation. Each output can be connected to one
/// input slot.
pub struct ForkResultOutput<T, E> {
    /// This output will be sent if an [`Ok`] is sent into the fork.
    pub ok: Output<T>,
    /// This output will be sent if an [`Err`] is sent into the fork.
    pub err: Output<E>,
}

/// The output of a fork option operation. Each output can be connected to one
/// input slot.
pub struct ForkOptionOutput<T> {
    /// This output will be sent if a [`Some`] is sent into the fork.
    pub some: Output<T>,
    /// This output will be sent if a [`None`] is sent into the fork.
    pub none: Output<()>,
}
