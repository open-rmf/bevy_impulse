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

use bevy::prelude::{Entity, Commands, BuildChildren};

use std::future::Future;

use crate::{
    Provider, UnusedTarget, StreamPack, Node, InputSlot, Output, StreamTargetMap,
    Buffer, BufferSettings, AddOperation, OperateBuffer, Scope, OperateScope,
    ScopeSettings, BeginCancel, ScopeEndpoints, IntoBlockingMap, IntoAsyncMap,
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
    /// The scope that this builder is meant to help build
    pub(crate) scope: Entity,
    /// The target for cancellation workflows
    pub(crate) finish_scope_cancel: Entity,
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
        provider.connect(Some(self.scope), source, target, self.commands);

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

    /// Create a [node](Node) that provides a [blocking map](crate::BlockingMap).
    pub fn create_map_block<T, U>(
        &mut self,
        f: impl FnMut(T) -> U + 'static + Send + Sync,
    ) -> Node<T, U, ()>
    where
        T: 'static + Send + Sync,
        U: 'static + Send + Sync,
    {
        self.create_node(f.into_blocking_map())
    }

    /// Create a [node](Node) that provides an [async map](crate::AsyncMap).
    pub fn create_map_async<T, Task>(
        &mut self,
        f: impl FnMut(T) -> Task + 'static + Send + Sync,
    ) -> Node<T, Task::Output, ()>
    where
        T: 'static + Send + Sync,
        Task: Future + 'static + Send + Sync,
        Task::Output: 'static + Send + Sync,
    {
        self.create_node(f.into_async_map())
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
            Some(self.scope),
            source,
            OperateBuffer::<T>::new(settings),
        ));

        Buffer { scope: self.scope, source, _ignore: Default::default() }
    }

    /// Create an isolated scope within the workflow. This can be useful for
    /// racing multiple branches, creating an uninterruptible segment within
    /// your workflow, or being able to run the same multiple instances of the
    /// same sub-workflow in parallel without them interfering with each other.
    ///
    /// A value can be sent into the scope by connecting an [`Output`] of a node
    /// in the parent scope to the [`InputSlot`] of the node which gets returned
    /// by this function. Each time a value is sent into the scope, it will run
    /// through the workflow of the scope with a unique session ID. Even if
    /// multiple values are sent in from the same session, they will each be
    /// assigned their own unique session ID while inside of this scope.
    pub fn create_scope<Request, Response, Streams>(
        &mut self,
        settings: ScopeSettings,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder),
    ) -> Node<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let scope_id = self.commands.spawn(()).id();
        let exit_scope = self.commands.spawn(UnusedTarget).id();
        self.create_scope_impl(scope_id, exit_scope, settings, build)
    }

    /// It is possible for a scope to be cancelled before it terminates. Even a
    /// scope which is marked as uninterruptible will still experience a
    /// cancellation if its terminal node becomes unreachable.
    ///
    /// This method allows you to define a workflow that branches off of this
    /// scope that will active if and only if the scope gets cancelled. The
    /// workflow will be activated once for each item in the buffer, and each
    /// activation will have its own session.
    ///
    /// If you only want this cancellation workflow to activate once per
    /// cancelled session, then you should use a buffer that has a limit of one
    /// item.
    ///
    /// The cancelled scope will only finish its cleanup after all cancellation
    /// workflows for the cancelled scope have finished, either by terminating
    /// or by being cancelled themselves.
    //
    // TODO(@mxgrey): Consider offering a setting to choose between whether each
    // buffer item gets its own session or whether they share a session.
    pub fn on_cancel<T: 'static + Send + Sync>(
        &mut self,
        from_buffer: Buffer<T>,
        settings: ScopeSettings,
        build: impl FnOnce(Scope<T, (), ()>, &mut Builder),
    ) {
        let cancelling_scope_id = self.commands.spawn(()).id();
        let _ = self.create_scope_impl::<T, (), ()>(
            cancelling_scope_id,
            self.finish_scope_cancel,
            settings,
            build,
        );

        let begin_cancel = self.commands.spawn(()).set_parent(self.scope).id();
        self.commands.add(AddOperation::new(
            None,
            begin_cancel,
            BeginCancel::<T>::new(self.scope, from_buffer.source, cancelling_scope_id),
        ));
    }

    /// Get the scope that this builder is building for.
    pub fn scope(&self) -> Entity {
        self.scope
    }

    /// Borrow the commands for the builder
    pub fn commands(&'a mut self) -> &'a mut Commands<'w, 's> {
        &mut self.commands
    }

    /// Used internally to create scopes in different ways.
    pub(crate) fn create_scope_impl<Request, Response, Streams>(
        &mut self,
        scope_id: Entity,
        exit_scope: Entity,
        settings: ScopeSettings,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder),
    ) -> Node<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let ScopeEndpoints {
            terminal,
            enter_scope,
            finish_scope_cancel
        } = OperateScope::<Request, Response, Streams>::add(
            Some(self.scope()), scope_id, Some(exit_scope), settings, self.commands,
        );

        let (stream_in, stream_out) = Streams::spawn_scope_streams(
            scope_id,
            self.scope,
            self.commands,
        );

        let mut builder = Builder {
            scope: scope_id,
            finish_scope_cancel,
            commands: self.commands,
        };

        let scope = Scope {
            input: Output::new(scope_id, enter_scope),
            terminate: InputSlot::new(scope_id, terminal),
            streams: stream_in,
        };

        build(scope, &mut builder);

        Node {
            input: InputSlot::new(self.scope, scope_id),
            output: Output::new(self.scope, exit_scope),
            streams: stream_out,
        }
    }
}
