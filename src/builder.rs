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

use bevy_ecs::prelude::{Commands, Entity};
use bevy_hierarchy::prelude::BuildChildren;

use std::future::Future;

use smallvec::SmallVec;

use crate::{
    AddOperation, AsMap, BeginCleanupWorkflow, Buffer, BufferKeys, BufferLocation, BufferMap,
    BufferSettings, Bufferable, Buffered, Chain, Collect, ForkClone, ForkCloneOutput,
    ForkTargetStorage, Gate, GateRequest, IncompatibleLayout, Injection, InputSlot, IntoAsyncMap,
    IntoBlockingMap, JoinedItem, JoinedValue, Node, OperateBuffer, OperateBufferAccess,
    OperateDynamicGate, OperateScope, OperateSplit, OperateStaticGate, Output, Provider,
    RequestOfMap, ResponseOfMap, Scope, ScopeEndpoints, ScopeSettings, ScopeSettingsStorage,
    Sendish, Service, SplitOutputs, Splittable, StreamPack, StreamTargetMap, StreamsOfMap, Trim,
    TrimBranch, UnusedTarget,
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
        let (bundle, streams) =
            <P::Streams as StreamPack>::spawn_node_streams(source, &mut map, self);
        self.commands.entity(source).insert((bundle, map));
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
        Task: Future + 'static + Sendish,
        Task::Output: 'static + Send + Sync,
    {
        self.create_node(f.into_async_map())
    }

    /// Create a map (either a [blocking map][1] or an
    /// [async map][2]) by providing a function that takes [`BlockingMap`][1] or
    /// [AsyncMap][2] as its only argument.
    ///
    /// [1]: crate::BlockingMap
    /// [2]: crate::AsyncMap
    pub fn create_map<M, F: AsMap<M>>(
        &mut self,
        f: F,
    ) -> Node<RequestOfMap<M, F>, ResponseOfMap<M, F>, StreamsOfMap<M, F>>
    where
        F::MapType: Provider,
        RequestOfMap<M, F>: 'static + Send + Sync,
        ResponseOfMap<M, F>: 'static + Send + Sync,
        StreamsOfMap<M, F>: StreamPack,
    {
        self.create_node(f.as_map())
    }

    /// Create a node that takes in a `(request, service)` at runtime and then
    /// passes the `request` into the `service`. All streams will be forwarded
    /// and the response of the service will be the node's output.
    ///
    /// This allows services to be injected into workflows as input, or for a
    /// service to be chosen during runtime.
    pub fn create_injection_node<Request, Response, Streams>(
        &mut self,
    ) -> Node<(Request, Service<Request, Response, Streams>), Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync + Unpin,
        Streams: StreamPack,
    {
        let source = self.commands.spawn(()).id();
        self.create_injection_impl::<Request, Response, Streams>(source)
    }

    /// Connect the output of one into the input slot of another node.
    pub fn connect<T: 'static + Send + Sync>(&mut self, output: Output<T>, input: InputSlot<T>) {
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

        Buffer {
            location: BufferLocation {
                scope: self.scope(),
                source,
            },
            _ignore: Default::default(),
        }
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
    pub fn create_scope<Request, Response, Streams, Settings>(
        &mut self,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder) -> Settings,
    ) -> Node<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
        Settings: Into<ScopeSettings>,
    {
        let scope_id = self.commands.spawn(()).id();
        let exit_scope = self.commands.spawn(UnusedTarget).id();
        self.create_scope_impl(scope_id, exit_scope, build)
    }

    /// Alternative to [`Self::create_scope`] for pure input/output scopes (i.e.
    /// there are no output streams). Using this signature should allow the
    /// compiler to infer all the generic arguments when there are no streams.
    pub fn create_io_scope<Request, Response, Settings>(
        &mut self,
        build: impl FnOnce(Scope<Request, Response, ()>, &mut Builder) -> Settings,
    ) -> Node<Request, Response, ()>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Settings: Into<ScopeSettings>,
    {
        self.create_scope::<Request, Response, (), Settings>(build)
    }

    /// Create a node that clones its inputs and sends them off to any number of
    /// targets.
    pub fn create_fork_clone<T>(&mut self) -> (InputSlot<T>, ForkCloneOutput<T>)
    where
        T: Clone + 'static + Send + Sync,
    {
        let source = self.commands.spawn(()).id();
        self.commands.add(AddOperation::new(
            Some(self.scope),
            source,
            ForkClone::<T>::new(ForkTargetStorage::new()),
        ));
        (
            InputSlot::new(self.scope, source),
            ForkCloneOutput::new(self.scope, source),
        )
    }

    /// Alternative way of calling [`Bufferable::join`]
    pub fn join<'b, B: Bufferable>(&'b mut self, buffers: B) -> Chain<'w, 's, 'a, 'b, JoinedItem<B>>
    where
        B::BufferType: 'static + Send + Sync,
        JoinedItem<B>: 'static + Send + Sync,
    {
        buffers.join(self)
    }

    /// Try joining a map of buffers into a single value.
    pub fn try_join_into<'b, Joined: JoinedValue>(
        &'b mut self,
        buffers: impl Into<BufferMap>,
    ) -> Result<Chain<'w, 's, 'a, 'b, Joined>, IncompatibleLayout> {
        Joined::try_join_into(buffers.into(), self)
    }

    /// Join an appropriate layout of buffers into a single value.
    pub fn join_into<'b, Joined: JoinedValue>(
        &'b mut self,
        buffers: Joined::Buffers,
    ) -> Chain<'w, 's, 'a, 'b, Joined> {
        Joined::join_into(buffers, self)
    }

    /// Alternative way of calling [`Bufferable::listen`].
    pub fn listen<'b, B: Bufferable>(
        &'b mut self,
        buffers: B,
    ) -> Chain<'w, 's, 'a, 'b, BufferKeys<B>>
    where
        B::BufferType: 'static + Send + Sync,
        BufferKeys<B>: 'static + Send + Sync,
    {
        buffers.listen(self)
    }

    /// Create a node that combines its inputs with access to some buffers. You
    /// must specify one ore more buffers to access. FOr multiple buffers,
    /// combine then into a tuple or an [`Iterator`]. Tuples of buffers can be
    /// nested inside each other.
    ///
    /// Other [outputs](Output) can also be passed in as buffers. These outputs
    /// will be transformed into a buffer with default buffer settings.
    pub fn create_buffer_access<T, B>(&mut self, buffers: B) -> Node<T, (T, BufferKeys<B>)>
    where
        T: 'static + Send + Sync,
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        BufferKeys<B>: 'static + Send + Sync,
    {
        let buffers = buffers.into_buffer(self);
        let source = self.commands.spawn(()).id();
        let target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(AddOperation::new(
            Some(self.scope),
            source,
            OperateBufferAccess::<T, B::BufferType>::new(buffers, target),
        ));

        Node {
            input: InputSlot::new(self.scope, source),
            output: Output::new(self.scope, target),
            streams: (),
        }
    }

    /// Collect incoming workflow threads into a container.
    ///
    /// If `max` is specified, the collection will always be sent out once it
    /// reaches that maximum value.
    ///
    /// If `min` is greater than 0 then the collection will not be sent out unless
    /// it is equal to or greater than that value. Note that this means the
    /// collect operation could force the workflow into cancelling if it cannot
    /// reach the minimum number of elements. A `min` of 0 means that if an
    /// upstream thread is disposed and the collect node is no longer reachable
    /// then it will fire off with an empty collection.
    ///
    /// If the `min` limit is satisfied and there are no remaining workflow
    /// threads that can reach this collect operation, then the collection will
    /// be sent out with however many elements it happens to have collected.
    pub fn create_collect<T, const N: usize>(
        &mut self,
        min: usize,
        max: Option<usize>,
    ) -> Node<T, SmallVec<[T; N]>>
    where
        T: 'static + Send + Sync,
    {
        if let Some(max) = max {
            assert!(0 < max);
            assert!(min <= max);
        }

        let source = self.commands.spawn(()).id();
        let target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(AddOperation::new(
            Some(self.scope),
            source,
            Collect::<T, N>::new(target, min, max),
        ));

        Node {
            input: InputSlot::new(self.scope, source),
            output: Output::new(self.scope, target),
            streams: (),
        }
    }

    /// Collect all workflow threads that are moving towards this node.
    pub fn create_collect_all<T, const N: usize>(&mut self) -> Node<T, SmallVec<[T; N]>>
    where
        T: 'static + Send + Sync,
    {
        self.create_collect(0, None)
    }

    /// Collect an exact number of threads that are moving towards this node.
    pub fn create_collect_n<T, const N: usize>(&mut self, n: usize) -> Node<T, SmallVec<[T; N]>>
    where
        T: 'static + Send + Sync,
    {
        self.create_collect(n, Some(n))
    }

    /// Create a new split operation in the workflow. The [`InputSlot`] can take
    /// in values that you want to split, and [`SplitOutputs::build`] will let
    /// you build connections to the split value.
    pub fn create_split<T>(&mut self) -> (InputSlot<T>, SplitOutputs<T>)
    where
        T: 'static + Send + Sync + Splittable,
    {
        let source = self.commands.spawn(()).id();
        self.commands.add(AddOperation::new(
            Some(self.scope),
            source,
            OperateSplit::<T>::default(),
        ));

        (
            InputSlot::new(self.scope, source),
            SplitOutputs::new(self.scope, source),
        )
    }

    /// This method allows you to define a cleanup workflow that branches off of
    /// this scope that will activate during the scope's cleanup phase. The
    /// input to the cleanup workflow will be a key to access to one or more
    /// buffers from the parent scope.
    ///
    /// Each different cleanup workflow that you define using this function will
    /// be given its own unique session ID when it gets run. You can define any
    /// number of cleanup workflows.
    ///
    /// The parent scope will only finish its cleanup phase after all cleanup
    /// workflows for the scope have finished, either by terminating or by being
    /// cancelled themselves.
    ///
    /// Cleanup workflows that you define with this function will always be run
    /// no matter how the scope finished. If you want a cleanup workflow that
    /// only runs when the scope is cancelled, use [`Self::on_cancel`]. If you
    /// want a cleanup workflow that only runs when the scope terminates
    /// successfully, then use [`Self::on_terminate`]. For easier runtime
    /// flexibility you can also use [`Self::on_cleanup_if`].
    pub fn on_cleanup<B, Settings>(
        &mut self,
        from_buffers: B,
        build: impl FnOnce(Scope<BufferKeys<B>, (), ()>, &mut Builder) -> Settings,
    ) where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        BufferKeys<B>: 'static + Send + Sync,
        Settings: Into<ScopeSettings>,
    {
        self.on_cleanup_if(
            CleanupWorkflowConditions::always_if(true, true),
            from_buffers,
            build,
        )
    }

    /// Define a cleanup workflow that only gets run if the scope was cancelled.
    ///
    /// When the scope gets dropped before it terminates (i.e. the parent scope
    /// finished while this scope was active, and this scope is interruptible)
    /// that also counts as cancelled.
    ///
    /// A scope which is set to be uninterruptible will still experience a
    /// cancellation if its terminal node becomes unreachable.
    ///
    /// If you want a cleanup workflow that only runs when the scope terminates
    /// successfully then use [`Self::on_terminate`]. If you want a cleanup
    /// workflow that always runs when the scope is finished, then use
    /// [`Self::on_cleanup`].
    pub fn on_cancel<B, Settings>(
        &mut self,
        from_buffers: B,
        build: impl FnOnce(Scope<BufferKeys<B>, (), ()>, &mut Builder) -> Settings,
    ) where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        BufferKeys<B>: 'static + Send + Sync,
        Settings: Into<ScopeSettings>,
    {
        self.on_cleanup_if(
            CleanupWorkflowConditions::always_if(false, true),
            from_buffers,
            build,
        )
    }

    /// Define a cleanup workflow that only gets run if the scope was successfully
    /// terminated. That means an input reached its terminal node, and now the
    /// scope is cleaning up.
    ///
    /// If you want a cleanup workflow that only runs when the scope is cancelled
    /// then use [`Self::on_cancel`]. If you want a cleanup workflow that always
    /// runs when then scope is finished, then use [`Self::on_cleanup`].
    pub fn on_terminate<B, Settings>(
        &mut self,
        from_buffers: B,
        build: impl FnOnce(Scope<BufferKeys<B>, (), ()>, &mut Builder) -> Settings,
    ) where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        BufferKeys<B>: 'static + Send + Sync,
        Settings: Into<ScopeSettings>,
    {
        self.on_cleanup_if(
            CleanupWorkflowConditions::always_if(true, false),
            from_buffers,
            build,
        )
    }

    /// Define a sub-workflow that will be run when this workflow is being cleaned
    /// up if the conditions are met. A workflow enters the cleanup stage when
    /// its terminal node is reached or if it gets cancelled.
    pub fn on_cleanup_if<B, Settings>(
        &mut self,
        conditions: CleanupWorkflowConditions,
        from_buffers: B,
        build: impl FnOnce(Scope<BufferKeys<B>, (), ()>, &mut Builder) -> Settings,
    ) where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        BufferKeys<B>: 'static + Send + Sync,
        Settings: Into<ScopeSettings>,
    {
        let cancelling_scope_id = self.commands.spawn(()).id();
        let _ = self.create_scope_impl::<BufferKeys<B>, (), (), Settings>(
            cancelling_scope_id,
            self.finish_scope_cancel,
            build,
        );

        let begin_cancel = self.commands.spawn(()).set_parent(self.scope).id();
        let buffers = from_buffers.into_buffer(self);
        buffers.verify_scope(self.scope);
        self.commands.add(AddOperation::new(
            None,
            begin_cancel,
            BeginCleanupWorkflow::<B::BufferType>::new(
                self.scope,
                buffers,
                cancelling_scope_id,
                conditions.run_on_terminate,
                conditions.run_on_cancel,
            ),
        ));
    }

    /// Create a node that trims (cancels) other nodes in the workflow when it
    /// gets activated. The input into the node will be passed along as output
    /// after the trimming is confirmed to be completed.
    pub fn create_trim<T>(&mut self, branches: impl IntoIterator<Item = TrimBranch>) -> Node<T, T>
    where
        T: 'static + Send + Sync,
    {
        let branches: SmallVec<[_; 16]> = branches.into_iter().collect();
        for branch in &branches {
            branch.verify_scope(self.scope);
        }

        let source = self.commands.spawn(()).id();
        let target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(AddOperation::new(
            Some(self.scope),
            source,
            Trim::<T>::new(branches, target),
        ));

        Node {
            input: InputSlot::new(self.scope, source),
            output: Output::new(self.scope, target),
            streams: (),
        }
    }

    /// Create a gate node that can open and close the gates on one or more
    /// buffers. Feed a [`GateRequest`] into the node and all the associated
    /// buffers will be opened or closed based on the action inside the request.
    ///
    /// The data inside the request will be passed along as output once the
    /// gate action is finished.
    ///
    /// See [`Gate`] to understand what happens when a gate is opened or closed.
    pub fn create_gate<T, B>(&mut self, buffers: B) -> Node<GateRequest<T>, T>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        T: 'static + Send + Sync,
    {
        let buffers = buffers.into_buffer(self);
        buffers.verify_scope(self.scope);

        let source = self.commands.spawn(()).id();
        let target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(AddOperation::new(
            Some(self.scope),
            source,
            OperateDynamicGate::<T, _>::new(buffers, target),
        ));

        Node {
            input: InputSlot::new(self.scope, source),
            output: Output::new(self.scope, target),
            streams: (),
        }
    }

    /// Create a gate node that always opens or always closes the gates on one
    /// or more buffers.
    ///
    /// The data sent into the node will be passed back out as input, unchanged.
    ///
    /// See [`Gate`] to understand what happens when a gate is opened or closed.
    pub fn create_gate_action<T, B>(&mut self, action: Gate, buffers: B) -> Node<T, T>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        T: 'static + Send + Sync,
    {
        let buffers = buffers.into_buffer(self);
        buffers.verify_scope(self.scope);

        let source = self.commands.spawn(()).id();
        let target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(AddOperation::new(
            Some(self.scope),
            source,
            OperateStaticGate::<T, _>::new(buffers, target, action),
        ));

        Node {
            input: InputSlot::new(self.scope, source),
            output: Output::new(self.scope, target),
            streams: (),
        }
    }

    /// Create a gate node that always opens the gates on one or more buffers.
    ///
    /// See [`Gate`] to understand what happens when a gate is opened or closed.
    pub fn create_gate_open<B, T>(&mut self, buffers: B) -> Node<T, T>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        T: 'static + Send + Sync,
    {
        self.create_gate_action(Gate::Open, buffers)
    }

    /// Create a gate node that always closes the gates on one or more buffers.
    ///
    /// See [`Gate`] to understand what happens when a gate is opened or closed.
    pub fn create_gate_close<T, B>(&mut self, buffers: B) -> Node<T, T>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        T: 'static + Send + Sync,
    {
        self.create_gate_action(Gate::Closed, buffers)
    }

    /// Get the scope that this builder is building for.
    pub fn scope(&self) -> Entity {
        self.scope
    }

    /// Borrow the commands for the builder
    pub fn commands(&mut self) -> &mut Commands<'w, 's> {
        self.commands
    }

    /// Used internally to create scopes in different ways.
    pub(crate) fn create_scope_impl<Request, Response, Streams, Settings>(
        &mut self,
        scope_id: Entity,
        exit_scope: Entity,
        build: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder) -> Settings,
    ) -> Node<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
        Settings: Into<ScopeSettings>,
    {
        let ScopeEndpoints {
            terminal,
            enter_scope,
            finish_scope_cancel,
        } = OperateScope::<Request, Response, Streams>::add(
            Some(self.scope()),
            scope_id,
            Some(exit_scope),
            self.commands,
        );

        let (stream_in, stream_out) =
            Streams::spawn_scope_streams(scope_id, self.scope, self.commands);

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

        let settings = build(scope, &mut builder).into();
        self.commands
            .entity(scope_id)
            .insert(ScopeSettingsStorage(settings));

        Node {
            input: InputSlot::new(self.scope, scope_id),
            output: Output::new(self.scope, exit_scope),
            streams: stream_out,
        }
    }

    pub(crate) fn create_injection_impl<Request, Response, Streams>(
        &mut self,
        source: Entity,
    ) -> Node<(Request, Service<Request, Response, Streams>), Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        let target = self.commands.spawn(UnusedTarget).id();

        let mut map = StreamTargetMap::default();
        let (bundle, streams) = Streams::spawn_node_streams(source, &mut map, self);
        self.commands.entity(source).insert((bundle, map));
        self.commands.add(AddOperation::new(
            Some(self.scope),
            source,
            Injection::<Request, Response, Streams>::new(target),
        ));

        Node {
            input: InputSlot::new(self.scope, source),
            output: Output::new(self.scope, target),
            streams,
        }
    }
}

/// This struct is used to describe when a cleanup workflow should run. Currently
/// this is only two simple booleans, but in the future it could potentially
/// contain systems that make decisions at runtime, so for now we encapsulate
/// the idea of cleanup conditions in this struct so we enhance the capabilities
/// later without breaking API.
#[derive(Clone)]
pub struct CleanupWorkflowConditions {
    run_on_terminate: bool,
    run_on_cancel: bool,
}

impl CleanupWorkflowConditions {
    pub fn always_if(run_on_terminate: bool, run_on_cancel: bool) -> Self {
        CleanupWorkflowConditions {
            run_on_terminate,
            run_on_cancel,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*, CancellationCause};
    use smallvec::SmallVec;

    #[test]
    fn test_disconnected_workflow() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|_, _| {
            // Do nothing. Totally empty workflow.
        });
        // Test this repeatedly because we cache the result after the first time.
        check_unreachable(workflow, 1, &mut context);
        check_unreachable(workflow, 1, &mut context);
        check_unreachable(workflow, 1, &mut context);

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let node = builder.create_map_block(|v| v);
            builder.connect(scope.input, node.input);
            // Create a tight infinite loop that will never reach the terminal
            builder.connect(node.output, node.input);
        });
        check_unreachable(workflow, 1, &mut context);
        check_unreachable(workflow, 1, &mut context);
        check_unreachable(workflow, 1, &mut context);

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder).map_block(|v| v).fork_clone((
                |chain: Chain<()>| chain.map_block(|v| v).map_block(|v| v).unused(),
                |chain: Chain<()>| chain.map_block(|v| v).map_block(|v| v).unused(),
                |chain: Chain<()>| chain.map_block(|v| v).map_block(|v| v).unused(),
            ));

            // Create an exit node that never connects to the scope's input.
            let exit_node = builder.create_map_block(|v| v);
            builder.connect(exit_node.output, scope.terminate);
        });
        check_unreachable(workflow, 1, &mut context);
        check_unreachable(workflow, 1, &mut context);
        check_unreachable(workflow, 1, &mut context);

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let entry_buffer = builder.create_buffer::<()>(BufferSettings::keep_all());
            scope.input.chain(builder).map_block(|v| v).fork_clone((
                |chain: Chain<()>| chain.map_block(|v| v).connect(entry_buffer.input_slot()),
                |chain: Chain<()>| chain.map_block(|v| v).connect(entry_buffer.input_slot()),
                |chain: Chain<()>| chain.map_block(|v| v).connect(entry_buffer.input_slot()),
            ));

            // Create an exit buffer with no relationship to the entry buffer
            // which is the only thing that connects to the terminal node.
            let exit_buffer = builder.create_buffer::<()>(BufferSettings::keep_all());
            builder
                .listen(exit_buffer)
                .map_block(|_| ())
                .connect(scope.terminate);
        });
        check_unreachable(workflow, 1, &mut context);
        check_unreachable(workflow, 1, &mut context);
        check_unreachable(workflow, 1, &mut context);
    }

    fn check_unreachable(
        workflow: Service<(), ()>,
        flush_cycles: usize,
        context: &mut TestingContext,
    ) {
        let mut promise =
            context.command(|commands| commands.request((), workflow).take_response());

        context.run_with_conditions(&mut promise, flush_cycles);
        assert!(promise
            .take()
            .cancellation()
            .is_some_and(|c| matches!(*c.cause, CancellationCause::Unreachable(_))));
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_fork_clone() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let fork = scope.input.fork_clone(builder);
            let branch_a = fork.clone_output(builder);
            let branch_b = fork.clone_output(builder);
            builder.connect(branch_a, scope.terminate);
            builder.connect(branch_b, scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request(5.0, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert!(promise.take().available().is_some_and(|v| v == 5.0));
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder).fork_clone((
                |chain: Chain<f64>| chain.connect(scope.terminate),
                |chain: Chain<f64>| chain.connect(scope.terminate),
            ));
        });

        let mut promise =
            context.command(|commands| commands.request(3.0, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert!(promise.take().available().is_some_and(|v| v == 3.0));
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder).fork_clone((
                |chain: Chain<f64>| {
                    chain
                        .map_block(|t| WaitRequest {
                            duration: Duration::from_secs_f64(10.0 * t),
                            value: 10.0 * t,
                        })
                        .map(|r: AsyncMap<WaitRequest<f64>>| wait(r.request))
                        .connect(scope.terminate)
                },
                |chain: Chain<f64>| {
                    chain
                        .map_block(|t| WaitRequest {
                            duration: Duration::from_secs_f64(t / 100.0),
                            value: t / 100.0,
                        })
                        .map(|r: AsyncMap<WaitRequest<f64>>| wait(r.request))
                        .connect(scope.terminate)
                },
            ));
        });

        let mut promise =
            context.command(|commands| commands.request(1.0, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs_f64(0.5));
        assert!(promise.take().available().is_some_and(|v| v == 0.01));
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let (fork_input, fork_output) = builder.create_fork_clone();
            builder.connect(scope.input, fork_input);
            let a = fork_output.clone_output(builder);
            let b = fork_output.clone_output(builder);
            builder.join((a, b)).connect(scope.terminate);
        });

        let mut promise = context.command(|commands| commands.request(5, workflow).take_response());

        context.run_with_conditions(&mut promise, 1);
        assert!(promise.take().available().is_some_and(|v| v == (5, 5)));
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_stream_reachability() {
        let mut context = TestingContext::minimal_plugins();

        // Test for streams from a blocking node
        let workflow = context.spawn_io_workflow(|scope, builder| {
            let stream_node = builder.create_map(|_: BlockingMap<(), StreamOf<u32>>| {
                // Do nothing. The purpose of this node is to just return without
                // sending off any streams.
            });

            builder.connect(scope.input, stream_node.input);
            stream_node
                .streams
                .chain(builder)
                .inner()
                .map_block(|value| 2 * value)
                .connect(scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request((), workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.peek().is_cancelled());
        assert!(context.no_unhandled_errors());

        // Test for streams from an async node
        let workflow = context.spawn_io_workflow(|scope, builder| {
            let stream_node = builder.create_map(|_: AsyncMap<(), StreamOf<u32>>| {
                async { /* Do nothing */ }
            });

            builder.connect(scope.input, stream_node.input);
            stream_node
                .streams
                .chain(builder)
                .inner()
                .map_block(|value| 2 * value)
                .connect(scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request((), workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.peek().is_cancelled());
        assert!(context.no_unhandled_errors());
    }

    use tokio::sync::mpsc::unbounded_channel;

    #[test]
    fn test_on_cleanup() {
        let mut context = TestingContext::minimal_plugins();

        let (sender, mut receiver) = unbounded_channel();
        let workflow = context.spawn_io_workflow(|scope, builder| {
            let input = scope.input.fork_clone(builder);

            let buffer = builder.create_buffer(BufferSettings::default());
            let input_to_buffer = input.clone_output(builder);
            builder.connect(input_to_buffer, buffer.input_slot());

            let none_node = builder.create_map_block(produce_none);
            let input_to_node = input.clone_output(builder);
            builder.connect(input_to_node, none_node.input);
            none_node
                .output
                .chain(builder)
                .cancel_on_none()
                .connect(scope.terminate);

            // The chain coming out of the none_node will result in the scope
            // being cancelled. After that, this scope should run, and the value
            // that went into the buffer should get sent over the channel.
            builder.on_cancel(buffer, |scope, builder| {
                scope
                    .input
                    .chain(builder)
                    .consume_buffer::<8>()
                    .map_block(move |values| {
                        for value in values {
                            sender.send(value).unwrap();
                        }
                    })
                    .connect(scope.terminate);
            });
        });

        let mut promise = context.command(|commands| commands.request(5, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(
            context.no_unhandled_errors(),
            "{:#?}",
            context.get_unhandled_errors(),
        );
        assert!(promise.peek().is_cancelled());
        let channel_output = receiver.try_recv().unwrap();
        assert_eq!(channel_output, 5);
        assert!(receiver.try_recv().is_err());
        assert!(context.confirm_buffers_empty().is_ok());

        let (cancel_sender, mut cancel_receiver) = unbounded_channel();
        let (terminate_sender, mut terminate_receiver) = unbounded_channel();
        let (cleanup_sender, mut cleanup_receiver) = unbounded_channel();
        let workflow = context.spawn_io_workflow(|scope, builder| {
            let input = scope.input.fork_clone(builder);

            let cancel_buffer = builder.create_buffer(BufferSettings::default());
            let input_to_cancel = input.clone_output(builder);
            builder.connect(input_to_cancel, cancel_buffer.input_slot());

            let terminate_buffer = builder.create_buffer(BufferSettings::default());
            let input_to_terminate = input.clone_output(builder);
            builder.connect(input_to_terminate, terminate_buffer.input_slot());

            let cleanup_buffer = builder.create_buffer(BufferSettings::default());
            let input_to_cleanup = input.clone_output(builder);
            builder.connect(input_to_cleanup, cleanup_buffer.input_slot());

            let filter_node =
                builder.create_map_block(|value: u64| if value >= 5 { Some(value) } else { None });
            let input_to_filter_node = input.clone_output(builder);
            builder.connect(input_to_filter_node, filter_node.input);
            filter_node
                .output
                .chain(builder)
                .cancel_on_none()
                .connect(scope.terminate);

            builder.on_cancel(cancel_buffer, |scope, builder| {
                scope
                    .input
                    .chain(builder)
                    .consume_buffer::<8>()
                    .map_block(move |values| {
                        for value in values {
                            cancel_sender.send(value).unwrap();
                        }
                    })
                    .connect(scope.terminate);
            });

            builder.on_terminate(terminate_buffer, |scope, builder| {
                scope
                    .input
                    .chain(builder)
                    .consume_buffer::<8>()
                    .map_block(move |values| {
                        for value in values {
                            terminate_sender.send(value).unwrap();
                        }
                    })
                    .connect(scope.terminate);
            });

            builder.on_cleanup(cleanup_buffer, |scope, builder| {
                scope
                    .input
                    .chain(builder)
                    .consume_buffer::<8>()
                    .map_block(move |values| {
                        for value in values {
                            cleanup_sender.send(value).unwrap();
                        }
                    })
                    .connect(scope.terminate);
            });
        });

        let mut promise = context.command(|commands| commands.request(3, workflow).take_response());

        context.run_with_conditions(&mut promise, 10);
        assert!(promise.peek().is_cancelled());
        assert_eq!(cancel_receiver.try_recv().unwrap(), 3);
        assert!(cancel_receiver.try_recv().is_err());
        assert_eq!(cleanup_receiver.try_recv().unwrap(), 3);
        assert!(cleanup_receiver.try_recv().is_err());
        assert!(terminate_receiver.try_recv().is_err());
        assert!(context.no_unhandled_errors());
        assert!(context.confirm_buffers_empty().is_ok());

        let mut promise = context.command(|commands| commands.request(6, workflow).take_response());

        context.run_with_conditions(&mut promise, 10);
        assert!(promise.take().available().is_some_and(|v| v == 6));
        assert_eq!(terminate_receiver.try_recv().unwrap(), 6);
        assert!(terminate_receiver.try_recv().is_err());
        assert_eq!(cleanup_receiver.try_recv().unwrap(), 6);
        assert!(cleanup_receiver.try_recv().is_err());
        assert!(cancel_receiver.try_recv().is_err());
        assert!(context.no_unhandled_errors());
        assert!(context.confirm_buffers_empty().is_ok());
    }

    #[test]
    fn test_double_collection() {
        let mut context = TestingContext::minimal_plugins();

        let delay = context.spawn_delay(Duration::from_secs_f32(0.01));

        let workflow = context.spawn_io_workflow(|scope, builder| {
            // We make the later collect node first so that its disposal update
            // gets triggered first. If we don't implement collect correctly
            // then the later collect may think it's unreachable and send out
            // its collection prematurely. This test is checking for that edge
            // case.
            let later_collect = builder.create_collect_all::<i32, 8>();
            let earlier_collect = builder.create_collect_all::<i32, 8>();

            scope
                .input
                .chain(builder)
                .spread()
                .then(delay)
                .map_block(|v| if v <= 4 { Some(v) } else { None })
                .dispose_on_none()
                .connect(earlier_collect.input);

            earlier_collect
                .output
                .chain(builder)
                .spread()
                .connect(later_collect.input);

            later_collect.output.chain(builder).connect(scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request([1, 2, 3, 4, 5], workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(5));
        assert!(promise
            .take()
            .available()
            .is_some_and(|v| &v[..] == [1, 2, 3, 4]));
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            // We create a circular dependency between two collect operations.
            // This makes it impossible to determine which should fire when the
            // upstream is exhausted because both collect operations are upstream
            // of each other. This workflow should be cancelled before it even
            // starts.
            let earlier_collect = builder.create_collect_all::<i32, 8>();
            let later_collect = builder.create_collect_all::<i32, 8>();

            scope
                .input
                .chain(builder)
                .spread()
                .then(delay)
                .connect(earlier_collect.input);

            earlier_collect
                .output
                .chain(builder)
                .spread()
                .connect(later_collect.input);

            later_collect.output.chain(builder).spread().fork_clone((
                |chain: Chain<i32>| chain.connect(earlier_collect.input),
                |chain: Chain<i32>| chain.connect(scope.terminate),
            ));
        });

        let mut promise =
            context.command(|commands| commands.request([1, 2, 3, 4, 5], workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().is_cancelled());
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            // We create a circulaur dependency between two collect operations,
            // but this time one of those collect operations is scoped, and that
            // disambiguates the behavior of the collect operations. We should
            // get the intuitive workflow output from this.
            let earlier_collect = builder.create_collect_all::<i32, 8>();

            scope
                .input
                .chain(builder)
                .spread()
                .then(delay)
                .map_block(|v| if v <= 4 { Some(v) } else { None })
                .dispose_on_none()
                .connect(earlier_collect.input);

            let _ = earlier_collect
                .output
                .chain(builder)
                .then_io_scope(|scope, builder| {
                    scope
                        .input
                        .chain(builder)
                        .spread()
                        .collect_all::<8>()
                        .connect(scope.terminate);
                })
                .fork_clone((
                    |chain: Chain<_>| chain.spread().connect(earlier_collect.input),
                    |chain: Chain<_>| chain.connect(scope.terminate),
                ));
        });

        check_collections(workflow, [1, 2, 3, 4], [1, 2, 3, 4], &mut context);
        check_collections(workflow, [1, 2, 3, 4, 5, 6], [1, 2, 3, 4], &mut context);
        check_collections(workflow, [1, 8, 2, 7, 3, 6], [1, 2, 3], &mut context);
        check_collections(
            workflow,
            [8, 7, 6, 5, 4, 3, 2, 1],
            [4, 3, 2, 1],
            &mut context,
        );
        check_collections(workflow, [6, 7, 8, 9, 10], [], &mut context);
    }

    fn check_collections(
        workflow: Service<SmallVec<[i32; 8]>, SmallVec<[i32; 8]>>,
        input: impl IntoIterator<Item = i32>,
        expectation: impl IntoIterator<Item = i32>,
        context: &mut TestingContext,
    ) {
        let input: SmallVec<[i32; 8]> = SmallVec::from_iter(input);
        let expectation: SmallVec<[i32; 8]> = SmallVec::from_iter(expectation);
        let mut promise =
            context.command(|commands| commands.request(input, workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|v| v == expectation));
        assert!(context.no_unhandled_errors());
    }
}
