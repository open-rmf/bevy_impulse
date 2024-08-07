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

use std::future::Future;

use bevy_ecs::prelude::Entity;

use smallvec::SmallVec;

use std::error::Error;

use crate::{
    UnusedTarget, AddOperation, Node, InputSlot, Builder, IntoBlockingCallback,
    StreamPack, Provider, ProvideOnce, Scope, StreamOf, Trim, TrimBranch,
    AsMap, IntoBlockingMap, IntoAsyncMap, Output, Noop,
    ForkTargetStorage, StreamTargetMap, ScopeSettings, CreateCancelFilter,
    CreateDisposalFilter, Bufferable, BufferKey, BufferKeys, OperateBufferAccess,
    GateRequest, OperateDynamicGate, OperateStaticGate, Gate, Buffered,
    Spread, Collect, Sendish, Service, Cancellation,
    make_result_branching, make_option_branching,
};

pub mod fork_clone_builder;
pub use fork_clone_builder::*;

pub(crate) mod premade;
use premade::*;

pub mod unzip;
pub use unzip::*;


/// Chain operations onto the output of a workflow node.
///
/// Make sure to use [`Self::connect`] when you're done chaining so that the
/// final output of the chain gets connected into another node. If the final
/// output of the chain is meant to be the final output of your workflow then
/// you should connect it to [`Scope::terminate`].
#[must_use]
pub struct Chain<'w, 's, 'a, 'b, T> {
    target: Entity,
    builder: &'b mut Builder<'w, 's, 'a>,
    _ignore: std::marker::PhantomData<T>,
}

impl<'w, 's, 'a, 'b, T: 'static + Send + Sync> Chain<'w, 's, 'a, 'b, T> {
    /// Get the raw [`Output`] slot for the current link in the chain. You can
    /// use this to resume building this chain later.
    ///
    /// Note that if you do not connect some path of your workflow into the
    /// `terminate` slot of your [`Scope`] then the workflow will not be able
    /// to run.
    #[must_use]
    pub fn output(self) -> Output<T> {
        Output::new(self.scope(), self.target)
    }

    /// Connect this output into an input slot.
    ///
    /// Pass a [terminate](crate::Scope::terminate) into this function to
    /// end a chain.
    pub fn connect(self, input: InputSlot<T>) {
        let output = Output::new(self.scope(), self.target);
        self.builder.connect(output, input)
    }

    /// Explicitly terminate the chain by indicating that you want it to remain
    /// unused.
    pub fn unused(self) {
        // Do nothing
    }

    /// Connect the response at the end of the chain into a new provider. Get
    /// the response of the new provider as a chain so you can continue chaining
    /// operations.
    #[must_use]
    pub fn then<P: Provider<Request = T>>(
        self,
        provider: P,
    ) -> Chain<'w, 's, 'a, 'b, P::Response>
    where
        P::Response: 'static + Send + Sync,
    {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();
        provider.connect(Some(self.builder.scope), source, target, self.builder.commands);
        Chain::new(target, self.builder)
    }

    /// Connect the response in the chain into a new provider. Get the node
    /// slots that wrap around the new provider.
    #[must_use]
    pub fn then_node<P: Provider<Request = T>>(
        self,
        provider: P,
    ) -> Node<T, P::Response, P::Streams>
    where
        P::Response: 'static + Send + Sync,
        P::Streams: StreamPack,
    {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();
        provider.connect(Some(self.scope()), source, target, self.builder.commands);

        let mut map = StreamTargetMap::default();
        let (bundle, streams) = <P::Streams as StreamPack>::spawn_node_streams(
            source, &mut map, self.builder,
        );
        self.builder.commands.entity(source).insert((bundle, map));
        Node {
            input: InputSlot::new(self.builder.scope, source),
            output: Output::new(self.builder.scope, target),
            streams,
        }
    }

    /// Apply a function whose input is [`BlockingMap<T>`](crate::BlockingMap)
    /// or [`AsyncMap<T>`](crate::AsyncMap).
    #[must_use]
    pub fn map<M, F: AsMap<M>>(
        self,
        f: F,
    ) -> Chain<'w, 's, 'a, 'b, <F::MapType as ProvideOnce>::Response>
    where
        F::MapType: Provider<Request=T>,
        <F::MapType as ProvideOnce>::Response: 'static + Send + Sync,
    {
        self.then(f.as_map())
    }

    /// Same as [`Self::map`] but receive the new node instead of continuing a
    /// chain.
    #[must_use]
    pub fn map_node<M, F: AsMap<M>>(
        self,
        f: F,
    ) -> Node<T, <F::MapType as ProvideOnce>::Response, <F::MapType as ProvideOnce>::Streams>
    where
        F::MapType: Provider<Request = T>,
        <F::MapType as ProvideOnce>::Response: 'static + Send + Sync,
        <F::MapType as ProvideOnce>::Streams: StreamPack,
    {
        self.then_node(f.as_map())
    }

    /// Apply a function whose input is the Response of the current Chain. The
    /// output of the map will be the Response of the returned Chain.
    ///
    /// This takes in a regular blocking function rather than an async function,
    /// so while the function is executing, it will block all systems and all
    /// other workflows from running.
    #[must_use]
    pub fn map_block<U>(
        self,
        f: impl FnMut(T) -> U + 'static + Send + Sync,
    ) -> Chain<'w, 's, 'a, 'b, U>
    where
        U: 'static + Send + Sync,
    {
        self.then(f.into_blocking_map())
    }

    /// Same as [`Self::map_block`] but receive the new node instead of
    /// continuing a chain.
    #[must_use]
    pub fn map_block_node<U>(
        self,
        f: impl FnMut(T) -> U + 'static + Send + Sync,
    ) -> Node<T, U, ()>
    where
        U: 'static + Send + Sync,
    {
        self.then_node(f.into_blocking_map())
    }

    /// Apply a map whose output is a Future that will be run in the
    /// [`AsyncComputeTaskPool`](bevy::tasks::AsyncComputeTaskPool) (unless
    /// the `single_threaded_async` feature is active). The output of the Future
    /// will be the Response of the returned Chain.
    #[must_use]
    pub fn map_async<Task>(
        self,
        f: impl FnMut(T) -> Task + 'static + Send + Sync,
    ) -> Chain<'w, 's, 'a, 'b, Task::Output>
    where
        Task: Future + 'static + Sendish,
        Task::Output: 'static + Send + Sync,
    {
        self.then(f.into_async_map())
    }

    /// Same as [`Self::map_async`] but receive the new node instead of
    /// continuing a chain.
    #[must_use]
    pub fn map_async_node<Task>(
        self,
        f: impl FnMut(T) -> Task + 'static + Send + Sync,
    ) -> Node<T, Task::Output, ()>
    where
        Task: Future + 'static + Sendish,
        Task::Output: 'static + Send + Sync,
    {
        self.then_node(f.into_async_map())
    }

    /// Build a workflow scope to be used as an element in this chain.
    ///
    /// If you want to connect to the stream outputs or be able to loop back
    /// to the input of this scope, use [`Self::then_scope_node`] instead.
    #[must_use]
    pub fn then_scope<Response, Streams, Settings>(
        self,
        build: impl FnOnce(Scope<T, Response, Streams>, &mut Builder) -> Settings,
    ) -> Chain<'w, 's, 'a, 'b, Response>
    where
        Response: 'static + Send + Sync,
        Streams: StreamPack,
        Settings: Into<ScopeSettings>,
    {
        let exit_scope = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.create_scope_impl::<T, Response, Streams, Settings>(
            self.target, exit_scope, build,
        ).output.chain(self.builder)
    }

    /// Simplified version of [`Self::then_scope`] limited to a simple input and
    /// output.
    ///
    /// Unlike `then_scope`, this function can infer the types for the generics
    /// so you don't need to explicitly specify them.
    pub fn then_io_scope<Response, Settings>(
        self,
        build: impl FnOnce(Scope<T, Response>, &mut Builder) -> Settings,
    ) -> Chain<'w, 's, 'a, 'b, Response>
    where
        Response: 'static + Send + Sync,
        Settings: Into<ScopeSettings>,
    {
        self.then_scope(build)
    }

    /// From the current target in the chain, build a [scoped](Scope) workflow
    /// and then get back a node that represents that scoped workflow.
    #[must_use]
    pub fn then_scope_node<Response, Streams, Settings>(
        self,
        build: impl FnOnce(Scope<T, Response, Streams>, &mut Builder) -> Settings,
    ) -> Node<T, Response, Streams>
    where
        Response: 'static + Send + Sync,
        Streams: StreamPack,
        Settings: Into<ScopeSettings>,
    {
        let exit_scope = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.create_scope_impl::<T, Response, Streams, Settings>(
            self.target, exit_scope, build,
        )
    }

    /// Simplified version of [`Self::then_scope_node`] limited to a simple
    /// input and output.
    ///
    /// Unlike `then_scope_node`, this function can infer the types for the
    /// generics so you don't need to explicitly specify them.
    pub fn then_io_scope_node<Response, Settings>(
        self,
        build: impl FnOnce(Scope<T, Response>, &mut Builder) -> Settings,
    ) -> Node<T, Response, ()>
    where
        Response: 'static + Send + Sync,
        Settings: Into<ScopeSettings>,
    {
        self.then_scope_node(build)
    }

    /// Many services just need to be triggered without being give any particular
    /// input. The convention for those services is to take an input of `()`.
    ///
    /// We call this "triggering", and this function is a convenient way to turn
    /// any output type into a trigger.
    pub fn trigger(self) -> Chain<'w, 's, 'a, 'b, ()> {
        self.map_block(|_| ())
    }

    /// Combine the output with access to some buffers. You must specify one or
    /// more buffers to access. For multiple buffers, combine them into a tuple
    /// or an [`Iterator`]. Tuples of buffers can be nested inside each other.
    ///
    /// Other [outputs](Output) can also be passed in as buffers. Those outputs
    /// will be transformed into a buffer with default buffer settings.
    ///
    /// To obtain a set of buffer keys each time a buffer is modified, use
    /// [`listen`](crate::Bufferable::listen).
    pub fn with_access<B>(
        self,
        buffers: B,
    ) -> Chain<'w, 's, 'a, 'b, (T, BufferKeys<B>)>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
        BufferKeys<B>: 'static + Send + Sync,
    {
        let buffers = buffers.as_buffer(self.builder);
        buffers.verify_scope(self.builder.scope);

        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.commands.add(AddOperation::new(
            Some(self.builder.scope),
            source,
            OperateBufferAccess::<T, B::BufferType>::new(buffers, target),
        ));

        Chain::new(target, self.builder)
    }

    /// Apply a [`Provider`] that filters the response by returning an [`Option`].
    /// If the filter returns [`None`] then a [`Cancellation`](crate::Cancellation)
    /// is triggered. Otherwise the chain continues with the value that was
    /// inside [`Some`].
    ///
    /// This is conceptually similar to [`Iterator::filter_map`]. You can also
    /// use [`Chain::disposal_filter`] to dispose of the value instead of
    /// cancelling the entire scope.
    #[must_use]
    pub fn cancellation_filter<ThenResponse, F>(
        self,
        filter_provider: F
    ) -> Chain<'w, 's, 'a, 'b, ThenResponse>
    where
        ThenResponse: 'static + Send + Sync,
        F: Provider<Request = T, Response = Option<ThenResponse>>,
        F::Response: 'static + Send + Sync,
        F::Streams: StreamPack,
    {
        self.then(filter_provider).cancel_on_none()
    }

    /// Same as [`Chain::cancellation_filter`] but the chain will be disposed
    /// instead of cancelled, so the workflow may continue if the termination
    /// node can still be reached.
    pub fn disposal_filter<ThenResponse, F>(
        self,
        filter_provider: F,
    ) -> Chain<'w, 's, 'a, 'b, ThenResponse>
    where
        ThenResponse: 'static + Send + Sync,
        F: Provider<Request = T, Response = Option<ThenResponse>>,
        F::Response: 'static + Send + Sync,
        F::Streams: StreamPack,
    {
        self.then(filter_provider).dispose_on_none()
    }

    /// When the response is delivered, we will make a clone of it and
    /// simultaneously pass that clone along two different branches chains: one
    /// determined by the `build` function passed into this operation and the
    /// other determined by the [`Chain`] that gets returned.
    ///
    /// This can only be applied when `Response` can be cloned.
    ///
    /// See also [`Chain::fork_clone`]
    #[must_use]
    pub fn branch_clone(
        self,
        build: impl FnOnce(Chain<T>),
    ) -> Chain<'w, 's, 'a, 'b, T>
    where
        T: Clone,
    {
        Chain::<T>::new(self.target, self.builder)
            .fork_clone((
                |chain: Chain<T>| chain.output(),
                build,
            )).0
            .chain(self.builder)
    }

    /// When the response is delivered, we will make clones of it and
    /// simultaneously pass that clone along mutliple impulse chains, each one
    /// determined by a different element of the tuple that gets passed in as
    /// a builder.
    ///
    /// The return values of the individual chain builders will be zipped into
    /// one tuple return value by this function. If all of the builders return
    /// [`Output`] then you can easily continue chaining more operations using
    /// [`join`](crate::Bufferable::join), or destructure them into individual
    /// outputs that you can continue to build with.
    pub fn fork_clone<Build: ForkCloneBuilder<T>>(
        self,
        build: Build,
    ) -> Build::Outputs
    where
        T: Clone,
    {
        build.build_fork_clone(Output::new(self.scope(), self.target), self.builder)
    }

    /// If you have a `Chain<(A, B, C, ...)>` with a tuple response then
    /// `unzip` allows you to convert it into a tuple of chains:
    /// `(Output<A>, Output<B>, Output<C>, ...)`.
    ///
    /// You can also consider using `fork_unzip` to continue building each
    /// chain in the tuple independently by providing a builder function for
    /// each element of the tuple.
    pub fn unzip(self) -> T::Unzipped
    where
        T: Unzippable,
    {
        T::unzip_output(Output::<T>::new(self.scope(), self.target), self.builder)
    }

    /// If you have a `Chain<(A, B, C, ...)>` with a tuple response then
    /// `fork_unzip` allows you to split it into multiple chains (one for each
    /// tuple element) and apply a separate builder function to each chain. You
    /// will be passed back the zipped return values of all the builder functions.
    pub fn fork_unzip<Build>(self, build: Build) -> Build::ReturnType
    where
        Build: UnzipBuilder<T>
    {
        build.fork_unzip(Output::<T>::new(self.scope(), self.target), self.builder)
    }

    /// If `T` implements [`Iterator`] then you can fire off each of its elements
    /// as a new thread within the workflow. Each thread will still have the same
    /// session ID.
    ///
    /// This is similar to streams which can produce multiple outputs, which also
    /// potentially creates multiple threads in the workflow. If the input to
    /// the spread operator is empty, then a disposal notice will go out, and
    /// the workflow will be cancelled if it is no longer possible to reach the
    /// terminal node.
    pub fn spread(self) -> Chain<'w, 's, 'a, 'b, T::Item>
    where
        T: IntoIterator,
        T::Item: 'static + Send + Sync,
    {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.commands.add(AddOperation::new(
            Some(self.builder.scope),
            source,
            Spread::<T>::new(target),
        ));

        Chain::new(target, self.builder)
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
    pub fn collect<const N: usize>(
        self,
        min: usize,
        max: Option<usize>,
    ) -> Chain<'w, 's, 'a, 'b, SmallVec<[T; N]>> {
        if let Some(max) = max {
            assert!(0 < max);
            assert!(min <= max);
        }

        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.commands.add(AddOperation::new(
            Some(self.builder.scope),
            source,
            Collect::<T, N>::new(target, min, max),
        ));

        Chain::new(target, self.builder)
    }

    /// Collect all workflow threads that are moving towards this node.
    pub fn collect_all<const N: usize>(self) -> Chain<'w, 's, 'a, 'b, SmallVec<[T; N]>> {
        self.collect::<N>(0, None)
    }

    /// Collect an exact number of threads that are moving towards this node.
    pub fn collect_n<const N: usize>(self, n: usize) -> Chain<'w, 's, 'a, 'b, SmallVec<[T; N]>> {
        self.collect::<N>(n, Some(n))
    }

    // TODO(@mxgrey): We could offer a collect_array that always collects exactly
    // N, defined at compile time, and outputs a fixed-size array. This will require
    // a second implementation of Collect, or a more generic implementation of it.

    /// Run a trimming operation when the workflow reaches this point.
    ///
    /// See also: [`Self::then_trim_node`], [`Builder::create_trim`].
    pub fn then_trim(
        self,
        branches: impl IntoIterator<Item=TrimBranch>,
    ) -> Chain<'w, 's, 'a, 'b, T> {
        let branches: SmallVec<[_; 16]> = branches.into_iter().collect();
        for branch in &branches {
            branch.verify_scope(self.builder.scope);
        }

        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.commands.add(AddOperation::new(
            Some(self.builder.scope),
            source,
            Trim::<T>::new(branches, target),
        ));

        Chain::new(target, self.builder)
    }

    /// Run a trimming operation when the workflow reaches this point. This will
    /// return a [`Node`] so you can connect other inputs into the operation.
    ///
    /// See also: [`Self::then_trim`], [`Builder::create_trim`].
    pub fn then_trim_node(
        self,
        branches: impl IntoIterator<Item = TrimBranch>,
    ) -> Node<T, T> {
        let source = self.target;
        let scope = self.builder.scope;
        let target = self.then_trim(branches).output().id();
        Node {
            input: InputSlot::new(scope, source),
            output: Output::new(scope, target),
            streams: (),
        }
    }

    /// Apply a [gate action](Gate) to one or more buffers at this point
    /// in the workflow.
    pub fn then_gate_action<B>(
        self,
        action: Gate,
        buffers: B,
    ) -> Chain<'w, 's, 'a, 'b, T>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
    {
        let buffers = buffers.as_buffer(self.builder);
        buffers.verify_scope(self.builder.scope);

        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.commands.add(AddOperation::new(
            Some(self.builder.scope),
            source,
            OperateStaticGate::<T, _>::new(buffers, target, action),
        ));

        Chain::new(target, self.builder)
    }

    /// [Open the gates](Gate) of one or more buffers at this point in the
    /// workflow.
    pub fn then_gate_open<B>(self, buffers: B) -> Chain<'w, 's, 'a, 'b, T>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
    {
        self.then_gate_action(Gate::Open, buffers)
    }

    /// [Close the gates](Gate) of one or more buffers at this point in
    /// the workflow.
    pub fn then_gate_close<B>(self, buffers: B) -> Chain<'w, 's, 'a, 'b, T>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
    {
        self.then_gate_action(Gate::Closed, buffers)
    }

    /// If the chain's response implements the [`Future`] trait, applying
    /// `.flatten()` to the chain will yield the output of that Future as the
    /// chain's response.
    #[must_use]
    pub fn flatten(self) -> Chain<'w, 's, 'a, 'b, T::Output>
    where
        T: Future,
        T::Output: 'static + Send + Sync,
    {
        self.map_async(|r| async { r.await })
    }

    /// Add a [no-op][1] to the current end of the chain.
    ///
    /// As the name suggests, a no-op will not actually do anything, but it adds
    /// a new operation point into the workflow. That operation point could be
    /// used as a reference point for other operations, like
    /// [trimming](Self::then_trim).
    ///
    /// [1]: `<https://en.wikipedia.org/wiki/NOP_(code)>`
    #[must_use]
    pub fn noop(self) -> Chain<'w, 's, 'a, 'b, T> {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()), source, Noop::<T>::new(target),
        ));
        Chain::new(target, self.builder)
    }

    /// Get a whole node that is simply a [no-op](Self::noop).
    pub fn noop_node(self) -> Node<T, T> {
        let source = self.target;
        let scope = self.builder.scope;
        let target = self.noop().output().id();
        Node {
            input: InputSlot::new(scope, source),
            output: Output::new(scope, target),
            streams: (),
        }
    }

    pub fn scope(&self) -> Entity {
        self.builder.scope
    }

    pub fn target(&self) -> Entity {
        self.target
    }
}

impl<'w, 's, 'a, 'b, T> Chain<'w, 's, 'a, 'b, StreamOf<T>>
where
    T: 'static + Send + Sync,
{
    /// When the output value is wrapped in a [`StreamOf`] container, this will
    /// strip it out of that wrapper.
    pub fn inner(self) -> Chain<'w, 's, 'a, 'b, T> {
        self.map_block(|v| v.0)
    }
}

impl<'w, 's, 'a, 'b, T, E> Chain<'w, 's, 'a, 'b, Result<T, E>>
where
    T: 'static + Send + Sync,
    E: 'static + Send + Sync,
{
    /// Build a chain that activates if the response is an [`Err`]. If the
    /// response is [`Ok`] then this branch will not be activated.
    ///
    /// This function returns a chain that will be activated if the result was
    /// [`Ok`] so you can continue building your response to the [`Ok`] case.
    #[must_use]
    pub fn branch_for_err(
        self,
        build_err: impl FnOnce(Chain<E>),
    ) -> Chain<'w, 's, 'a, 'b, T> {
        Chain::<Result<T, E>>::new(
            self.target, self.builder,
        ).fork_result(
            |chain| chain.output(),
            build_err,
        ).0.chain(self.builder)
    }

    /// Build two branching chains, one for the case where the response is [`Ok`]
    /// and one if the response is [`Err`]. Whichever chain does not get activated
    /// will be disposed, which means it gets dropped without triggering any
    /// cancellation effects.
    ///
    /// The outputs of both builder functions will be zipped as the return value
    /// of this function.
    pub fn fork_result<U, V>(
        self,
        build_ok: impl FnOnce(Chain<T>) -> U,
        build_err: impl FnOnce(Chain<E>) -> V,
    ) -> (U, V) {
        let source = self.target;
        let target_ok = self.builder.commands.spawn(UnusedTarget).id();
        let target_err = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()),
            source,
            make_result_branching::<T, E>(
                ForkTargetStorage::from_iter([target_ok, target_err])
            ),
        ));

        let u = build_ok(Chain::new(target_ok, self.builder));
        let v = build_err(Chain::new(target_err, self.builder));
        (u, v)
    }

    /// If the result contains an [`Err`] value then the entire scope that
    /// contains this operation will be immediately cancelled. If the scope is
    /// within a node of an outer workflow, then the node will emit a disposal
    /// for its outer workflow. Otherwise if this is the root scope of a workflow
    /// then the whole workflow is immediately cancelled. This effect will happen
    /// even if the scope is set to be uninterruptible.
    ///
    /// This operation only works for results with an [`Err`] variant that
    /// implements the [`Error`] trait. If your [`Err`] variant does not implement
    /// that trait, then you can use [`Self::cancel_on_quiet_err`] instead.
    ///
    /// ```
    /// use bevy_impulse::{*, testing::*};
    ///
    /// let mut context = TestingContext::minimal_plugins();
    ///
    /// let workflow = context.spawn_io_workflow(|scope, builder| {
    ///     scope.input.chain(builder)
    ///         .map_block(produce_err)
    ///         .cancel_on_err()
    ///         .connect(scope.terminate);
    /// });
    ///
    /// let mut promise = context.command(|commands| {
    ///     commands
    ///     .request("hello", workflow)
    ///     .take_response()
    /// });
    ///
    /// context.run_while_pending(&mut promise);
    /// assert!(promise.peek().is_cancelled());
    /// ```
    #[must_use]
    pub fn cancel_on_err(self) -> Chain<'w, 's, 'a, 'b, T>
    where
        E: Error,
    {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()),
            source,
            CreateCancelFilter::on_err::<T, E>(target),
        ));

        Chain::new(target, self.builder)
    }

    /// Same as [`Self::cancel_on_err`] except it also works for [`Err`] variants
    /// that do not implement [`Error`]. The catch is that their error message
    /// will not be included in the [`Filtered`](crate::Filtered) information
    /// that gets propagated outward.
    #[must_use]
    pub fn cancel_on_quiet_err(self) -> Chain<'w, 's, 'a, 'b, T> {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()),
            source,
            CreateCancelFilter::on_quiet_err::<T, E>(target),
        ));

        Chain::new(target, self.builder)
    }

    /// If the output contains an [`Err`] value then the output will be disposed.
    ///
    /// Disposal means that the node that the output is connected to will simply
    /// not be triggered, but the workflow is not necessarily cancelled. If a
    /// disposal makes it impossible for the workflow to terminate, then the
    /// workflow will be cancelled immediately.
    #[must_use]
    pub fn dispose_on_err(self) -> Chain<'w, 's, 'a, 'b, T>
    where
        E: Error,
    {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()),
            source,
            CreateDisposalFilter::on_err::<T, E>(target),
        ));

        Chain::new(target, self.builder)
    }

    #[must_use]
    pub fn dispose_on_quiet_err(self) -> Chain<'w, 's, 'a, 'b, T> {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()),
            source,
            CreateDisposalFilter::on_quiet_err::<T, E>(target),
        ));

        Chain::new(target, self.builder)
    }
}

impl<'w, 's, 'a, 'b, T> Chain<'w, 's, 'a, 'b, Option<T>>
where
    T: 'static + Send + Sync,
{
    /// Build a chain that activates if the response is [`None`]. If the response
    /// is [`Some`] then the branch built by this function will not be activated.
    ///
    /// This function returns a chain that will be activated if the result was
    /// [`Some`] so you can continue building your response to the [`Some`] case.
    #[must_use]
    pub fn branch_for_none(
        self,
        build_none: impl FnOnce(Chain<()>),
    ) -> Chain<'w, 's, 'a, 'b, T> {
        Chain::<Option<T>>::new(
            self.target, self.builder,
        ).fork_option(
            |chain| chain.output(),
            build_none,
        ).0.chain(self.builder)
    }

    /// Build two branching chains, one for the case where the response is [`Some`]
    /// and one if the response is [`None`]. Whichever chain does not get activated
    /// will be disposed, which means it gets dropped without triggering any
    /// cancellation effects.
    ///
    /// The outputs of both builder functions will be zipped as the return value
    /// of this function.
    pub fn fork_option<U, V>(
        self,
        build_some: impl FnOnce(Chain<T>) -> U,
        build_none: impl FnOnce(Chain<()>) -> V,
    ) -> (U, V) {
        let source = self.target;
        let target_some = self.builder.commands.spawn(UnusedTarget).id();
        let target_none = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()),
            source,
            make_option_branching::<T>(
                ForkTargetStorage::from_iter([target_some, target_none])
            ),
        ));

        let u = build_some(Chain::new(target_some, self.builder));
        let v = build_none(Chain::new(target_none, self.builder));
        (u, v)
    }

    /// If the result contains a [`None`] value then the chain will be cancelled
    /// from this link onwards. The next link in the chain will receive a `T` if
    /// the chain is not cancelled.
    #[must_use]
    pub fn cancel_on_none(self) -> Chain<'w, 's, 'a, 'b, T> {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()),
            source,
            CreateCancelFilter::on_none::<T>(target),
        ));

        Chain::new(target, self.builder)
    }

    /// If the output contains [`None`] value then the output will be disposed.
    ///
    /// Disposal means that the node that the output is connected to will simply
    /// not be triggered, but the workflow is not necessarily cancelled. If a
    /// disposal makes it impossible for the workflow to terminate, then the
    /// workflow will be cancelled immediately.
    #[must_use]
    pub fn dispose_on_none(self) -> Chain<'w, 's, 'a, 'b, T> {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            Some(self.scope()),
            source,
            CreateDisposalFilter::on_none::<T>(target),
        ));

        Chain::new(target, self.builder)
    }
}

impl<'w, 's, 'a, 'b, Request, Response, Streams> Chain<'w, 's, 'a, 'b, (Request, Service<Request, Response, Streams>)>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync + Unpin,
    Streams: StreamPack,
    Streams::Receiver: Unpin,
{
    /// Given the input `(request, service)`, pass `request` into `service` and
    /// forward its response. This is called `injection` because it's a
    /// [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection)
    /// operation.
    ///
    /// Since it's possible for `service` to fail for
    /// various reasons, this returns a [`Result`]. Follow this with
    /// `.dispose_on_err` to filter away errors.
    ///
    /// To access the streams of the service, use [`Chain::then_request_node`].
    pub fn then_injection(self) -> Chain<'w, 's, 'a, 'b, Result<Response, Cancellation>> {
        self.map(inject_service)
    }

    /// Given the input `(request, service)`, pass `request` into `service` and
    /// forward its streams and response. This is called `injection` because it's a
    /// [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection)
    /// operation.
    ///
    /// Since it's possible for `service` to
    /// fail for various reasons, this returns a [`Result`]. Follow this with
    /// `.dispose_on_err` to filter away errors.
    pub fn then_injection_node(self) -> Node<
        (Request, Service<Request, Response, Streams>),
        Result<Response, Cancellation>,
        Streams,
    > {
        self.map_node(inject_service)
    }
}

impl<'w, 's, 'a, 'b, T> Chain<'w, 's, 'a, 'b, GateRequest<T>>
where
    T: 'static + Send + Sync,
{
    pub fn then_gate<B>(self, buffers: B) -> Chain<'w, 's, 'a, 'b, T>
    where
        B: Bufferable,
        B::BufferType: 'static + Send + Sync,
    {
        let buffers = buffers.as_buffer(self.builder);
        buffers.verify_scope(self.builder.scope);

        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();
        self.builder.commands.add(AddOperation::new(
            Some(self.builder.scope),
            source,
            OperateDynamicGate::<T, _>::new(buffers, target),
        ));

        Chain::new(target, self.builder)
    }
}

impl<'w, 's, 'a, 'b, T> Chain<'w, 's, 'a, 'b, BufferKey<T>>
where
    T: 'static + Send + Sync,
{
    pub fn consume_buffer<const N: usize>(self) -> Chain<'w, 's, 'a, 'b, SmallVec<[T; N]>> {
        self.then(consume_buffer.into_blocking_callback())
    }
}

impl<'w, 's, 'a, 'b, T: 'static + Send + Sync> Chain<'w, 's, 'a, 'b, T> {
    /// Used internally to create a [`Chain`] that can accept a label
    /// and hook into streams.
    pub(crate) fn new(
        target: Entity,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Self {
        Self {
            target,
            builder,
            _ignore: Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{*, testing::*};
    use smallvec::SmallVec;

    #[test]
    fn test_join() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder)
                // (2.0, 2.0)
                .fork_unzip((
                    |chain: Chain<f64>| chain
                        // 2.0
                        .map_block(|value|
                            WaitRequest {
                                duration: Duration::from_secs_f64(value/100.0),
                                value,
                            }
                        )
                        .map_async(wait)
                        // 2.0
                        .output(),
                    |chain: Chain<f64>| chain
                        // 2.0
                        .map_block(|value| 2.0*value)
                        // 4.0
                        .output(),
                ))
                .join(builder)
                // (2.0, 4.0)
                .map_block(add)
                // 6.0
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands|
            commands
            .request((2.0, 2.0), workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|value| value == 6.0));
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_race() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder)
            // (2.0, 2.0)
            .map_block(add)
            // 4.0
            .then_io_scope(|scope, builder| {
                scope.input.chain(builder)
                // 4.0
                .fork_clone((
                    |chain: Chain<f64>| {
                        // 4.0
                        chain.map_block(|value|
                            WaitRequest {
                                duration: Duration::from_secs_f64(0.01*value),
                                value,
                            }
                        )
                        .map_async(wait)
                        // 4.0
                        .connect(scope.terminate);
                    },
                    |chain: Chain<f64>| {
                        // 4.0
                        chain.map_block(|a| (a, a))
                        // (4.0, 4.0)
                        .map_block(add)
                        // 8.0
                        .connect(scope.terminate);
                    }
                ));
            })
            // This should be won by the 8.0 branch because it does not wait,
            // while the 4.0 branch should wait for 0.04s.
            .map_block(|a| (a, a))
            // (8.0, 8.0)
            .map_block(add)
            // 16.0
            .connect(scope.terminate);
        });

        let mut promise = context.command(|commands|
            commands
            .request((2.0, 2.0), workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, Duration::from_secs(2));

        assert_eq!(promise.take().available(), Some(16.0));
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_unzip() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder)
            .map_block(add)
            .map_block(|v| (v, 2.0*v))
            .then_io_scope(|scope, builder| {
                scope.input.chain(builder)
                .fork_unzip((
                    |chain: Chain<f64>| {
                        chain
                        .map_block(|v| (v, 10.0))
                        .map_block(add)
                        .connect(scope.terminate);
                    },
                    |chain: Chain<f64>| {
                        chain
                        .map_block(|value|
                            WaitRequest{
                                duration: std::time::Duration::from_secs_f64(0.01),
                                value,
                            }
                        )
                        .map_async(wait)
                        .connect(scope.terminate);
                    },
                ));
            })
            .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
            .request((2.0, 3.0), workflow)
            .take_response()
        });

        context.run_while_pending(&mut promise);
        assert_eq!(promise.take().available(), Some(15.0));
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_cancel_on_special_case() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder)
                .map_block(duplicate)
                .map_block(add)
                .map_block(produce_none)
                .cancel_on_none()
                .map_block(duplicate)
                .map_block(add)
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
            .request(2.0, workflow)
            .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.peek().is_cancelled());
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder)
                .map_block(duplicate)
                .map_block(add)
                .map_block(produce_err)
                .cancel_on_quiet_err()
                .map_block(duplicate)
                .map_block(add)
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
            .request(2.0, workflow)
            .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.peek().is_cancelled());
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_disposal() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder)
                .map_block(duplicate)
                .map_block(add)
                .map_block(produce_none)
                .dispose_on_none()
                .map_block(duplicate)
                .map_block(add)
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands| {
            commands
            .request(2.0, workflow)
            .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.peek().is_cancelled());
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(
            |scope: Scope<Result<f64, Result<f64, TestError>>, f64>, builder| {
                scope.input.chain(builder)
                    .fork_result(
                        |chain| chain.connect(scope.terminate),
                        |chain|
                            chain.dispose_on_err().connect(scope.terminate)
                    );
        });

        let mut promise = context.command(|commands| {
            commands
            .request(Ok(1.0), workflow)
            .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|v| v == 1.0));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands| {
            commands
            .request(Err(Ok(5.0)), workflow)
            .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|v| v == 5.0));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands| {
            commands
            .request(Err(Err(TestError)), workflow)
            .take_response()
        });

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.peek().is_cancelled());
        assert!(context.no_unhandled_errors());
    }

    #[test]
    fn test_spread() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let buffer = builder.create_buffer(BufferSettings::keep_all());

            scope.input.chain(builder)
                .map_block(|value| {
                    let mut duplicated_values: SmallVec<[i32; 16]> = SmallVec::new();
                    for _ in 0..value {
                        duplicated_values.push(value);
                    }
                    duplicated_values
                })
                .spread()
                .connect(buffer.input_slot());

            buffer.listen(builder)
                .then(watch_for_quantity.into_blocking_callback())
                .dispose_on_none()
                .connect(scope.terminate);

        });

        let mut promise = context.command(|commands|
            commands
            .request(7, workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, 1);
        assert!(promise.take().available().is_some_and(|v| {
            v.len() == 7 && v.iter().find(|a| **a != 7).is_none()
        }));
        assert!(context.no_unhandled_errors());
    }

    // This is essentially a collect operation specific to one of our spread
    // operation tests. We expect to gather up a number of elements equal to the
    // integer value of those elements. We don't use the collect operation for
    // this test so that we can test each of those operations in isolation.
    fn watch_for_quantity(
        In(key): In<BufferKey<i32>>,
        mut access: BufferAccessMut<i32>,
    ) -> Option<SmallVec<[i32; 16]>> {
        let mut buffer = access.get_mut(&key).unwrap();
        let expected_count = *buffer.newest()? as usize;
        if buffer.len() < expected_count {
            return None;
        }

        Some(buffer.drain(..).collect())
    }

    #[test]
    fn test_collect() {
        let mut context = TestingContext::minimal_plugins();

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let node = scope.input.chain(builder).map_node(
                |input: BlockingMap<i32, StreamOf<i32>>| {
                    for _ in 0..input.request {
                        input.streams.send(StreamOf(input.request));
                    }
                }
            );

            node.streams.chain(builder)
                .inner()
                .collect_all::<16>()
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands|
            commands
            .request(8, workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, 1);
        assert!(promise.take().available().is_some_and(|v| {
            v.len() == 8 && v.iter().find(|a| **a != 8).is_none()
        }));
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let node = scope.input.chain(builder).map_node(
                |input: BlockingMap<i32, StreamOf<i32>>| {
                    for _ in 0..input.request {
                        input.streams.send(StreamOf(input.request));
                    }
                }
            );

            node.streams.chain(builder)
                .inner()
                .collect::<16>(4, None)
                .connect(scope.terminate);
        });

        check_collection(0, 4, workflow, &mut context);
        check_collection(2, 4, workflow, &mut context);
        check_collection(3, 4, workflow, &mut context);
        check_collection(4, 4, workflow, &mut context);
        check_collection(5, 4, workflow, &mut context);
        check_collection(8, 4, workflow, &mut context);

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let bogus_node = builder.create_map_block(|v: i32| v);
            bogus_node.output.chain(builder)
                .collect_all::<16>()
                .connect(scope.terminate);

            scope.input.chain(builder)
                .map_block(|v: i32| Some(v))
                .fork_option(
                    |chain: Chain<i32>| chain
                        .map_async(|v| async move { v })
                        .collect_all::<16>()
                        .connect(scope.terminate),
                    |chain: Chain<()>| chain
                        .map_block(|()| unreachable!())
                        .unused(),
                );
        });

        let mut promise = context.command(|commands|
            commands
            .request(2, workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, Duration::from_secs(2));
        assert!(promise.take().available().is_some_and(|v|
            v.len() == 1 && v.iter().find(|a| **a != 2).is_none()
        ));
        assert!(context.no_unhandled_errors());

        let workflow = context.spawn_io_workflow(|scope, builder| {
            scope.input.chain(builder)
                .map_block(|v| if v < 4 { None } else { Some(v) })
                .dispose_on_none()
                .collect_all::<8>()
                .connect(scope.terminate);
        });

        let mut promise = context.command(|commands|
            commands
            .request(2, workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, 1);
        assert!(promise.take().available().is_some_and(|v| v.is_empty()));
        assert!(context.no_unhandled_errors());

        let mut promise = context.command(|commands|
            commands
            .request(5, workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, 1);
        assert!(promise.take().available().is_some_and(|v|
            v.len() == 1 && v.iter().find(|a| **a != 5).is_none()
        ));
        assert!(context.no_unhandled_errors());
    }

    fn check_collection(
        value: i32,
        min: i32,
        workflow: Service<i32, SmallVec<[i32; 16]>>,
        context: &mut TestingContext,
    ) {
        let mut promise = context.command(|commands|
            commands
            .request(value, workflow)
            .take_response()
        );

        context.run_with_conditions(&mut promise, 1);
        if value < min {
            assert!(promise.take().is_cancelled());
        } else {
            assert!(promise.take().available().is_some_and(|v| {
                v.len() == value as usize && v.iter().find(|a| **a != value).is_none()
            }));
        }
        assert!(context.no_unhandled_errors());
    }
}
