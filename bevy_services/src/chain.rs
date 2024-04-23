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

use crate::{
    UnusedTarget, Terminate, PerformOperation,
    ForkClone, Chosen, ApplyLabel, Stream, Provider,
    AsMap, IntoBlockingMap, IntoAsyncMap, OperateCancel,
    DetachDependency, DisposeOnCancel, Promise, Noop,
    Cancelled, ForkTargetStorage, Branching,
    make_result_branching, make_cancel_filter_on_err,
    make_option_branching, make_cancel_filter_on_none,
};

use bevy::prelude::{Entity, Commands};

use smallvec::SmallVec;

pub mod dangling;
pub use dangling::*;

pub mod fork_clone_builder;
pub use fork_clone_builder::*;

pub mod unzip;
pub use unzip::*;

/// After submitting a service request, use [`Chain`] to describe how
/// the response should be handled. At a minimum, for the response to be
/// delivered, you must choose one of:
/// - `.detach()`: Let the service run to completion and then discard the
///   response data.
/// - `.take()`: As long as the [`Promise`] or one of its clones is alive,
///   the service will continue running to completion and you will be able to
///   view the response (or take the response, but only once). If all clones of
///   the [`Promise`] are dropped before the service is delivered, it will
///   be canceled.
/// - `.detach_and_take()`: As long as the [`Promise`] or one of its clones is
///   alive, you will be able to view the response (or take the response, but
///   only once). The service will run to completion even if every clone of the
///   [`Promise`] is dropped.
///
/// If you do not select one of the above then the service request will be
/// canceled without ever attempting to run.
#[must_use]
pub struct Chain<'w, 's, 'a, Response, Streams, M> {
    source: Entity,
    target: Entity,
    commands: &'a mut Commands<'w, 's>,
    response: std::marker::PhantomData<Response>,
    streams: std::marker::PhantomData<Streams>,
    modifiers: std::marker::PhantomData<M>,
}

pub struct Modifiers<IsLabeled, HasOnCancel> {
    _ignore: std::marker::PhantomData<(IsLabeled, HasOnCancel)>,
}

/// No request modifiers have been set.
pub type ModifiersUnset = Modifiers<(), ()>;

/// The request is unlabeled but may have other modifiers.
pub type NotLabeled<C> = Modifiers<(), C>;

/// The request is labeled and may have other modifiers.
pub type Labeled<C> = Modifiers<Chosen, C>;

/// The request does not have an on_cancel behavior set and may have other modifiers.
pub type NoOnCancel<L> = Modifiers<L, ()>;

/// The request has an on_cancel behavior set and may have other modifiers.
pub type WithOnCancel<L> = Modifiers<L, Chosen>;

/// All possible request modifiers have been chosen or can no longer be set.
pub type ModifiersClosed = Modifiers<Chosen, Chosen>;

impl<'w, 's, 'a, Response: 'static + Send + Sync, Streams, L, C> Chain<'w, 's, 'a, Response, Streams, Modifiers<L, C>> {
    /// Have the service chain run until it is finished without holding onto any
    /// [`Promise`].
    pub fn detach(self) {
        self.commands.add(PerformOperation::new(
            self.target,
            Terminate::<Response>::new(None, true),
        ));
    }

    /// Take a [`Promise`] so you can receive the final response in the chain later.
    /// If the [`Promise`] is dropped then the entire service chain will
    /// automatically be canceled from whichever link in the chain has not been
    /// completed yet, triggering every on_cancel branch from that link to the
    /// end of the chain.
    pub fn take(self) -> Promise<Response> {
        let (promise, sender) = Promise::new();
        self.commands.add(PerformOperation::new(
            self.target,
            Terminate::new(Some(sender), false),
        ));
        promise
    }

    /// Take the promise so you can reference it later. The service request
    /// will continue to be fulfilled even if you drop the [`Promise`].
    ///
    /// This is effectively equivalent to running both [`Chain::detach`] and
    /// [`Chain::take`].
    pub fn detach_and_take(self) -> Promise<Response> {
        let (promise, sender) = Promise::new();
        self.commands.add(PerformOperation::new(
            self.target,
            Terminate::new(Some(sender), true),
        ));
        promise
    }

    /// Have the ancestor links in the service chain run until they are finished,
    /// even if the remainder of this chain gets dropped. You can continue adding
    /// links as if this is one continuous chain.
    ///
    /// If the ancestor links get canceled, the cancellation cascade will still
    /// continue past this link. To prevent that from happening, use
    /// [`Chain::sever_cancel_cascade`].
    pub fn detach_and_chain(self) -> Chain<'w, 's, 'a, Response, Streams, ModifiersClosed> {
        self.commands.entity(self.source).insert(DetachDependency);
        Chain::new(self.source, self.target, self.commands)
    }

    /// Change this into a [`Dangling`] chain. You can use this to resume building
    /// this chain later.
    ///
    /// Note that if you do not finish building the dangling chain before the
    /// next flush, the chain will be canceled up to its closest
    /// [`Chain::detach_and_chain`] link. You can use [`Chain::detach_and_dangle`]
    /// to obtain a [`Dangling`] while still ensuring that this chain will be executed.
    pub fn dangle(self) -> Dangling<Response, Streams> {
        Dangling::new(self.source, self.target)
    }

    /// A combination of [`Chain::detach`] and [`Chain::dangle`].
    pub fn detach_and_dangle(self) -> Dangling<Response, Streams> {
        self.detach_and_chain().dangle()
    }

    /// If any ancestor links in this chain get canceled, the cancellation cascade
    /// will be stopped at this link and the remainder of the chain will be
    /// disposed instead of cancelled. No child links from this one will have
    /// their cancellation branches triggered from a cancellation that happens
    /// before this link. Any cancellation behavior assigned to this link will
    /// still apply.
    ///
    /// Any cancellation that happens after this link will cascade down as
    /// normal until it reaches the next instance of `dispose_on_cancel`.
    ///
    /// If a non-detached descendant of this link gets dropped, the ancestors of
    /// this link will still be cancelled as usual. To prevent a dropped
    /// descendant from cancelling its ancestors, use [`Chain::detach_and_chain`].
    pub fn dispose_on_cancel(self) -> Chain<'w, 's, 'a, Response, (), ModifiersClosed> {
        self.commands.entity(self.source).insert(DisposeOnCancel);
        Chain::new(self.source, self.target, self.commands)
    }

    /// Use the response in the chain as a new request as soon as the response
    /// is delivered. If you apply a label or hook into streams after calling
    /// this function, then those will be applied to this new request.
    pub fn then<P: Provider<Request = Response>>(
        self,
        provider: P,
    ) -> Chain<'w, 's, 'a, P::Response, P::Streams, ModifiersUnset>
    where
        P::Response: 'static + Send + Sync,
        P::Streams: Stream,
    {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();
        provider.provide(source, target, self.commands);
        Chain::new(source, target, self.commands)
    }

    /// Apply a one-time callback whose input is a [`BlockingMap`](crate::BlockingMap)
    /// or an [`AsyncMap`](crate::AsyncMap).
    pub fn map<M, F: AsMap<M>>(
        self,
        f: F,
    ) -> Chain<'w, 's, 'a, <F::MapType as Provider>::Response, <F::MapType as Provider>::Streams, ModifiersUnset>
    where
        F::MapType: Provider<Request=Response>,
        <F::MapType as Provider>::Response: 'static + Send + Sync,
        <F::MapType as Provider>::Streams: Stream,
    {
        self.then(f.as_map())
    }

    /// Apply a one-time callback whose input is the Response of the current
    /// Chain. The output of the map will be the Response of the returned Chain.
    ///
    /// This takes in a regular blocking function rather than an async function,
    /// so while the function is executing, it will block all systems from
    /// running, similar to how [`Commands`] are flushed.
    pub fn map_block<U>(
        self,
        f: impl FnOnce(Response) -> U + 'static + Send + Sync,
    ) -> Chain<'w, 's, 'a, U, (), ModifiersUnset>
    where
        U: 'static + Send + Sync,
    {
        self.then(f.into_blocking_map())
    }

    /// Apply a one-time callback whose output is a Future that will be run in
    /// the [`AsyncComputeTaskPool`](bevy::tasks::AsyncComputeTaskPool). The
    /// output of the Future will be the Response of the returned Chain.
    pub fn map_async<Task>(
        self,
        f: impl FnOnce(Response) -> Task + 'static + Send + Sync,
    ) -> Chain<'w, 's, 'a, Task::Output, (), ModifiersUnset>
    where
        Task: Future + 'static + Send + Sync,
        Task::Output: 'static + Send + Sync,
    {
        self.then(f.into_async_map())
    }

    /// Apply a [`Provider`] that filters the response by returning an [`Option`].
    /// If the filter returns [`None`] then a cancellation is triggered.
    /// Otherwise the chain continues with the value given inside [`Some`].
    ///
    /// This is conceptually similar to [`Iterator::filter_map`]. You can also
    /// use [`Chain::disposal_filter`] to dispose the remainder of the chain
    /// instead of cancelling it.
    pub fn cancellation_filter<ThenResponse, F>(
        self,
        filter_provider: F
    ) -> Chain<'w, 's, 'a, ThenResponse, (), ModifiersUnset>
    where
        ThenResponse: 'static + Send + Sync,
        F: Provider<Request = Response, Response = Option<ThenResponse>>,
        F::Response: 'static + Send + Sync,
        F::Streams: Stream,
    {
        self.then(filter_provider).cancel_on_none()
    }

    /// Same as [`Chain::cancellation_filter`] but the chain will be disposed
    /// instead of cancelled, so the chain will be dropped without any
    /// cancellation behavior occurring.
    pub fn disposal_filter<ThenResponse, F>(
        self,
        filter_provider: F,
    ) -> Chain<'w, 's, 'a, ThenResponse, (), ModifiersClosed>
    where
        ThenResponse: 'static + Send + Sync,
        F: Provider<Request = Response, Response = Option<ThenResponse>>,
        F::Response: 'static + Send + Sync,
        F::Streams: Stream,
    {
        self.cancellation_filter(filter_provider).dispose_on_cancel()
    }

    /// When the response is delivered, we will make a clone of it and
    /// simultaneously pass that clone along two different service chains: one
    /// determined by the `build` callback provided to this function and the
    /// other determined by the [`Chain`] that gets returned by this function.
    ///
    /// This can only be applied when `Response` can be cloned.
    ///
    /// See also [`Chain::fork_clone_zip`]
    pub fn fork_clone(
        self,
        build: impl FnOnce(OutputChain<Response>),
    ) -> OutputChain<'w, 's, 'a, Response>
    where
        Response: Clone,
    {
        Chain::<'w, 's, '_, Response, (), ModifiersClosed>::new(
            self.source, self.target, self.commands,
        ).fork_clone_zip((
            |chain: OutputChain<Response>| chain.dangle(),
            build
        )).0.resume(self.commands)
    }

    /// When the response is delivered, we will make clones of it and
    /// simultaneously pass that clone along mutliple service chains, each one
    /// determined by a different element of the tuple that gets passed in as
    /// a builder.
    ///
    /// The outputs of the individual chain builders will be zipped into one
    /// output by this function. If all of the builders output [`Dangling`] then
    /// you can easily continue chaining more operations like `join` and `race`
    /// from the [`ZippedChains`] trait.
    pub fn fork_clone_zip<Builder: ForkCloneBuilder<Response>>(
        self,
        builder: Builder,
    ) -> Builder::Outputs
    where
        Response: Clone,
    {
        builder.build_fork_clone(self.target, self.commands)
    }

    /// Similar to [`Chain::fork_clone_zip`], except you provide only one
    /// builder function and indicate a number of forks to produce. Each fork
    /// will be produced using the same builder, and the output of this method
    /// will be the bundled output of each build.
    ///
    /// If your function outputs [`Dangling`] then you can easily continue
    /// chaining more operations like `join` and `race` from the [`BundledChains`]
    /// trait.
    pub fn fork_clone_bundle<const N: usize, U>(
        self,
        mut builder: impl FnMut(OutputChain<Response>) -> U,
    ) -> [U; N]
    where
        Response: Clone,
    {
        let source = self.target;
        let targets: [Entity; N] = core::array::from_fn(
            |_| self.commands.spawn(UnusedTarget).id()
        );

        self.commands.add(PerformOperation::new(
            source,
            ForkClone::<Response>::new(ForkTargetStorage::from_iter(targets)),
        ));

        let output = targets.map(
            |target| builder(OutputChain::new(source, target, self.commands))
        );

        output
    }

    /// Same as [`Chain::fork_clone_bundle`] but you can create a number of
    /// forks determined at runtime instead of compile time.
    ///
    /// This function still takes a constant integer argument which can be
    /// thought of as a size hint that would allow the operation to avoid some
    /// heap allocations if it is greater than or equal to the number of forks
    /// that are actually produced. This value should be kept somewhat modest,
    /// like 8 - 16, to avoid excessively large stack frames.
    pub fn fork_clone_bundle_vec<const N: usize, U>(
        self,
        number_forks: usize,
        mut builder: impl FnMut(OutputChain<Response>) -> U,
    ) -> SmallVec<[U; N]>
    where
        Response: Clone,
    {
        let source = self.target;
        let mut targets = ForkTargetStorage::new();
        targets.0.reserve(number_forks);

        self.commands.add(PerformOperation::new(
            source,
            ForkClone::<Response>::new(targets.clone())
        ));

        targets.0.into_iter().map(
            |target| builder(OutputChain::new(source, target, self.commands))
        ).collect()
    }

    /// If you have a `Chain<(A, B, C, ...), _, _>` with a tuple response then
    /// `unzip` allows you to convert it into a tuple of chains:
    /// `(Dangling<A>, Dangling<B>, Dangling<C>, ...)`.
    ///
    /// You can also consider using `unzip_build` to continue building each
    /// chain in the tuple independently by providing a builder function for
    /// each element of the tuple.
    pub fn unzip(self) -> Response::Unzipped
    where
        Response: Unzippable,
    {
        Response::unzip_chain(self.target, self.commands)
    }

    /// If you have a `Chain<(A, B, C, ...), _, _>` with a tuple response then
    /// `unzip_build` allows you to split it into multiple chains and apply a
    /// separate builder function to each chain. You will be passed back the
    /// zipped output of all the builder functions.
    pub fn unzip_build<Builders>(self, builders: Builders) -> Builders::Output
    where
        Builders: UnzipBuilder<Response>
    {
        builders.unzip_build(self.target, self.commands)
    }

    /// If the chain's response implements the [`Future`] trait, applying
    /// `.flatten()` to the chain will yield the output of that Future as the
    /// chain's response.
    pub fn flatten(self) -> Chain<'w, 's, 'a, Response::Output, (), ModifiersUnset>
    where
        Response: Future,
        Response::Output: 'static + Send + Sync,
    {
        self.map_async(|r| async { r.await })
    }

    /// "Pull" means that another chain will be activated when the execution of
    /// the current chain delivers the response for this link.
    ///
    /// `pull_one` lets you build one chain that will be activated when the
    /// current link in the chain receives its response. The builder function
    /// must be self-contained, so you are advised to use [`Chain::detach`] when
    /// you are finished building it.
    ///
    /// The return value of `pull_one` will be the original chain that was being
    /// built.
    ///
    /// * `value` - an initial value to provide for the pulled chain that will
    /// be triggered by the pull.
    /// * `builder` - a function that builds the pulled chain.
    pub fn pull_one<InitialValue>(
        self,
        value: InitialValue,
        builder: impl FnOnce(OutputChain<InitialValue>)
    ) -> OutputChain<'w, 's, 'a, Response>
    where
        InitialValue: 'static + Send + Sync
    {
        Chain::<'w, 's, '_, Response, (), ModifiersClosed>::new(
            self.source, self.target, self.commands,
        ).pull_one_zip(value, builder).0.resume(self.commands)
    }

    /// "Pull" means that another chain will be activated when the execution of
    /// the current chain delivers the response for this link.
    ///
    /// `pull_one_zip` is the same as [`Chain::pull_one`] except that the output
    /// of the builder function will be zipped with a [`Dangling`] of the original
    /// chain and provided as the return value of this function.
    ///
    /// * `value` - an initial value to provide for the pulled chain that will
    /// be triggered by the pull.
    /// * `builder` - a function that builds the pulled chain.
    pub fn pull_one_zip<InitialValue, U>(
        self,
        value: InitialValue,
        builder: impl FnOnce(OutputChain<InitialValue>) -> U,
    ) -> (Dangling<Response>, U)
    where
        InitialValue: 'static + Send + Sync,
    {
        self
        .pull_zip(
            (value,),
            (
                |chain: OutputChain<Response>| chain.dangle(),
                builder
            )
        )
    }

    /// "Pull" means that other chains will be activated when the execution of
    /// the current chain delivers the response for this link.
    ///
    /// `pull_zip` takes in a tuple of initial values and a tuple of builders.
    /// The tuple of builders needs to have one more element on the front, which
    /// continues building the original chain.
    ///
    /// * `values` - a tuple `(a, b, c, ...)`` of initial values to provide to
    /// the new chains that are being built.
    ///
    /// * `builder` - a tuple of functions `(f, f_a, f_b, f_c, ...)` whose first
    /// element continues building the original chain that `pull_zip` is being
    /// called on, and the remaining elements build each element of the `values`
    /// tuple.
    ///
    /// ```
    /// use bevy::prelude::{World, Commands, CommandQueue};
    /// use bevy_services::{*, samples::*};
    ///
    /// let world = World::new();
    /// let mut command_queue = CommandQueue::default();
    /// let mut commands = Commands::new(&mut command_queue, &world);
    ///
    /// let promise = commands
    ///     .request("thanks".to_owned(), to_uppercase.into_blocking_map())
    ///     .pull_zip(
    ///         (-2.0, [0xF0, 0x9F, 0x90, 0x9F]),
    ///         (
    ///             |chain: OutputChain<String>| chain.dangle(),
    ///             |chain: OutputChain<f64>| chain.map_block(double).map_block(negative).dangle(),
    ///             |chain: OutputChain<[u8; 4]>| chain.map_block(string_from_utf8).dangle(),
    ///         )
    ///     )
    ///     .join()
    ///     .bundle()
    ///     .map_block(|string_bundle| string_bundle.join(" "))
    ///     .take();
    /// ```
    pub fn pull_zip<InitialValues, Builders>(
        self,
        values: InitialValues,
        builders: Builders,
    ) -> Builders::Output
    where
        InitialValues: Unzippable + 'static + Send + Sync,
        InitialValues::Prepended<Response>: 'static + Send + Sync,
        Builders: UnzipBuilder<InitialValues::Prepended<Response>>,
    {
        self
        .map_block(move |r| values.prepend(r))
        .unzip_build(builders)
    }

    /// Add a [no-op][1] to the current end of the chain.
    ///
    /// As the name suggests, a no-op will not actually do anything, but it adds
    /// a new link (entity) into the chain which resets link modifiers. That
    /// lets you add a new label or an additional cancel behavior into the chain,
    /// but cuts off access to any remaining streams in the parent link.
    ///
    /// [1]: https://en.wikipedia.org/wiki/NOP_(code)
    pub fn noop(self) -> Chain<'w, 's, 'a, Response, (), ModifiersUnset> {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();

        self.commands.add(PerformOperation::new(
            source, Noop::<Response>::new(target))
        );
        Chain::new(source, target, self.commands)
    }
}

impl<'w, 's, 'a, T, E, Streams, M> Chain<'w, 's, 'a, Result<T, E>, Streams, M>
where
    T: 'static + Send + Sync,
    E: 'static + Send + Sync,
{
    /// Build a chain that activates if the response is an [`Err`]. If the
    /// response is [`Ok`] then the branch built by this function will be disposed,
    /// which means it gets dropped without triggering any cancellation behavior.
    ///
    /// This function returns a chain that will be activated if the result was
    /// [`Ok`] so you can continue building your response to the [`Ok`] case.
    ///
    /// You should make sure to [`detach`](Chain::detach) the chain inside your
    /// builder or else it will be disposed during the first flush, even if an
    /// [`Err`] value arrives.
    pub fn branch_for_err(
        self,
        build_err: impl FnOnce(OutputChain<E>),
    ) -> OutputChain<'w, 's, 'a, T> {
        Chain::<'w, 's, '_, Result<T, E>, Streams, M>::new(
            self.source, self.target, self.commands,
        ).branch_result_zip(
            |chain| chain.dangle(),
            build_err,
        ).0.resume(self.commands)
    }

    /// Build two branching chains, one for the case where the response is [`Ok`]
    /// and one if the response is [`Err`]. Whichever chain does not get activated
    /// will be disposed, which means it gets dropped without triggering any
    /// cancellation effects.
    ///
    /// The outputs of both builder functions will be zipped as the return value
    /// of this function.
    pub fn branch_result_zip<U, V>(
        self,
        build_ok: impl FnOnce(OutputChain<T>) -> U,
        build_err: impl FnOnce(OutputChain<E>) -> V,
    ) -> (U, V) {
        let source = self.target;
        let target_ok = self.commands.spawn(UnusedTarget).id();
        let target_err = self.commands.spawn(UnusedTarget).id();

        self.commands.add(PerformOperation::new(
            source,
            make_result_branching::<T, E>(
                ForkTargetStorage::from_iter([target_ok, target_err])
            ),
        ));

        let u = build_ok(Chain::new(self.target, target_ok, self.commands));
        let v = build_err(Chain::new(self.target, target_err, self.commands));
        (u, v)
    }

    /// If the result contains an [`Err`] value then the chain will be cancelled
    /// from this link onwards. The next link in the chain will receive a `T` if
    /// the chain is not cancelled.
    ///
    /// Note that when cancelling in this way, you will lose the `E` data inside
    /// of the [`Err`] variant. If you want to divert the execution flow during
    /// an [`Err`] result but still want to access the `E` data, then you can
    /// use `Chain::branch_for_err` or `Chain::branch_result_zip` instead.
    pub fn cancel_on_err(self) -> Chain<'w, 's, 'a, T, (), ModifiersUnset> {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();

        self.commands.add(PerformOperation::new(
            source,
            make_cancel_filter_on_err::<T, E>(target),
        ));

        Chain::new(source, target, self.commands)
    }

    /// If the result contains an [`Err`] value then the chain will be disposed
    /// from this link onwards. Disposal means that the chain will terminate at
    /// this point but no cancellation behavior will be triggered.
    pub fn dispose_on_err(self) -> Chain<'w, 's, 'a, T, (), ModifiersClosed> {
        self.cancel_on_err().dispose_on_cancel()
    }
}

impl<'w, 's, 'a, T, Streams, M> Chain<'w, 's, 'a, Option<T>, Streams, M>
where
    T: 'static + Send + Sync,
{
    /// Build a chain that activates if the response is [`None`]. If the response
    /// is [`Some`] then the branch built by this function will be disposed,
    /// which means it gets dropped without triggering any cancellation behavior.
    ///
    /// This function returns a chain that will be activated if the result was
    /// [`Some`] so you can continue building your response to the [`Some`] case.
    ///
    /// You should make sure to [`detach`](Chain::detach) the chain inside this
    /// builder or else it will be disposed during the first flush, even if a
    /// [`None`] value arrives.
    pub fn branch_for_none(
        self,
        build_none: impl FnOnce(OutputChain<()>),
    ) -> OutputChain<'w, 's, 'a, T> {
        Chain::<'w, 's, '_, Option<T>, Streams, M>::new(
            self.source, self.target, self.commands,
        ).branch_option_zip(
            |chain| chain.dangle(),
            build_none,
        ).0.resume(self.commands)
    }

    /// Build two branching chains, one for the case where the response is [`Some`]
    /// and one if the response is [`None`]. Whichever chain does not get activated
    /// will be disposed, which means it gets dropped without triggering any
    /// cancellation effects.
    ///
    /// The outputs of both builder functions will be zipped as the return value
    /// of this function.
    pub fn branch_option_zip<U, V>(
        self,
        build_some: impl FnOnce(OutputChain<T>) -> U,
        build_none: impl FnOnce(OutputChain<()>) -> V,
    ) -> (U, V) {
        let source = self.target;
        let target_some = self.commands.spawn(UnusedTarget).id();
        let target_none = self.commands.spawn(UnusedTarget).id();

        self.commands.add(PerformOperation::new(
            source,
            make_option_branching::<T>(
                ForkTargetStorage::from_iter([target_some, target_none])
            ),
        ));

        let u = build_some(Chain::new(self.target, target_some, self.commands));
        let v = build_none(Chain::new(self.target, target_none, self.commands));
        (u, v)
    }

    /// If the result contains a [`None`] value then the chain will be cancelled
    /// from this link onwards. The next link in the chain will receive a `T` if
    /// the chain is not cancelled.
    pub fn cancel_on_none(self) -> Chain<'w, 's, 'a, T, (), ModifiersUnset> {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();

        self.commands.add(PerformOperation::new(
            source,
            make_cancel_filter_on_none::<T>(target),
        ));

        Chain::new(source, target, self.commands)
    }
}

impl<'w, 's, 'a, Signal: 'static + Send + Sync, Streams, L, C> Chain<'w, 's, 'a, Cancelled<Signal>, Streams, Modifiers<L, C>> {
    /// Get only the inner signal of the [`Cancelled`] struct, discarding
    /// information about why the cancellation happened.
    pub fn cancellation_signal(self) -> Chain<'w, 's, 'a, Signal, (), ModifiersUnset> {
        self.map_block(|c| c.signal)
    }
}

impl<'w, 's, 'a, Response: 'static + Send + Sync, Streams, C> Chain<'w, 's, 'a, Response, Streams, NotLabeled<C>> {
    /// Apply a label to the request. For more information about request labels
    /// see [`crate::LabelBuilder`].
    pub fn label(
        self,
        label: impl ApplyLabel,
    ) -> Chain<'w, 's, 'a, Response, Streams, Labeled<C>> {
        label.apply(&mut self.commands.entity(self.source));
        Chain::new(self.source, self.target, self.commands)
    }
}

impl<'w, 's, 'a, Response: 'static + Send + Sync, Streams, L> Chain<'w, 's, 'a, Response, Streams, NoOnCancel<L>> {
    /// Build a child chain of services that will be triggered if the request gets
    /// canceled at the current point in the service chain.
    pub fn on_cancel<Signal: 'static + Send + Sync>(
        self,
        signal: Signal,
        f: impl FnOnce(Chain<'w, 's, '_, Cancelled<Signal>, (), ModifiersClosed>),
    ) -> Chain<'w, 's, 'a, Response, Streams, ModifiersClosed> {
        Chain::<'w, 's, '_, Response, Streams, NoOnCancel<L>>::new(
            self.source, self.target, self.commands,
        ).on_cancel_zip(signal, f).0.resume(self.commands)
    }

    /// Trigger a specific [`Provider`] in the event that the request gets canceled
    /// at the current point in the service chain.
    ///
    /// This is a convenience wrapper around [`Chain::on_cancel`] for
    /// cases where only a single provider needs to be triggered
    pub fn on_cancel_then<Signal, P>(
        self,
        signal: Signal,
        provider: P,
    ) -> Chain<'w, 's, 'a, Response, Streams, ModifiersClosed>
    where
        Signal: 'static + Send + Sync,
        P: Provider<Request = Cancelled<Signal>, Response = (), Streams = ()>,
    {
        self.on_cancel(signal, |cmds| { cmds.then(provider).detach(); })
    }

    /// Same as [`Chain::on_cancel`], but it can take in a function that
    /// returns a value, and it will return that value zipped with the next chain
    /// of the Chain.
    pub fn on_cancel_zip<Signal: 'static + Send + Sync, U>(
        self,
        signal: Signal,
        f: impl FnOnce(Chain<'w, 's, '_, Cancelled<Signal>, (), ModifiersClosed>) -> U,
    ) -> (Dangling<Response, Streams>, U) {
        let cancel_target = self.commands.spawn(UnusedTarget).id();
        let signal_target = self.commands.spawn(UnusedTarget).id();
        self.commands.add(PerformOperation::new(
            cancel_target,
            OperateCancel::new(self.source, signal_target, signal),
        ));

        let u = f(Chain::new(cancel_target, signal_target, self.commands));
        (Dangling::new(self.source, self.target), u)
    }
}

impl<'w, 's, 'a, Response: 'static + Send + Sync, Streams, M> Chain<'w, 's, 'a, Response, Streams, M> {
    /// Used internally to create a [`Chain`] that can accept a label
    /// and hook into streams.
    pub(crate) fn new(
        source: Entity,
        target: Entity,
        commands: &'a mut Commands<'w, 's>,
    ) -> Self {
        Self {
            source,
            target,
            commands,
            response: Default::default(),
            streams: Default::default(),
            modifiers: Default::default(),
        }
    }
}

/// This is a convenience alias for a [`Chain`] produced after some outcome is
/// determined, such as a race, join, or fork.
pub type OutputChain<'w, 's, 'a, Response> = Chain<'w, 's, 'a, Response, (), ModifiersClosed>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RequestExt, dangling::*, sample::*};
    use bevy::{
        prelude::{World, Commands},
        ecs::system::CommandQueue,
    };

    #[test]
    fn test_fork_clone() {
        let mut world = World::new();
        let mut command_queue = CommandQueue::default();
        let mut commands = Commands::new(&mut command_queue, &world);
        let _promise = commands
            .request((2.0, 3.0), add.into_blocking_map())
            .fork_clone_zip((
                |chain: OutputChain<f64>| chain.dangle(),
                |chain: OutputChain<f64>| {
                    chain
                    .map_block(|a| (a, 5.0))
                    .map_block(add)
                    .dangle()
                }
            ))
            .race(
                &mut commands,
                (
                    |chain: OutputChain<f64>| {
                        chain
                        .map_block(|a| (a, -2.0))
                        .map_block(add)
                        .dangle()
                    },
                    |chain: OutputChain<f64>| {
                        chain
                        .dangle()
                    }
                ),
            )
            .bundle()
            .race(&mut commands)
            .take();

        command_queue.apply(&mut world);
        dbg!(world.entities().len());
    }

    #[test]
    fn test_unzip() {
        let mut world = World::new();
        let mut command_queue = CommandQueue::default();
        let mut commands = Commands::new(&mut command_queue, &world);

        let promise = commands
            .request((2.0, 3.0), add.into_blocking_map())
            .map_block(|v| (v, 2.0*v))
            .unzip_build((
                |chain: OutputChain<f64>| {
                    chain
                    .map_block(|v| (v, -v))
                    .map_block(add)
                    .dangle()
                },
                |chain: OutputChain<f64>| {
                    chain
                    .map_block(|value|
                        WaitRequest{
                            duration: std::time::Duration::from_secs_f64(0.01),
                            value,
                        }
                    )
                    .map_async(wait)
                    .dangle()
                }
            ))
            .bundle()
            .race(&mut commands)
            .take();
    }
}
