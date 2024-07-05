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
    UnusedTarget, AddOperation, Node, InputSlot, Builder,
    ForkClone, StreamPack, Provider, ProvideOnce,
    AsMap, IntoBlockingMap, IntoAsyncMap, Output, Noop,
    ForkTargetStorage,
    make_result_branching, make_cancel_filter_on_err,
    make_option_branching, make_cancel_filter_on_none,
};

use bevy::prelude::{Entity, Commands};

use smallvec::SmallVec;

pub mod fork_clone_builder;
pub use fork_clone_builder::*;

pub mod unzip;
pub use unzip::*;

pub mod zipped;
pub use zipped::*;

/// After submitting a service request, use [`Chain`] to describe how
/// the response should be handled. At a minimum, for the response to be
/// delivered, you must choose one of:
/// - `.detach()`: Let the service run to completion and then discard the
///   response data.
/// - `.take()`: As long as the [`Promise`] or one of its clones is alive,
///   the service will continue running to completion and you will be able to
///   view the response (or take the response, but only once). If all clones of
///   the [`Promise`] are dropped before the service is delivered, it will
///   be cancelled.
/// - `.detach_and_take()`: As long as the [`Promise`] or one of its clones is
///   alive, you will be able to view the response (or take the response, but
///   only once). The service will run to completion even if every clone of the
///   [`Promise`] is dropped.
///
/// If you do not select one of the above then the service request will be
/// cancelled without ever attempting to run.
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
    /// `terminate` slot of your [`Scope`][1] then the workflow will not be able
    /// to run.
    ///
    /// [1]: crate::Scope
    pub fn output(self) -> Output<T> {
        Output::new(self.builder.scope, self.target)
    }

    /// Connect the response at the end of the chain into a new provider. Get
    /// the response of the new provider as a chain so you can continue chaining
    /// operations.
    pub fn then<P: Provider<Request = T>>(
        self,
        provider: P,
    ) -> Chain<'w, 's, 'a, P::Response>
    where
        P::Response: 'static + Send + Sync,
    {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();
        provider.connect(source, target, self.commands);
        Chain::new(self.scope, target, self.commands)
    }

    /// Connect the response in the chain into a new provider. Get the node
    /// slots that wrap around the new provider.
    pub fn then_node<P: Provider<Request = T>>(
        self,
        provider: P,
    ) -> Node<T, P::Response, P::Streams>
    where
        P::Response: 'static + Send + Sync,
        P::Streams: StreamPack,
    {
        let source = self.target;
        let target = self.commands.spawn(UnusedTarget).id();
        provider.connect(source, target, self.commands);
        let (bundle, streams) = <P::Streams as StreamPack>::spawn_node_streams(
            self.scope, self.commands,
        );
        self.commands.entity(source).insert(bundle);
        Node {
            input: InputSlot::new(self.scope, source),
            output: Output::new(self.scope, target),
            streams,
        }
    }

    /// Apply a one-time map whose input is a [`BlockingMap`](crate::BlockingMap)
    /// or an [`AsyncMap`](crate::AsyncMap).
    pub fn map<M, F: AsMap<M>>(
        self,
        f: F,
    ) -> Chain<'w, 's, 'a, <F::MapType as ProvideOnce>::Response>
    where
        F::MapType: Provider<Request=T>,
        <F::MapType as ProvideOnce>::Response: 'static + Send + Sync,
    {
        self.then(f.as_map())
    }

    /// Same as [`Self::map`] but receive the new node instead of continuing a
    /// chain.
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

    /// Apply a map whose input is the Response of the current Chain. The
    /// output of the map will be the Response of the returned Chain.
    ///
    /// This takes in a regular blocking function rather than an async function,
    /// so while the function is executing, it will block all systems from
    /// running, similar to how [`Commands`] are flushed.
    pub fn map_block<U>(
        self,
        f: impl FnMut(T) -> U + 'static + Send + Sync,
    ) -> Chain<'w, 's, 'a, U>
    where
        U: 'static + Send + Sync,
    {
        self.then(f.into_blocking_map())
    }

    /// Same as [`Self::map_block`] but receive the new node instead of
    /// continuing a chain.
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
    /// [`AsyncComputeTaskPool`](bevy::tasks::AsyncComputeTaskPool). The
    /// output of the Future will be the Response of the returned Chain.
    pub fn map_async<Task>(
        self,
        f: impl FnMut(T) -> Task + 'static + Send + Sync,
    ) -> Chain<'w, 's, 'a, Task::Output>
    where
        Task: Future + 'static + Send + Sync,
        Task::Output: 'static + Send + Sync,
    {
        self.then(f.into_async_map())
    }

    /// Same as [`Self::map_async`] but receive the new node instead of
    /// continuing a chain.
    pub fn map_async_node<Task>(
        self,
        f: impl FnMut(T) -> Task + 'static + Send + Sync,
    ) -> Node<T, Task::Output, ()>
    where
        Task: Future + 'static + Send + Sync,
        Task::Output: 'static + Send + Sync,
    {
        self.then_node(f.into_async_map())
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
    ) -> Chain<'w, 's, 'a, ThenResponse>
    where
        ThenResponse: 'static + Send + Sync,
        F: Provider<Request = T, Response = Option<ThenResponse>>,
        F::Response: 'static + Send + Sync,
        F::Streams: StreamPack,
    {
        self.then(filter_provider).cancel_on_none()
    }

    // /// Same as [`Chain::cancellation_filter`] but the chain will be disposed
    // /// instead of cancelled, so the workflow may continue if the termination
    // /// node can still be reached.
    // pub fn disposal_filter<ThenResponse, F>(
    //     self,
    //     filter_provider: F,
    // ) -> Chain<'w, 's, 'a, ThenResponse>
    // where
    //     ThenResponse: 'static + Send + Sync,
    //     F: Provider<Request = Response, Response = Option<ThenResponse>>,
    //     F::Response: 'static + Send + Sync,
    //     F::Streams: StreamPack,
    // {
    //     self.cancellation_filter(filter_provider).dispose_on_cancel()
    // }

    /// When the response is delivered, we will make a clone of it and
    /// simultaneously pass that clone along two different impulse chains: one
    /// determined by the `build` map provided to this function and the
    /// other determined by the [`Chain`] that gets returned by this function.
    ///
    /// This can only be applied when `Response` can be cloned.
    ///
    /// See also [`Chain::fork_clone_zip`]
    pub fn fork_clone(
        self,
        build: impl FnOnce(Chain<T>),
    ) -> Chain<'w, 's, 'a, T>
    where
        T: Clone,
    {
        Chain::<'w, 's, '_, T>::new(
            self.scope, self.target, self.commands,
        ).fork_clone_zip((
            |chain: Chain<T>| chain.output(),
            build
        )).0.chain(self.commands)
    }

    /// When the response is delivered, we will make clones of it and
    /// simultaneously pass that clone along mutliple impulse chains, each one
    /// determined by a different element of the tuple that gets passed in as
    /// a builder.
    ///
    /// The outputs of the individual chain builders will be zipped into one
    /// output by this function. If all of the builders output [`Dangling`] then
    /// you can easily continue chaining more operations like `join` and `race`
    /// from the [`ZippedChains`] trait.
    pub fn fork_clone_zip<Builder: ForkCloneBuilder<T>>(
        self,
        builder: Builder,
    ) -> Builder::Outputs
    where
        T: Clone,
    {
        builder.build_fork_clone(self.target, self.builder)
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
        mut build: impl FnMut(Chain<T>) -> U,
    ) -> [U; N]
    where
        T: Clone,
    {
        let source = self.target;
        let targets: [Entity; N] = core::array::from_fn(
            |_| self.builder.commands.spawn(UnusedTarget).id()
        );

        self.builder.commands.add(AddOperation::new(
            source,
            ForkClone::<T>::new(ForkTargetStorage::from_iter(targets)),
        ));

        let output = targets.map(
            |target| build(Chain::new(target, self.builder))
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
        mut build: impl FnMut(Chain<T>) -> U,
    ) -> SmallVec<[U; N]>
    where
        T: Clone,
    {
        let source = self.target;
        let mut targets = ForkTargetStorage::new();
        targets.0.reserve(number_forks);

        self.builder.commands.add(AddOperation::new(
            source,
            ForkClone::<T>::new(targets.clone())
        ));

        targets.0.into_iter().map(
            |target| build(Chain::new(target, self.builder))
        ).collect()
    }

    /// If you have a `Chain<(A, B, C, ...), _, _>` with a tuple response then
    /// `unzip` allows you to convert it into a tuple of chains:
    /// `(Dangling<A>, Dangling<B>, Dangling<C>, ...)`.
    ///
    /// You can also consider using `unzip_build` to continue building each
    /// chain in the tuple independently by providing a builder function for
    /// each element of the tuple.
    pub fn unzip(self) -> T::Unzipped
    where
        T: Unzippable,
    {
        T::unzip_chain(self.target, self.builder)
    }

    /// If you have a `Chain<(A, B, C, ...), _, _>` with a tuple response then
    /// `unzip_build` allows you to split it into multiple chains and apply a
    /// separate builder function to each chain. You will be passed back the
    /// zipped output of all the builder functions.
    pub fn unzip_build<Builders>(self, builders: Builders) -> Builders::Output
    where
        Builders: UnzipBuilder<T>
    {
        builders.unzip_build(self.target, self.builder)
    }

    /// If the chain's response implements the [`Future`] trait, applying
    /// `.flatten()` to the chain will yield the output of that Future as the
    /// chain's response.
    pub fn flatten(self) -> Chain<'w, 's, 'a, 'b, T::Output>
    where
        T: Future,
        T::Output: 'static + Send + Sync,
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
        build: impl FnOnce(Chain<InitialValue>)
    ) -> Chain<'w, 's, 'a, 'b, T>
    where
        InitialValue: Clone + 'static + Send + Sync
    {
        Chain::<T>::new(
            self.target, self.builder,
        ).pull_one_zip(value, build).0.chain(self.builder)
    }

    /// "Pull" means that another chain will be activated when the execution of
    /// the current chain delivers the response for this link.
    ///
    /// `pull_one_zip` is the same as [`Chain::pull_one`] except that the output
    /// of the builder function will be zipped with a [`Output`] of the original
    /// chain and provided as the return value of this function.
    ///
    /// * `value` - an initial value to provide for the pulled chain that will
    /// be triggered by the pull.
    /// * `builder` - a function that builds the pulled chain.
    pub fn pull_one_zip<InitialValue, U>(
        self,
        value: InitialValue,
        build: impl FnOnce(Chain<InitialValue>) -> U,
    ) -> (Output<T>, U)
    where
        InitialValue: Clone + 'static + Send + Sync,
    {
        self
        .pull_zip(
            (value,),
            (
                |chain: Chain<T>| chain.output(),
                build
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
    /// use bevy_impulse::{*, testing::*};
    /// let mut context = TestingContext::minimal_plugins();
    /// let mut promise = context.build(|commands| {
    ///     commands
    ///     .request("thanks".to_owned(), to_uppercase.into_blocking_map())
    ///     .pull_zip(
    ///         (4.0, [0xF0, 0x9F, 0x90, 0x9F]),
    ///         (
    ///             |chain: OutputChain<String>| {
    ///                 chain.dangle()
    ///             },
    ///             |chain: OutputChain<f64>| {
    ///                 chain
    ///                 .map_block(|value| value.to_string())
    ///                 .dangle()
    ///             },
    ///             |chain: OutputChain<[u8; 4]>| {
    ///                 chain
    ///                 .map_block(string_from_utf8)
    ///                 .cancel_on_err()
    ///                 .dangle()
    ///             },
    ///         )
    ///     )
    ///     .bundle()
    ///     .join_bundle(commands)
    ///     .map_block(|string_bundle| string_bundle.join(" "))
    ///     .take()
    /// });
    ///
    /// context.run_while_pending(&mut promise);
    /// assert_eq!(
    ///     promise.peek().available().map(|v| v.as_str()),
    ///     Some("THANKS 4 🐟"),
    /// );
    /// ```
    pub fn pull_zip<InitialValues, Builders>(
        self,
        values: InitialValues,
        builders: Builders,
    ) -> Builders::Output
    where
        InitialValues: Unzippable + Clone + 'static + Send + Sync,
        InitialValues::Prepended<T>: 'static + Send + Sync,
        Builders: UnzipBuilder<InitialValues::Prepended<T>>,
    {
        self
        .map_block(move |r| values.clone().prepend(r))
        .unzip_build(builders)
    }

    /// Add a [no-op][1] to the current end of the chain.
    ///
    /// As the name suggests, a no-op will not actually do anything, but it adds
    /// a new link (entity) into the chain.
    /// [1]: https://en.wikipedia.org/wiki/NOP_(code)
    pub fn noop(self) -> Chain<'w, 's, 'a, 'b, T> {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            source, Noop::<T>::new(target),
        ));
        Chain::new(target, self.builder)
    }

    pub fn scope(&self) -> Entity {
        self.builder.scope
    }

    pub fn target(&self) -> Entity {
        self.target
    }
}

impl<'w, 's, 'a, 'b, T, E> Chain<'w, 's, 'a, 'b, Result<T, E>>
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
        build_err: impl FnOnce(Chain<E>),
    ) -> Chain<'w, 's, 'a, 'b, T> {
        Chain::<Result<T, E>>::new(
            self.target, self.builder,
        ).branch_result_zip(
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
    pub fn branch_result_zip<U, V>(
        self,
        build_ok: impl FnOnce(Chain<T>) -> U,
        build_err: impl FnOnce(Chain<E>) -> V,
    ) -> (U, V) {
        let source = self.target;
        let target_ok = self.builder.commands.spawn(UnusedTarget).id();
        let target_err = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            source,
            make_result_branching::<T, E>(
                ForkTargetStorage::from_iter([target_ok, target_err])
            ),
        ));

        let u = build_ok(Chain::new(target_ok, self.builder));
        let v = build_err(Chain::new(target_err, self.builder));
        (u, v)
    }

    // /// If the result contains an [`Err`] value then the chain will be cancelled
    // /// from this link onwards. The next link in the chain will receive a `T` if
    // /// the chain is not cancelled.
    // ///
    // /// Note that when cancelling in this way, you will lose the `E` data inside
    // /// of the [`Err`] variant. If you want to divert the execution flow during
    // /// an [`Err`] result but still want to access the `E` data, then you can
    // /// use `Chain::branch_for_err` or `Chain::branch_result_zip` instead.
    // ///
    // /// ```
    // /// use bevy_impulse::{*, testing::*};
    // /// let mut context = TestingContext::minimal_plugins();
    // /// let mut promise = context.build(|commands| {
    // ///     commands
    // ///     .provide("hello")
    // ///     .map_block(produce_err)
    // ///     .cancel_on_err()
    // ///     .take()
    // /// });
    // ///
    // /// context.run_while_pending(&mut promise);
    // /// assert!(promise.peek().is_cancelled());
    // /// ```
    // pub fn cancel_on_err(self) -> Chain<'w, 's, 'a, T> {
    //     let source = self.target;
    //     let target = self.commands.spawn(UnusedTarget).id();

    //     self.commands.add(AddOperation::new(
    //         source,
    //         make_cancel_filter_on_err::<T, E>(target),
    //     ));

    //     Chain::new(self.scope, target, self.commands)
    // }

    // /// If the result contains an [`Err`] value then the chain will be disposed
    // /// from this link onwards. Disposal means that the chain will terminate at
    // /// this point but no cancellation behavior will be triggered.
    // pub fn dispose_on_err(self) -> Chain<'w, 's, 'a, T> {
    //     self.cancel_on_err().dispose_on_cancel()
    // }
}

impl<'w, 's, 'a, 'b, T> Chain<'w, 's, 'a, 'b, Option<T>>
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
        build_none: impl FnOnce(Chain<()>),
    ) -> Chain<'w, 's, 'a, 'b, T> {
        Chain::<Option<T>>::new(
            self.target, self.builder,
        ).branch_option_zip(
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
    pub fn branch_option_zip<U, V>(
        self,
        build_some: impl FnOnce(Chain<T>) -> U,
        build_none: impl FnOnce(Chain<()>) -> V,
    ) -> (U, V) {
        let source = self.target;
        let target_some = self.builder.commands.spawn(UnusedTarget).id();
        let target_none = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
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
    pub fn cancel_on_none(self) -> Chain<'w, 's, 'a, 'b, T> {
        let source = self.target;
        let target = self.builder.commands.spawn(UnusedTarget).id();

        self.builder.commands.add(AddOperation::new(
            source,
            make_cancel_filter_on_none::<T>(target),
        ));

        Chain::new(target, self.builder)
    }
}

impl<'w, 's, 'a, 'b, Response: 'static + Send + Sync> Chain<'w, 's, 'a, 'b, Response> {
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{*, testing::*};
//     use std::time::Duration;

//     #[test]
//     fn test_async_map() {
//         let mut context = TestingContext::minimal_plugins();

//         let mut promise = context.build(|commands| {
//             commands
//             .request(
//                 WaitRequest {
//                     duration: Duration::from_secs_f64(0.001),
//                     value: "hello".to_owned(),
//                 },
//                 wait.into_async_map(),
//             )
//             .take_response()
//         });

//         context.run_with_conditions(
//             &mut promise,
//             FlushConditions::new()
//             .with_timeout(Duration::from_secs_f64(5.0)),
//         );

//         assert!(promise.peek().available().is_some_and(|v| v == "hello"));
//     }

//     #[test]
//     fn test_race_zip() {
//         let mut context = TestingContext::minimal_plugins();

//         let mut promise = context.build(|commands| {
//             commands
//             .request((2.0, 3.0), add.into_blocking_map())
//             .fork_clone_zip((
//                 |chain: Chain<f64>| {
//                     chain
//                     .map_block(|value|
//                         WaitRequest {
//                             duration: std::time::Duration::from_secs_f64(value),
//                             value,
//                         }
//                     )
//                     .map_async(wait)
//                     .output() // 5.0
//                 },
//                 |chain: Chain<f64>| {
//                     chain
//                     .map_block(|a| (a, a))
//                     .map_block(add)
//                     .output() // 10.0
//                 }
//             ))
//             .race_zip(
//                 commands,
//                 (
//                     |chain: Chain<f64>| {
//                         chain
//                         .map_block(|a| (a, a))
//                         .map_block(add)
//                         .output() // 10.0
//                     },
//                     |chain: Chain<f64>| {
//                         chain
//                         .map_block(|a| (a, a))
//                         .map_block(add)
//                         .output() // 20.0
//                     }
//                 ),
//             )
//             .bundle()
//             .race_bundle(commands)
//             .take()
//         });

//         context.run_with_conditions(
//             &mut promise,
//             FlushConditions::new()
//             .with_update_count(5),
//         );
//         assert_eq!(promise.peek().available().copied(), Some(20.0));
//     }

//     #[test]
//     fn test_unzip() {
//         let mut context = TestingContext::minimal_plugins();

//         let mut promise = context.build(|commands| {
//             commands
//             .request((2.0, 3.0), add.into_blocking_map())
//             .map_block(|v| (v, 2.0*v))
//             .unzip_build((
//                 |chain: Chain<f64>| {
//                     chain
//                     .map_block(|v| (v, 10.0))
//                     .map_block(add)
//                     .dangle()
//                 },
//                 |chain: Chain<f64>| {
//                     chain
//                     .map_block(|value|
//                         WaitRequest{
//                             duration: std::time::Duration::from_secs_f64(0.01),
//                             value,
//                         }
//                     )
//                     .map_async(wait)
//                     .dangle()
//                 }
//             ))
//             .bundle()
//             .race_bundle(commands)
//             .take()
//         });

//         context.run_while_pending(&mut promise);
//         assert_eq!(promise.peek().available().copied(), Some(15.0));
//     }

//     #[test]
//     fn test_dispose_on_cancel() {
//         let mut context = TestingContext::minimal_plugins();

//         let mut promise = context.build(|commands| {
//             commands
//             .provide("hello")
//             .map_block(produce_err)
//             .cancel_on_err()
//             .dispose_on_cancel()
//             .take()
//         });

//         context.run_while_pending(&mut promise);
//         assert!(promise.peek().is_disposed());

//         // If we flip the order of cancel_on_err and dispose_on_cancel then the
//         // outcome should be a cancellation instead of a disposal, because the
//         // disposal was requested for a part of the chain that did not get
//         // cancelled.
//         let mut promise = context.build(|commands| {
//             commands
//             .provide("hello")
//             .map_block(produce_err)
//             .dispose_on_cancel()
//             .cancel_on_err()
//             .take()
//         });

//         context.run_while_pending(&mut promise);
//         assert!(promise.peek().is_cancelled());
//     }
// }
