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

use crate::{
    Chain, OutputChain, ModifiersClosed, ModifiersUnset, UnusedTarget,
    FunnelSourceStorage, RaceInput, ZipRace, BundleRace, JoinInput, ZipJoin,
    BundleJoin, PerformOperation, ForkTargetStorage,
};

use bevy::prelude::{Entity, Commands};

use smallvec::SmallVec;

/// While building a [`Chain`] you may need to pause building the chain and
/// resume chaining later. You can also zip multiple [`Dangling`] instances
/// together with a tuple and join or race them.
///
/// Use [`Chain::dangle`] to obtain a [`Dangling`].
#[must_use]
pub struct Dangling<Response, Streams=()> {
    source: Entity,
    target: Entity,
    _ignore: std::marker::PhantomData<(Response, Streams)>,
}

impl<Response: 'static + Send + Sync, Streams> Dangling<Response, Streams> {
    /// Resume operating on this [`Dangling`] chain by providing it with a fresh
    /// mutable borrow of a [`Commands`].
    pub fn resume<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>
    ) -> Chain<'w, 's, 'a, Response, Streams, ModifiersClosed> {
        Chain::new(self.source, self.target, commands)
    }

    pub(crate) fn new(source: Entity, target: Entity) -> Self {
        Self { source, target, _ignore: Default::default() }
    }
}

/// This trait is for [`Dangling`] [`Chains`](Chain) that are "zipped" together in a tuple. The
/// chains may all have different types and therefore must be handled
/// independently even if we want to handle them simultaneously.
pub trait ZippedChains {
    /// The type that gets returned after this zipped set of chains gets joined.
    type JoinedResponse;

    /// Join the zipped chains, producing a single chain whose response is the
    /// zip of the responses of all the chains.
    fn join_zip<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>
    ) -> OutputChain<'w, 's, 'a, Self::JoinedResponse>;

    /// Race the zipped chains, with a different builder for each chain. Each
    /// builder will be provided with a chain that will get triggered if its
    /// element won the race.
    ///
    /// Any elements who lost the race will get their chain cancelled. That
    /// cancellation will cascade both up the dependency chain as well as down
    /// the dependent chain. Use the [`Chain::detach`] and [`Chain::dispose_on_cancel`]
    /// methods of [`Chain`] to control the cascading according to your needs.
    /// If a chain lost the race because it was disposed then the dependent chain
    /// will also be disposed instead of cancelled.
    ///
    /// This function will return the zipped outputs of all the builder functions.
    fn race_zip<'w, 's, 'a, Builders: ZippedBuilders<'w, 's, Self>>(
        self,
        commands: &'a mut Commands<'w, 's>,
        builders: Builders,
    ) -> Builders::Output
    where
        Self: Sized;

    /// Build the zipped chains, with a different builder for each chain.
    ///
    /// There will be no dependency or synchronization added between any of the
    /// chains by using this function; it's simply an ergonomic way to continue
    /// building chains after they have been zipped together.
    fn build_zip<'w, 's, 'a, Builders: ZippedBuilders<'w, 's, Self>>(
        self,
        commands: &'a mut Commands<'w, 's>,
        builders: Builders,
    ) -> Builders::Output
    where
        Self: Sized;
}

impl<A, StreamsA, B, StreamsB> ZippedChains for (Dangling<A, StreamsA>, Dangling<B, StreamsB>)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
{
    type JoinedResponse = (A, B);
    fn join_zip<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>
    ) -> OutputChain<'w, 's, 'a, Self::JoinedResponse> {
        let input_a = self.0.target;
        let input_b = self.1.target;
        let joiner = commands.spawn(()).id();
        let target = commands.spawn(UnusedTarget).id();

        commands.add(PerformOperation::new(input_a, JoinInput::<A>::new(joiner)));
        commands.add(PerformOperation::new(input_b, JoinInput::<B>::new(joiner)));
        commands.add(PerformOperation::new(
            joiner,
            ZipJoin::<Self::JoinedResponse>::new(
                FunnelSourceStorage::from_iter([input_a, input_b]),
                target,
            )
        ));

        Chain::new(joiner, target, commands)
    }

    fn race_zip<'w, 's, 'a, Builders: ZippedBuilders<'w, 's, Self>>(
        self,
        commands: &'a mut Commands<'w, 's>,
        builders: Builders
    ) -> Builders::Output
    where
        Self: Sized
    {
        let input_a = self.0.target;
        let input_b = self.1.target;
        let racer = commands.spawn(()).id();
        let target_a = commands.spawn(UnusedTarget).id();
        let target_b = commands.spawn(UnusedTarget).id();

        commands.add(PerformOperation::new(input_a, RaceInput::<A>::new(racer)));
        commands.add(PerformOperation::new(input_b, RaceInput::<B>::new(racer)));
        commands.add(PerformOperation::new(
            racer,
            ZipRace::<Self::JoinedResponse>::new(
                FunnelSourceStorage::from_iter([input_a, input_b]),
                ForkTargetStorage::from_iter([target_a, target_b]),
            ),
        ));

        builders.apply_zipped_builders(
            (
                Dangling::new(racer, target_a),
                Dangling::new(racer, target_b),
            ),
            commands
        )
    }

    fn build_zip<'w, 's, 'a, Builders: ZippedBuilders<'w, 's, Self>>(
        self,
        commands: &'a mut Commands<'w, 's>,
        builders: Builders,
    ) -> Builders::Output
    where
        Self: Sized
    {
        builders.apply_zipped_builders(self, commands)
    }
}

impl<A, StreamsA, B, StreamsB, C, StreamsC> ZippedChains for (
    Dangling<A, StreamsA>,
    Dangling<B, StreamsB>,
    Dangling<C, StreamsC>,
)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    C: 'static + Send + Sync,
{
    type JoinedResponse = (A, B, C);
    fn join_zip<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>
    ) -> OutputChain<'w, 's, 'a, Self::JoinedResponse> {
        let input_a = self.0.target;
        let input_b = self.1.target;
        let input_c = self.2.target;
        let joiner = commands.spawn(()).id();
        let target = commands.spawn(UnusedTarget).id();

        commands.add(PerformOperation::new(input_a, JoinInput::<A>::new(joiner)));
        commands.add(PerformOperation::new(input_b, JoinInput::<B>::new(joiner)));
        commands.add(PerformOperation::new(input_c, JoinInput::<C>::new(joiner)));
        commands.add(PerformOperation::new(
            joiner,
            ZipJoin::<Self::JoinedResponse>::new(
                FunnelSourceStorage::from_iter([input_a, input_b, input_c]),
                target,
            )
        ));

        Chain::new(joiner, target, commands)
    }

    fn race_zip<'w, 's, 'a, Builders: ZippedBuilders<'w, 's, Self>>(
        self,
        commands: &'a mut Commands<'w, 's>,
        builders: Builders,
    ) -> Builders::Output
    where
        Self: Sized
    {
        let input_a = self.0.target;
        let input_b = self.1.target;
        let input_c = self.2.target;
        let racer = commands.spawn(()).id();
        let target_a = commands.spawn(UnusedTarget).id();
        let target_b = commands.spawn(UnusedTarget).id();
        let target_c = commands.spawn(UnusedTarget).id();

        commands.add(PerformOperation::new(input_a, RaceInput::<A>::new(racer)));
        commands.add(PerformOperation::new(input_b, RaceInput::<B>::new(racer)));
        commands.add(PerformOperation::new(input_c, RaceInput::<C>::new(racer)));
        commands.add(PerformOperation::new(
            racer,
            ZipRace::<Self::JoinedResponse>::new(
                FunnelSourceStorage::from_iter([input_a, input_b, input_c]),
                ForkTargetStorage::from_iter([target_a, target_b, target_c]),
            ),
        ));

        builders.apply_zipped_builders(
            (
                Dangling::new(racer, target_a),
                Dangling::new(racer, target_b),
                Dangling::new(racer, target_c),
            ),
            commands
        )
    }

    fn build_zip<'w, 's, 'a, Builders: ZippedBuilders<'w, 's, Self>>(
        self,
        commands: &'a mut Commands<'w, 's>,
        builders: Builders,
    ) -> Builders::Output
    where
        Self: Sized
    {
        builders.apply_zipped_builders(self, commands)
    }
}

/// This trait determines what kinds of constructs are able to able to be used
/// by the [`ZippedChains`] trait to individually build chains that have been
/// zipped together.
pub trait ZippedBuilders<'w, 's, Z> {
    type Output;
    fn apply_zipped_builders<'a>(self, zip: Z, commands: &'a mut Commands<'w, 's>) -> Self::Output;
}

impl<'w, 's, A, StreamsA, Fa, Ua, B, StreamsB, Fb, Ub> ZippedBuilders<'w, 's, (Dangling<A, StreamsA>, Dangling<B, StreamsB>)> for (Fa, Fb)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    Fa: FnOnce(OutputChain<'w, 's, '_, A>) -> Ua,
    Fb: FnOnce(OutputChain<'w, 's, '_, B>) -> Ub,
{
    type Output = (Ua, Ub);
    fn apply_zipped_builders<'a>(
        self,
        (dangle_a, dangle_b): (Dangling<A, StreamsA>, Dangling<B, StreamsB>),
        commands: &'a mut Commands<'w, 's>
    ) -> Self::Output {
        let (f_a, f_b) = self;
        let u_a = (f_a)(Chain::new(dangle_a.source, dangle_a.target, commands));
        let u_b = (f_b)(Chain::new(dangle_b.source, dangle_b.target, commands));
        (u_a, u_b)
    }
}

/// This trait allows a set of zipped chains to be converted into a bundle. This
/// is only implemented for zipped chains that have a uniform
pub trait ZippedChainsToBundle {
    type Response;
    type Bundle: IntoIterator<Item=Dangling<Self::Response>>;

    fn bundle(self) -> Self::Bundle;
}

impl<Response: 'static + Send + Sync, StreamA, StreamB> ZippedChainsToBundle for (Dangling<Response, StreamA>, Dangling<Response, StreamB>) {
    type Response = Response;
    type Bundle = [Dangling<Response>; 2];
    fn bundle(self) -> Self::Bundle {
        [
            Dangling::new(self.0.source, self.0.target),
            Dangling::new(self.1.source, self.1.target),
        ]
    }
}

impl<Response, StreamsA, StreamsB, StreamsC> ZippedChainsToBundle for (
    Dangling<Response, StreamsA>,
    Dangling<Response, StreamsB>,
    Dangling<Response, StreamsC>,
)
where
    Response: 'static + Send + Sync,
{
    type Response = Response;
    type Bundle = [Dangling<Response>; 3];
    fn bundle(self) -> Self::Bundle {
        [
            Dangling::new(self.0.source, self.0.target),
            Dangling::new(self.1.source, self.1.target),
            Dangling::new(self.2.source, self.2.target),
        ]
    }
}

/// A type alias to ensure a consistent SmallVec type across the whole implementation
pub type JoinedBundle<T> = SmallVec<[T; 8]>;

/// This trait is for [`Dangling`] [`Chains`](Chain) that are bundled into an
/// [`IntoIterator`] type. This implies that the chains must all share the same
/// Response type. Streams and modifiers are ignored.
pub trait BundledChains {
    type Response;

    /// Join the bundle into one [`Chain`] whose response is the combined
    /// responses of all the chains.
    fn join_bundle<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>,
    ) -> Chain<'w, 's, 'a, JoinedBundle<Self::Response>, (), ModifiersUnset>;

    /// Race the bundle elements against each other, producing a [`Chain`] whose
    /// response is the value of the first chain in the bundle to arrive.
    fn race_bundle<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>,
    ) -> Chain<'w, 's, 'a, Self::Response, (), ModifiersUnset>;
}

impl<Response, Streams, T> BundledChains for T
where
    Response: 'static + Send + Sync,
    T: IntoIterator<Item=Dangling<Response, Streams>>,
{
    type Response = Response;
    fn join_bundle<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>,
    ) -> Chain<'w, 's, 'a, JoinedBundle<Self::Response>, (), ModifiersUnset> {
        let inputs = FunnelSourceStorage::from_iter(
            self.into_iter().map(|dangle| dangle.target)
        );
        let joiner = commands.spawn(()).id();
        for input in &inputs.0 {
            commands.add(PerformOperation::new(
                *input,
                JoinInput::<Response>::new(joiner),
            ));
        }

        let target = commands.spawn(UnusedTarget).id();
        commands.add(PerformOperation::new(
            joiner,
            BundleJoin::<Response>::new(inputs, target),
        ));

        Chain::new(joiner, target, commands)
    }

    fn race_bundle<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>,
    ) -> Chain<'w, 's, 'a, Self::Response, (), ModifiersUnset> {
        let inputs = FunnelSourceStorage::from_iter(
            self.into_iter().map(|dangle| dangle.target)
        );
        let racer = commands.spawn(()).id();
        for input in &inputs.0 {
            commands.add(PerformOperation::new(
                *input,
                RaceInput::<Response>::new(racer),
            ));
        }

        let target = commands.spawn(UnusedTarget).id();
        commands.add(PerformOperation::new(
            racer,
            BundleRace::<Response>::new(inputs, target),
        ));

        Chain::new(racer, target, commands)
    }
}
