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
    Chain, UnusedTarget, Output,
    FunnelInputStorage, JoinInput, ZipJoin,
    BundleJoin, AddOperation,
};

use bevy::prelude::Commands;

use smallvec::SmallVec;

/// This trait is for [`Dangling`] [`Chains`](Chain) that are "zipped" together in a tuple. The
/// chains may all have different types and therefore must be handled
/// independently even if we want to handle them simultaneously.
pub trait ZippedOutputs {
    /// The type that gets returned after this zipped set of chains gets joined.
    type JoinedResponse;

    /// Join the zipped chains, producing a single chain whose response is the
    /// zip of the responses of all the chains.
    fn join_zip<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>
    ) -> Chain<'w, 's, 'a, Self::JoinedResponse>;

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

impl<A, B> ZippedOutputs for (Output<A>, Output<B>)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
{
    type JoinedResponse = (A, B);
    fn join_zip<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>
    ) -> Chain<'w, 's, 'a, Self::JoinedResponse> {
        let input_a = self.0.id();
        let input_b = self.1.id();
        let joiner = commands.spawn(()).id();
        let target = commands.spawn(UnusedTarget).id();

        commands.add(AddOperation::new(input_a, JoinInput::<A>::new(joiner)));
        commands.add(AddOperation::new(input_b, JoinInput::<B>::new(joiner)));
        commands.add(AddOperation::new(
            joiner,
            ZipJoin::<Self::JoinedResponse>::new(
                FunnelInputStorage::from_iter([input_a, input_b]),
                target,
            )
        ));

        Chain::new(joiner, target, commands)
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

impl<A, B, C> ZippedOutputs for (Output<A>, Output<B>, Output<C>)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    C: 'static + Send + Sync,
{
    type JoinedResponse = (A, B, C);
    fn join_zip<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>
    ) -> Chain<'w, 's, 'a, Self::JoinedResponse> {
        let input_a = self.0.id();
        let input_b = self.1.id();
        let input_c = self.2.id();
        let joiner = commands.spawn(()).id();
        let target = commands.spawn(UnusedTarget).id();

        commands.add(AddOperation::new(input_a, JoinInput::<A>::new(joiner)));
        commands.add(AddOperation::new(input_b, JoinInput::<B>::new(joiner)));
        commands.add(AddOperation::new(input_c, JoinInput::<C>::new(joiner)));
        commands.add(AddOperation::new(
            joiner,
            ZipJoin::<Self::JoinedResponse>::new(
                FunnelInputStorage::from_iter([input_a, input_b, input_c]),
                target,
            )
        ));

        Chain::new(joiner, target, commands)
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

impl<'w, 's, A, Fa, Ua, B, Fb, Ub> ZippedBuilders<'w, 's, (Output<A>, Output<B>)> for (Fa, Fb)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    Fa: FnOnce(Chain<'w, 's, '_, A>) -> Ua,
    Fb: FnOnce(Chain<'w, 's, '_, B>) -> Ub,
{
    type Output = (Ua, Ub);
    fn apply_zipped_builders<'a>(
        self,
        (output_a, output_b): (Output<A>, Output<B>),
        commands: &'a mut Commands<'w, 's>
    ) -> Self::Output {
        let (f_a, f_b) = self;
        let u_a = (f_a)(Chain::new(output_a.scope(), output_a.id(), commands));
        let u_b = (f_b)(Chain::new(output_b.scope(), output_b.id(), commands));
        (u_a, u_b)
    }
}

/// This trait allows a set of zipped chains to be converted into a bundle. This
/// is only implemented for zipped chains that have a uniform
pub trait ZippedChainsToBundle {
    type Response;
    type Bundle: IntoIterator<Item=Output<Self::Response>>;

    fn bundle(self) -> Self::Bundle;
}

impl<Response: 'static + Send + Sync> ZippedChainsToBundle for (Output<Response>, Output<Response>) {
    type Response = Response;
    type Bundle = [Output<Response>; 2];
    fn bundle(self) -> Self::Bundle {
        [
            Output::new(self.0.scope(), self.0.id()),
            Output::new(self.1.scope(), self.1.id()),
        ]
    }
}

impl<Response> ZippedChainsToBundle for (Output<Response>, Output<Response>, Output<Response>)
where
    Response: 'static + Send + Sync,
{
    type Response = Response;
    type Bundle = [Output<Response>; 3];
    fn bundle(self) -> Self::Bundle {
        [
            Output::new(self.0.scope(), self.0.id()),
            Output::new(self.1.scope(), self.1.id()),
            Output::new(self.2.scope(), self.2.id()),
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
    ) -> Chain<'w, 's, 'a, JoinedBundle<Self::Response>>;
}

impl<Response, T> BundledChains for T
where
    Response: 'static + Send + Sync,
    T: IntoIterator<Item=Output<Response>>,
{
    type Response = Response;
    fn join_bundle<'w, 's, 'a>(
        self,
        commands: &'a mut Commands<'w, 's>,
    ) -> Chain<'w, 's, 'a, JoinedBundle<Self::Response>> {
        let inputs = FunnelInputStorage::from_iter(
            self.into_iter().map(|output| output.id())
        );
        let joiner = commands.spawn(()).id();
        for input in &inputs.0 {
            commands.add(AddOperation::new(
                *input,
                JoinInput::<Response>::new(joiner),
            ));
        }

        let target = commands.spawn(UnusedTarget).id();
        commands.add(AddOperation::new(
            joiner,
            BundleJoin::<Response>::new(inputs, target),
        ));

        Chain::new(joiner, target, commands)
    }
}
