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

use crate::{Chain, ModifiersClosed, ModifiersUnset, UnusedTarget};

use bevy::prelude::{Entity, Commands};

/// While building a [`Chain`] you may need to pause building the chain and
/// resume chaining later. You can also zip multiple [`Dangling`] instances
/// together with a tuple and join or race them.
///
/// Use [`Chain::dangle`] to obtain a [`Dangling`].
#[must_use]
pub struct Dangling<Response, Streams> {
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

pub trait ZippedChains {
    type Joined<'w, 's, 'a> where 's: 'a, 'w: 'a;
    fn join<'w, 's, 'a>(self, commands: &'a mut Commands<'w, 's>) -> Self::Joined<'w, 's, 'a>;

    fn race<'w, 's, 'a, Handlers: RaceHandlers<'w, 's, Self>>(
        self,
        commands: &'a mut Commands<'w, 's>,
        handlers: Handlers
    ) -> Handlers::Output
    where
        Self: Sized;
}

impl<A, StreamsA, B, StreamsB> ZippedChains for (Dangling<A, StreamsA>, Dangling<B, StreamsB>)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
{
    type Joined<'w, 's, 'a> = Chain<'w, 's, 'a, (A, B), (), ModifiersUnset> where 's: 'a, 'w: 'a;
    fn join<'w, 's, 'a>(self, commands: &'a mut Commands<'w, 's>) -> Self::Joined<'w, 's, 'a> {
        let source = commands.spawn((/* Something to manage the joining */)).id();
        let target = commands.spawn(UnusedTarget).id();
        Chain::new(source, target, commands)
    }

    fn race<'w, 's, 'a, Handlers: RaceHandlers<'w, 's, Self>>(
        self,
        commands: &'a mut Commands<'w, 's>,
        handlers: Handlers
    ) -> Handlers::Output
    where
        Self: Sized
    {
        handlers.apply_handlers(self, commands)
    }
}

pub trait RaceHandlers<'w, 's, Z> {
    type Output;
    fn apply_handlers<'a>(self, zip: Z, commands: &'a mut Commands<'w, 's>) -> Self::Output;
}

impl<'w, 's, A, StreamsA, Fa, Ua, B, StreamsB, Fb, Ub> RaceHandlers<'w, 's, (Dangling<A, StreamsA>, Dangling<B, StreamsB>)> for (Fa, Fb)
where
    A: 'static + Send + Sync,
    B: 'static + Send + Sync,
    Fa: FnOnce(Chain<'w, 's, '_, A, (), ModifiersUnset>) -> Ua,
    Fb: FnOnce(Chain<'w, 's, '_, B, (), ModifiersUnset>) -> Ub,
{
    type Output = (Ua, Ub);
    fn apply_handlers<'a>(
        self,
        (dangle_a, dangle_b): (Dangling<A, StreamsA>, Dangling<B, StreamsB>),
        commands: &'a mut Commands<'w, 's>
    ) -> Self::Output {
        // FIXME TODO(@mxgrey): Funnel the dangles into a single target and then fan
        // them out again to their individual handlers. The current implementation
        // is a temporary short-cut for proof of concept.
        let u_a = (self.0)(Chain::new(dangle_a.source, dangle_a.target, commands));
        let u_b = (self.1)(Chain::new(dangle_b.source, dangle_b.target, commands));
        (u_a, u_b)
    }
}
