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

use bevy_ecs::{
    hierarchy::ChildOf,
    prelude::{Entity, World},
};
use variadics_please::all_tuples;

use smallvec::SmallVec;

use crate::{
    AddOperation, BeginCleanupWorkflow, Buffer, BufferAccessors, BufferKey, BufferKeyBuilder,
    BufferKeyLifecycle, BufferStorage, Builder, Chain, CleanupWorkflowConditions, CloneFromBuffer,
    ForkTargetStorage, Gate, GateState, InputSlot, InspectBuffer, Join, Listen, ManageBuffer, Node,
    OperateBufferAccess, OperationError, OperationResult, OperationRoster, OrBroken, Output, Scope,
    ScopeSettings, SingleInputStorage, UnusedTarget,
};

pub trait Buffering: 'static + Send + Sync + Clone {
    fn verify_scope(&self, scope: Entity);

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError>;

    fn buffered_count_for(
        &self,
        buffer: Entity,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError>;

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult;

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;

    fn as_input(&self) -> SmallVec<[Entity; 8]>;

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult;
}

pub trait Joining: Buffering {
    type Item: 'static + Send + Sync;
    fn fetch_for_join(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError>;

    /// Join these bufferable workflow elements. Each time every buffer contains
    /// at least one element, this will pull the oldest element from each buffer
    /// and join them into a tuple that gets sent to the target.
    ///
    /// If you need a more general way to get access to one or more buffers,
    /// use [`listen`](Accessing::listen) instead.
    fn join<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Self::Item> {
        let scope = builder.scope();
        self.verify_scope(scope);

        let join = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.queue(AddOperation::new(
            Some(scope),
            join,
            Join::new(self, target),
        ));

        Output::new(scope, target).chain(builder)
    }
}

pub trait Accessing: Buffering {
    type Key: 'static + Send + Sync + Clone;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult;
    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key;
    fn deep_clone_key(key: &Self::Key) -> Self::Key;
    fn is_key_in_use(key: &Self::Key) -> bool;

    /// Create an operation that will output buffer access keys each time any
    /// one of the buffers is modified. This can be used to create a node in a
    /// workflow that wakes up every time one or more buffers change, and then
    /// operates on those buffers.
    ///
    /// For an operation that simply joins the contents of two or more outputs
    /// or buffers, use [`join`](Joining::join) instead.
    fn listen<'w, 's, 'a, 'b>(
        self,
        builder: &'b mut Builder<'w, 's, 'a>,
    ) -> Chain<'w, 's, 'a, 'b, Self::Key> {
        let scope = builder.scope();
        self.verify_scope(scope);

        let listen = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.queue(AddOperation::new(
            Some(scope),
            listen,
            Listen::new(self, target),
        ));

        Output::new(scope, target).chain(builder)
    }

    fn access<T: 'static + Send + Sync>(self, builder: &mut Builder) -> Node<T, (T, Self::Key)> {
        let source = builder.commands.spawn(()).id();
        let target = builder.commands.spawn(UnusedTarget).id();
        builder.commands.queue(AddOperation::new(
            Some(builder.scope()),
            source,
            OperateBufferAccess::<T, Self>::new(self, target),
        ));

        Node {
            input: InputSlot::new(builder.scope(), source),
            output: Output::new(builder.scope(), target),
            streams: (),
        }
    }

    /// Alternative way to call [`Builder::on_cleanup`].
    fn on_cleanup<Settings>(
        self,
        builder: &mut Builder,
        build: impl FnOnce(Scope<Self::Key, (), ()>, &mut Builder) -> Settings,
    ) where
        Settings: Into<ScopeSettings>,
    {
        self.on_cleanup_if(
            builder,
            CleanupWorkflowConditions::always_if(true, true),
            build,
        )
    }

    /// Alternative way to call [`Builder::on_cancel`].
    fn on_cancel<Settings>(
        self,
        builder: &mut Builder,
        build: impl FnOnce(Scope<Self::Key, (), ()>, &mut Builder) -> Settings,
    ) where
        Settings: Into<ScopeSettings>,
    {
        self.on_cleanup_if(
            builder,
            CleanupWorkflowConditions::always_if(false, true),
            build,
        )
    }

    /// Alternative way to call [`Builder::on_terminate`].
    fn on_terminate<Settings>(
        self,
        builder: &mut Builder,
        build: impl FnOnce(Scope<Self::Key, (), ()>, &mut Builder) -> Settings,
    ) where
        Settings: Into<ScopeSettings>,
    {
        self.on_cleanup_if(
            builder,
            CleanupWorkflowConditions::always_if(true, false),
            build,
        )
    }

    /// Alternative way to call [`Builder::on_cleanup_if`].
    fn on_cleanup_if<Settings>(
        self,
        builder: &mut Builder,
        conditions: CleanupWorkflowConditions,
        build: impl FnOnce(Scope<Self::Key, (), ()>, &mut Builder) -> Settings,
    ) where
        Settings: Into<ScopeSettings>,
    {
        let cancelling_scope_id = builder.commands.spawn(()).id();
        let _ = builder.create_scope_impl::<Self::Key, (), (), Settings>(
            cancelling_scope_id,
            builder.context.finish_scope_cancel,
            build,
        );

        let scope = builder.scope();
        let begin_cancel = builder.commands.spawn(()).insert(ChildOf(scope)).id();
        self.verify_scope(builder.scope());
        builder.commands.queue(AddOperation::new(
            None,
            begin_cancel,
            BeginCleanupWorkflow::<Self>::new(
                builder.scope(),
                self,
                cancelling_scope_id,
                conditions.run_on_terminate,
                conditions.run_on_cancel,
            ),
        ));
    }
}

impl<T: 'static + Send + Sync> Buffering for Buffer<T> {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope());
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        world
            .get_entity(self.id())
            .or_broken()?
            .buffered_count::<T>(session)
    }

    fn buffered_count_for(
        &self,
        buffer: Entity,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        if self.id() != buffer {
            return Ok(0);
        }

        self.buffered_count(session, world)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        add_listener_to_source(self.id(), listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        GateState::apply(self.id(), session, action, world, roster)
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.id()])
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        world
            .get_mut::<BufferStorage<T>>(self.id())
            .or_broken()?
            .ensure_session(session);
        Ok(())
    }
}

impl<T: 'static + Send + Sync> Joining for Buffer<T> {
    type Item = T;
    fn fetch_for_join(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        world
            .get_entity_mut(self.id())
            .or_broken()?
            .pull_from_buffer::<T>(session)
    }
}

impl<T: 'static + Send + Sync> Accessing for Buffer<T> {
    type Key = BufferKey<T>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        world
            .get_mut::<BufferAccessors>(self.id())
            .or_broken()?
            .add_accessor(accessor);
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        Self::Key::create_key(&self, builder)
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        key.deep_clone()
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        key.is_in_use()
    }
}

impl<T: 'static + Send + Sync + Clone> Buffering for CloneFromBuffer<T> {
    fn verify_scope(&self, scope: Entity) {
        assert_eq!(scope, self.scope());
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        world
            .get_entity(self.id())
            .or_broken()?
            .buffered_count::<T>(session)
    }

    fn buffered_count_for(
        &self,
        buffer: Entity,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        if buffer != self.id() {
            return Ok(0);
        }

        self.buffered_count(session, world)
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        add_listener_to_source(self.id(), listener, world)
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        GateState::apply(self.id(), session, action, world, roster)
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        SmallVec::from_iter([self.id()])
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        world
            .get_entity_mut(self.id())
            .or_broken()?
            .ensure_session::<T>(session)
    }
}

impl<T: 'static + Send + Sync + Clone> Joining for CloneFromBuffer<T> {
    type Item = T;
    fn fetch_for_join(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        world
            .get_entity(self.id())
            .or_broken()?
            .try_clone_from_buffer(session)
            .and_then(|r| r.or_broken())
    }
}

impl<T: 'static + Send + Sync + Clone> Accessing for CloneFromBuffer<T> {
    type Key = BufferKey<T>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        world
            .get_mut::<BufferAccessors>(self.id())
            .or_broken()?
            .add_accessor(accessor);
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        Self::Key::create_key(&(*self).into(), builder)
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        key.deep_clone()
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        key.is_in_use()
    }
}

macro_rules! impl_buffered_for_tuple {
    ($(($T:ident, $K:ident)),*) => {
        #[allow(non_snake_case)]
        impl<$($T: Buffering),*> Buffering for ($($T,)*)
        {
            fn verify_scope(&self, scope: Entity) {
                let ($($T,)*) = self;
                $(
                    $T.verify_scope(scope);
                )*
            }

            fn buffered_count(
                &self,
                session: Entity,
                world: &World,
            ) -> Result<usize, OperationError> {
                let ($($T,)*) = self;
                Ok([
                    $(
                        $T.buffered_count(session, world)?,
                    )*
                ].iter().copied().min().unwrap_or(0))
            }

            fn buffered_count_for(
                &self,
                buffer: Entity,
                session: Entity,
                world: &World,
            ) -> Result<usize, OperationError> {
                let ($($T,)*) = self;
                Ok([
                    $(
                        $T.buffered_count_for(buffer, session, world)?,
                    )*
                ].iter().copied().max().unwrap_or(0))
            }

            fn add_listener(
                &self,
                listener: Entity,
                world: &mut World,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.add_listener(listener, world)?;
                )*
                Ok(())
            }

            fn gate_action(
                &self,
                session: Entity,
                action: Gate,
                world: &mut World,
                roster: &mut OperationRoster,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.gate_action(session, action, world, roster)?;
                )*
                Ok(())
            }

            fn as_input(&self) -> SmallVec<[Entity; 8]> {
                let mut inputs = SmallVec::new();
                let ($($T,)*) = self;
                $(
                    inputs.extend($T.as_input());
                )*
                inputs
            }

            fn ensure_active_session(
                &self,
                session: Entity,
                world: &mut World,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.ensure_active_session(session, world)?;
                )*
                Ok(())
            }
        }

        #[allow(non_snake_case)]
        impl<$($T: Joining),*> Joining for ($($T,)*)
        {
            type Item = ($($T::Item),*);
            fn fetch_for_join(
                &self,
                session: Entity,
                world: &mut World,
            ) -> Result<Self::Item, OperationError> {
                let ($($T,)*) = self;
                Ok(($(
                    $T.fetch_for_join(session, world)?,
                )*))
            }
        }

        #[allow(non_snake_case)]
        impl<$($T: Accessing),*> Accessing for ($($T,)*)
        {
            type Key = ($($T::Key), *);
            fn add_accessor(
                &self,
                accessor: Entity,
                world: &mut World,
            ) -> OperationResult {
                let ($($T,)*) = self;
                $(
                    $T.add_accessor(accessor, world)?;
                )*
                Ok(())
            }

            fn create_key(
                &self,
                builder: &BufferKeyBuilder,
            ) -> Self::Key {
                let ($($T,)*) = self;
                ($(
                    $T.create_key(builder),
                )*)
            }

            fn deep_clone_key(key: &Self::Key) -> Self::Key {
                let ($($K,)*) = key;
                ($(
                    $T::deep_clone_key($K),
                )*)
            }

            fn is_key_in_use(key: &Self::Key) -> bool {
                let ($($K,)*) = key;
                false $(
                    || $T::is_key_in_use($K)
                )*
            }
        }
    }
}

// Implements the `Buffering` trait for all tuples between size 2 and 12
// (inclusive) made of types that implement `Buffering`
all_tuples!(impl_buffered_for_tuple, 2, 12, T, K);

impl<T: Buffering, const N: usize> Buffering for [T; N] {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self.iter() {
            buffer.verify_scope(scope);
        }
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        let mut min_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count(session, world)?;
            if !min_count.is_some_and(|min| min < count) {
                min_count = Some(count);
            }
        }

        Ok(min_count.unwrap_or(0))
    }

    fn buffered_count_for(
        &self,
        buffer_entity: Entity,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        let mut max_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count_for(buffer_entity, session, world)?;
            if max_count.is_none_or(|max| max < count) {
                max_count = Some(count);
            }
        }

        Ok(max_count.unwrap_or(0))
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_listener(listener, world)?;
        }
        Ok(())
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        for buffer in self {
            buffer.gate_action(session, action, world, roster)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
    }
}

impl<T: Joining, const N: usize> Joining for [T; N] {
    // TODO(@mxgrey) We may be able to use [T::Item; N] here instead of SmallVec
    // when try_map is stabilized: https://github.com/rust-lang/rust/issues/79711
    type Item = SmallVec<[T::Item; N]>;
    fn fetch_for_join(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        self.iter()
            .map(|buffer| buffer.fetch_for_join(session, world))
            .collect()
    }
}

impl<T: Accessing, const N: usize> Accessing for [T; N] {
    type Key = SmallVec<[T::Key; N]>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_accessor(accessor, world)?;
        }
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        let mut keys = SmallVec::new();
        for buffer in self {
            keys.push(buffer.create_key(builder));
        }
        keys
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        let mut keys = SmallVec::new();
        for k in key {
            keys.push(T::deep_clone_key(k));
        }
        keys
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        for k in key {
            if T::is_key_in_use(k) {
                return true;
            }
        }

        false
    }
}

impl<T: Buffering, const N: usize> Buffering for SmallVec<[T; N]> {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self.iter() {
            buffer.verify_scope(scope);
        }
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        let mut min_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count(session, world)?;
            if !min_count.is_some_and(|min| min < count) {
                min_count = Some(count);
            }
        }

        Ok(min_count.unwrap_or(0))
    }

    fn buffered_count_for(
        &self,
        buffer_entity: Entity,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        let mut max_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count_for(buffer_entity, session, world)?;
            if max_count.is_none_or(|max| max < count) {
                max_count = Some(count);
            }
        }

        Ok(max_count.unwrap_or(0))
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_listener(listener, world)?;
        }
        Ok(())
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        for buffer in self {
            buffer.gate_action(session, action, world, roster)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
    }
}

impl<T: Joining, const N: usize> Joining for SmallVec<[T; N]> {
    type Item = SmallVec<[T::Item; N]>;
    fn fetch_for_join(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        self.iter()
            .map(|buffer| buffer.fetch_for_join(session, world))
            .collect()
    }
}

impl<T: Accessing, const N: usize> Accessing for SmallVec<[T; N]> {
    type Key = SmallVec<[T::Key; N]>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_accessor(accessor, world)?;
        }
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        let mut keys = SmallVec::new();
        for buffer in self {
            keys.push(buffer.create_key(builder));
        }
        keys
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        let mut keys = SmallVec::new();
        for k in key {
            keys.push(T::deep_clone_key(k));
        }
        keys
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        for k in key {
            if T::is_key_in_use(k) {
                return true;
            }
        }

        false
    }
}

impl<B: Buffering> Buffering for Vec<B> {
    fn verify_scope(&self, scope: Entity) {
        for buffer in self {
            buffer.verify_scope(scope);
        }
    }

    fn buffered_count(&self, session: Entity, world: &World) -> Result<usize, OperationError> {
        let mut min_count = None;
        for buffer in self {
            let count = buffer.buffered_count(session, world)?;
            if !min_count.is_some_and(|min| min < count) {
                min_count = Some(count);
            }
        }

        Ok(min_count.unwrap_or(0))
    }

    fn buffered_count_for(
        &self,
        buffer_entity: Entity,
        session: Entity,
        world: &World,
    ) -> Result<usize, OperationError> {
        let mut max_count = None;
        for buffer in self.iter() {
            let count = buffer.buffered_count_for(buffer_entity, session, world)?;
            if max_count.is_none_or(|max| max < count) {
                max_count = Some(count);
            }
        }

        Ok(max_count.unwrap_or(0))
    }

    fn add_listener(&self, listener: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_listener(listener, world)?;
        }
        Ok(())
    }

    fn gate_action(
        &self,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        for buffer in self {
            buffer.gate_action(session, action, world, roster)?;
        }
        Ok(())
    }

    fn as_input(&self) -> SmallVec<[Entity; 8]> {
        self.iter().flat_map(|buffer| buffer.as_input()).collect()
    }

    fn ensure_active_session(&self, session: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.ensure_active_session(session, world)?;
        }

        Ok(())
    }
}

impl<B: Joining> Joining for Vec<B> {
    type Item = Vec<B::Item>;
    fn fetch_for_join(
        &self,
        session: Entity,
        world: &mut World,
    ) -> Result<Self::Item, OperationError> {
        self.iter()
            .map(|buffer| buffer.fetch_for_join(session, world))
            .collect()
    }
}

impl<B: Accessing> Accessing for Vec<B> {
    type Key = Vec<B::Key>;
    fn add_accessor(&self, accessor: Entity, world: &mut World) -> OperationResult {
        for buffer in self {
            buffer.add_accessor(accessor, world)?;
        }
        Ok(())
    }

    fn create_key(&self, builder: &BufferKeyBuilder) -> Self::Key {
        let mut keys = Vec::new();
        for buffer in self {
            keys.push(buffer.create_key(builder));
        }
        keys
    }

    fn deep_clone_key(key: &Self::Key) -> Self::Key {
        let mut keys = Vec::new();
        for k in key {
            keys.push(B::deep_clone_key(k));
        }
        keys
    }

    fn is_key_in_use(key: &Self::Key) -> bool {
        for k in key {
            if B::is_key_in_use(k) {
                return true;
            }
        }

        false
    }
}

pub(crate) fn add_listener_to_source(
    source: Entity,
    listener: Entity,
    world: &mut World,
) -> OperationResult {
    let mut targets = world.get_mut::<ForkTargetStorage>(source).or_broken()?;
    if !targets.0.contains(&listener) {
        targets.0.push(listener);
    }

    if let Some(mut input_storage) = world.get_mut::<SingleInputStorage>(listener) {
        input_storage.add(source);
    } else {
        world
            .get_entity_mut(listener)
            .or_broken()?
            .insert(SingleInputStorage::new(source));
    }

    Ok(())
}
