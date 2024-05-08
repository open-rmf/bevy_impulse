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

use bevy::{
    prelude::{Entity, Component},
};

use smallvec::SmallVec;

use std::collections::HashMap;

use crate::{
    SingleTargetStorage, Cancelled, Operation, InputBundle,
    OperationRoster, NextOperationLink, Cancel, BufferSettings,
    Cancellation, Unreachability, SingleInputStorage, OperationReachability,
    ReachabilityResult, Buffer, ManageInput, OperationCleanup,
    OperationResult, OrBroken, OperationSetup, OperationRequest,
    Input, BeginCancelStorage, FinishCancelStorage, execute_operation,
};

pub(crate) struct CancelledSession {
    parent_session: Entity,
    status: CancelStatus,
}

impl CancelledSession {
    pub(crate) fn new(
        parent_session: Entity,
        status: CancelStatus,
    ) -> Self {
        Self { parent_session, status }
    }
}

pub(crate) enum CancelStatus {
    Cleanup,
    Cancelled(Cancellation),
    Unreachable(Unreachability),
}

pub(crate) struct CancelInputBuffer<T> {
    settings: BufferSettings,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Operation for CancelInputBuffer<T>
where
    T: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert(Buffer::<T>::new(self.settings));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        source_mut.transfer_to_buffer::<T>(roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>();
        clean.notify_cleaned()
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        reachability.has_input::<T>()
        // If this node is active then there is nothing upstream of it, so no
        // need to crawl further up than this.
    }
}

pub(crate) struct BeginCancel<T> {
    from_scope: Entity,
    buffer: Entity,
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Operation for BeginCancel<T>
where
    T: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            CancelInputBufferStorage(self.buffer),
            SingleTargetStorage(self.target),
            CancelFromScope(self.from_scope),
            InputBundle::<()>::new(),
        ));

        world.get_entity_mut(self.from_scope).or_broken()?
            .get_mut::<BeginCancelStorage>().or_broken()?.0
            .push(source);

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session: scoped_session, .. } = source_mut.take_input::<()>()?;
        let input = source_mut.get::<CancelInputBufferStorage>().or_broken()?.0;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let from_scope = source_mut.get::<CancelFromScope>().or_broken()?.0;
        let finish_cancel = world.get::<FinishCancelStorage>(from_scope).or_broken()?.0;

        while let Some(data) = world.get_entity_mut(input)
            .or_broken()?
            .try_from_buffer::<T>(scoped_session)?
        {
            let cancellation_session = world.spawn(()).id();
            world.get_entity_mut(target).or_broken()?
                .give_input(cancellation_session, data, roster);

            world.get_entity_mut(finish_cancel).or_broken()?
                .get_mut::<AwaitingCancelStorage>().or_broken()?
                .map.get_mut(&scoped_session).or_broken()?
                .cancellation_sessions.push(cancellation_session);
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<()>();
        clean.notify_cleaned()
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<()>()? {
            return Ok(true);
        }

        let input = r.world().get::<CancelInputBufferStorage>(r.source()).or_broken()?.0;
        r.check_upstream(input)
    }
}

pub(crate) struct EnterCancel {
    from_scope: Entity,
}

impl Operation for EnterCancel {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(source).or_broken()?.insert((
            CancelFromScope(self.from_scope),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session: scoped_session, data: cancelled } = source_mut.take_input::<CancelledSession>()?;
        let from_scope = source_mut.get::<CancelFromScope>().or_broken()?.0;
        let scope_ref = world.get_entity(from_scope).or_broken()?;

        let finish_cancel = scope_ref.get::<FinishCancelStorage>().or_broken()?.0;
        let begin_cancels = scope_ref.get::<BeginCancelStorage>().or_broken()?.0.clone();

        let mut finish_cancel_mut = world.get_entity_mut(finish_cancel).or_broken()?;
        finish_cancel_mut
            .get_mut::<AwaitingCancelStorage>().or_broken()?
            .map.insert(scoped_session, AwaitingCancel::new(cancelled));
        finish_cancel_mut.give_input(scoped_session, CheckAwaitingSession, roster)?;

        for begin in begin_cancels {
            // We execute the begin nodes immediately so that they can load up the
            // finish_cancel node with all their cancellation behavior IDs before
            // the finish_cancel node gets executed.
            world.get_entity_mut(begin).or_broken()?.give_input(session, (), roster)?;
            execute_operation(OperationRequest { source: begin, world, roster });
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {

    }
}

pub(crate) struct CancelFinished {
    from_scope: Entity,
}

impl Operation for CancelFinished {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        // world.get_entity_mut(entity)
        world.entity_mut(source).insert((
            CancelFromScope(self.from_scope),
            InputBundle::<()>::new(),
            InputBundle::<CheckAwaitingSession>::new(),
            AwaitingCancelStorage::default(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        // 1. Get CheckAwaitingSession input and see if its cancellation_sessions
        //    are empty.
        // 2. Get () input and deduct its session from whichever awaiting session
        //    it belongs to.
        // 3. Check cancellation input and deduct its session from whichever
        //    awaiting session it belongs to.
    }
}

#[derive(Component)]
struct CancelInputBufferStorage(Entity);

#[derive(Component)]
struct CancelFromScope(Entity);

#[derive(Component, Default)]
struct AwaitingCancelStorage {
    /// Key: Scoped Session
    map: HashMap<Entity, AwaitingCancel>,
}

struct AwaitingCancel {
    cancelled: CancelledSession,
    cancellation_sessions: SmallVec<[Entity; 8]>,
}

impl AwaitingCancel {
    fn new(cancelled: CancelledSession) -> Self {
        Self { cancelled, cancellation_sessions: Default::default() }
    }
}

struct CheckAwaitingSession;
