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
    Operation, Input, ManageInput, InputBundle, OperationRequest, OperationResult,
    OperationReachability, ReachabilityResult, OperationSetup, Stream,
    SingleInputStorage, SingleTargetStorage, OrBroken, OperationCleanup,
    ScopeContents, ScopeStorage,
};

use bevy::prelude::{Component, Entity};

use smallvec::SmallVec;

use std::collections::{HashMap, hash_map::Entry};

struct OperateScope<Request, Streams, Response> {
    /// The first node that is inside of the scope
    enter_scope: Entity,
    /// The staging area where results go when they're finished but are waiting
    /// for the session to be cleaned up within the scope.
    ///
    /// Note that the finished staging node is a child node of the scope but it
    /// is not a node inside of the scoped contents.
    finished_staging: Entity,
    /// The target that the output of this scope should be fed to
    exit_scope: Entity,
    _ignore: std::marker::PhantomData<(Request, Streams, Response)>
}

pub(crate) struct ScopedSession {
    /// ID for a session that was input to the scope
    parent_session: Entity,
    /// ID for the scoped session associated with the input session
    scoped_session: Entity,
    /// What is the current status for the scoped session
    status: ScopedSessionStatus,
}

impl ScopedSession {
    pub fn ongoing(parent_session: Entity, scoped_session: Entity) -> Self {
        Self { parent_session, scoped_session, status: ScopedSessionStatus::Ongoing }
    }
}

pub(crate) enum ScopedSessionStatus {
    Ongoing,
    Finished,
    Cleanup,
}

impl ScopedSessionStatus {
    pub(crate) fn to_cleanup(&mut self) {
        *self = Self::Cleanup;
    }

    pub(crate) fn to_finished(&mut self) {
        // To prevent a scope that's in cleanup mode from pushing a value out
        // after it's supposed to be cancelled, we only assign the Finished
        // status if the session is not in cleanup mode.
        if !matches!(self, Self::Cleanup) {
            *self = Self::Finished;
        }
    }

    fn is_finished(&self) -> bool {
        matches!(self, Self::Finished)
    }
}

#[derive(Component, Default)]
struct ScopedSessionStorage(SmallVec<[ScopedSession; 8]>);

/// Store the terminating nodes for this scope
#[derive(Component)]
pub struct TerminalStorage(pub SmallVec<[Entity; 8]>);

impl TerminalStorage {
    pub fn new() -> Self {
        TerminalStorage(SmallVec::new())
    }
}

impl<Request, Streams, Response> Operation for OperateScope<Request, Streams, Response>
where
    Request: 'static + Send + Sync,
    Streams: Stream,
    Response: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        if let Some(mut target_mut) = world.get_entity_mut(self.exit_scope) {
            target_mut.insert(SingleInputStorage::new(source));
        }

        world.entity_mut(source).insert((
            InputBundle::<Request>::new(),
            ScopeEntryStorage(self.enter_scope),
            FinishedStagingStorage(self.finished_staging),
            ScopeContents::new(),
            SingleTargetStorage(self.exit_scope),
            ScopedSessionStorage::default(),
            TerminalStorage::new(),
            FinalizeScopeCleanup(Self::finalize_scope_cleanup),
        ));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let scoped_session = world.spawn(()).id();
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let enter_scope = source_mut.get::<ScopeEntryStorage>().or_broken()?.0;
        let Input { session: parent_session, data } = source_mut.take_input::<Request>()?;
        source_mut.get_mut::<ScopedSessionStorage>().or_broken()?.0.push(
            ScopedSession::ongoing(parent_session, scoped_session)
        );
        world.get_entity_mut(enter_scope).or_broken()?.give_input(
            scoped_session, data, roster,
        )
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        let mut source_mut = clean.world.get_entity_mut(clean.source).or_broken()?;
        let pairs: SmallVec<[_; 16]> = source_mut.get_mut::<ScopedSessionStorage>()
            .or_broken()?.0
            .iter()
            .filter(|pair| pair.parent_session == clean.session)
            .collect();

        if pairs.is_empty() {
            // We have no record of the mentioned session in this scope, so it
            // is already clean.
            return clean.notify_cleaned();
        };

        for pair in pairs {
            pair.status.to_cleanup();
            let scoped_session = pair.scoped_session;
            let staging_node = source_mut.get::<FinishedStagingStorage>().or_broken()?.0;

            let nodes = source_mut.get::<ScopeContents>().or_broken()?.nodes().clone();
            let mut session_clean = clean.for_session(scoped_session);
            for node in nodes {
                session_clean.for_node(node).clean();
            }

            // OperateScope::cleanup gets called when the entire scope is being cancelled
            // so we need to clear out the staging node as well.
            session_clean.for_node(staging_node).clean();
        }

        Ok(())
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<Request>()? {
            return Ok(true);
        }

        let source_ref = reachability.world.get_entity(reachability.source).or_broken()?;
        let staging = source_ref.get::<FinishedStagingStorage>().or_broken()?.0;

        if let Some(pair) = source_ref
            .get::<ScopedSessionStorage>().or_broken()?
            .0.iter().find(|pair| pair.parent_session == reachability.session)
        {
            let mut visited = HashMap::new();
            let mut scoped_reachability = OperationReachability::new(
                pair.scoped_session,
                reachability.source,
                reachability.world,
                &mut visited
            );

            if scoped_reachability.check_upstream(staging)? {
                return Ok(true);
            }

            let terminals = source_ref.get::<TerminalStorage>().or_broken()?;
            for terminal in &terminals.0 {
                if scoped_reachability.check_upstream(*terminal)? {
                    return Ok(true);
                }
            }
        }

        SingleInputStorage::is_reachable(reachability)
    }
}

impl<Request, Streams, Response> OperateScope<Request, Streams, Response>
where
    Request: 'static + Send + Sync,
    Streams: Stream,
    Response: 'static + Send + Sync,
{
    fn finalize_scope_cleanup(mut clean: OperationCleanup) -> OperationResult {
        let mut source_mut = clean.world.get_entity_mut(clean.source).or_broken()?;
        let mut pairs = source_mut.get_mut::<ScopedSessionStorage>().or_broken()?;
        let (index, _) = pairs.0.iter().enumerate().find(
            |(_, pair)| pair.scoped_session == clean.session
        ).or_not_ready()?;
        let pair = pairs.0.remove(index);
        if pair.status.is_finished() {
            let staging = source_mut.get::<FinishedStagingStorage>().or_broken()?.0;
            let exit_scope = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
            let response = clean.world
                .get_mut::<Staging<Response>>(staging).or_broken()?.0
                .remove(&clean.session).or_broken()?;
            clean.world.get_entity_mut(exit_scope).or_broken()?.give_input(
                pair.parent_session, response, clean.roster,
            );
        } else {
            // The scope is being cleaned up for this session. We should check
            // if all scoped sessions associated with the parent session have
            // been finalized yet.
            let parent_session = pair.parent_session;
            if pairs.0.iter().find(
                |pair| pair.parent_session == parent_session
            ).is_none() {
                // There are no other scoped sessions related to this parent
                // session, so we can notify the parent that cleanup is finished
                // for this scoped node.
                clean.notify_cleaned();
            }
        }

        clean.world.despawn(clean.session);

        Ok(())
    }
}

#[derive(Component, Clone, Copy)]
pub struct FinalizeScopeCleanup(pub(crate) fn(OperationCleanup) -> OperationResult);

#[derive(Component)]
struct ScopeEntryStorage(Entity);

#[derive(Component)]
pub struct FinishedStagingStorage(Entity);

impl FinishedStagingStorage {
    pub fn get(&self) -> Entity {
        self.0
    }
}

pub struct FinishedStaging<T> {
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Operation for FinishedStaging<T>
where
    T: 'static + Send + Sync
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            Staging::<T>::new(),
        ));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<T>()?;
        let mut staging = source_mut.get_mut::<Staging<T>>().or_broken()?;
        match staging.0.entry(session) {
            Entry::Occupied(_) => {
                // We only accept the first terminating output so we will ignore
                // this.
                return Ok(());
            }
            Entry::Vacant(vacant) => {
                vacant.insert(data);
            }
        }

        let scope = source_mut.get::<ScopeStorage>().or_broken()?.get();
        let nodes = world.get::<ScopeContents>(scope).or_broken()?.nodes().clone();
        let mut clean = OperationCleanup { source: scope, session, world, roster };
        for node in nodes {
            clean.for_node(node).clean();
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        let mut staging = clean.world.get_mut::<Staging<T>>(clean.source).or_broken()?;
        staging.0.retain(|session, _| *session != clean.session);
        // We don't call clean.notify_cleaned() here because the staging operation
        // is not considered to be a node inside the workspace so we don't want
        // to register it as a node that has been cleaned; that would throw off
        // the equality check that sees whether all nodes have cleaned up.
        Ok(())
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        let staging = reachability.world.get::<Staging<T>>(reachability.source).or_broken()?;
        Ok(staging.0.contains_key(&reachability.session))
    }
}

/// Map from scoped_session ID to the first output that terminated the scope
#[derive(Component)]
struct Staging<T>(HashMap<Entity, T>);

impl<T> Staging<T> {
    fn new() -> Self {
        Self(HashMap::new())
    }
}
