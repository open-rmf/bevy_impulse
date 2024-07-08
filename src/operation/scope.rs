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
    OperationReachability, ReachabilityResult, OperationSetup, StreamPack,
    SingleInputStorage, SingleTargetStorage, OrBroken, OperationCleanup,
    Cancellation, Unreachability, InspectDisposals, execute_operation,
    Cancellable, OperationRoster, ManageCancellation,
    OperationError, OperationCancel, Cancel, UnhandledErrors, check_reachability,
    Blocker, Stream, StreamTargetStorage, StreamRequest, AddOperation,
    ScopeSettings, StreamTargetMap,
};

use backtrace::Backtrace;

use bevy::prelude::{Component, Entity, World, Commands};

use smallvec::SmallVec;

use std::collections::{HashMap, hash_map::Entry};

#[derive(Component)]
pub struct ParentSession(Entity);

impl ParentSession {
    pub fn new(entity: Entity) -> Self {
        Self(entity)
    }

    pub fn get(&self) -> Entity {
        self.0
    }
}

#[derive(Clone)]
pub(crate) struct OperateScope<Request, Response, Streams> {
    /// The first node that is inside of the scope
    enter_scope: Entity,
    /// The final target of the nodes inside the scope. It receives the final
    /// output to be produced by the scope. When this target is triggered, the
    /// scope will begin its cleanup.
    ///
    /// Note that the finished staging node is a child node of the scope but it
    /// is not a node inside of the scoped contents.
    terminal: Entity,
    /// The target that the output of this scope should be fed to
    exit_scope: Option<Entity>,
    /// Cancellation finishes at this node
    finish_cancel: Entity,
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<Request, Response, Streams> OperateScope<Request, Response, Streams> {
    pub(crate) fn terminal(&self) -> Entity {
        self.terminal
    }

    pub(crate) fn enter_scope(&self) -> Entity {
        self.enter_scope
    }
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

#[derive(Component)]
pub(crate) enum ScopedSessionStatus {
    Ongoing,
    Finished,
    Cleanup,
    Cancelled(Cancellation),
}

impl ScopedSessionStatus {
    fn to_cleanup(&mut self) -> bool {
        if matches!(self, Self::Cleanup) {
            return false;
        }

        *self = Self::Cleanup;
        true
    }

    pub(crate) fn to_finished(&mut self) -> bool {
        // Only switch it to finished if it was still ongoing. Anything else
        // should not get converted to a finished status.
        if matches!(self, Self::Ongoing) {
            *self = Self::Finished;
            return true;
        }
        false
    }

    fn to_cancelled(&mut self, cancellation: Cancellation) -> bool {
        if matches!(self, Self::Ongoing) {
            *self = Self::Cancelled(cancellation);
            return true;
        }
        false
    }
}

#[derive(Component, Default)]
struct ScopedSessionStorage(SmallVec<[ScopedSession; 8]>);

/// Store the terminating nodes for this scope
#[derive(Component)]
pub struct TerminalStorage(Entity);

impl TerminalStorage {
    pub fn get(&self) -> Entity {
        self.0
    }
}

impl<Request, Response, Streams> Operation for OperateScope<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Streams: StreamPack,
    Response: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {

        let mut source_mut = world.entity_mut(source);
        source_mut.insert((
            InputBundle::<Request>::new(),
            ScopeEntryStorage(self.enter_scope),
            FinishedStagingStorage(self.terminal),
            ScopeContents::new(),
            ScopedSessionStorage::default(),
            TerminalStorage(self.terminal),
            Cancellable::new(Self::receive_cancel),
            ValidateScopeReachability(Self::validate_scope_reachability),
            FinalizeScopeCleanup(Self::finalize_scope_cleanup),
            BeginCancelStorage::default(),
            FinishCancelStorage(self.finish_cancel),
        ));

        if let Some(exit_scope) = self.exit_scope {
            source_mut.insert(SingleTargetStorage::new(exit_scope));

            world.get_entity_mut(exit_scope).or_broken()?
                .insert(SingleInputStorage::new(source));
        } else {
            source_mut.insert(ExitTargetStorage::default());
        }

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let input = world.get_entity_mut(source).or_broken()?
            .take_input::<Request>()?;

        let scoped_session = world.spawn(ParentSession(input.session)).id();
        let result = begin_scope(
            input,
            scoped_session,
            OperationRequest { source, world, roster },
        );

        if result.is_err() {
            // We won't be executing this scope after all, so despawn the scoped
            // session that we created.
            world.despawn(scoped_session);
        }

        result
    }

    fn cleanup(
        OperationCleanup { source, session, world, roster }: OperationCleanup
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let pairs: SmallVec<[_; 16]> = source_mut
            .get_mut::<ScopedSessionStorage>()
            .or_broken()?
            .0
            .iter_mut()
            .filter(|pair| pair.parent_session == session)
            .filter_map(|p| {
                if p.status.to_cleanup() {
                    Some(p.scoped_session)
                } else {
                    None
                }
            })
            .collect();

        if pairs.is_empty() {
            // We have no record of the mentioned session in this scope, so it
            // is already clean.
            return OperationCleanup { source, session, world, roster }.notify_cleaned();
        };

        for scoped_session in pairs {
            let source_ref = world.get_entity(source).or_broken()?;
            let staging_node = source_ref.get::<FinishedStagingStorage>().or_broken()?.0;
            let nodes = source_ref.get::<ScopeContents>().or_broken()?.nodes().clone();
            for node in nodes {
                OperationCleanup { source: node, session: scoped_session, world, roster }.clean();
            }

            // OperateScope::cleanup gets called when the entire scope is being cancelled
            // so we need to clear out the staging node as well.
            OperationCleanup { source: staging_node, session: scoped_session, world, roster }.clean();
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

            let terminal = source_ref.get::<TerminalStorage>().or_broken()?.0;
            if scoped_reachability.check_upstream(terminal)? {
                return Ok(true);
            }
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

pub(crate) fn begin_scope<Request>(
    Input { session: parent_session, data }: Input<Request>,
    scoped_session: Entity,
    OperationRequest { source, world, roster }: OperationRequest,
) -> OperationResult
where
    Request: 'static + Send + Sync,
{
    let mut source_mut = world.get_entity_mut(source).or_broken()?;
    let enter_scope = source_mut.get::<ScopeEntryStorage>().or_broken()?.0;

    source_mut
        .get_mut::<ScopedSessionStorage>()
        .or_broken()?.0
        .push(ScopedSession::ongoing(parent_session, scoped_session));

    world.get_entity_mut(enter_scope).or_broken()?.give_input(
        scoped_session, data, roster,
    )?;

    Ok(())
}

impl<Request, Response, Streams> OperateScope<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    pub(crate) fn new(
        scope_id: Entity,
        exit_scope: Option<Entity>,
        settings: ScopeSettings,
        commands: &mut Commands,
    ) -> Self {
        let enter_scope = commands.spawn(()).id();
        let terminal = commands.spawn(()).id();
        let finish_cancel = commands.spawn(()).id();
        commands.add(AddOperation::new(
            finish_cancel,
            FinishCancel { from_scope: scope_id },
        ));

        let scope = OperateScope {
            enter_scope,
            terminal,
            exit_scope,
            finish_cancel,
            _ignore: Default::default(),
        };

        scope
    }

    fn receive_cancel(
        OperationCancel {
            cancel: Cancel { source: _origin, target: source, session, cancellation },
            world,
            roster
        }: OperationCancel,
    ) -> OperationResult {
        if let Some(session) = session {
            // We only need to cancel one specific session
            return Self::cancel_one(session, source, cancellation, world, roster);
        }

        // We need to cancel all sessions. This is usually because a workflow
        // is fundamentally broken.
        let all_scoped_sessions: SmallVec<[Entity; 16]> = world.get::<ScopedSessionStorage>(source)
            .or_broken()?.0
            .iter()
            .map(|p| p.scoped_session)
            .collect();

        for scoped_session in all_scoped_sessions {
            if let Err(error) = Self::cancel_one(
                scoped_session, source, cancellation.clone(), world, roster
            ) {
                world
                .get_resource_or_insert_with(|| UnhandledErrors::default())
                .operations.push(error);
            }
        }

        Ok(())
    }

    fn cancel_one(
        session: Entity,
        source: Entity,
        cancellation: Cancellation,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let source_ref = world.get_entity(source).or_broken()?;
        let sessions = source_ref.get::<ScopedSessionStorage>().or_broken()?;

        // The cancelled session could be a scoped session or it could be a
        // parent session (e.g. because the target was dropped). We won't be
        // able to tell without checking against both.
        let scoped_sessions: SmallVec<[Entity; 16]> = sessions.0
            .iter()
            .filter(|pair| pair.scoped_session == session || pair.parent_session == session)
            .map(|p| p.scoped_session)
            .collect();

        for scoped_session in scoped_sessions {
            let mut sessions = world.get_mut::<ScopedSessionStorage>(source).or_broken()?;
            let Some(pair) = sessions.0
                .iter_mut()
                .find(|pair| pair.scoped_session == scoped_session) else
            {
                continue;
            };

            if pair.status.to_cancelled(cancellation.clone()) {
                cleanup_entire_scope(OperationCleanup {
                    source, session: scoped_session, world, roster,
                })?;
            }
        }

        Ok(())
    }

    /// Check if the terminal node of the scope can be reached. If not, cancel
    /// the scope immediately.
    fn validate_scope_reachability(clean: OperationCleanup) -> OperationResult {
        let scoped_session = clean.session;
        let source_ref = clean.world.get_entity(clean.source).or_broken()?;
        let terminal = source_ref.get::<TerminalStorage>().or_broken()?.0;
        if check_reachability(scoped_session, terminal, clean.world)? {
            // The terminal node can still be reached, so we're done
            return Ok(());
        }

        // The terminal node cannot be reached so we should cancel this scope.
        let nodes = clean.world.get::<ScopeContents>(clean.source).or_broken()?
            .nodes();

        let mut disposals = Vec::new();
        for node in nodes {
            if let Some(node_disposals) = clean.world.get_entity(*node)
                .or_broken()?
                .get_disposals(scoped_session)
            {
                disposals.extend(node_disposals.iter().cloned());
            }
        }

        let mut source_mut = clean.world.get_entity_mut(clean.source).or_broken()?;
        let mut sessions = source_mut.get_mut::<ScopedSessionStorage>().or_broken()?;
        let pair = sessions.0
            .iter_mut()
            .find(|pair| pair.scoped_session == scoped_session)
            .or_not_ready()?;

        if pair.status.to_cancelled(Unreachability {
            scope: clean.source,
            session: pair.parent_session,
            disposals,
        }.into()) {
            cleanup_entire_scope(clean)?;
        }

        Ok(())
    }

    fn finalize_scope_cleanup(clean: OperationCleanup) -> OperationResult {
        let mut source_mut = clean.world.get_entity_mut(clean.source).or_broken()?;
        let mut pairs = source_mut.get_mut::<ScopedSessionStorage>().or_broken()?;
        let scoped_session = clean.session;
        let (index, _) = pairs.0.iter().enumerate().find(
            |(_, pair)| pair.scoped_session == scoped_session
        ).or_not_ready()?;
        let pair = pairs.0.remove(index);
        let parent_session = pair.parent_session;
        match pair.status {
            ScopedSessionStatus::Ongoing => {
                // We shouldn't be in this function if the session is still ongoing
                // so we'll return a broken error here.
                None.or_broken()?;
            }
            ScopedSessionStatus::Finished => {
                let staging = source_mut.get::<FinishedStagingStorage>().or_broken()?.0;
                let (target, blocker) = source_mut.get_mut::<ExitTargetStorage>()
                    .and_then(|mut storage| storage.map.remove(&scoped_session))
                    .map(|exit| (exit.target, exit.blocker))
                    .or_else(|| {
                        source_mut
                        .get::<SingleTargetStorage>()
                        .map(|target| (target.get(), None))
                    })
                    .or_broken()?;

                let response = clean.world
                    .get_mut::<Staging<Response>>(staging).or_broken()?.0
                    .remove(&clean.session).or_broken()?;
                clean.world.get_entity_mut(target).or_broken()?.give_input(
                    pair.parent_session, response, clean.roster,
                )?;

                if let Some(blocker) = blocker {
                    let serve_next = blocker.serve_next;
                    serve_next(blocker, clean.world, clean.roster);
                }

                clean.world.despawn(clean.session);
            }
            ScopedSessionStatus::Cleanup => {
                let status = CancelStatus::Cleanup;
                Self::begin_cancellation_workflows(
                    CancelledSession { parent_session, status },
                    clean,
                )?;
            }
            ScopedSessionStatus::Cancelled(cancellation) => {
                let status = CancelStatus::Cancelled(cancellation);
                Self::begin_cancellation_workflows(
                    CancelledSession { parent_session, status },
                    clean,
                )?;
            }
        }

        Ok(())
    }

    fn begin_cancellation_workflows(
        cancelled: CancelledSession,
        OperationCleanup { source, session, world, roster }: OperationCleanup,
    ) -> OperationResult {
        let scoped_session = session;
        let scope_ref = world.get_entity(source).or_broken()?;
        let finish_cancel = scope_ref.get::<FinishCancelStorage>().or_broken()?.0;
        let begin_cancels = scope_ref.get::<BeginCancelStorage>().or_broken()?.0.clone();

        let mut finish_cancel_mut = world.get_entity_mut(finish_cancel).or_broken()?;
        finish_cancel_mut
            .get_mut::<AwaitingCancelStorage>().or_broken()?.0
            .push(AwaitingCancel::new(scoped_session, cancelled));
        finish_cancel_mut.give_input(scoped_session, CheckAwaitingSession, roster)?;

        for begin in begin_cancels {
            // We execute the begin nodes immediately so that they can load up the
            // finish_cancel node with all their cancellation behavior IDs before
            // the finish_cancel node gets executed.
            unsafe {
                // INVARIANT: We can use sneak_input here because we execute the
                // recipient node immediately after giving the input.
                world.get_entity_mut(begin).or_broken()?
                .sneak_input(scoped_session, ())?;
            }
            execute_operation(OperationRequest { source: begin, world, roster });
        }

        Ok(())
    }
}

#[derive(Component, Clone, Copy)]
pub struct ValidateScopeReachability(pub(crate) fn(OperationCleanup) -> OperationResult);

#[derive(Component, Clone, Copy)]
pub struct FinalizeScopeCleanup(pub(crate) fn(OperationCleanup) -> OperationResult);

#[derive(Component)]
struct ScopeEntryStorage(Entity);

#[derive(Component)]
struct CancelEntryStorage(Entity);

#[derive(Component)]
pub struct FinishedStagingStorage(Entity);

impl FinishedStagingStorage {
    pub fn get(&self) -> Entity {
        self.0
    }
}

pub(crate) struct Terminate<T> {
    _ignore: std::marker::PhantomData<T>,
}

impl<T> Terminate<T> {
    pub(crate) fn new() -> Self {
        Self { _ignore: Default::default() }
    }
}

fn cleanup_entire_scope(
    OperationCleanup { source, session, world, roster }: OperationCleanup
) -> OperationResult {
    let nodes = world.get::<ScopeContents>(source).or_broken()?.nodes().clone();
    for node in nodes {
        OperationCleanup { source: node, session, world, roster }.clean();
    }
    Ok(())
}

impl<T> Operation for Terminate<T>
where
    T: 'static + Send + Sync
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            Staging::<T>::new(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session: scoped_session, data } = source_mut.take_input::<T>()?;

        let mut staging = source_mut.get_mut::<Staging<T>>().or_broken()?;
        match staging.0.entry(scoped_session) {
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
        let mut pairs = world.get_mut::<ScopedSessionStorage>(scope).or_broken()?;
        let pair = pairs.0
            .iter_mut()
            .find(|pair| pair.scoped_session == scoped_session)
            .or_broken()?;

        if !pair.status.to_finished() {
            // This will not actually change the status of the scoped session,
            // so skip the rest of this function.
            return Ok(());
        }

        let clean = OperationCleanup { source: scope, session: scoped_session, world, roster };
        cleanup_entire_scope(clean)
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


/// The scope that the node exists inside of.
#[derive(Component, Clone, Copy)]
pub struct ScopeStorage(Entity);

impl ScopeStorage {
    pub fn get(&self) -> Entity {
        self.0
    }
}

/// The contents inside a scope entity.
#[derive(Default, Component)]
pub struct ScopeContents {
    nodes: SmallVec<[Entity; 16]>,
    cleanup: HashMap<Entity, SmallVec<[Entity; 16]>>,
}

impl ScopeContents {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: Entity) {
        if let Err(index) = self.nodes.binary_search(&node) {
            self.nodes.insert(index, node);
        }
    }

    pub fn register_cleanup_of_node(&mut self, session: Entity, node: Entity) -> bool {
        let cleanup = self.cleanup.entry(session).or_default();
        if let Err(index) = cleanup.binary_search(&node) {
            cleanup.insert(index, node);
        }

        self.nodes == *cleanup
    }

    pub fn nodes(&self) -> &SmallVec<[Entity; 16]> {
        &self.nodes
    }
}

#[derive(Component, Default)]
pub(crate) struct BeginCancelStorage(pub(crate) SmallVec<[Entity; 8]>);

#[derive(Component)]
pub(crate) struct FinishCancelStorage(pub(crate) Entity);

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
}

impl CancelStatus {
    fn is_cleanup(&self) -> bool {
        matches!(self, Self::Cleanup)
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
            SingleTargetStorage::new(self.target),
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

        let buffer = world.get_entity_mut(input)
            .or_broken()?
            .consume_buffer::<T>(scoped_session)?;

        for data in buffer {
            let cancellation_session = world.spawn(ParentSession(scoped_session)).id();
            world.get_entity_mut(target).or_broken()?
                .give_input(cancellation_session, data, roster)?;

            world.get_entity_mut(finish_cancel).or_broken()?
                .get_mut::<AwaitingCancelStorage>().or_broken()?.0.iter_mut()
                .find(|a| a.scoped_session == scoped_session).or_broken()?
                .cancellation_sessions.push(cancellation_session);
        }

        Ok(())
    }

    fn cleanup(_: OperationCleanup) -> OperationResult {
        // This should never get called. BeginCancel should never exist as a
        // node that's inside of a scope.
        Err(OperationError::Broken(Some(Backtrace::new())))
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<()>()? {
            return Ok(true);
        }

        let input = r.world().get::<CancelInputBufferStorage>(r.source()).or_broken()?.0;
        r.check_upstream(input)
    }
}

pub(crate) struct FinishCancel {
    from_scope: Entity,
}

impl Operation for FinishCancel {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            CancelFromScope(self.from_scope),
            InputBundle::<()>::new(),
            InputBundle::<CheckAwaitingSession>::new(),
            Cancellable::new(Self::receive_cancel),
            AwaitingCancelStorage::default(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        if let Some(Input { session: new_scoped_session, .. }) = source_mut.try_take_input::<CheckAwaitingSession>()? {
            let mut awaiting = source_mut.get_mut::<AwaitingCancelStorage>().or_broken()?;
            if let Some((index, a)) = awaiting.0.iter_mut().enumerate().find(
                |(_, a)| a.scoped_session == new_scoped_session)
            {
                if a.cancellation_sessions.is_empty() {
                    // No cancellation sessions were started for this scoped
                    // session so we can immediately clean it up.
                    Self::finalize_scoped_session(
                        index,
                        OperationRequest { source, world, roster },
                    )?;
                }
            }
        } else if let Some(Input { session: cancellation_session, .. }) = source_mut.try_take_input::<()>()? {
            Self::deduct_finished_cancellation(
                source, cancellation_session, world, roster, None,
            )?;
        }

        Ok(())
    }

    fn cleanup(_: OperationCleanup) -> OperationResult {
        // This should never get called. FinishCancel should never exist as a
        // node that's inside of a scope.
        Err(OperationError::Broken(Some(Backtrace::new())))
    }

    fn is_reachable(_: OperationReachability) -> ReachabilityResult {
        // This should never get called. FinishCancel should never exist as a
        // node that's inside of a scope.
        Err(OperationError::Broken(Some(Backtrace::new())))
    }
}

impl FinishCancel {
    fn receive_cancel(
        OperationCancel {
            cancel: Cancel { source: _origin, target: source, session, cancellation },
            world,
            roster
        }: OperationCancel,
    ) -> OperationResult {
        if let Some(cancellation_session) = session {
            // We just need to cancel a specific cancellation session. The
            // cancellation signal for a FinishCancel always comes from a child
            // cancellation session, never from an outside source.
            return Self::deduct_finished_cancellation(
                source, cancellation_session, world, roster, Some(cancellation),
            );
        }

        // All cancellation sessions need to be wiped out. This usually implies
        // that some workflow has broken entities.
        let cancellation_sessions: SmallVec<[Entity; 16]> = world
            .get::<AwaitingCancelStorage>(source).or_broken()?.0
            .iter()
            .flat_map(|a| a.cancellation_sessions.iter())
            .copied()
            .collect();

        for cancellation_session in cancellation_sessions {
            // TODO(@mxgrey): Should we try to cancel the cancellation workflow?
            // This is a pretty extreme edge case so it would be tricky to wind
            // this down correctly.
            if let Err(error) = Self::deduct_finished_cancellation(
                source, cancellation_session, world, roster, Some(cancellation.clone())
            ) {
                world
                .get_resource_or_insert_with(|| UnhandledErrors::default())
                .operations.push(error);
            }
        }

        Ok(())
    }

    fn deduct_finished_cancellation(
        source: Entity,
        cancellation_session: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
        inner_cancellation: Option<Cancellation>,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let mut awaiting = source_mut.get_mut::<AwaitingCancelStorage>().or_broken()?;
        if let Some((index, a)) = awaiting.0.iter_mut().enumerate().find(
            |(_, a)| a.cancellation_sessions.iter().find(|s| **s == cancellation_session).is_some()
        ) {
            if let Some(inner_cancellation) = inner_cancellation {
                match &mut a.cancelled.status {
                    CancelStatus::Cancelled(cancellation) => {
                        cancellation.while_cancelling.push(inner_cancellation);
                    }
                    CancelStatus::Cleanup => {
                        // Do nothing. We have no sensible way to communicate
                        // the occurrence of the cancellation to the requester.
                        // This should be okay since a cleanup is happening,
                        // which implies the user is getting what they need from
                        // us, making a cancellation irrelevant.
                        //
                        // We could consider moving the cancellation into the
                        // unhandled errors resource, but this seems unnecessary
                        // for now.
                    }
                }
            }

            a.cancellation_sessions.retain(|s| *s != cancellation_session);
            if a.cancellation_sessions.is_empty() {
                // All cancellation sessions for this scoped session have
                // finished so we can clean it up now.
                Self::finalize_scoped_session(
                    index,
                    OperationRequest { source, world, roster }
                )?;
            }
        }
        Ok(())
    }

    fn finalize_scoped_session(
        index: usize,
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let scope = source_mut.get::<ScopeStorage>().or_broken()?.get();
        let mut awaiting = source_mut.get_mut::<AwaitingCancelStorage>().or_broken()?;
        let a = awaiting.0.get(index).or_broken()?;
        let parent_session = a.cancelled.parent_session;
        if !a.cancelled.status.is_cleanup() {
            // We can remove this right away since it's a cancellation.
            let a = awaiting.0.remove(index);
            if let CancelStatus::Cancelled(cancellation) = a.cancelled.status {
                source_mut.emit_cancel(parent_session, cancellation, roster);
            }
        }

        // Check if the scope is being cleaned up for the parent session. We
        // should check that no more cancellation behaviors are pending for the
        // parent scope and that no other scoped sessions are still running for
        // the parent scope. Only after all of that is finished can we notify
        // that we the parent session is cleaned.
        let mut being_cleaned = false;
        let mut cleaning_finished = true;
        for a in &source_mut.get::<AwaitingCancelStorage>().or_broken()?.0 {
            if a.cancelled.parent_session == parent_session {
                if !a.cancellation_sessions.is_empty() {
                    cleaning_finished = false;
                }

                if a.cancelled.status.is_cleanup() {
                    being_cleaned = true;
                }
            }
        }

        if being_cleaned && cleaning_finished {
            // Also check the scope for any ongoing scoped sessions related
            // to this parent.
            let scope = source_mut.get::<CancelFromScope>().or_broken()?.0;
            let scope_ref = world.get_entity(scope).or_broken()?;
            let pairs = scope_ref.get::<ScopedSessionStorage>().or_broken()?;
            if pairs.0.iter().find(
                |pair| pair.parent_session == parent_session
            ).is_some() {
                cleaning_finished = false;
            }
        }

        if being_cleaned && cleaning_finished {
            // The cleaning is finished so we can purge all memory of the
            // parent session and then notify the parent scope that it's
            // clean.
            let mut awaiting = world.get_mut::<AwaitingCancelStorage>(source).or_broken()?;
            awaiting.0.retain(|p| p.cancelled.parent_session != parent_session);
            OperationCleanup { source: scope, session: parent_session, world, roster }
                .notify_cleaned()?;
        }

        Ok(())
    }
}

#[derive(Component)]
struct CancelInputBufferStorage(Entity);

#[derive(Component)]
struct CancelFromScope(Entity);

#[derive(Component, Default)]
struct AwaitingCancelStorage(SmallVec<[AwaitingCancel; 8]>);

struct AwaitingCancel {
    scoped_session: Entity,
    cancelled: CancelledSession,
    cancellation_sessions: SmallVec<[Entity; 8]>,
}

impl AwaitingCancel {
    fn new(scoped_session: Entity, cancelled: CancelledSession) -> Self {
        Self { scoped_session, cancelled, cancellation_sessions: Default::default() }
    }
}

struct CheckAwaitingSession;

#[derive(Component, Default)]
pub(crate) struct ExitTargetStorage {
    /// Map from session value to the target
    pub(crate) map: HashMap<Entity, ExitTarget>,
}

pub(crate) struct ExitTarget {
    pub(crate) target: Entity,
    pub(crate) source: Entity,
    pub(crate) parent_session: Entity,
    pub(crate) blocker: Option<Blocker>,
}

pub(crate) struct RedirectScopeStream<T: Stream> {
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T: Stream> RedirectScopeStream<T> {
    pub(crate) fn new(target: Entity) -> Self {
        Self { target, _ignore: Default::default() }
    }
}

impl<T: Stream> Operation for RedirectScopeStream<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            SingleTargetStorage::new(self.target),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;

        // The target is optional because we want to "send" this stream even if
        // there is no target listening, because streams may have custom sending
        // behavior
        let target = source_mut.get::<SingleTargetStorage>().map(|t| t.get());
        let Input { session: scoped_session, data } = source_mut.take_input::<T>()?;
        let parent_session = world.get::<ParentSession>(scoped_session).or_broken()?.get();
        data.send(StreamRequest {
            source,
            session: parent_session,
            target,
            world,
            roster
        })
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<T>()? {
            return Ok(true);
        }

        let scope = r.world.get::<ScopeStorage>(r.source).or_broken()?.get();
        r.check_upstream(scope)

        // TODO(@mxgrey): Consider whether we can/should identify more
        // specifically whether the current state of the scope would be able to
        // reach this specific stream.
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()
    }
}

pub(crate) struct RedirectWorkflowStream<T: Stream> {
    _ignore: std::marker::PhantomData<T>,
}

impl<T: Stream> RedirectWorkflowStream<T> {
    pub(crate) fn new() -> Self {
        Self { _ignore: Default::default() }
    }
}

impl<T: Stream> Operation for RedirectWorkflowStream<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert(
            InputBundle::<T>::new(),
        );
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session: scoped_session, data } = source_mut.take_input::<T>()?;
        let scope = source_mut.get::<ScopeStorage>().or_broken()?.get();
        let exit = world.get::<ExitTargetStorage>(scope).or_broken()?
            .map.get(&scoped_session)
            // If the map does not have this session in it, that should simply
            // mean that the workflow has terminated, so we should discard this
            // stream data.
            //
            // TODO(@mxgrey): Consider whether this should count as a disposal.
            .or_not_ready()?;
        let exit_source = exit.source;
        let parent_session = exit.parent_session;

        let stream_target_map = world.get::<StreamTargetMap>(exit_source).or_broken()?;
        let stream_target = world.get::<StreamTargetStorage<T>>(exit_source)
            .map(|target| stream_target_map.get(target.get()))
            .flatten();

        data.send(StreamRequest {
            source,
            session: parent_session,
            target: stream_target,
            world,
            roster,
        })
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<T>()? {
            return Ok(true);
        }

        let scope = r.world.get::<ScopeStorage>(r.source).or_broken()?.get();
        r.check_upstream(scope)

        // TODO(@mxgrey): Consider whether we can/should identify more
        // specifically whether the current state of the scope would be able to
        // reach this specific stream.
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()
    }
}
