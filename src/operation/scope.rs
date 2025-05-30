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
    check_reachability,
    dyn_node::{DynInputSlot, DynOutput},
    execute_operation, is_downstream_of,
    type_info::TypeInfo,
    Accessing, AddOperation, Blocker, Broken, BufferKeyBuilder, Builder, BuilderScopeContext,
    Cancel, Cancellable, Cancellation, Cleanup, CleanupContents, ClearBufferFn, CollectMarker,
    DisposalListener, DisposalUpdate, FinalizeCleanup, FinalizeCleanupRequest, Input, InputBundle,
    InspectDisposals, ManageCancellation, ManageInput, NamedTarget, NamedValue, Operation,
    OperationCancel, OperationCleanup, OperationError, OperationReachability, OperationRequest,
    OperationResult, OperationRoster, OperationSetup, OrBroken, ReachabilityResult, ScopeSettings,
    SingleInputStorage, SingleTargetStorage, StreamEffect, StreamRequest, StreamTargetMap,
    UnhandledErrors, Unreachability, UnusedTarget,
};

use backtrace::Backtrace;

use bevy_derive::Deref;
use bevy_ecs::{
    prelude::{Commands, Component, Entity, World},
    system::Command,
};
use bevy_hierarchy::{BuildChildren, DespawnRecursiveExt};

use smallvec::SmallVec;

use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

use thiserror::Error as ThisError;

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

#[derive(Component)]
pub enum SessionStatus {
    Active,
    Cleaning,
}

pub(crate) struct OperateScope {
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
    finish_scope_cancel: Entity,
    components: Option<TypeSensitiveScopeComponents>,
}

struct TypeSensitiveScopeComponents {
    dyn_scope_request: DynScopeRequest,
    finalize_cleanup: FinalizeCleanup,
}

impl TypeSensitiveScopeComponents {
    fn apply(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        (self.dyn_scope_request.setup_input)(OperationSetup { source, world })?;

        world
            .entity_mut(source)
            .insert((self.dyn_scope_request, self.finalize_cleanup));
        Ok(())
    }
}

struct InsertTypeSensitiveComponents {
    source: Entity,
    components: TypeSensitiveScopeComponents,
}

impl Command for InsertTypeSensitiveComponents {
    fn apply(self, world: &mut World) {
        let r = self.components.apply(OperationSetup {
            source: self.source,
            world,
        });
        if let Err(OperationError::Broken(backtrace)) = r {
            world
                .get_resource_or_insert_with(|| UnhandledErrors::default())
                .broken
                .push(Broken {
                    node: self.source,
                    backtrace,
                });
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ScopeEndpoints {
    pub(crate) terminal: Entity,
    pub(crate) enter_scope: Entity,
    pub(crate) finish_scope_cancel: Entity,
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
        Self {
            parent_session,
            scoped_session,
            status: ScopedSessionStatus::Ongoing,
        }
    }
}

#[derive(Component)]
pub(crate) struct ScopeSettingsStorage(pub(crate) ScopeSettings);

#[derive(Component)]
pub(crate) enum ScopedSessionStatus {
    Ongoing,
    Terminated,
    /// The scope was asked to cleanup from an external source, but it has an
    /// uninterruptible setting. We are waiting for a termination or internal
    /// cancel to trigger before doing a cleanup.
    DeferredCleanup,
    /// The scope has already begun the cleanup process
    Cleanup,
    Cancelled,
}

impl ScopedSessionStatus {
    #[allow(clippy::wrong_self_convention)]
    fn to_cleanup(&mut self, uninterruptible: bool) -> bool {
        if matches!(self, Self::Cleanup) {
            return false;
        }

        if uninterruptible {
            *self = Self::DeferredCleanup;
            false
        } else {
            *self = Self::Cleanup;
            true
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_finished(&mut self) -> bool {
        // Only switch it to finished if it was still ongoing. Anything else
        // should not get converted to a finished status.
        if matches!(self, Self::Ongoing) {
            *self = Self::Terminated;
            return true;
        }

        if matches!(self, Self::DeferredCleanup) {
            // We've been waiting for the scope to finish before beginning
            // cleanup because the scope is uninterruptible.
            *self = Self::Cleanup;
            return true;
        }

        false
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_cancelled(&mut self) -> bool {
        if matches!(self, Self::Ongoing | Self::DeferredCleanup) {
            *self = Self::Cancelled;
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

#[derive(Component, Default)]
pub struct ScopeContents {
    nodes: SmallVec<[Entity; 16]>,
}

impl ScopeContents {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_node(&mut self, node: Entity) {
        self.nodes.push(node);
    }

    pub fn nodes(&self) -> &SmallVec<[Entity; 16]> {
        &self.nodes
    }
}

impl Operation for OperateScope {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        if let Some(components) = self.components {
            components.apply(OperationSetup { source, world })?;
        }

        let mut source_mut = world.entity_mut(source);
        source_mut.insert((
            ScopeEntryStorage(self.enter_scope),
            ScopedSessionStorage::default(),
            TerminalStorage(self.terminal),
            Cancellable::new(receive_cancel),
            ValidateScopeReachability(validate_scope_reachability),
            CleanupContents::new(),
            ScopeContents::new(),
            BeginCleanupWorkflowStorage::default(),
            FinishCleanupWorkflowStorage(self.finish_scope_cancel),
        ));

        if let Some(exit_scope) = self.exit_scope {
            source_mut.insert(SingleTargetStorage::new(exit_scope));

            world
                .get_entity_mut(exit_scope)
                .or_broken()?
                .insert(SingleInputStorage::new(source));
        } else {
            source_mut.insert(ExitTargetStorage::default());
        }

        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let begin_scope = world
            .get_entity(source)
            .or_broken()?
            .get::<DynScopeRequest>()
            .or_broken()?
            .begin_scope;

        (begin_scope)(OperationRequest {
            source,
            world,
            roster,
        })
    }

    fn cleanup(clean: OperationCleanup) -> OperationResult {
        let cleanup = clean
            .world
            .get_entity(clean.source)
            .or_broken()?
            .get::<DynScopeRequest>()
            .or_broken()?
            .cleanup;

        (cleanup)(clean)
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        let is_reachable = reachability
            .world
            .get_entity(reachability.source)
            .or_broken()?
            .get::<DynScopeRequest>()
            .or_broken()?
            .is_reachable;

        (is_reachable)(reachability)
    }
}

#[derive(Component, Clone, Copy)]
struct DynScopeRequest {
    setup_input: fn(OperationSetup) -> OperationResult,
    begin_scope: fn(OperationRequest) -> OperationResult,
    cleanup: fn(OperationCleanup) -> OperationResult,
    is_reachable: fn(OperationReachability) -> ReachabilityResult,
}

impl DynScopeRequest {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self {
            setup_input: dyn_setup_input::<T>,
            begin_scope: dyn_begin_scope::<T>,
            cleanup: dyn_cleanup::<T>,
            is_reachable: dyn_is_reachable::<T>,
        }
    }
}

fn dyn_setup_input<Request: 'static + Send + Sync>(
    OperationSetup { source, world }: OperationSetup,
) -> OperationResult {
    let mut source_mut = world.entity_mut(source);
    source_mut.insert(InputBundle::<Request>::new());
    Ok(())
}

fn dyn_begin_scope<Request: 'static + Send + Sync>(
    OperationRequest {
        source,
        world,
        roster,
    }: OperationRequest,
) -> OperationResult {
    let input = world
        .get_entity_mut(source)
        .or_broken()?
        .take_input::<Request>()?;

    let scoped_session = world
        .spawn((ParentSession(input.session), SessionStatus::Active))
        .id();

    begin_scope(
        input,
        scoped_session,
        OperationRequest {
            source,
            world,
            roster,
        },
    )
}

fn dyn_cleanup<Request: 'static + Send + Sync>(
    OperationCleanup {
        source,
        cleanup,
        world,
        roster,
    }: OperationCleanup,
) -> OperationResult {
    let parent_session = cleanup.session;

    let mut source_mut = world.get_entity_mut(source).or_broken()?;
    source_mut.cleanup_inputs::<Request>(cleanup.session);

    let uninterruptible = source_mut
        .get::<ScopeSettingsStorage>()
        .or_broken()?
        .0
        .is_uninterruptible();

    let relevant_scoped_sessions: SmallVec<[_; 16]> = source_mut
        .get_mut::<ScopedSessionStorage>()
        .or_broken()?
        .0
        .iter_mut()
        .filter(|pair| pair.parent_session == parent_session)
        .filter_map(|p| {
            if p.status.to_cleanup(uninterruptible) {
                Some(p.scoped_session)
            } else {
                None
            }
        })
        .collect();

    if relevant_scoped_sessions.is_empty() {
        // We have no record of the mentioned session in this scope, so it
        // is already clean.
        return OperationCleanup {
            source,
            cleanup,
            world,
            roster,
        }
        .notify_cleaned();
    };

    cleanup_entire_scope(
        source,
        cleanup,
        FinishStatus::EarlyCleanup,
        relevant_scoped_sessions,
        world,
        roster,
    )
}

fn dyn_is_reachable<Request: 'static + Send + Sync>(
    mut reachability: OperationReachability,
) -> ReachabilityResult {
    if reachability.has_input::<Request>()? {
        return Ok(true);
    }

    let source_ref = reachability
        .world
        .get_entity(reachability.source)
        .or_broken()?;

    if let Some(pair) = source_ref
        .get::<ScopedSessionStorage>()
        .or_broken()?
        .0
        .iter()
        .find(|pair| pair.parent_session == reachability.session)
    {
        let mut visited = HashMap::new();
        let mut scoped_reachability = OperationReachability::new(
            pair.scoped_session,
            reachability.source,
            reachability.disposed,
            reachability.world,
            &mut visited,
        );

        let terminal = source_ref.get::<TerminalStorage>().or_broken()?.0;
        if scoped_reachability.check_upstream(terminal)? {
            return Ok(true);
        }
    }

    SingleInputStorage::is_reachable(&mut reachability)
}

pub(crate) fn begin_scope<Request>(
    input: Input<Request>,
    scoped_session: Entity,
    OperationRequest {
        source,
        world,
        roster,
    }: OperationRequest,
) -> OperationResult
where
    Request: 'static + Send + Sync,
{
    let result = begin_scope_impl(
        input,
        scoped_session,
        OperationRequest {
            source,
            world,
            roster,
        },
    );

    if result.is_err() {
        // We won't be executing this scope after all, so despawn the scoped
        // session that we created.
        if let Some(scoped_session_mut) = world.get_entity_mut(scoped_session) {
            scoped_session_mut.despawn_recursive();
        }
        return result;
    }

    if let Some(reachability) = world.get::<InitialReachability>(source) {
        if let InitialReachability::Error(cancellation) = reachability {
            cancel_one(scoped_session, source, cancellation.clone(), world, roster)?;
            return Ok(());
        }

        if reachability.is_invalidated() {
            // We have found in the past that this workflow cannot reach its
            // terminal point from its start point, so we should trigger the
            // reachability check to begin the cancellation process right away.
            validate_scope_reachability(ValidationRequest {
                source,
                origin: source,
                session: scoped_session,
                world,
                roster,
            })?;
        }
    } else {
        let circular_collects = find_circular_collects(source, world)?;
        if !circular_collects.is_empty() {
            // There are circular collect operations in this workflow, making it
            // invalid, so we will cancel this workflow without running it.
            let cancellation = Cancellation::circular_collect(circular_collects);
            world
                .get_entity_mut(source)
                .or_broken()?
                .insert(InitialReachability::Error(cancellation.clone()));
            cancel_one(scoped_session, source, cancellation, world, roster)?;
            return Ok(());
        }

        // We should do a one-time check to make sure the terminal node can be
        // reached from the scope entry node. As long as this passes, we will
        // not run it again.
        let is_reachable = validate_scope_reachability(ValidationRequest {
            source,
            origin: source,
            session: scoped_session,
            world,
            roster,
        })?;

        world
            .get_entity_mut(source)
            .or_broken()?
            .insert(InitialReachability::new(is_reachable));
    }

    Ok(())
}

fn find_circular_collects(
    scope: Entity,
    world: &World,
) -> Result<Vec<[Entity; 2]>, OperationError> {
    let nodes = world.get::<ScopeContents>(scope).or_broken()?.nodes();
    let mut conflicts = Vec::new();
    for (i, node_i) in nodes.iter().enumerate() {
        if world.get::<CollectMarker>(*node_i).is_none() {
            continue;
        }

        for node_j in &nodes[i + 1..] {
            if world.get::<CollectMarker>(*node_j).is_none() {
                continue;
            }

            if is_downstream_of(*node_i, *node_j, world)
                && is_downstream_of(*node_j, *node_i, world)
            {
                // Both nodes are downstream of each other, which means they
                // exist in a cycle. Both are collect operations, so these are
                // circular connect operations, which are not allowed.
                conflicts.push([*node_i, *node_j]);
            }
        }
    }

    Ok(conflicts)
}

fn begin_scope_impl<Request>(
    Input {
        session: parent_session,
        data,
    }: Input<Request>,
    scoped_session: Entity,
    OperationRequest {
        source,
        world,
        roster,
    }: OperationRequest,
) -> OperationResult
where
    Request: 'static + Send + Sync,
{
    let mut source_mut = world.get_entity_mut(source).or_broken()?;
    let enter_scope = source_mut.get::<ScopeEntryStorage>().or_broken()?.0;

    source_mut
        .get_mut::<ScopedSessionStorage>()
        .or_broken()?
        .0
        .push(ScopedSession::ongoing(parent_session, scoped_session));

    world
        .get_entity_mut(enter_scope)
        .or_broken()?
        .give_input(scoped_session, data, roster)?;

    Ok(())
}

impl OperateScope {
    pub(crate) fn add<Request, Response>(
        parent_scope: Option<Entity>,
        scope_id: Entity,
        exit_scope: Option<Entity>,
        commands: &mut Commands,
    ) -> ScopeEndpoints
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        // NOTE(@mxgrey): When changing this implementation, remember to similarly
        // update the implementation of IncrementalScopeBuilder.
        let enter_scope = commands.spawn((EntryForScope(scope_id), UnusedTarget)).id();

        let terminal = commands.spawn(()).set_parent(scope_id).id();
        let finish_scope_cancel = commands
            .spawn(FinishCleanupForScope(scope_id))
            .set_parent(scope_id)
            .id();

        let scope = OperateScope {
            enter_scope,
            terminal,
            exit_scope,
            finish_scope_cancel,
            components: Some(TypeSensitiveScopeComponents {
                dyn_scope_request: DynScopeRequest::new::<Request>(),
                finalize_cleanup: FinalizeCleanup(begin_cleanup_workflows::<Response>),
            }),
        };

        // Note: We need to make sure the scope object gets set up before any of
        // its endpoints, otherwise the ScopeContents component will be missing
        // during setup.
        commands.add(AddOperation::new(parent_scope, scope_id, scope));

        commands.add(AddOperation::new(
            // We do not consider the terminal node to be "inside" the scope,
            // otherwise it will get cleaned up prematurely
            None,
            terminal,
            Terminate::<Response>::new(scope_id),
        ));

        commands.add(AddOperation::new(
            // We do not consider the finish cancel node to be "inside" the
            // scope, otherwise it will get cleaned up prematurely
            None,
            finish_scope_cancel,
            FinishCleanup::<Response>::new(scope_id),
        ));

        ScopeEndpoints {
            finish_scope_cancel,
            terminal,
            enter_scope,
        }
    }
}

/// This struct facilitates the gradual building of a scope as the types of the
/// request and response messages is discovered. This is used by the scope
/// operation of the diagram module, which needs to build the scope gradually
/// while it discovers type information at runtime.
#[derive(Clone)]
pub(crate) struct IncrementalScopeBuilder {
    inner: Arc<Mutex<IncrementalScopeBuilderInner>>,
}

#[derive(ThisError, Debug)]
#[error("An error happened while building a dynamic scope")]
pub enum IncrementalScopeError {
    #[error("This request type for this scope has already been set to a different message type")]
    RequestMismatch {
        already_set: TypeInfo,
        attempted: TypeInfo,
    },
    #[error("The response type for this scope has already been set so it cannot be set again")]
    ResponseMismatch {
        already_set: TypeInfo,
        attempted: TypeInfo,
    },
    #[error(
        "The scope has not finished building yet. request: {request_set}, response: {response_set}"
    )]
    Unfinished {
        request_set: bool,
        response_set: bool,
    },
}

#[derive(Debug)]
pub(crate) struct IncrementalScopeRequest {
    pub(crate) external_input: DynInputSlot,
    pub(crate) begin_scope: Option<DynOutput>,
}

pub(crate) type IncrementalScopeRequestResult =
    Result<IncrementalScopeRequest, IncrementalScopeError>;

#[derive(Debug)]
pub(crate) struct IncrementalScopeResponse {
    pub(crate) terminate: DynInputSlot,
    pub(crate) external_output: Option<DynOutput>,
}

pub(crate) type IncrementalScopeResponseResult =
    Result<IncrementalScopeResponse, IncrementalScopeError>;

impl IncrementalScopeBuilder {
    pub(crate) fn begin(settings: ScopeSettings, builder: &mut Builder) -> IncrementalScopeBuilder {
        let parent_scope = builder.scope();
        let commands = builder.commands();

        // TODO(@mxgrey): Consider how to refactor this to share an implementation
        // with OperateScope::add.
        let scope_id = commands.spawn(()).id();
        let exit_scope = commands.spawn(UnusedTarget).id();
        let enter_scope = commands.spawn((EntryForScope(scope_id), UnusedTarget)).id();
        let terminal = commands.spawn(()).set_parent(scope_id).id();
        let finish_scope_cancel = commands
            .spawn(FinishCleanupForScope(scope_id))
            .set_parent(scope_id)
            .id();

        commands
            .entity(scope_id)
            .insert(ScopeSettingsStorage(settings));

        let scope = OperateScope {
            enter_scope,
            terminal,
            exit_scope: Some(exit_scope),
            finish_scope_cancel,
            components: None,
        };
        commands.add(AddOperation::new(Some(parent_scope), scope_id, scope));

        IncrementalScopeBuilder {
            inner: Arc::new(Mutex::new(IncrementalScopeBuilderInner {
                parent_scope,
                scope_id,
                enter_scope,
                terminal,
                exit_scope,
                finish_scope_cancel,
                request: None,
                response: None,
                already_built: false,
                begin_scope_not_sent: true,
                external_output_not_sent: true,
            })),
        }
    }

    pub(crate) fn builder_scope_context(&self) -> BuilderScopeContext {
        let inner = self.inner.lock().unwrap();
        BuilderScopeContext {
            scope: inner.scope_id,
            finish_scope_cancel: inner.finish_scope_cancel,
        }
    }

    pub(crate) fn set_request<Request: 'static + Send + Sync>(
        &mut self,
        commands: &mut Commands,
    ) -> IncrementalScopeRequestResult {
        let mut inner = self.inner.lock().unwrap();
        let message_info = TypeInfo::of::<Request>();
        if let Some((_, expected_info)) = inner.request.as_ref() {
            if *expected_info != message_info {
                return Err(IncrementalScopeError::RequestMismatch {
                    already_set: *expected_info,
                    attempted: message_info,
                });
            }
        } else {
            inner.request = Some((DynScopeRequest::new::<Request>(), message_info));
        }

        inner.consider_building(commands);

        let request = IncrementalScopeRequest {
            external_input: DynInputSlot::new(inner.parent_scope, inner.scope_id, message_info),
            begin_scope: inner
                .begin_scope_not_sent
                .then(|| DynOutput::new(inner.scope_id, inner.enter_scope, message_info)),
        };

        inner.begin_scope_not_sent = false;
        Ok(request)
    }

    pub(crate) fn set_response<Response: 'static + Send + Sync>(
        &mut self,
        commands: &mut Commands,
    ) -> IncrementalScopeResponseResult {
        let mut inner = self.inner.lock().unwrap();
        let message_info = TypeInfo::of::<Response>();
        if let Some((_, expected_info)) = inner.response.as_ref() {
            if *expected_info != message_info {
                return Err(IncrementalScopeError::ResponseMismatch {
                    already_set: *expected_info,
                    attempted: message_info,
                });
            }
        } else {
            commands.add(AddOperation::new(
                // We do not consider the terminal node to be "inside" the scope,
                // otherwise it will get cleaned up prematurely
                None,
                inner.terminal,
                Terminate::<Response>::new(inner.scope_id),
            ));

            commands.add(AddOperation::new(
                // We do not consider the finish cancel node to be "inside" the
                // scope, otherwise it will get cleaned up prematurely
                None,
                inner.finish_scope_cancel,
                FinishCleanup::<Response>::new(inner.scope_id),
            ));

            inner.response = Some((
                FinalizeCleanup::new(begin_cleanup_workflows::<Response>),
                message_info,
            ));
        }

        inner.consider_building(commands);

        let response = IncrementalScopeResponse {
            terminate: DynInputSlot::new(inner.scope_id, inner.terminal, message_info),
            external_output: inner
                .external_output_not_sent
                .then(|| DynOutput::new(inner.parent_scope, inner.exit_scope, message_info)),
        };

        inner.external_output_not_sent = false;
        Ok(response)
    }

    pub(crate) fn is_finished(&self) -> Result<(), IncrementalScopeError> {
        let inner = self.inner.lock().unwrap();
        if inner.begin_scope_not_sent || inner.external_output_not_sent {
            return Err(IncrementalScopeError::Unfinished {
                request_set: !inner.begin_scope_not_sent,
                response_set: !inner.external_output_not_sent,
            });
        }

        Ok(())
    }
}

struct IncrementalScopeBuilderInner {
    parent_scope: Entity,
    scope_id: Entity,
    enter_scope: Entity,
    terminal: Entity,
    exit_scope: Entity,
    finish_scope_cancel: Entity,
    request: Option<(DynScopeRequest, TypeInfo)>,
    response: Option<(FinalizeCleanup, TypeInfo)>,
    already_built: bool,
    begin_scope_not_sent: bool,
    external_output_not_sent: bool,
}

impl IncrementalScopeBuilderInner {
    fn consider_building(&mut self, commands: &mut Commands) {
        if self.already_built {
            return;
        }

        let (Some((dyn_scope_request, _)), Some((finalize_cleanup, _))) = (
            self.request.as_ref().copied(),
            self.response.as_ref().copied(),
        ) else {
            return;
        };

        commands.add(InsertTypeSensitiveComponents {
            source: self.scope_id,
            components: TypeSensitiveScopeComponents {
                dyn_scope_request,
                finalize_cleanup,
            },
        });
        self.already_built = true;
    }
}

fn receive_cancel(
    OperationCancel {
        cancel:
            Cancel {
                origin: _origin,
                target: source,
                session,
                cancellation,
            },
        world,
        roster,
    }: OperationCancel,
) -> OperationResult {
    if let Some(session) = session {
        // We only need to cancel one specific session
        return cancel_one(session, source, cancellation, world, roster);
    }

    // We need to cancel all sessions. This is usually because a workflow
    // is fundamentally broken.
    let all_scoped_sessions: SmallVec<[Entity; 16]> = world
        .get::<ScopedSessionStorage>(source)
        .or_broken()?
        .0
        .iter()
        .map(|p| p.scoped_session)
        .collect();

    for scoped_session in all_scoped_sessions {
        if let Err(error) = cancel_one(scoped_session, source, cancellation.clone(), world, roster)
        {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .operations
                .push(error);
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
    let mut source_mut = world.get_entity_mut(source).or_broken()?;
    let relevant_scoped_sessions = source_mut
        .get_mut::<ScopedSessionStorage>()
        .or_broken()?
        .0
        .iter_mut()
        // The cancelled session could be a scoped session or it could be a
        // parent session (e.g. an external cancellation trigger). We won't
        // be able to tell without checking against both.
        .filter(|pair| pair.scoped_session == session || pair.parent_session == session)
        .filter_map(|p| {
            if p.status.to_cancelled() {
                Some(p.scoped_session)
            } else {
                None
            }
        })
        .collect();

    let cleanup = Cleanup {
        cleaner: source,
        node: source,
        session,
        cleanup_id: session,
    };
    cleanup_entire_scope(
        source,
        cleanup,
        FinishStatus::Cancelled(cancellation),
        relevant_scoped_sessions,
        world,
        roster,
    )
}

/// Check if the terminal node of the scope can be reached. If not, cancel
/// the scope immediately.
fn validate_scope_reachability(
    ValidationRequest {
        source,
        origin,
        session,
        world,
        roster,
    }: ValidationRequest,
) -> ReachabilityResult {
    let nodes = world
        .get::<ScopeContents>(source)
        .or_broken()?
        .nodes()
        .clone();
    for node in nodes.iter() {
        let Some(disposal_listener) = world.get::<DisposalListener>(*node) else {
            continue;
        };
        let f = disposal_listener.0;
        f(DisposalUpdate {
            source: *node,
            origin,
            session,
            world,
            roster,
        })?;
    }

    let scoped_session = session;
    let source_ref = world.get_entity(source).or_broken()?;
    let terminal = source_ref.get::<TerminalStorage>().or_broken()?.0;
    if check_reachability(scoped_session, terminal, Some(origin), world)? {
        // The terminal node can still be reached, so we're done
        return Ok(true);
    }

    // The terminal node cannot be reached so we should cancel this scope.
    let mut disposals = Vec::new();
    for node in nodes.iter() {
        if let Some(node_disposals) = world
            .get_entity(*node)
            .or_broken()?
            .get_disposals(scoped_session)
        {
            disposals.extend(node_disposals.iter().cloned());
        }
    }

    let mut source_mut = world.get_entity_mut(source).or_broken()?;
    let mut sessions = source_mut.get_mut::<ScopedSessionStorage>().or_broken()?;
    let pair = sessions
        .0
        .iter_mut()
        .find(|pair| pair.scoped_session == scoped_session)
        .or_not_ready()?;

    let cancellation: Cancellation = Unreachability {
        scope: source,
        session: pair.parent_session,
        disposals,
    }
    .into();
    if pair.status.to_cancelled() {
        let cleanup = Cleanup {
            cleaner: source,
            node: source,
            session: scoped_session,
            cleanup_id: scoped_session,
        };
        cleanup_entire_scope(
            source,
            cleanup,
            FinishStatus::Cancelled(cancellation),
            SmallVec::from_iter([scoped_session]),
            world,
            roster,
        )?;
    }

    Ok(false)
}

fn begin_cleanup_workflows<Response: 'static + Send + Sync>(
    FinalizeCleanupRequest {
        cleanup,
        world,
        roster,
    }: FinalizeCleanupRequest,
) -> OperationResult {
    let scope = cleanup.cleaner;
    let scoped_session = cleanup.session;
    let mut scope_mut = world.get_entity_mut(scope).or_broken()?;
    scope_mut
        .get_mut::<ScopedSessionStorage>()
        .or_broken()?
        .0
        .retain(|p| p.scoped_session != scoped_session);

    let finish_cleanup = scope_mut
        .get::<FinishCleanupWorkflowStorage>()
        .or_broken()?
        .0;
    let begin_cleanup_workflows = scope_mut
        .get::<BeginCleanupWorkflowStorage>()
        .or_broken()?
        .0
        .clone();
    let mut finish_cleanup_workflow_mut = world.get_entity_mut(finish_cleanup).or_broken()?;
    let mut awaiting_storage = finish_cleanup_workflow_mut
        .get_mut::<AwaitingCleanupStorage>()
        .or_broken()?;
    let awaiting = awaiting_storage.get(scoped_session).or_broken()?;
    awaiting.cleanup_workflow_sessions = Some(Default::default());

    let is_terminated = awaiting.info.status.is_terminated();

    for begin in begin_cleanup_workflows {
        let run_this_workflow =
            (is_terminated && begin.on_terminate) || (!is_terminated && begin.on_cancelled);

        if !run_this_workflow {
            continue;
        }

        // We execute the begin nodes immediately so that they can load up the
        // finish_cancel node with all their cancellation behavior IDs before
        // the finish_cancel node gets executed.
        let execute = unsafe {
            // INVARIANT: We can use sneak_input here because we execute the
            // recipient node immediately after giving the input.
            world
                .get_entity_mut(begin.source)
                .or_broken()?
                .sneak_input(scoped_session, (), false, roster)?
        };
        if execute {
            execute_operation(OperationRequest {
                source: begin.source,
                world,
                roster,
            });
        }
    }

    // Check if there are any cleanup workflows waiting to be run. If not,
    // the workflow can fully terminate.
    FinishCleanup::<Response>::check_awaiting_session(
        finish_cleanup,
        scoped_session,
        world,
        roster,
    )?;

    Ok(())
}

/// This component keeps track of whether the terminal node of a scope can be
/// reached at all from its entry point. This only needs to be tested once (the
/// first time the entry node is given its initial input), and then we can reuse
/// the result on all future runs.
#[derive(Component)]
enum InitialReachability {
    Confirmed,
    // TODO(@mxgrey): Consider merging Invalidated into Error
    Invalidated,
    Error(Cancellation),
}

impl InitialReachability {
    fn new(is_reachable: bool) -> Self {
        match is_reachable {
            true => Self::Confirmed,
            false => Self::Invalidated,
        }
    }

    fn is_invalidated(&self) -> bool {
        matches!(self, Self::Invalidated)
    }
}

#[derive(Component, Clone, Copy)]
pub struct ValidateScopeReachability(pub(crate) fn(ValidationRequest) -> ReachabilityResult);

pub struct ValidationRequest<'a> {
    pub source: Entity,
    pub origin: Entity,
    pub session: Entity,
    pub world: &'a mut World,
    pub roster: &'a mut OperationRoster,
}

#[derive(Component)]
pub(crate) struct ScopeEntryStorage(pub(crate) Entity);

/// Store the scope entity for the first node within a scope
#[derive(Component)]
pub(crate) struct EntryForScope(pub(crate) Entity);

/// Store the scope entity for the FinishCleanup operation within a scope
#[derive(Component)]
struct FinishCleanupForScope(Entity);

pub(crate) struct Terminate<T> {
    scope: Entity,
    _ignore: std::marker::PhantomData<fn(T)>,
}

impl<T> Terminate<T> {
    pub(crate) fn new(scope: Entity) -> Self {
        Self {
            scope,
            _ignore: Default::default(),
        }
    }
}

fn cleanup_entire_scope(
    scope: Entity,
    cleanup: Cleanup,
    status: FinishStatus,
    relevant_scoped_sessions: SmallVec<[Entity; 16]>,
    world: &mut World,
    roster: &mut OperationRoster,
) -> OperationResult {
    let scope_ref = world.get_entity(scope).or_broken()?;
    let nodes = scope_ref
        .get::<ScopeContents>()
        .or_broken()?
        .nodes()
        .clone();
    let finish_cleanup_workflow = scope_ref
        .get::<FinishCleanupWorkflowStorage>()
        .or_broken()?
        .0;

    for scoped_session in relevant_scoped_sessions {
        let parent_session = world
            .get::<ParentSession>(scoped_session)
            .or_broken()?
            .get();
        world
            .get_mut::<AwaitingCleanupStorage>(finish_cleanup_workflow)
            .or_broken()?
            .0
            .push(AwaitingCleanup::new(
                scoped_session,
                CleanupInfo {
                    parent_session,
                    cleanup,
                    status: status.clone(),
                },
            ));

        if nodes.is_empty() {
            // There are no nodes to clean up (... meaning the workflow is totally
            // empty and not even a valid workflow to begin with, but oh well ...)
            // so we should immediately trigger a finished cleaning notice.
            roster.cleanup_finished(Cleanup {
                cleaner: scope,
                node: scope,
                session: scoped_session,
                cleanup_id: cleanup.cleanup_id,
            });
        } else {
            world
                .get_mut::<CleanupContents>(scope)
                .or_broken()?
                .add_cleanup(cleanup.cleanup_id, nodes.clone());

            for node in nodes.iter() {
                OperationCleanup::new(
                    scope,
                    *node,
                    scoped_session,
                    cleanup.cleanup_id,
                    world,
                    roster,
                )
                .clean();
            }
        }

        *world.get_mut::<SessionStatus>(scoped_session).or_broken()? = SessionStatus::Cleaning;
    }

    Ok(())
}

impl<T> Operation for Terminate<T>
where
    T: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            SingleInputStorage::empty(),
            Staging::<T>::new(),
            ScopeStorage::new(self.scope),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input {
            session: scoped_session,
            data,
        } = source_mut.take_input::<T>()?;

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
        let pair = pairs
            .0
            .iter_mut()
            .find(|pair| pair.scoped_session == scoped_session)
            .or_broken()?;

        if !pair.status.to_finished() {
            // This will not actually change the status of the scoped session,
            // so skip the rest of this function.
            return Ok(());
        }

        let cleanup = Cleanup {
            cleaner: scope,
            node: scope,
            cleanup_id: scoped_session,
            session: scoped_session,
        };
        cleanup_entire_scope(
            scope,
            cleanup,
            FinishStatus::Terminated,
            SmallVec::from_iter([scoped_session]),
            world,
            roster,
        )
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        // Terminate may be called by a trim operation, but we should not heed
        // it. Just notify the cleanup right away without doing anything. The
        // scope cleanup process will prevent memory leaks for this operation.
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        let staging = reachability
            .world
            .get::<Staging<T>>(reachability.source)
            .or_broken()?;
        if staging.0.contains_key(&reachability.session) {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
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

impl<T> std::fmt::Debug for Staging<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

/// The scope that the node exists inside of.
#[derive(Component, Clone, Copy, Deref)]
pub struct ScopeStorage(Entity);

impl ScopeStorage {
    pub fn new(scope: Entity) -> Self {
        Self(scope)
    }

    pub fn get(&self) -> Entity {
        self.0
    }
}

#[derive(Component, Default)]
pub(crate) struct BeginCleanupWorkflowStorage(SmallVec<[CleanupWorkflow; 8]>);

#[derive(Clone, Copy)]
struct CleanupWorkflow {
    source: Entity,
    on_terminate: bool,
    on_cancelled: bool,
}

#[derive(Component)]
pub(crate) struct FinishCleanupWorkflowStorage(pub(crate) Entity);

pub(crate) struct CleanupInfo {
    parent_session: Entity,
    cleanup: Cleanup,
    status: FinishStatus,
}

#[derive(Clone)]
pub(crate) enum FinishStatus {
    Terminated,
    EarlyCleanup,
    Cancelled(Cancellation),
}

impl FinishStatus {
    fn is_terminated(&self) -> bool {
        matches!(self, Self::Terminated)
    }
    fn is_early_cleanup(&self) -> bool {
        matches!(self, Self::EarlyCleanup)
    }
}

pub(crate) struct BeginCleanupWorkflow<B> {
    from_scope: Entity,
    buffer: B,
    target: Entity,
    on_terminate: bool,
    on_cancelled: bool,
}

impl<B> BeginCleanupWorkflow<B> {
    pub(crate) fn new(
        from_scope: Entity,
        buffer: B,
        target: Entity,
        on_terminate: bool,
        on_cancelled: bool,
    ) -> Self {
        Self {
            from_scope,
            buffer,
            target,
            on_terminate,
            on_cancelled,
        }
    }
}

impl<B> Operation for BeginCleanupWorkflow<B>
where
    B: Accessing + 'static + Send + Sync,
    B::Key: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            CleanupInputBufferStorage(self.buffer),
            SingleTargetStorage::new(self.target),
            CleanupForScope(self.from_scope),
            InputBundle::<()>::new(),
        ));

        world
            .get_entity_mut(self.from_scope)
            .or_broken()?
            .get_mut::<BeginCleanupWorkflowStorage>()
            .or_broken()?
            .0
            .push(CleanupWorkflow {
                source,
                on_terminate: self.on_terminate,
                on_cancelled: self.on_cancelled,
            });

        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input {
            session: scoped_session,
            ..
        } = source_mut.take_input::<()>()?;
        let buffers = source_mut
            .get::<CleanupInputBufferStorage<B>>()
            .or_broken()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let from_scope = source_mut.get::<CleanupForScope>().or_broken()?.0;

        let key_builder = BufferKeyBuilder::without_tracking(from_scope, scoped_session, source);

        let keys = buffers.0.create_key(&key_builder);

        let cancellation_session = world
            .spawn((ParentSession(scoped_session), SessionStatus::Active))
            .id();
        world
            .get_entity_mut(target)
            .or_broken()?
            .give_input(cancellation_session, keys, roster)?;

        let finish_cleanup = world
            .get::<FinishCleanupWorkflowStorage>(from_scope)
            .or_broken()?
            .0;
        world
            .get_entity_mut(finish_cleanup)
            .or_broken()?
            .get_mut::<AwaitingCleanupStorage>()
            .or_broken()?
            .get(scoped_session)
            .or_broken()?
            .cleanup_workflow_sessions
            .as_mut()
            .or_broken()?
            .push(cancellation_session);

        Ok(())
    }

    fn cleanup(_: OperationCleanup) -> OperationResult {
        // This should never get called. BeginCancel should never exist as a
        // node that's inside of a scope.
        Err(OperationError::Broken(Some(Backtrace::new())))
    }

    fn is_reachable(r: OperationReachability) -> ReachabilityResult {
        r.has_input::<()>()
    }
}

pub(crate) struct FinishCleanup<T> {
    from_scope: Entity,
    _ignore: std::marker::PhantomData<fn(T)>,
}

impl<T> FinishCleanup<T> {
    fn new(from_scope: Entity) -> Self {
        Self {
            from_scope,
            _ignore: Default::default(),
        }
    }
}

impl<T: 'static + Send + Sync> Operation for FinishCleanup<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            CleanupForScope(self.from_scope),
            InputBundle::<()>::new(),
            Cancellable::new(Self::receive_cancel),
            AwaitingCleanupStorage::default(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Some(Input {
            session: cancellation_session,
            ..
        }) = source_mut.try_take_input::<()>()?
        else {
            return Ok(());
        };

        Self::deduct_finished_cleanup(source, cancellation_session, world, roster, None)
    }

    fn cleanup(_: OperationCleanup) -> OperationResult {
        // This should never get called. FinishCleanup should never exist as a
        // node that's inside of a scope.
        Err(OperationError::Broken(Some(Backtrace::new())))
    }

    fn is_reachable(_: OperationReachability) -> ReachabilityResult {
        // This should never get called. FinishCleanup should never exist as a
        // node that's inside of a scope.
        Err(OperationError::Broken(Some(Backtrace::new())))
    }
}

impl<T: 'static + Send + Sync> FinishCleanup<T> {
    fn receive_cancel(
        OperationCancel {
            cancel:
                Cancel {
                    origin: _origin,
                    target: source,
                    session,
                    cancellation,
                },
            world,
            roster,
        }: OperationCancel,
    ) -> OperationResult {
        if let Some(cancellation_session) = session {
            // We just need to cancel a specific cancellation session. The
            // cancellation signal for a FinishCleanup always comes from a child
            // cancellation session, never from an outside source.
            return Self::deduct_finished_cleanup(
                source,
                cancellation_session,
                world,
                roster,
                Some(cancellation),
            );
        }

        // All cleanup workflows need to be wiped out. This usually implies
        // that some workflow has broken entities.
        let cancellation_sessions: SmallVec<[Entity; 16]> = world
            .get::<AwaitingCleanupStorage>(source)
            .or_broken()?
            .0
            .iter()
            .flat_map(|a| a.cleanup_workflow_sessions.iter().flat_map(|w| w.iter()))
            .copied()
            .collect();

        for cancellation_session in cancellation_sessions {
            // TODO(@mxgrey): Should we try to cancel the cancellation workflow?
            // This is a pretty extreme edge case so it would be tricky to wind
            // this down correctly.
            if let Err(error) = Self::deduct_finished_cleanup(
                source,
                cancellation_session,
                world,
                roster,
                Some(cancellation.clone()),
            ) {
                world
                    .get_resource_or_insert_with(UnhandledErrors::default)
                    .operations
                    .push(error);
            }
        }

        Ok(())
    }

    fn check_awaiting_session(
        source: Entity,
        new_scoped_session: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let mut awaiting = source_mut.get_mut::<AwaitingCleanupStorage>().or_broken()?;
        if let Some((index, a)) = awaiting
            .0
            .iter_mut()
            .enumerate()
            .find(|(_, a)| a.scoped_session == new_scoped_session)
        {
            if a.cleanup_workflow_sessions
                .as_ref()
                .is_some_and(|s| s.is_empty())
            {
                // No cancellation sessions were started for this scoped
                // session so we can immediately clean it up.
                Self::finalize_scoped_session(
                    index,
                    OperationRequest {
                        source,
                        world,
                        roster,
                    },
                )?;
            }
        }

        Ok(())
    }

    fn deduct_finished_cleanup(
        source: Entity,
        cancellation_session: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
        inner_cancellation: Option<Cancellation>,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let mut awaiting = source_mut.get_mut::<AwaitingCleanupStorage>().or_broken()?;
        if let Some((index, a)) = awaiting.0.iter_mut().enumerate().find(|(_, a)| {
            a.cleanup_workflow_sessions
                .iter()
                .any(|w| w.iter().any(|s| *s == cancellation_session))
        }) {
            if let Some(inner_cancellation) = inner_cancellation {
                match &mut a.info.status {
                    FinishStatus::Cancelled(cancellation) => {
                        cancellation.while_cancelling.push(inner_cancellation);
                    }
                    FinishStatus::EarlyCleanup | FinishStatus::Terminated => {
                        // Do nothing. We have no sensible way to communicate
                        // to the requester that the cleanup workflow was
                        // cancelled.
                        //
                        // We could consider moving the cancellation into the
                        // unhandled errors resource, but this seems unnecessary
                        // for now.
                    }
                }
            }

            if let Some(cleanup_workflow_sessions) = &mut a.cleanup_workflow_sessions {
                cleanup_workflow_sessions.retain(|s| *s != cancellation_session);
                if cleanup_workflow_sessions.is_empty() {
                    // All cancellation sessions for this scoped session have
                    // finished so we can clean it up now.
                    Self::finalize_scoped_session(
                        index,
                        OperationRequest {
                            source,
                            world,
                            roster,
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    fn finalize_scoped_session(
        index: usize,
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let scope = source_mut.get::<FinishCleanupForScope>().or_broken()?.0;
        let mut awaiting = source_mut.get_mut::<AwaitingCleanupStorage>().or_broken()?;
        let a = awaiting.0.get(index).or_broken()?;
        let parent_session = a.info.parent_session;
        let cleanup = a.info.cleanup;
        let cleanup_id = cleanup.cleanup_id;
        let scoped_session = a.scoped_session;
        let terminating = a.info.status.is_terminated();
        if !a.info.status.is_early_cleanup() {
            // We can remove this right away since it's a cancellation or
            // termination, so we don't need to track when to notify the parent
            // of a cleanup.
            let a = awaiting.0.remove(index);
            if let FinishStatus::Cancelled(cancellation) = a.info.status {
                source_mut.emit_cancel(parent_session, cancellation, roster);
            }
        }

        // Check if the scope is being cleaned up for the parent session. We
        // should check that no more cleanup behaviors are pending for the
        // parent scope and that no other scoped sessions are still running for
        // the parent scope. Only after all of that is finished can we notify
        // that the parent session is cleaned.

        // Early cleanup implies that the workflow was stopped by its parent and
        // told to clean up before it terminated. This is distinct from a
        // cancelation.
        let mut early_cleanup = false;
        let mut cleaning_finished = true;
        for a in &source_mut.get::<AwaitingCleanupStorage>().or_broken()?.0 {
            if a.info.cleanup.cleanup_id == cleanup_id {
                if !a
                    .cleanup_workflow_sessions
                    .as_ref()
                    .is_some_and(|s| s.is_empty())
                {
                    cleaning_finished = false;
                }

                if a.info.status.is_early_cleanup() {
                    early_cleanup = true;
                }
            }
        }

        if early_cleanup && cleaning_finished {
            // Also check the scope for any ongoing scoped sessions related
            // to this parent because it's possible for the workflow to be
            // running multiple times simultaneously for the same parent session.
            // In that case, we need to make sure that ALL instances of this
            // workflow for the parent session are finished before we notify
            // that cleanup is finished.
            let scope = source_mut.get::<CleanupForScope>().or_broken()?.0;
            let scope_ref = world.get_entity(scope).or_broken()?;
            let pairs = scope_ref.get::<ScopedSessionStorage>().or_broken()?;
            if pairs
                .0
                .iter()
                .any(|pair| pair.parent_session == parent_session)
            {
                cleaning_finished = false;
            }
        }

        if early_cleanup && cleaning_finished {
            // The cleaning is finished so we can purge all memory of the
            // parent session and then notify the parent scope that it's
            // clean.
            let mut awaiting = world
                .get_mut::<AwaitingCleanupStorage>(source)
                .or_broken()?;
            awaiting
                .0
                .retain(|p| p.info.parent_session != parent_session);
            cleanup.notify_cleaned(world, roster)?;
        }

        let mut scope_mut = world.get_entity_mut(scope).or_broken()?;
        let terminal = scope_mut.get::<TerminalStorage>().or_broken()?.0;
        let (target, blocker) = scope_mut
            .get_mut::<ExitTargetStorage>()
            .and_then(|mut storage| storage.map.remove(&scoped_session))
            .map(|exit| (exit.target, exit.blocker))
            .or_else(|| {
                scope_mut
                    .get::<SingleTargetStorage>()
                    .map(|target| (target.get(), None))
            })
            .or_broken()?;

        if terminating {
            let mut staging = world.get_mut::<Staging<T>>(terminal).or_broken()?;

            let response = staging.0.remove(&scoped_session).or_broken()?;

            world.get_entity_mut(target).or_broken()?.give_input(
                parent_session,
                response,
                roster,
            )?;
        } else {
            // Make certain there is nothing related to the scoped session
            // lingering in the terminal node.
            let mut terminal_mut = world.get_entity_mut(terminal).or_broken()?;
            terminal_mut.cleanup_inputs::<T>(scoped_session);
            terminal_mut
                .get_mut::<Staging<T>>()
                .or_broken()?
                .0
                .remove(&scoped_session);
        }

        if let Some(blocker) = blocker {
            let serve_next = blocker.serve_next;
            serve_next(blocker, world, roster);
        }

        clear_scope_buffers(scope, scoped_session, world)?;

        if world.get_entity(scoped_session).is_some() {
            if let Some(scoped_session_mut) = world.get_entity_mut(scoped_session) {
                scoped_session_mut.despawn_recursive();
            }
        }

        Ok(())
    }
}

fn clear_scope_buffers(scope: Entity, session: Entity, world: &mut World) -> OperationResult {
    let nodes = world
        .get::<ScopeContents>(scope)
        .or_broken()?
        .nodes()
        .clone();
    for node in nodes {
        if let Some(clear_buffer) = world.get::<ClearBufferFn>(node) {
            let clear_buffer = clear_buffer.0;
            clear_buffer(node, session, world)?;
        }
    }
    Ok(())
}

#[derive(Component)]
struct CleanupInputBufferStorage<B>(B);

/// The entity that holds this component is responsible for some part of the
/// cleanup process for the scope referred to by the inner value of the component
#[derive(Component)]
struct CleanupForScope(Entity);

#[derive(Component, Default)]
struct AwaitingCleanupStorage(SmallVec<[AwaitingCleanup; 8]>);

impl AwaitingCleanupStorage {
    fn get(&mut self, scoped_session: Entity) -> Option<&mut AwaitingCleanup> {
        self.0
            .iter_mut()
            .find(|a| a.scoped_session == scoped_session)
    }
}

struct AwaitingCleanup {
    scoped_session: Entity,
    info: CleanupInfo,
    /// When this is None, that means the cleanup workflows have not started
    /// yet. This may happen when an async service is running or if the scope
    /// is uninterruptible. We must NOT issue any cleanup notification while
    /// this is None. We can only issue the cleanup notification when this is
    /// both Some and the collection is empty. Think of "None" as indicating
    /// that the cleanup workflows have not even begun running yet.
    cleanup_workflow_sessions: Option<SmallVec<[Entity; 8]>>,
}

impl AwaitingCleanup {
    fn new(scoped_session: Entity, info: CleanupInfo) -> Self {
        Self {
            scoped_session,
            info,
            cleanup_workflow_sessions: None,
        }
    }
}

#[derive(Component, Default)]
pub(crate) struct ExitTargetStorage {
    /// Map from session value to the target
    pub(crate) map: HashMap<Entity, ExitTarget>,
}

#[derive(Debug)]
pub(crate) struct ExitTarget {
    pub(crate) target: Entity,
    pub(crate) source: Entity,
    pub(crate) parent_session: Entity,
    pub(crate) blocker: Option<Blocker>,
}

pub(crate) struct RedirectScopeStream<T: StreamEffect> {
    target: Entity,
    _ignore: std::marker::PhantomData<fn(T)>,
}

impl<T: StreamEffect> RedirectScopeStream<T> {
    pub(crate) fn new(target: Entity) -> Self {
        Self {
            target,
            _ignore: Default::default(),
        }
    }
}

impl<S: StreamEffect> Operation for RedirectScopeStream<S> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<S::Input>::new(),
            SingleTargetStorage::new(self.target),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;

        // The target is optional because we want to "send" this stream even if
        // there is no target listening, because streams may have custom sending
        // behavior
        let target = source_mut.get::<SingleTargetStorage>().map(|t| t.get());
        let Input {
            session: scoped_session,
            data,
        } = source_mut.take_input::<S::Input>()?;
        let parent_session = world
            .get::<ParentSession>(scoped_session)
            .or_broken()?
            .get();

        let mut request = StreamRequest {
            source,
            session: parent_session,
            target,
            world,
            roster,
        };

        let output = S::side_effect(data, &mut request)?;
        request.send_output(output)
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<S>()? {
            return Ok(true);
        }

        let scope = r.world.get::<ScopeStorage>(r.source).or_broken()?.get();
        r.check_upstream(scope)

        // TODO(@mxgrey): Consider whether we can/should identify more
        // specifically whether the current state of the scope would be able to
        // reach this specific stream.
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        // TODO(@mxgrey): Consider whether we should cleanup the inputs by
        // pushing them out of the scope instead of just dropping them.
        clean.cleanup_inputs::<S>()?;
        clean.notify_cleaned()
    }
}

pub struct RedirectWorkflowStream<S: StreamRedirect> {
    pub redirect: S,
}

impl<S: StreamRedirect> RedirectWorkflowStream<S> {
    pub fn new(redirect: S) -> Self {
        Self { redirect }
    }
}

#[derive(Component, Deref)]
pub struct StreamNameStorage(pub Option<Cow<'static, str>>);

impl<S: StreamRedirect> Operation for RedirectWorkflowStream<S> {
    fn setup(self, info: OperationSetup) -> OperationResult {
        StreamRedirect::setup(self.redirect, info)
    }

    fn execute(request: OperationRequest) -> OperationResult {
        S::execute(request)
    }

    fn is_reachable(mut r: OperationReachability) -> ReachabilityResult {
        if r.has_input::<S::Input>()? {
            return Ok(true);
        }

        let scope = r.world.get::<ScopeStorage>(r.source).or_broken()?.get();
        r.check_upstream(scope)

        // TODO(@mxgrey): Consider whether we can/should identify more
        // specifically whether the current state of the scope would be able to
        // reach this specific stream.
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        // TODO(@mxgrey): Consider whether we should cleanup the inputs by
        // pushing them out of the scope instead of just dropping them.
        clean.cleanup_inputs::<S::Input>()?;
        clean.notify_cleaned()
    }
}

pub trait StreamRedirect {
    type Input: 'static + Send + Sync;
    fn setup(self, info: OperationSetup) -> OperationResult;
    fn execute(request: OperationRequest) -> OperationResult;
}

pub struct AnonymousStreamRedirect<S: StreamEffect> {
    name: Option<Cow<'static, str>>,
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S: StreamEffect> AnonymousStreamRedirect<S> {
    pub fn new(name: Option<Cow<'static, str>>) -> Self {
        Self {
            name,
            _ignore: Default::default(),
        }
    }
}

impl<S: StreamEffect> StreamRedirect for AnonymousStreamRedirect<S> {
    type Input = S::Input;

    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .entity_mut(source)
            .insert((StreamNameStorage(self.name), InputBundle::<S::Input>::new()));
        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input {
            session: scoped_session,
            data,
        } = source_mut.take_input::<S::Input>()?;
        let scope = source_mut.get::<ScopeStorage>().or_broken()?.get();
        let name = source_mut.get::<StreamNameStorage>().or_broken()?.0.clone();

        let exit = world
            .get::<ExitTargetStorage>(scope)
            .or_broken()?
            .map
            .get(&scoped_session)
            // If the map does not have this session in it, that should simply
            // mean that the workflow has terminated, so we should discard this
            // stream data.
            //
            // TODO(@mxgrey): Consider whether this should count as a disposal.
            .or_not_ready()?;
        let exit_source = exit.source;
        let parent_session = exit.parent_session;

        let stream_targets = world.get::<StreamTargetMap>(exit_source).or_broken()?;

        if let Some(name) = name {
            let target = stream_targets.get_named_or_anonymous::<S::Output>(&name);
            let mut request = StreamRequest {
                source,
                session: parent_session,
                target: target.map(NamedTarget::as_entity),
                world,
                roster,
            };

            S::side_effect(data, &mut request).and_then(|value| {
                target
                    .map(|t| t.send_output(NamedValue { name, value }, request))
                    .unwrap_or(Ok(()))
            })?;
        } else {
            let target = stream_targets.get_anonymous::<S::Output>();
            let mut request = StreamRequest {
                source,
                session: parent_session,
                target,
                world,
                roster,
            };

            S::side_effect(data, &mut request).and_then(|output| request.send_output(output))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dyn_node::DynOutput,
        operation::{IncrementalScopeError, IncrementalScopeRequest, IncrementalScopeResponse},
        prelude::*,
        testing::*,
    };

    #[test]
    fn test_scope_race() {
        let mut context = TestingContext::minimal_plugins();

        let short_delay = context.spawn_delay(Duration::from_secs_f32(0.01));
        let long_delay = context.spawn_delay(Duration::from_secs_f32(10.0));

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let inner_scope = builder.create_io_scope(|scope, builder| {
                builder.chain(scope.input).fork_clone((
                    |chain: Chain<_>| {
                        chain
                            .then(long_delay)
                            .map_block(|_| "slow")
                            .connect(scope.terminate)
                    },
                    |chain: Chain<_>| {
                        chain
                            .then(short_delay)
                            .map_block(|_| "fast")
                            .connect(scope.terminate)
                    },
                ));
            });

            builder.connect(scope.input, inner_scope.input);
            builder.connect(inner_scope.output, scope.terminate);
        });

        let mut promise =
            context.command(|commands| commands.request((), workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert!(context.no_unhandled_errors());
        let result = promise.take().available().unwrap();
        assert_eq!(result, "fast");
    }

    #[test]
    fn test_incremental_scope_race() {
        let mut context = TestingContext::minimal_plugins();

        let short_delay = context.spawn_delay::<()>(Duration::from_secs_f32(0.01));
        let long_delay = context.spawn_delay::<()>(Duration::from_secs_f32(10.0));

        let workflow = context.spawn_io_workflow(|scope, builder| {
            let mut incremental_scope_builder =
                crate::IncrementalScopeBuilder::begin(ScopeSettings::default(), builder);
            assert!(incremental_scope_builder.is_finished().is_err());

            let ((fork_input, fork_outputs), slow, fast) = {
                let mut builder = Builder {
                    context: incremental_scope_builder.builder_scope_context(),
                    commands: builder.commands(),
                };

                let fork = builder.create_fork_clone::<()>();
                let slow = builder.create_map_block::<(), _>(|_| "slow");
                let fast = builder.create_map_block::<(), _>(|_| "fast");
                (fork, slow, fast)
            };

            let IncrementalScopeRequest {
                external_input,
                begin_scope,
            } = incremental_scope_builder
                .set_request::<()>(builder.commands())
                .unwrap();
            let begin_scope = begin_scope.unwrap();

            assert!(incremental_scope_builder.is_finished().is_err());

            // Call set_request a second time to see that we get the intended behavior
            let second_set_request = incremental_scope_builder
                .set_request::<()>(builder.commands())
                .unwrap();
            // The external_input should be provided again, and it should be
            // equivalent to the first one we had received.
            assert_eq!(external_input, second_set_request.external_input);
            // The begin_scope should be none now because it was already provided
            // earlier. Each DynOutput in existence should uniquely identify a
            // single output.
            assert!(second_set_request.begin_scope.is_none());

            // Call set_request again with a different type to see that it
            // produces the correct error.
            let bad_set_request = incremental_scope_builder
                .set_request::<i64>(builder.commands())
                .unwrap_err();
            assert!(matches!(
                bad_set_request,
                IncrementalScopeError::RequestMismatch { .. }
            ));

            assert!(incremental_scope_builder.is_finished().is_err());

            let IncrementalScopeResponse {
                terminate,
                external_output,
            } = incremental_scope_builder
                .set_response::<&'static str>(builder.commands())
                .unwrap();
            let external_output = external_output.unwrap();

            assert!(incremental_scope_builder.is_finished().is_ok());

            // Call set_response a second time to see that we get the intended behavior
            let second_set_response = incremental_scope_builder
                .set_response::<&'static str>(builder.commands())
                .unwrap();
            // The terminate should be provided again, and it should be
            // equivalent to the first one we had received.
            assert_eq!(terminate, second_set_response.terminate);
            // The external_output should be none now because it was already
            // provided earlier. Each DynOutput in existence should uniquely
            // identify a single output.
            assert!(second_set_response.external_output.is_none());

            // Call set_response again with a different type to see that it
            // produces teh correct error.
            let bad_set_response = incremental_scope_builder
                .set_response::<String>(builder.commands())
                .unwrap_err();
            assert!(matches!(
                bad_set_response,
                IncrementalScopeError::ResponseMismatch { .. }
            ));

            assert!(incremental_scope_builder.is_finished().is_ok());

            let workflow_input: DynOutput = scope.input.into();
            workflow_input
                .connect_to(&external_input.into(), builder)
                .unwrap();

            {
                let mut builder = Builder {
                    context: incremental_scope_builder.builder_scope_context(),
                    commands: builder.commands(),
                };

                fork_outputs
                    .clone_chain(&mut builder)
                    .then(long_delay)
                    .connect(slow.input);
                fork_outputs
                    .clone_chain(&mut builder)
                    .then(short_delay)
                    .connect(fast.input);

                begin_scope
                    .connect_to(&fork_input.into(), &mut builder)
                    .unwrap();

                let slow_output: DynOutput = slow.output.into();
                slow_output.connect_to(&terminate, &mut builder).unwrap();

                let fast_output: DynOutput = fast.output.into();
                fast_output.connect_to(&terminate, &mut builder).unwrap();
            }

            external_output
                .connect_to(&scope.terminate.into(), builder)
                .unwrap();
        });

        let mut promise =
            context.command(|commands| commands.request((), workflow).take_response());

        context.run_with_conditions(&mut promise, Duration::from_secs(1));
        assert!(
            context.no_unhandled_errors(),
            "{:#?}",
            context.get_unhandled_errors()
        );
        let result: &'static str = promise.take().available().unwrap();
        assert_eq!(result, "fast");
    }
}
