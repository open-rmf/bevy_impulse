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
    prelude::{Entity, Component, World},
    ecs::world::{EntityMut, EntityRef},
};

use backtrace::Backtrace;

use std::{collections::HashMap, sync::Arc};

use smallvec::SmallVec;

use crate::{
    OperationRoster, operation::ScopeStorage, Cancellation, UnhandledErrors,
    DisposalFailure, ImpulseMarker,
};

#[derive(Debug, Clone)]
pub struct Disposal {
    pub cause: Arc<DisposalCause>,
}

impl<T: Into<DisposalCause>> From<T> for Disposal {
    fn from(value: T) -> Self {
        Disposal { cause: Arc::new(value.into())}
    }
}

impl Disposal {
    pub fn service_unavailable(service: Entity, for_node: Entity) -> Disposal {
        ServiceUnavailable { service, for_node }.into()
    }

    pub fn task_despawned(task: Entity, node: Entity) -> Disposal {
        TaskDespawned { task, node }.into()
    }

    pub fn branching(
        branched_at_node: Entity,
        disposed_for_target: Entity,
        reason: Option<anyhow::Error>,
    ) -> Disposal {
        DisposedBranch { branched_at_node, disposed_for_target, reason }.into()
    }

    pub fn buffer_key(
        accessor_node: Entity,
        key_for_buffer: Entity,
    ) -> Disposal {
        DisposedBufferKey { accessor_node, key_for_buffer }.into()
    }

    pub fn supplanted(
        supplanted_at_node: Entity,
        supplanted_by_node: Entity,
        supplanting_session: Entity,
    ) -> Disposal {
        Supplanted { supplanted_at_node, supplanted_by_node, supplanting_session }.into()
    }

    pub fn filtered(filtered_at_node: Entity, reason: Option<anyhow::Error>) -> Self {
        Filtered { filtered_at_node, reason }.into()
    }

    pub fn trimming(trimmer: Entity, nodes: SmallVec<[Entity; 16]>) -> Self {
        Trimming { trimmer, nodes }.into()
    }

    pub fn closed_gate(
        gate_node: Entity,
        closed_buffers: SmallVec<[Entity; 8]>,
    ) -> Self {
        ClosedGate { gate_node, closed_buffers }.into()
    }

    pub fn empty_spread(
        spread_node: Entity,
    ) -> Self {
        EmptySpread { spread_node }.into()
    }
}

#[derive(Debug)]
pub enum DisposalCause {
    /// Some services will queue up requests to deliver them one at a time.
    /// Depending on the label of the incoming requests, a new request might
    /// supplant an earlier one, causing the earlier request to be disposed.
    Supplanted(Supplanted),

    /// A node filtered out a response.
    Filtered(Filtered),

    /// A node disposed of one of its output branches.
    Branching(DisposedBranch),

    /// A buffer key was disposed, so a buffer will no longer be able to update.
    BufferKey(DisposedBufferKey),

    /// A [`Service`](crate::Service) provider needed by the chain was despawned
    /// or had a critical component removed. The entity provided in the variant
    /// is the unavailable service.
    ServiceUnavailable(ServiceUnavailable),

    /// An entity that was managing the execution of a task was despawned,
    /// causing the task to be cancelled and making it impossible to deliver a
    /// response.
    TaskDespawned(TaskDespawned),

    /// An output was disposed because a mutex was poisoned.
    PoisonedMutex(PoisonedMutexDisposal),

    /// A scope was cancelled so its output has been disposed.
    Scope(Cancellation),

    /// One or more streams from a node never emitted any signal. This can lead
    /// to unexpected
    UnusedStreams(UnusedStreams),

    /// Some nodes in the workflow were trimmed.
    Trimming(Trimming),

    /// A gate was closed, which cut off the ability of a workflow to proceed.
    ClosedGate(ClosedGate),

    /// A spread operation was given an empty collection so there was nothing to
    /// spread. As a result, no signal was sent out of the node after it
    /// received a signal.
    EmptySpread(EmptySpread),
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct Supplanted {
    /// ID of the node whose service request was supplanted
    pub supplanted_at_node: Entity,
    /// ID of the node that did the supplanting
    pub supplanted_by_node: Entity,
    /// ID of the session that did the supplanting
    pub supplanting_session: Entity,
}

impl Supplanted {
    pub fn new(
        cancelled_at_node: Entity,
        supplanting_node: Entity,
        supplanting_session: Entity,
    ) -> Self {
        Self { supplanted_at_node: cancelled_at_node, supplanted_by_node: supplanting_node, supplanting_session }
    }
}

impl From<Supplanted> for DisposalCause {
    fn from(value: Supplanted) -> Self {
        DisposalCause::Supplanted(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct Filtered {
    /// ID of the node that did the filtering
    pub filtered_at_node: Entity,
    /// Optionally, a reason given for why the filtering happened.
    pub reason: Option<anyhow::Error>,
}

impl Filtered {
    pub fn new(filtered_at_node: Entity, reason: Option<anyhow::Error>) -> Self {
        Self { filtered_at_node, reason }
    }
}

impl From<Filtered> for DisposalCause {
    fn from(value: Filtered) -> Self {
        Self::Filtered(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct DisposedBranch {
    /// The node where the branching happened
    pub branched_at_node: Entity,
    /// The target node whose input was disposed
    pub disposed_for_target: Entity,
    /// Optionally, a reason given for the branching
    pub reason: Option<anyhow::Error>,
}

impl From<DisposedBranch> for DisposalCause {
    fn from(value: DisposedBranch) -> Self {
        Self::Branching(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct DisposedBufferKey {
    pub accessor_node: Entity,
    pub key_for_buffer: Entity,
}

impl From<DisposedBufferKey> for DisposalCause {
    fn from(value: DisposedBufferKey) -> Self {
        Self::BufferKey(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct ServiceUnavailable {
    /// The service that is no longer available
    pub service: Entity,
    /// The node that intended to use the service
    pub for_node: Entity,
}

impl From<ServiceUnavailable> for DisposalCause {
    fn from(value: ServiceUnavailable) -> Self {
        Self::ServiceUnavailable(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct TaskDespawned {
    /// The entity that was managing the task
    pub task: Entity,
    /// The node that the task was spawned by
    pub node: Entity,
}

impl From<TaskDespawned> for DisposalCause {
    fn from(value: TaskDespawned) -> Self {
        Self::TaskDespawned(value)
    }
}
/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct PoisonedMutexDisposal {
    /// The node containing the poisoned mutex
    pub for_node: Entity,
}

impl From<PoisonedMutexDisposal> for DisposalCause {
    fn from(value: PoisonedMutexDisposal) -> Self {
        Self::PoisonedMutex(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct UnusedStreams {
    /// The node which did not use all its streams
    pub node: Entity,
    /// The streams which went unused.
    pub streams: Vec<&'static str>,
}

impl UnusedStreams {
    pub fn new(node: Entity) -> Self {
        Self { node, streams: Default::default() }
    }
}

impl From<UnusedStreams> for DisposalCause {
    fn from(value: UnusedStreams) -> Self {
        Self::UnusedStreams(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct Trimming {
    pub trimmer: Entity,
    pub nodes: SmallVec<[Entity; 16]>,
}

impl From<Trimming> for DisposalCause {
    fn from(value: Trimming) -> Self {
        Self::Trimming(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct ClosedGate {
    /// The gate node which triggered the closing
    pub gate_node: Entity,
    /// The buffers which were closed by the gate node
    pub closed_buffers: SmallVec<[Entity; 8]>,
}

impl From<ClosedGate> for DisposalCause {
    fn from(value: ClosedGate) -> Self {
        Self::ClosedGate(value)
    }
}

/// A variant of [`DisposalCause`]
#[derive(Debug)]
pub struct EmptySpread {
    /// The node that was doing the spreading
    pub spread_node: Entity,
}

impl From<EmptySpread> for DisposalCause {
    fn from(value: EmptySpread) -> Self {
        Self::EmptySpread(value)
    }
}

pub trait ManageDisposal {
    fn emit_disposal(
        &mut self,
        session: Entity,
        disposal: Disposal,
        roster: &mut OperationRoster,
    );

    fn clear_disposals(&mut self, session: Entity);
}

pub trait InspectDisposals {
    fn get_disposals(&self, session: Entity) -> Option<&Vec<Disposal>>;
}

impl<'w> ManageDisposal for EntityMut<'w> {
    fn emit_disposal(
        &mut self,
        session: Entity,
        disposal: Disposal,
        roster: &mut OperationRoster,
    ) {
        let Some(scope) = self.get::<ScopeStorage>() else {
            if !self.contains::<ImpulseMarker>() {
                // If the emitting node does not have a scope as not part of an
                // impulse chain, then something is broken.
                let broken_node = self.id();
                self.world_scope(|world| {
                    world
                    .get_resource_or_insert_with(|| UnhandledErrors::default())
                    .disposals
                    .push(DisposalFailure {
                        disposal, broken_node, backtrace: Some(Backtrace::new())
                    });
                });
            }
            return;
        };
        let scope = scope.get();

        if let Some(mut storage) = self.get_mut::<DisposalStorage>() {
            storage.disposals.entry(session).or_default().push(disposal);
        } else {
            let mut storage = DisposalStorage::default();
            storage.disposals.entry(session).or_default().push(disposal);
            self.insert(storage);
        }

        roster.disposed(scope, session);
    }

    fn clear_disposals(&mut self, session: Entity) {
        if let Some(mut storage) = self.get_mut::<DisposalStorage>() {
            storage.disposals.remove(&session);
        }
    }
}

impl<'w> InspectDisposals for EntityMut<'w> {
    fn get_disposals(&self, session: Entity) -> Option<&Vec<Disposal>> {
        if let Some(storage) = self.get::<DisposalStorage>() {
            return storage.disposals.get(&session);
        }

        None
    }
}

impl<'w> InspectDisposals for EntityRef<'w> {
    fn get_disposals(&self, session: Entity) -> Option<&Vec<Disposal>> {
        if let Some(storage) = self.get::<DisposalStorage>() {
            return storage.disposals.get(&session);
        }

        None
    }
}

pub fn emit_disposal(
    source: Entity,
    session: Entity,
    disposal: Disposal,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    if let Some(mut source_mut) = world.get_entity_mut(source) {
        source_mut.emit_disposal(session, disposal, roster);
    } else {
        world
        .get_resource_or_insert_with(|| UnhandledErrors::default())
        .disposals
        .push(DisposalFailure {
            disposal,
            broken_node: source,
            backtrace: Some(Backtrace::new()),
        });
    }
}

#[derive(Component, Default)]
struct DisposalStorage {
    /// A map from a session to all the disposals that occurred for the session
    disposals: HashMap<Entity, Vec<Disposal>>,
}
