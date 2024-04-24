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

use crate::{RequestLabelId, Cancel, CancellationCause};

use bevy::{
    prelude::{Entity, World, Component, Query},
    ecs::system::{Command, SystemParam},
};

use std::{
    collections::VecDeque,
    sync::Arc,
};

use smallvec::SmallVec;

mod branching;
pub(crate) use branching::*;

mod cancel_filter;
pub(crate) use cancel_filter::*;

mod fork_clone;
pub(crate) use fork_clone::*;

mod fork_unzip;
pub(crate) use fork_unzip::*;

mod join;
pub(crate) use join::*;

mod noop;
pub(crate) use noop::*;

mod operate_cancel;
pub(crate) use operate_cancel::*;

mod operate_handler;
pub(crate) use operate_handler::*;

mod operate_map;
pub(crate) use operate_map::*;

mod operate_service;
pub(crate) use operate_service::*;

mod race;
pub(crate) use race::*;

mod terminate;
pub(crate) use terminate::*;

/// Keep track of the source for a link in a service chain
#[derive(Component, Clone, Copy)]
pub(crate) struct SingleSourceStorage(pub(crate) Entity);

/// Keep track of the status of one input into a funnel (e.g. `join` or `race`)
#[derive(Component, Debug, Clone)]
pub enum FunnelInputStatus {
    /// The input is ready to be consumed
    Ready,
    /// The input is not ready yet but might be ready later
    Pending,
    /// The input has been cancelled so it will never be ready
    Cancelled(Arc<CancellationCause>),
    /// The input has been disposed so it will never be ready
    Disposed,
    /// The input could not be delivered and this fact has been handled.
    Closed,
}

impl FunnelInputStatus {
    fn ready(&mut self) {
        if self.is_closed() {
            return;
        }
        *self = Self::Ready;
    }

    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    fn cancel(&mut self, cause: Arc<CancellationCause>) {
        if self.is_closed() {
            return;
        }
        *self = Self::Cancelled(cause);
    }

    fn cancelled(&self) -> Option<Arc<CancellationCause>> {
        match self {
            Self::Cancelled(cause) => Some(Arc::clone(cause)),
            _ => None,
        }
    }

    fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled(_))
    }

    fn dispose(&mut self) {
        if self.is_closed() {
            return;
        }
        *self = Self::Disposed;
    }

    fn is_disposed(&self) -> bool {
        matches!(self, Self::Disposed)
    }

    fn undeliverable(&self) -> bool {
        self.is_cancelled() || self.is_disposed()
    }

    fn close(&mut self) {
        *self = Self::Closed;
    }

    fn is_closed(&self) -> bool {
        matches!(self, Self::Closed)
    }
}

/// Keep track of the sources that funnel into this link of the service chain.
/// This is for links that draw from multiple sources simultaneously, such as
/// join and race.
#[derive(Component, Clone)]
pub struct FunnelSourceStorage(pub SmallVec<[Entity; 8]>);

impl FunnelSourceStorage {
    pub fn new() -> Self {
        Self(SmallVec::new())
    }

    pub fn from_iter<T: IntoIterator<Item=Entity>>(iter: T) -> Self {
        Self(SmallVec::from_iter(iter))
    }
}

/// Keep track of the target for a link in a service chain
#[derive(Component, Clone, Copy)]
pub(crate) struct SingleTargetStorage(pub(crate) Entity);

/// Keep track of the targets for a fork in a service chain
#[derive(Component, Clone)]
pub struct ForkTargetStorage(pub SmallVec<[Entity; 8]>);

#[derive(Component)]
pub enum ForkTargetStatus {
    /// The target is able to receive an input
    Active,
    /// The target has been dropped and is no longer needed
    Dropped(Arc<CancellationCause>),
    /// The target has been dropped and this fact has been processed
    Closed,
}

impl ForkTargetStatus {
    pub fn active(&self) -> bool {
        matches!(self, Self::Active)
    }

    pub fn dropped(&self) -> Option<Arc<CancellationCause>> {
        match self {
            Self::Dropped(cause) => Some(Arc::clone(cause)),
            _ => None,
        }
    }

    pub fn drop_dependency(&mut self, cause: Arc<CancellationCause>) {
        if self.closed() {
            return;
        }
        *self = Self::Dropped(cause);
    }

    pub fn closed(&self) -> bool {
        matches!(self, Self::Closed)
    }

    pub fn close(&mut self) {
        *self = Self::Closed;
    }
}

impl ForkTargetStorage {
    pub fn new() -> Self {
        Self(SmallVec::new())
    }

    pub fn from_iter<T: IntoIterator<Item=Entity>>(iter: T) -> Self {
        Self(SmallVec::from_iter(iter))
    }
}

#[derive(SystemParam)]
pub(crate) struct NextServiceLink<'w, 's> {
    single_target: Query<'w, 's, &'static SingleTargetStorage>,
    fork_targets: Query<'w, 's, &'static ForkTargetStorage>,
}

impl<'w, 's> NextServiceLink<'w, 's> {
    pub(crate) fn iter(&self, entity: Entity) -> NextServiceLinkIter {
        if let Ok(target) = self.single_target.get(entity) {
            return NextServiceLinkIter::Target(Some(target.0));
        } else if let Ok(fork) = self.fork_targets.get(entity) {
            return NextServiceLinkIter::Fork(fork.0.clone());
        }

        return NextServiceLinkIter::Target(None);
    }
}

pub(crate) enum NextServiceLinkIter {
    Target(Option<Entity>),
    Fork(SmallVec<[Entity; 8]>),
}

impl Iterator for NextServiceLinkIter {
    type Item = Entity;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            NextServiceLinkIter::Target(target) => {
                return target.take();
            }
            NextServiceLinkIter::Fork(fork) => {
                return fork.pop();
            }
        }
    }
}

#[derive(Component)]
pub(crate) struct UnusedTarget;

#[derive(Default)]
pub struct OperationRoster {
    /// Operation sources that should be triggered
    pub(crate) operate: VecDeque<Entity>,
    /// Operation sources that should be canceled
    pub(crate) cancel: VecDeque<Cancel>,
    /// Async services that should pull their next item
    pub(crate) unblock: VecDeque<BlockingQueue>,
    /// Remove these entities as they are no longer needed
    pub(crate) dispose: Vec<Entity>,
    /// Remove the whole chain from this point on because it is no longer needed
    pub(crate) dispose_chain: Vec<Entity>,
    /// Indicate that there is no longer a need for this chain
    pub(crate) drop_dependency: Vec<Cancel>,
}

impl OperationRoster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn queue(&mut self, source: Entity) {
        self.operate.push_back(source);
    }

    pub fn cancel(&mut self, source: Cancel) {
        self.cancel.push_back(source);
    }

    pub(crate) fn unblock(&mut self, provider: BlockingQueue) {
        self.unblock.push_back(provider);
    }

    pub fn dispose(&mut self, entity: Entity) {
        self.dispose.push(entity);
    }

    pub fn dispose_chain(&mut self, entity: Entity) {
        self.dispose_chain.push(entity);
    }

    pub fn drop_dependency(&mut self, source: Cancel) {
        self.drop_dependency.push(source);
    }

    pub fn is_empty(&self) -> bool {
        self.operate.is_empty() && self.cancel.is_empty()
        && self.unblock.is_empty() && self.dispose.is_empty()
    }
}

#[derive(Component)]
pub(crate) struct BlockingQueue {
    /// The provider that is being blocked
    pub(crate) provider: Entity,
    /// The source that is doing the blocking
    pub(crate) source: Entity,
    /// The label of the queue that is being blocked
    pub(crate) label: Option<RequestLabelId>,
    /// Function pointer to call when this is no longer blocking
    pub(crate) serve_next: fn(BlockingQueue, &mut World, &mut OperationRoster),
}

#[derive(PartialEq, Eq)]
pub enum OperationStatus {
    /// The source entity is no longer needed so it should be despawned.
    Finished,
    /// Do not despawn the source entity of this operation yet because it will
    /// be needed for a service that has been queued. The service will be
    /// responsible for despawning the entity when it is no longer needed.
    Queued{ provider: Entity },
    /// Disregard the status of the operation. It will clean itself up.
    Disregard,
}

/// This component indicates that a source entity has been queued for a service
/// so it should not be despawned yet.
#[derive(Component)]
pub(crate) struct Queued(pub(crate) Entity);

pub(crate) trait Operation {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    );

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()>;
}

pub(crate) struct PerformOperation<Op: Operation> {
    source: Entity,
    operation: Op,
}

impl<Op: Operation> PerformOperation<Op> {
    pub(crate) fn new(source: Entity, operation: Op) -> Self {
        Self { source, operation }
    }
}

impl<Op: Operation + 'static + Sync + Send> Command for PerformOperation<Op> {
    fn apply(self, world: &mut World) {
        self.operation.set_parameters(self.source, world);
        let mut provider_mut = world.entity_mut(self.source);
        provider_mut
            .insert(Operate(perform_operation::<Op>))
            .remove::<UnusedTarget>();
    }
}

#[derive(Component)]
struct Operate(fn(Entity, &mut World, &mut OperationRoster));

pub(crate) fn operate(entity: Entity, world: &mut World, roster: &mut OperationRoster) {
    let Some(operator) = world.get::<Operate>(entity) else {
        roster.cancel(Cancel::broken(entity));
        return;
    };
    let operator = operator.0;
    operator(entity, world, roster);
}

fn perform_operation<Op: Operation>(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    match Op::execute(source, world, roster) {
        Ok(OperationStatus::Finished) => {
            roster.dispose(source);
        }
        Ok(OperationStatus::Queued{ provider }) => {
            if let Some(mut source_mut) = world.get_entity_mut(source) {
                source_mut.insert(Queued(provider));
            } else {
                // The source is no longer available even though it was queued.
                // We should cancel the job right away.
                roster.cancel(Cancel::broken(source));
            }
        }
        Ok(OperationStatus::Disregard) => {
            // Do nothing
        }
        Err(()) => {
            roster.cancel(Cancel::broken(source));
        }
    }
}
