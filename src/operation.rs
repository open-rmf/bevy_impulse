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

use crate::{RequestLabelId, Cancel, Cancellation};

use bevy::{
    prelude::{Entity, World, Component, Query},
    ecs::system::{Command, SystemParam},
};

use backtrace::Backtrace;

use std::collections::VecDeque;

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
    Cancelled(Cancellation),
    /// The input has been disposed so it will never be ready
    Disposed,
    /// The input could not be delivered and this fact has been handled.
    Closed,
}

impl FunnelInputStatus {
    pub fn ready(&mut self) {
        if self.is_closed() {
            return;
        }
        *self = Self::Ready;
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    pub fn cancel(&mut self, cause: Cancellation) {
        if self.is_closed() {
            return;
        }
        *self = Self::Cancelled(cause);
    }

    pub fn cancelled(&self) -> Option<Cancellation> {
        match self {
            Self::Cancelled(cause) => Some(cause.clone()),
            _ => None,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled(_))
    }

    pub fn dispose(&mut self) {
        if self.is_closed() {
            return;
        }
        *self = Self::Disposed;
    }

    pub fn is_disposed(&self) -> bool {
        matches!(self, Self::Disposed)
    }

    pub fn undeliverable(&self) -> bool {
        self.is_cancelled() || self.is_disposed()
    }

    pub fn close(&mut self) {
        *self = Self::Closed;
    }

    pub fn is_closed(&self) -> bool {
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
    Dropped(Cancellation),
    /// The target has been dropped and this fact has been processed
    Closed,
}

impl ForkTargetStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    pub fn dropped(&self) -> Option<Cancellation> {
        match self {
            Self::Dropped(cause) => Some(cause.clone()),
            _ => None,
        }
    }

    pub fn drop_dependency(&mut self, cause: Cancellation) {
        if self.is_closed() {
            return;
        }
        *self = Self::Dropped(cause);
    }

    pub fn is_closed(&self) -> bool {
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
pub(crate) struct NextOperationLink<'w, 's> {
    single_target: Query<'w, 's, &'static SingleTargetStorage>,
    fork_targets: Query<'w, 's, &'static ForkTargetStorage>,
}

impl<'w, 's> NextOperationLink<'w, 's> {
    pub(crate) fn iter(&self, entity: Entity) -> NextOperationLinkIter {
        if let Ok(target) = self.single_target.get(entity) {
            return NextOperationLinkIter::Target(Some(target.0));
        } else if let Ok(fork) = self.fork_targets.get(entity) {
            return NextOperationLinkIter::Fork(fork.0.clone());
        }

        return NextOperationLinkIter::Target(None);
    }
}

pub(crate) enum NextOperationLinkIter {
    Target(Option<Entity>),
    Fork(SmallVec<[Entity; 8]>),
}

impl Iterator for NextOperationLinkIter {
    type Item = Entity;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            NextOperationLinkIter::Target(target) => {
                return target.take();
            }
            NextOperationLinkIter::Fork(fork) => {
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
    /// Operation sources that should be cancelled
    pub(crate) cancel: VecDeque<Cancel>,
    /// Async services that should pull their next item
    pub(crate) unblock: VecDeque<BlockingQueue>,
    /// Remove these entities as they are no longer needed
    pub(crate) dispose: Vec<Entity>,
    /// Remove the whole chain from this point on because it is no longer needed
    pub(crate) dispose_chain_from: Vec<Entity>,
    /// Indicate that there is no longer a need for this chain
    pub(crate) drop_dependency: Vec<Cancel>,
    /// Indicate async tasks that should be polled
    pub(crate) poll: Vec<Entity>,
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

    pub fn dispose_chain_from(&mut self, entity: Entity) {
        self.dispose_chain_from.push(entity);
    }

    pub fn drop_dependency(&mut self, source: Cancel) {
        self.drop_dependency.push(source);
    }

    pub fn poll(&mut self, source: Entity) {
        self.poll.push(source);
    }

    pub fn is_empty(&self) -> bool {
        self.operate.is_empty() && self.cancel.is_empty()
        && self.unblock.is_empty() && self.dispose.is_empty()
        && self.dispose_chain_from.is_empty() && self.drop_dependency.is_empty()
        && self.poll.is_empty()
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
    /// The operation is not finished yet, so do not do any automatic cleanup.
    Unfinished,
}

pub struct OperationError {
    pub backtrace: Option<Backtrace>,
}

impl OperationError {
    pub fn here() -> Self {
        OperationError { backtrace: Some(Backtrace::new()) }
    }
}

pub type OperationResult = Result<OperationStatus, OperationError>;

/// Trait that defines a single operation within a chain.
pub trait Operation {
    /// Set the initial parameters for your operation. This gets called while
    /// the chain is being built.
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    );

    /// Execute this operation. This gets triggered when a new InputStorage
    /// component is added to `source` or when another operation puts `source`
    /// into the [`OperationRoster::queue`].
    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;
}

pub trait OrBroken: Sized {
    type Value;

    /// If the value is not available then we will have an operation error.
    /// This includes a backtrace of the stack to help with debugging.
    fn or_broken(self) -> Result<Self::Value, OperationError> {
        self.or_broken_impl(Some(Backtrace::new()))
    }

    /// If the value is not available then we will have an operation error.
    /// This does not include a backtrace, which makes it suitable for codebases
    /// that need to be kept hidden.
    fn or_broken_hide(self) -> Result<Self::Value, OperationError> {
        self.or_broken_impl(None)
    }

    /// This is what should be implemented by structs that provide this trait.
    fn or_broken_impl(self, backtrace: Option<Backtrace>) -> Result<Self::Value, OperationError>;
}

impl<T, E> OrBroken for Result<T, E> {
    type Value = T;
    fn or_broken_impl(self, backtrace: Option<Backtrace>) -> Result<T, OperationError> {
        self.map_err(|_| OperationError { backtrace })
    }
}

impl<T> OrBroken for Option<T> {
    type Value = T;
    fn or_broken_impl(self, backtrace: Option<Backtrace>) -> Result<T, OperationError> {
        self.ok_or_else(|| OperationError { backtrace })
    }
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

/// Insert this as a component on the source of any operation that needs to have
/// special handling for cancellation. So far this is only used by [`Terminate`].
#[derive(Component, Clone, Copy)]
pub struct CancellationBehavior {
    pub hook: fn(&Cancellation, Entity, &mut World, &mut OperationRoster),
}

pub(crate) fn operate(entity: Entity, world: &mut World, roster: &mut OperationRoster) {
    let Some(operator) = world.get::<Operate>(entity) else {
        roster.cancel(Cancel::broken_here(entity));
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
        Ok(OperationStatus::Unfinished) => {
            // Do nothing
        }
        Err(error) => {
            roster.cancel(Cancel::broken(source, error.backtrace));
        }
    }
}