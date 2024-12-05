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

use crate::{
    try_emit_broken, Broken, Cancel, DeliveryLabelId, InspectInput, SetupFailure, StreamTargetMap,
    UnhandledErrors,
};

use bevy_ecs::{
    prelude::{Component, Entity, World},
    system::Command,
};
use bevy_hierarchy::prelude::BuildWorldChildren;

use backtrace::Backtrace;

use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};

use smallvec::SmallVec;

mod branching;
pub(crate) use branching::*;

mod cleanup;
pub(crate) use cleanup::*;

mod collect;
pub(crate) use collect::*;

mod filter;
pub(crate) use filter::*;

mod fork_clone;
pub(crate) use fork_clone::*;

mod fork_unzip;
pub(crate) use fork_unzip::*;

mod injection;
pub(crate) use injection::*;

mod join;
pub(crate) use join::*;

mod listen;
pub(crate) use listen::*;

mod noop;
pub(crate) use noop::*;

mod operate_buffer;
pub use operate_buffer::*;

mod operate_buffer_access;
pub use operate_buffer_access::*;

mod operate_callback;
pub(crate) use operate_callback::*;

mod operate_gate;
pub(crate) use operate_gate::*;

mod operate_map;
pub(crate) use operate_map::*;

mod operate_service;
pub(crate) use operate_service::*;

mod operate_split;
pub(crate) use operate_split::*;

mod operate_task;
pub(crate) use operate_task::*;

mod operate_trim;
pub(crate) use operate_trim::*;

mod scope;
pub use scope::*;

mod spread;
pub(crate) use spread::*;

/// This component is given to nodes that get triggered each time any single
/// input is provided to them. There may be multiple nodes that can feed into
/// this node, but this node gets triggered any time any one of them provides
/// an input.
#[derive(Component, Clone)]
pub struct SingleInputStorage(SmallVec<[Entity; 8]>);

impl SingleInputStorage {
    pub fn new(input: Entity) -> Self {
        Self(SmallVec::from_iter([input]))
    }

    pub fn empty() -> Self {
        Self(SmallVec::new())
    }

    pub fn get(&self) -> &SmallVec<[Entity; 8]> {
        &self.0
    }

    pub fn take(self) -> SmallVec<[Entity; 8]> {
        self.0
    }

    pub(crate) fn add(&mut self, input: Entity) {
        if !self.0.contains(&input) {
            self.0.push(input);
        }
    }

    pub fn is_reachable(r: &mut OperationReachability) -> ReachabilityResult {
        let Some(inputs) = r.world.get_entity(r.source).or_broken()?.get::<Self>() else {
            return Ok(false);
        };
        for input in &inputs.0 {
            if r.check_upstream(*input)? {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

/// Keep track of the sources that funnel into this link of the impulse chain.
/// This is for links that draw from multiple sources simultaneously, such as
/// join and race.
#[derive(Component, Clone, Default)]
pub struct FunnelInputStorage(pub(crate) SmallVec<[Entity; 8]>);

impl FunnelInputStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self) -> &SmallVec<[Entity; 8]> {
        &self.0
    }
}

impl FromIterator<Entity> for FunnelInputStorage {
    fn from_iter<T: IntoIterator<Item = Entity>>(iter: T) -> Self {
        Self(SmallVec::from_iter(iter))
    }
}

impl From<SmallVec<[Entity; 8]>> for FunnelInputStorage {
    fn from(value: SmallVec<[Entity; 8]>) -> Self {
        Self(value)
    }
}

/// Keep track of the target for a link in a impulse chain
#[derive(Component, Clone, Copy, Debug)]
pub struct SingleTargetStorage(Entity);

impl SingleTargetStorage {
    pub fn new(target: Entity) -> Self {
        Self(target)
    }

    pub fn get(&self) -> Entity {
        self.0
    }

    pub(crate) fn set(&mut self, target: Entity) {
        self.0 = target;
    }
}

/// Keep track of the targets for a fork in a impulse chain
#[derive(Component, Clone, Default)]
pub struct ForkTargetStorage(pub SmallVec<[Entity; 8]>);

impl ForkTargetStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FromIterator<Entity> for ForkTargetStorage {
    fn from_iter<T: IntoIterator<Item = Entity>>(iter: T) -> Self {
        Self(SmallVec::from_iter(iter))
    }
}

#[derive(Component)]
pub(crate) struct UnusedTarget;

#[derive(Default)]
pub struct OperationRoster {
    /// Operation sources that should be triggered
    pub(crate) queue: VecDeque<Entity>,
    /// Tasks that should be awoken. If the task is already despawned, then
    /// it should not be considered an error.
    pub(crate) awake: VecDeque<Entity>,
    /// Operation sources that should be triggered after the next ChannelQueue
    /// flush. This is for the final outputs of polled tasks, to make sure their
    /// stream data gets flushed before their final output is flushed.
    pub(crate) deferred_queue: VecDeque<Entity>,
    /// Operation sources that should be cancelled
    pub(crate) cancel: VecDeque<Cancel>,
    /// Async services that should pull their next item
    pub(crate) unblock: VecDeque<Blocker>,
    /// Remove these entities as they are no longer needed
    pub(crate) disposed: Vec<DisposalNotice>,
    /// Tell a scope to attempt cleanup
    pub(crate) cleanup_finished: Vec<Cleanup>,
}

impl OperationRoster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn queue(&mut self, source: Entity) {
        self.queue.push_back(source);
    }

    pub fn awake(&mut self, source: Entity) {
        self.awake.push_back(source);
    }

    pub fn defer(&mut self, source: Entity) {
        self.deferred_queue.push_back(source);
    }

    pub fn cancel(&mut self, source: Cancel) {
        self.cancel.push_back(source);
    }

    pub(crate) fn unblock(&mut self, provider: Blocker) {
        self.unblock.push_back(provider);
    }

    pub fn disposed(&mut self, scope: Entity, origin: Entity, session: Entity) {
        self.disposed.push(DisposalNotice {
            source: scope,
            origin,
            session,
        });
    }

    pub fn cleanup_finished(&mut self, cleanup: Cleanup) {
        self.cleanup_finished.push(cleanup);
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
            && self.awake.is_empty()
            && self.deferred_queue.is_empty()
            && self.cancel.is_empty()
            && self.unblock.is_empty()
            && self.disposed.is_empty()
            && self.cleanup_finished.is_empty()
    }

    pub fn append(&mut self, other: &mut Self) {
        self.queue.append(&mut other.queue);
        self.cancel.append(&mut other.cancel);
        self.unblock.append(&mut other.unblock);
        self.disposed.append(&mut other.disposed);
        self.cleanup_finished.append(&mut other.cleanup_finished);
    }

    /// Remove all instances of the target from the roster. This prevents a
    /// despawned entity from needlessly tripping errors.
    pub fn purge(&mut self, target: Entity) {
        self.queue.retain(|e| *e != target);
        self.deferred_queue.retain(|e| *e != target);
    }

    /// Move all items from the deferred queue into the immediate queue
    pub fn process_deferals(&mut self) {
        for e in self.deferred_queue.drain(..) {
            self.queue.push_back(e);
        }
    }
}

/// Notify the scope manager that a disposal took place. This will prompt the
/// scope to check whether it's still possible to terminate.
pub struct DisposalNotice {
    /// The scope that needs to handle the disposal
    pub source: Entity,
    /// The operation that the disposal originated from
    pub origin: Entity,
    /// The session that experienced a disposal
    pub session: Entity,
}

pub(crate) struct Blocker {
    /// The provider that is being blocked
    pub(crate) provider: Entity,
    /// The source that is doing the blocking
    pub(crate) source: Entity,
    /// The session that is doing the blocking
    pub(crate) session: Entity,
    /// The label of the queue that is being blocked
    pub(crate) label: Option<DeliveryLabelId>,
    /// Function pointer to call when this is no longer blocking
    pub(crate) serve_next: fn(Blocker, &mut World, &mut OperationRoster),
}

impl std::fmt::Debug for Blocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f
            .debug_struct("Blocker")
            .field("provider", &self.provider)
            .field("source", &self.source)
            .field("session", &self.session)
            .field("label", &self.label)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum OperationError {
    Broken(Option<Backtrace>),
    NotReady,
}

impl OperationError {
    pub fn broken_here() -> Self {
        OperationError::Broken(Some(Backtrace::new()))
    }
}

pub type OperationResult = Result<(), OperationError>;
pub type ReachabilityResult = Result<bool, OperationError>;

pub struct OperationSetup<'a> {
    pub(crate) source: Entity,
    pub(crate) world: &'a mut World,
}

pub struct OperationRequest<'a> {
    pub source: Entity,
    pub world: &'a mut World,
    pub roster: &'a mut OperationRoster,
}

impl<'a> OperationRequest<'a> {
    pub fn pend(self) -> PendingOperationRequest {
        PendingOperationRequest {
            source: self.source,
        }
    }
}

pub struct OperationReachability<'a> {
    source: Entity,
    session: Entity,
    // If the reachability query was triggered by a disposal, this indicates
    // what was disposed. This is used by the collect operation to figure out if
    // it will emit a value in response to the disposal, which may affect the
    // reachability calculation.
    disposed: Option<Entity>,
    world: &'a World,
    visited: &'a mut HashMap<Entity, bool>,
}

impl<'a> OperationReachability<'a> {
    pub fn new(
        session: Entity,
        source: Entity,
        disposed: Option<Entity>,
        world: &'a World,
        visited: &'a mut HashMap<Entity, bool>,
    ) -> OperationReachability<'a> {
        Self {
            session,
            source,
            disposed,
            world,
            visited,
        }
    }

    pub fn check_upstream(&mut self, source: Entity) -> ReachabilityResult {
        match self.visited.entry(source) {
            Entry::Occupied(occupied) => {
                // We have looped back to this node, so whatever value we
                // currently have for it is what we should return.
                return Ok(*occupied.get());
            }
            Entry::Vacant(vacant) => {
                // We assume that the node is not reachable unless proven
                // otherwise.
                vacant.insert(false);
            }
        }

        let reachabiility = self
            .world
            .get_entity(source)
            .or_broken()?
            .get::<OperationReachabilityStorage>()
            .or_broken()?
            .0;

        let is_reachable = reachabiility(OperationReachability {
            source,
            session: self.session,
            disposed: self.disposed,
            world: self.world,
            visited: self.visited,
        })?;

        if is_reachable {
            self.visited.insert(source, is_reachable);
        }

        Ok(is_reachable)
    }

    pub fn has_input<T: 'static + Send + Sync>(&self) -> ReachabilityResult {
        self.world
            .get_entity(self.source)
            .or_broken()?
            .has_input::<T>(self.session)
    }

    pub fn source(&self) -> Entity {
        self.source
    }

    pub fn session(&self) -> Entity {
        self.session
    }

    pub fn world(&self) -> &World {
        self.world
    }
}

pub fn check_reachability(
    session: Entity,
    source: Entity,
    disposed: Option<Entity>,
    world: &World,
) -> ReachabilityResult {
    let mut visited = HashMap::new();
    let mut r = OperationReachability {
        source,
        session,
        disposed,
        world,
        visited: &mut visited,
    };
    r.check_upstream(source)
}

#[derive(Clone, Copy)]
pub struct PendingOperationRequest {
    pub source: Entity,
}

impl PendingOperationRequest {
    pub fn activate<'a>(
        self,
        world: &'a mut World,
        roster: &'a mut OperationRoster,
    ) -> OperationRequest<'a> {
        OperationRequest {
            source: self.source,
            world,
            roster,
        }
    }
}

/// Trait that defines a single operation within a chain.
pub trait Operation {
    /// Set the initial parameters for your operation. This gets called while
    /// the chain is being built.
    fn setup(self, info: OperationSetup) -> OperationResult;

    /// Execute this operation. This gets triggered when a new InputStorage
    /// component is added to `source` or when another operation puts `source`
    /// into the [`OperationRoster::queue`].
    fn execute(request: OperationRequest) -> OperationResult;

    /// A request has reached its termination, so this operation needs to clean
    /// up any residual data for it.
    fn cleanup(clean: OperationCleanup) -> OperationResult;

    /// Return whether this operation can be reached. Being reachable means there
    /// is some sequence of operations that could lead to this one being triggered
    /// for a given session, or this operation is currently active for the
    /// given session.
    ///
    /// This is primarily used to determine if a Join has stalled out or if
    /// a request will never be able to terminate.
    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult;
}

pub trait OrBroken: Sized {
    type Value;

    /// If the value is not available then we will have an operation error of
    /// not ready. This will not lead to a cancellation but instead indicates
    /// that the operation was not ready to perform yet.
    fn or_not_ready(self) -> Result<Self::Value, OperationError>;

    /// If the value is not available then we will have an operation error of
    /// broken. This includes a backtrace of the stack to help with debugging.
    fn or_broken(self) -> Result<Self::Value, OperationError> {
        self.or_broken_impl(true)
    }

    /// If the value is not available then we will have an operation error of
    /// broken. This does not include a backtrace, which makes it suitable for
    /// codebases that need to be kept hidden.
    fn or_broken_hide(self) -> Result<Self::Value, OperationError> {
        self.or_broken_impl(false)
    }

    /// This is what should be implemented by structs that provide this trait.
    fn or_broken_impl(self, with_backtrace: bool) -> Result<Self::Value, OperationError>;
}

impl<T, E> OrBroken for Result<T, E> {
    type Value = T;
    fn or_not_ready(self) -> Result<Self::Value, OperationError> {
        self.map_err(|_| OperationError::NotReady)
    }

    fn or_broken_impl(self, with_backtrace: bool) -> Result<T, OperationError> {
        if with_backtrace {
            self.map_err(|_| OperationError::Broken(Some(Backtrace::new())))
        } else {
            self.map_err(|_| OperationError::Broken(None))
        }
    }
}

impl<T> OrBroken for Option<T> {
    type Value = T;
    fn or_not_ready(self) -> Result<Self::Value, OperationError> {
        self.ok_or(OperationError::NotReady)
    }

    fn or_broken_impl(self, with_backtrace: bool) -> Result<T, OperationError> {
        if with_backtrace {
            self.ok_or_else(|| OperationError::Broken(Some(Backtrace::new())))
        } else {
            self.ok_or(OperationError::Broken(None))
        }
    }
}

pub(crate) struct AddOperation<Op: Operation> {
    scope: Option<Entity>,
    source: Entity,
    operation: Op,
}

impl<Op: Operation> AddOperation<Op> {
    pub(crate) fn new(scope: Option<Entity>, source: Entity, operation: Op) -> Self {
        Self {
            scope,
            source,
            operation,
        }
    }
}

impl<Op: Operation + 'static + Sync + Send> Command for AddOperation<Op> {
    fn apply(self, world: &mut World) {
        if let Err(error) = self.operation.setup(OperationSetup {
            source: self.source,
            world,
        }) {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .setup
                .push(SetupFailure {
                    broken_node: self.source,
                    error,
                });
        }

        let mut source_mut = world.entity_mut(self.source);
        source_mut.insert((
            OperationExecuteStorage(perform_operation::<Op>),
            OperationCleanupStorage(Op::cleanup),
            OperationReachabilityStorage(Op::is_reachable),
        ));
        if let Some(scope) = self.scope {
            source_mut
                .insert(ScopeStorage::new(scope))
                .set_parent(scope);
            match world.get_mut::<ScopeContents>(scope).or_broken() {
                Ok(mut contents) => {
                    contents.add_node(self.source);
                }
                Err(error) => {
                    world
                        .get_resource_or_insert_with(UnhandledErrors::default)
                        .setup
                        .push(SetupFailure {
                            broken_node: self.source,
                            error,
                        });
                }
            }
        }
    }
}

#[derive(Component)]
pub(crate) struct OperationExecuteStorage(pub(crate) fn(OperationRequest));

#[derive(Component)]
pub(crate) struct OperationReachabilityStorage(fn(OperationReachability) -> ReachabilityResult);

pub fn execute_operation(request: OperationRequest) {
    let Some(operator) = request.world.get::<OperationExecuteStorage>(request.source) else {
        if request.world.get::<UnusedTarget>(request.source).is_none() {
            // This can happen while using the async channel to issue requests
            // which end up getting dropped during a cleanup. In that case, the
            // source entity will be totally despawned, so check for that before
            // concluding that this is broken.
            if request.world.get_entity(request.source).is_some() {
                // The node does not have an operation and is not an unused target,
                // so this is broken somehow.
                request
                    .world
                    .get_resource_or_insert_with(UnhandledErrors::default)
                    .broken
                    .push(Broken {
                        node: request.source,
                        backtrace: Some(Backtrace::new()),
                    });
            }
        }
        return;
    };
    let operator = operator.0;
    operator(request);
}

pub fn awaken_task(request: OperationRequest) {
    let Some(operator) = request.world.get::<OperationExecuteStorage>(request.source) else {
        // If the task is not available, we just accept that it has despawned.
        return;
    };
    let operator = operator.0;
    operator(request);
}

fn perform_operation<Op: Operation>(
    OperationRequest {
        source,
        world,
        roster,
    }: OperationRequest,
) {
    match Op::execute(OperationRequest {
        source,
        world,
        roster,
    }) {
        Ok(()) => {
            // Do nothing
        }
        Err(OperationError::NotReady) => {
            // Do nothing
        }
        Err(OperationError::Broken(backtrace)) => {
            try_emit_broken(source, backtrace, world, roster);
        }
    }
}

pub struct DownstreamIter<'a> {
    output: DownstreamFinishIter<'a>,
    streams: Option<std::slice::Iter<'a, Entity>>,
}

impl<'a> Iterator for DownstreamIter<'a> {
    type Item = Entity;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(output) = self.output.next() {
            return Some(output);
        }

        if let Some(streams) = &mut self.streams {
            return streams.next().copied();
        }

        None
    }
}

enum DownstreamFinishIter<'a> {
    Single(Option<Entity>),
    Fork(std::slice::Iter<'a, Entity>),
}

impl<'a> Iterator for DownstreamFinishIter<'a> {
    type Item = Entity;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(iter) => iter.take(),
            Self::Fork(iter) => iter.next().copied(),
        }
    }
}

pub fn immediately_downstream_of(source: Entity, world: &World) -> DownstreamIter<'_> {
    let output = if let Some(target) = world.get::<SingleTargetStorage>(source) {
        DownstreamFinishIter::Single(Some(target.get()))
    } else if let Some(fork) = world.get::<ForkTargetStorage>(source) {
        DownstreamFinishIter::Fork(fork.0.iter())
    } else {
        DownstreamFinishIter::Single(None)
    };

    let streams = world.get::<StreamTargetMap>(source).map(|s| s.map.iter());

    DownstreamIter { output, streams }
}

/// Check if the `target` operation is somewhere downstream of the `source`
/// operation. This can be any number of generations downstream, and can account
/// for cycles where both operations can be seen as downstream of each other.
///
/// If `source` and `target` are the same then this immediately returns false.
pub fn is_downstream_of(source: Entity, target: Entity, world: &World) -> bool {
    if source == target {
        return false;
    }

    let mut queue: Vec<Entity> = Vec::new();
    let mut visited = HashSet::new();
    queue.push(source);
    while let Some(top) = queue.pop() {
        if top == target {
            return true;
        }

        if !visited.insert(top) {
            // We've already expanded this operation
            continue;
        }

        for next in immediately_downstream_of(top, world) {
            queue.push(next);
        }
    }

    // After examining all the downstream nodes in the workflow, the target was
    // not found, so it must not be downstream.
    false
}

pub struct DisposalUpdate<'a> {
    /// The operation that is being updated about the disposal
    pub source: Entity,
    /// The operation that the disposal originated from
    pub origin: Entity,
    /// The session that has experienced a disposal
    pub session: Entity,
    pub world: &'a mut World,
    pub roster: &'a mut OperationRoster,
}

#[derive(Component)]
pub struct DisposalListener(pub fn(DisposalUpdate) -> OperationResult);
