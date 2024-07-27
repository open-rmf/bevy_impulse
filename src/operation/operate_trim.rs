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
    OperationReachability, ReachabilityResult, OperationSetup, SingleInputStorage,
    SingleTargetStorage, OrBroken, OperationCleanup, Cancellation, ManageCancellation,
    Disposal, OperationError, Cleanup, TrimBranch, TrimPoint, TrimPolicy, CleanupContents,
    FinalizeCleanup, FinalizeCleanupRequest, ScopeStorage, ScopeEntryStorage,
    downstream_of, emit_disposal,
};

use bevy::prelude::{Component, Entity, World};

use smallvec::SmallVec;

use std::collections::{HashMap, hash_map::Entry, HashSet};


pub(crate) struct Trim<T> {
    /// The branches to be trimmed, as defined by the user.
    branches: SmallVec<[TrimBranch; 16]>,

    /// The target that will be notified when the trimming is finished and all
    /// the nodes report that they're clean.
    target: Entity,

    _ignore: std::marker::PhantomData<T>,
}

impl<T> Trim<T> {
    pub(crate) fn new(
        branches: SmallVec<[TrimBranch; 16]>,
        target: Entity
    ) -> Self {
        Self { branches, target, _ignore: Default::default() }
    }
}

#[derive(Component)]
struct TrimStorage {
    /// The branches to be trimmed, as defined by the user
    branches: SmallVec<[TrimBranch; 16]>,

    /// The actual operations which need to be cancelled, as calculated the
    /// first time the trim operation gets run. Before the first run, this will
    /// contain a None value.
    ///
    /// We wait until the first run of the operation before calculating the nodes
    /// because we can't be certain that the workflow is fully defined until the
    /// first time it runs. After that the workflow is fixed, so we can just
    /// reuse them.
    nodes_calculated: bool,
}

/// Data that's passing through this node will be held here until the trimming
/// is finished.
#[derive(Component)]
struct HoldingStorage<T> {
    // The key of this map is a cleanup_id
    map: HashMap<Entity, Input<T>>
}

impl<T> Default for HoldingStorage<T> {
    fn default() -> Self {
        Self { map: Default::default() }
    }
}

impl TrimStorage {
    fn new(branches: SmallVec<[TrimBranch; 16]>) -> Self {
        Self { branches, nodes_calculated: false }
    }
}

impl<T: 'static + Send + Sync> Operation for Trim<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            TrimStorage::new(self.branches),
            SingleTargetStorage::new(self.target),
            InputBundle::<T>::new(),
            CleanupContents::new(),
            FinalizeCleanup::new(Self::finalize_trim),
            HoldingStorage::<T>::default(),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let Input { session, data } = world.get_entity_mut(source).or_broken()?
            .take_input::<T>()?;

        let source_ref = world.get_entity(source).or_broken()?;
        let trim = source_ref.get::<TrimStorage>().or_broken()?;
        if !trim.nodes_calculated {
            let scope = world.get::<ScopeStorage>(source).or_broken()?.get();
            let scope_entry = world.get::<ScopeEntryStorage>(scope).or_broken()?.0;
            match calculate_nodes(scope_entry, &trim.branches, world) {
                Ok(Ok(nodes)) => {
                    let mut source_mut = world.get_entity_mut(source).or_broken()?;
                    let mut contents = source_mut.get_mut::<CleanupContents>().or_broken()?;
                    for node in nodes {
                        contents.add_node(node);
                    }

                    source_mut.get_mut::<TrimStorage>().or_broken()?
                        .nodes_calculated = true;
                }
                Ok(Err(cancellation)) => {
                    // There is something broken in how the branches to be
                    // trimmed re defined, so we should cancel the workflow.
                    world.get_entity_mut(source).or_broken()?
                        .emit_cancel(session, cancellation, roster);
                    return Ok(());
                }
                Err(broken) => {
                    return Err(broken);
                }
            }
        }

        let cleanup_id = world.spawn(()).id();
        world.get_mut::<HoldingStorage<T>>(source).or_broken()?
            .map.insert(cleanup_id, Input { data, session });
        let cleanup = Cleanup { cleaner: source, session, cleanup_id };

        let nodes = world.get::<CleanupContents>(source).or_broken()?.nodes().clone();
        for node in nodes {
            OperationCleanup { source: node, cleanup, world, roster }.clean();
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        clean.cleanup_disposals()?;
        let session = clean.cleanup.session;
        clean.world.get_mut::<HoldingStorage<T>>(clean.source).or_broken()?
            .map.retain(|_, input| input.session != session);
        Ok(())
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

impl<T: 'static + Send + Sync> Trim<T> {
    fn finalize_trim(
        FinalizeCleanupRequest { cleanup, world, roster }: FinalizeCleanupRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(cleanup.cleaner).or_broken()?;
        let Input { session, data } = source_mut
            .get_mut::<HoldingStorage<T>>().or_broken()?
            // It's possible for the entry to be erased if this trim node gets
            // cleaned up while this cleanup is happening.
            .map.remove(&cleanup.cleanup_id).or_not_ready()?;

        let nodes = source_mut.get::<CleanupContents>().or_broken()?.nodes().clone();
        source_mut.give_input(session, data, roster)?;

        let disposal = Disposal::trimming(nodes);
        emit_disposal(cleanup.cleaner, cleanup.session, disposal, world, roster);
        Ok(())
    }
}

fn calculate_nodes(
    scope_entry: Entity,
    branches: &SmallVec<[TrimBranch; 16]>,
    world: &World,
) -> Result<Result<SmallVec<[Entity; 16]>, Cancellation>, OperationError> {
    let mut all_nodes: SmallVec<[Entity; 16]> = SmallVec::new();
    for branch in branches {
        let result = match branch.policy() {
            TrimPolicy::Downstream => calculate_downstream(
                scope_entry, branch.from_point(), world,
            ),
            TrimPolicy::Span(span) => calculate_all_spans(
                scope_entry, branch.from_point(), span, world,
            ),
        };

        match result? {
            Ok(nodes) => {
                all_nodes.extend(nodes);
            }
            Err(cancellation) => {
                return Ok(Err(cancellation));
            }
        }
    }

    all_nodes.sort();
    all_nodes.dedup();

    Ok(Ok(all_nodes))
}

fn calculate_downstream(
    scope_entry: Entity,
    initial_point: TrimPoint,
    world: &World,
) -> Result<Result<SmallVec<[Entity; 16]>, Cancellation>, OperationError> {
    // First calculate the span from the scope entry to the initial point so we
    // can filter those nodes out while we calculate the downstream.
    let filter = calculate_span(scope_entry, initial_point.id(), &HashSet::new(), world);

    let mut visited = HashSet::new();
    let mut queue: Vec<Entity> = Vec::new();
    queue.push(initial_point.id());
    while let Some(top) = queue.pop() {
        if filter.contains(&top) {
            // No need to include this or expand from it
            continue;
        }

        if visited.insert(top) {
            for next in downstream_of(top, world) {
                queue.push(next);
            }
        }
    }

    if visited.is_empty() {
        return Ok(Err(Cancellation::invalid_span(initial_point.id(), None)));
    }

    Ok(Ok(visited.into_iter().filter(|n| initial_point.accept(*n)).collect()))
}

fn calculate_all_spans(
    scope_entry: Entity,
    initial_point: TrimPoint,
    span: &SmallVec<[TrimPoint; 16]>,
    world: &World,
) -> Result<Result<SmallVec<[Entity; 16]>, Cancellation>, OperationError> {
    let mut all_nodes: SmallVec<[Entity; 16]> = SmallVec::new();
    for to_point in span {
        match calculate_span_nodes(scope_entry, initial_point, *to_point, world)? {
            Ok(nodes) => {
                all_nodes.extend(nodes);
            }
            Err(cancellation) => {
                return Ok(Err(cancellation));
            }
        }
    }

    all_nodes.sort();
    all_nodes.dedup();
    Ok(Ok(all_nodes))
}

fn calculate_span_nodes(
    scope_entry: Entity,
    initial_point: TrimPoint,
    to_point: TrimPoint,
    world: &World,
) -> Result<Result<SmallVec<[Entity; 16]>, Cancellation>, OperationError> {
    let filter = calculate_span(scope_entry, initial_point.id(), &HashSet::new(), world);
    let span = calculate_span(initial_point.id(), to_point.id(), &filter, world);
    if span.is_empty() {
        return Ok(Err(Cancellation::invalid_span(
            initial_point.id(), Some(to_point.id()),
        )));
    }

    Ok(Ok(span.into_iter().filter(|n| {
        initial_point.accept(*n) && to_point.accept(*n)
    }).collect()))
}

fn calculate_span(
    initial_point: Entity,
    to_point: Entity,
    filter: &HashSet<Entity>,
    world: &World,
) -> HashSet<Entity> {
    // A map from a child node to all of its parents which will lead up towards
    // the initial point. First we build up this map until exhaustion, and then
    // we look up the entry for to_point and trace all paths backwards.
    let mut span_map: HashMap<Entity, HashSet<Entity>> = Default::default();
    span_map.insert(initial_point, Default::default());

    if filter.contains(&to_point) {
        // The goal point is part of the filtered set. This probably means it's
        // upstream of the initial_point, which makes this an invalid span.
        return HashSet::new();
    }

    let mut queue: Vec<Entity> = Vec::new();
    queue.push(initial_point);

    while let Some(top) = queue.pop() {
        if top == to_point {
            // No need to expand from here because we've reached the goal.
            continue;
        }

        for next in downstream_of(top, world) {
            if filter.contains(&next) {
                // Don't expand towards this node since it's part of the
                // filtered set.
                continue;
            }

            let entry = span_map.entry(next);
            let keep_expanding = matches!(&entry, Entry::Vacant(_));
            let children = entry.or_default();
            children.insert(top);

            if keep_expanding {
                queue.push(next);
            }
        }
    }

    // The map should be fully built by now, so go from the final point and
    // crawl upstream, gathering all the nodes that were traversed.
    let mut nodes = HashSet::new();
    queue.push(to_point);
    while let Some(top) = queue.pop() {
        if nodes.insert(top) {
            if let Some(parents) = span_map.get(&top) {
                for parent in parents {
                    queue.push(*parent);
                }
            }
        }
    }

    nodes
}
