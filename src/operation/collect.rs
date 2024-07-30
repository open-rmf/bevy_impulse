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

use bevy::prelude::{Entity, Component, World};

use std::collections::HashMap;

use smallvec::SmallVec;

use crate::{
    Operation, Input, ManageInput, InputBundle, OperationRequest, OperationResult,
    OperationReachability, ReachabilityResult, OperationSetup, Disposal,
    SingleInputStorage, SingleTargetStorage, OrBroken, OperationCleanup,
    DisposalListener, DisposalUpdate, OperationRoster,
    emit_disposal, check_reachability,
};

pub(crate) struct Collect<T, const N: usize> {
    target: Entity,
    min: usize,
    max: Option<usize>,
    _ignore: std::marker::PhantomData<T>,
}

impl<T, const N: usize> Collect<T, N> {
    pub(crate) fn new(target: Entity, min: usize, max: Option<usize>) -> Self {
        Self { target, min, max, _ignore: Default::default() }
    }
}

impl<T, const N: usize> Operation for Collect<T, N>
where
    T: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        if let Some(max) = self.max {
            assert!(0 < max);
            assert!(self.min <= max);
        }

        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            CollectionStorage::<T, N>::new(self.min, self.max),
            SingleTargetStorage::new(self.target),
            DisposalListener(collection_disposal_listener::<T, N>),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<T>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let mut collection = source_mut.get_mut::<CollectionStorage<T, N>>().or_broken()?;
        let min = collection.min;
        let max = collection.max;
        let progress = collection.map.entry(session).or_default();
        progress.push(data);
        let len = progress.len();

        if min < len && !max.is_some_and(|max| len < max) {
            // We have obtained enough elements to send off the collection.
            let output: SmallVec<[T; N]> = progress.drain(..).collect();
            world.get_entity_mut(target).or_broken()?
                .give_input(session, output, roster)?;
            return Ok(());
        }

        // We don't have the correct number of elements so we need to check if
        // any more threads will reach this operation.
        if !check_reachability(session, source, world)? {
            // This node is not reachable, so we need to either give an output
            // or emit a disposal.
            on_unreachable_collection::<T, N>(
                source, session, target, min, len, world, roster,
            )?;
        }

        // The collection node is still reachable so we can just wait until the
        // next time it gets triggered or a disposal happens.

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        clean.cleanup_disposals()?;
        clean.world.get_mut::<CollectionStorage<T, N>>(clean.source).or_broken()?
            .map.remove(&clean.cleanup.session);
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        // We ignore the contents that have already been collected because there
        // are only two places this function should be called:
        // 1. fn execute when we're deciding whether to dispose, send early, or wait
        // 2. in the scope reachability validation, which will first call
        //    collection_disposal_listener. The listener will already have determined
        //    if we should emit a disposal or an output based on reachability.

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

#[derive(Component)]
struct CollectionStorage<T, const N: usize> {
    map: HashMap<Entity, SmallVec<[T; N]>>,
    min: usize,
    max: Option<usize>,
}

impl<T, const N: usize> CollectionStorage<T, N> {
    fn new(min: usize, max: Option<usize>) -> Self {
        Self { map: Default::default(), min, max }
    }
}

fn collection_disposal_listener<T, const N: usize>(
    DisposalUpdate { source, session, world, roster }: DisposalUpdate,
) -> OperationResult
where
    T: 'static + Send + Sync,
{
    if check_reachability(session, source, world)? {
        // The collection node is still reachable, so no action is needed.
        return Ok(());
    }

    let source_ref = world.get_entity(source).or_broken()?;
    let target = source_ref.get::<SingleTargetStorage>().or_broken()?.get();
    let collection = source_ref.get::<CollectionStorage<T, N>>().or_broken()?;
    let min = collection.min;
    let len = collection.map.get(&session).map(|c| c.len()).unwrap_or(0);
    on_unreachable_collection::<T, N>(source, session, target, min, len, world, roster)
}

fn on_unreachable_collection<T: 'static + Send + Sync, const N: usize>(
    source: Entity,
    session: Entity,
    target: Entity,
    min: usize,
    len: usize,
    world: &mut World,
    roster: &mut OperationRoster,
) -> OperationResult {
    if len < min {
        // We have not reached the minimum number of entries in this
        // collection yet. Since we do not detect any more entries coming,
        // we need to emit a disposal notice.
        let disposal = Disposal::deficient_collection(source, min, len);
        emit_disposal(source, session, disposal, world, roster);
        return Ok(());
    }

    // The size of the collection is not smaller than the minimum length
    // which means we can go ahead and send it.
    let mut collection = world.get_mut::<CollectionStorage<T, N>>(source).or_broken()?;
    let output: SmallVec<[T; N]> = collection.map.entry(session).or_default()
        .drain(..).collect();
    world.get_entity_mut(target).or_broken()?
        .give_input(session, output, roster)?;
    return Ok(());
}
