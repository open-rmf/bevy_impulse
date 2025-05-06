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

use bevy_ecs::prelude::{Command, Component, Entity, World};
use smallvec::SmallVec;
use std::{collections::HashMap, sync::Arc};

use crate::{
    Broken, Disposal, ForkTargetStorage, Input, InputBundle, ManageDisposal, ManageInput,
    MiscellaneousFailure, Operation, OperationCleanup, OperationError, OperationReachability,
    OperationRequest, OperationResult, OperationSetup, OrBroken, ReachabilityResult,
    SingleInputStorage, SplitDispatcher, Splittable, UnhandledErrors,
};

#[derive(Component)]
pub(crate) struct OperateSplit<T: Splittable> {
    /// The connections that lead out of this split operation. These only change
    /// while the workflow is being built, afterwards they should be frozen.
    connections: HashMap<T::Key, usize>,
    /// A reverse map that keeps track of what key is at each index
    index_to_key: Vec<Arc<str>>,
    /// A cache used to transfer the split values from the input to the outputs.
    /// Every iteration this must be reset to all None values. If any one of them
    /// is a None after the Splittable has filled it in, we must issue a disposal
    /// notice because one of the outputs might not be receiving anything.
    outputs_cache: Option<Vec<Vec<T::Item>>>,
}

impl<T: Splittable> Default for OperateSplit<T> {
    fn default() -> Self {
        Self {
            connections: Default::default(),
            index_to_key: Vec::new(),
            outputs_cache: Some(Vec::new()),
        }
    }
}

impl<T: 'static + Splittable + Send + Sync> Operation for OperateSplit<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            self,
            InputBundle::<T>::new(),
            ForkTargetStorage::default(),
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
        let Input { session, data } = source_mut.take_input::<T>()?;
        let targets = source_mut.get::<ForkTargetStorage>().or_broken()?.0.clone();

        let mut split = source_mut.get_mut::<OperateSplit<T>>().or_broken()?;
        let mut outputs = split.outputs_cache.take().unwrap_or(Vec::new());
        let dispatcher = SplitDispatcher {
            connections: &split.connections,
            outputs: &mut outputs,
        };
        data.split(dispatcher)?;

        let mut missed_indices: SmallVec<[usize; 16]> = SmallVec::new();
        for (index, (items, target)) in outputs.iter_mut().zip(targets).enumerate() {
            if items.is_empty() {
                missed_indices.push(index);
            }

            for output in items.drain(..) {
                world
                    .get_entity_mut(target)
                    .or_broken()?
                    .give_input(session, output, roster)?;
            }
        }

        let mut source_mut = world.get_entity_mut(source).or_broken()?;

        if !missed_indices.is_empty() {
            let split = source_mut.get::<OperateSplit<T>>().or_broken()?;
            let missing_keys: SmallVec<[Option<Arc<str>>; 16]> = missed_indices
                .into_iter()
                .map(|index| split.index_to_key.get(index).cloned())
                .collect();

            source_mut.emit_disposal(
                session,
                Disposal::incomplete_split(source, missing_keys),
                roster,
            );
        }

        // Return the cache into the component
        source_mut
            .get_mut::<OperateSplit<T>>()
            .or_broken()?
            .outputs_cache
            .replace(outputs);

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

pub(crate) struct ConnectToSplit<T: Splittable> {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
    pub(crate) key: T::Key,
}

impl<T: 'static + Splittable> Command for ConnectToSplit<T> {
    fn apply(self, world: &mut World) {
        let node = self.source;
        if let Err(OperationError::Broken(backtrace)) = self.connect(world) {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .broken
                .push(Broken { node, backtrace });
        }
    }
}

impl<T: 'static + Splittable> ConnectToSplit<T> {
    fn connect(self, world: &mut World) -> Result<(), OperationError> {
        let mut target_storage = world
            .get_mut::<ForkTargetStorage>(self.source)
            .or_broken()?;
        let index = target_storage.0.len();
        target_storage.0.push(self.target);

        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(self.source));

        let mut split = world.get_mut::<OperateSplit<T>>(self.source).or_broken()?;
        let previous_index = split.connections.insert(self.key.clone(), index);
        split
            .outputs_cache
            .as_mut()
            .or_broken()?
            .resize_with(index + 1, Vec::new);
        if split.index_to_key.len() != index {
            // If the next element of the reverse map does not match the new index
            // then something has fallen out of sync. This doesn't really break
            // the workflow because this reverse map is only used to generate
            // disposal messages, but it does indicate a bug is present.
            let reverse_map_size = split.index_to_key.len();
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: Arc::new(anyhow::anyhow!(
                        "Mismatch between reverse map size [{}] and new connection index [{}]",
                        reverse_map_size,
                        index,
                    )),
                    backtrace: Some(backtrace::Backtrace::new()),
                });
        } else {
            split
                .index_to_key
                .push(format!("{:?}", self.key).as_str().into());
        }

        if let Some(previous_index) = previous_index {
            // If something was already using this key then there is a flaw in
            // the implementation of SplitBuilder and we should log it.
            let target_storage = world.get::<ForkTargetStorage>(self.source).or_broken()?;
            let previous_target = *target_storage.0.get(previous_index).or_broken()?;

            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: Arc::new(anyhow::anyhow!(
                        "Double-connected key [{:?}] for split node {:?}. Original target: {:?}, new target: {:?}",
                        self.key,
                        self.source,
                        previous_target,
                        self.target,
                    )),
                    backtrace: Some(backtrace::Backtrace::new()),
                });
        }

        Ok(())
    }
}
