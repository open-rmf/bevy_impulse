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

use bevy::prelude::{Component, Entity};

use crate::{
    Operation, SingleTargetStorage, InputBundle,
    SingleInputStorage, Input, ManageInput, Cancel, OperationResult,
    OrBroken, OperationRequest, OperationSetup, OperationCleanup,
    OperationReachability, ReachabilityResult,
};

pub struct CancelFilter<InputT, Output, F> {
    filter: F,
    target: Entity,
    _ignore: std::marker::PhantomData<(InputT, Output)>,
}

fn identity<Value>(value: Value) -> Value {
    value
}

pub(crate) fn make_cancel_filter_on_none<T>(target: Entity) -> CancelFilter<Option<T>, T, fn(Option<T>) -> Option<T>> {
    CancelFilter {
        filter: identity::<Option<T>>,
        target,
        _ignore: Default::default()
    }
}

pub(crate) fn make_cancel_filter_on_err<T, E>(target: Entity) -> CancelFilter<Result<T, E>, T, fn(Result<T, E>) -> Option<T>> {
    CancelFilter {
        filter: err_to_none::<T, E>,
        target,
        _ignore: Default::default(),
    }
}

fn err_to_none<T, E>(value: Result<T, E>) -> Option<T> {
    value.ok()
}

#[derive(Component)]
struct CancelFilterStorage<F: 'static + Send + Sync>(F);

impl<InputT, Output, F> Operation for CancelFilter<InputT, Output, F>
where
    InputT: 'static + Send + Sync,
    Output: 'static + Send + Sync,
    F: FnOnce(InputT) -> Option<Output> + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleInputStorage::new(source));
        }
        world.entity_mut(source).insert((
            InputBundle::<InputT>::new(),
            SingleTargetStorage(self.target),
            CancelFilterStorage(self.filter),
        ));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: input } = source_mut.take_input::<InputT>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let CancelFilterStorage::<F>(filter) = source_mut.take().or_broken()?;

        // This is where we cancel if the filter function does not return anything.
        let Some(output) = filter(input) else {
            roster.cancel(Cancel::filtered(source));
            // We've queued up a cancellation of this link, so we don't want any
            // automatic cleanup to happen.
            return Ok(());
        };

        // At this point we have the correct type to deliver to the target, so
        // we proceed with doing that.
        let mut target_mut = world.get_entity_mut(target).or_broken()?;
        target_mut.give_input(session, output, roster);
        roster.queue(target);

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<InputT>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<InputT>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(reachability)
    }
}
