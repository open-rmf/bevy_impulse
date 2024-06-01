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
    SingleInputStorage, Input, ManageInput, OperationResult,
    OrBroken, OperationRequest, OperationSetup, OperationCleanup,
    OperationReachability, ReachabilityResult, ManageCancellation,
    Cancellation,
};

use thiserror::Error as ThisError;
use std::error::Error;

pub struct CancelFilter<InputT, Output, F> {
    filter: F,
    target: Entity,
    _ignore: std::marker::PhantomData<(InputT, Output)>,
}

#[derive(ThisError, Debug)]
#[error("A scope was cancelled because an unacceptable None value was received")]
pub struct FilteredNone;

fn filter_none<Value>(value: Option<Value>) -> FilterResult<Value> {
    value.ok_or_else(|| Some(FilteredNone.into()))
}

pub(crate) fn make_cancel_filter_on_none<T>(target: Entity) -> CancelFilter<Option<T>, T, fn(Option<T>) -> FilterResult<T>> {
    CancelFilter {
        filter: filter_none::<T>,
        target,
        _ignore: Default::default()
    }
}

pub(crate) fn make_cancel_filter_on_err<T, E: Error + 'static + Send + Sync>(
    target: Entity,
) -> CancelFilter<Result<T, E>, T, fn(Result<T, E>) -> FilterResult<T>> {
    CancelFilter {
        filter: filter_err::<T, E>,
        target,
        _ignore: Default::default(),
    }
}

pub(crate) fn make_cancel_quietly_filter_on_err<T, E>(
    target: Entity,
) -> CancelFilter<Result<T, E>, T, fn(Result<T, E>) -> FilterResult<T>> {
    CancelFilter {
        filter: quietly_filter_err,
        target,
        _ignore: Default::default()
    }
}

#[derive(ThisError, Debug)]
#[error("A scope was cancelled because an unacceptable Err value was received")]
pub struct FilteredErr<E: Error + 'static + Send + Sync> {
    #[source]
    err: E,
}

fn filter_err<T, E: Error + 'static + Send + Sync>(value: Result<T, E>) -> FilterResult<T> {
    value.map_err(|err| Some(FilteredErr { err }.into()))
}

#[derive(ThisError, Debug)]
#[error("A scope was cancelled because an unacceptable Err value was received")]
pub struct QuietlyFilteredErr;

fn quietly_filter_err<T, E>(value: Result<T, E>) -> FilterResult<T> {
    value.map_err(|_| Some(QuietlyFilteredErr.into()))
}

#[derive(Component)]
struct CancelFilterStorage<F: 'static + Send + Sync>(F);

pub type FilterResult<T> = Result<T, Option<anyhow::Error>>;

impl<InputT, Output, F> Operation for CancelFilter<InputT, Output, F>
where
    InputT: 'static + Send + Sync,
    Output: 'static + Send + Sync,
    F: FnOnce(InputT) -> FilterResult<Output> + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<InputT>::new(),
            SingleTargetStorage::new(self.target),
            CancelFilterStorage(self.filter),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: input } = source_mut.take_input::<InputT>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let CancelFilterStorage::<F>(filter) = source_mut.take().or_broken()?;

        // This is where we cancel if the filter function does not return anything.
        let output = match filter(input) {
            Ok(output) => output,
            Err(reason) => {
                let cancellation = Cancellation::filtered(source, reason);
                source_mut.emit_cancel(session, cancellation, roster);
                // We've queued up a cancellation of this link, so we don't want any
                // automatic cleanup to happen.
                return Ok(());
            }
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

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<InputT>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}
