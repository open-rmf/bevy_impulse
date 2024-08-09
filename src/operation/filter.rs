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

use bevy_ecs::prelude::{Component, Entity};

use crate::{
    Operation, SingleTargetStorage, InputBundle,
    SingleInputStorage, Input, ManageInput, OperationResult,
    OrBroken, OperationRequest, OperationSetup, OperationCleanup,
    OperationReachability, ReachabilityResult, ManageCancellation,
    Cancellation, Disposal, ManageDisposal,
};

use thiserror::Error as ThisError;
use std::error::Error;

#[derive(ThisError, Debug)]
#[error("An unacceptable None value was received")]
pub struct FilteredNone;

fn filter_none<Value>(value: Option<Value>) -> FilterResult<Value> {
    value.ok_or_else(|| Some(FilteredNone.into()))
}

#[derive(ThisError, Debug)]
#[error("A Some value was received and we did not need to use it")]
pub struct FilteredSome;

fn filter_some<Value>(value: Option<Value>) -> FilterResult<()> {
    match value {
        Some(_) => FilterResult::Err(Some(FilteredSome.into())),
        None => FilterResult::Ok(()),
    }
}

#[derive(ThisError, Debug)]
#[error("An unacceptable Err value was received")]
pub struct FilteredErr<E: Error + 'static + Send + Sync> {
    #[source]
    err: E,
}

fn filter_err<T, E: Error + 'static + Send + Sync>(value: Result<T, E>) -> FilterResult<T> {
    value.map_err(|err| Some(FilteredErr { err }.into()))
}

#[derive(ThisError, Debug)]
#[error("An unacceptable Err value was received")]
pub struct QuietlyFilteredErr;

fn filter_quiet_err<T, E>(value: Result<T, E>) -> FilterResult<T> {
    value.map_err(|_| Some(QuietlyFilteredErr.into()))
}

#[derive(ThisError, Debug)]
#[error("An Ok value was received and we did not need to use it")]
pub struct FilterOk;

fn filter_ok<T, E>(value: Result<T, E>) -> FilterResult<E> {
    match value {
        Ok(_) => FilterResult::Err(Some(FilterOk.into())),
        Err(err) => FilterResult::Ok(err),
    }
}

#[derive(Component, Clone, Copy)]
struct FilterStorage<F: Copy + 'static + Send + Sync>(F);

pub type FilterResult<T> = Result<T, Option<anyhow::Error>>;

pub struct CancelFilter<InputT, OutputT, F> {
    filter: F,
    target: Entity,
    _ignore: std::marker::PhantomData<(InputT, OutputT)>,
}

pub struct CreateCancelFilter;

impl CreateCancelFilter {
    pub fn on_err<T, E>(target: Entity) -> CancelFilter<Result<T, E>, T, fn(Result<T, E>) -> FilterResult<T>>
    where
        T: 'static + Send + Sync,
        E: Error + 'static + Send + Sync,
    {
        CancelFilter {
            filter: filter_err::<T, E>,
            target,
            _ignore: Default::default(),
        }
    }

    pub fn on_quiet_err<T, E>(target: Entity) -> CancelFilter<Result<T, E>, T, fn(Result<T, E>) -> FilterResult<T>>
    where
        T: 'static + Send + Sync,
        E: 'static + Send + Sync,
    {
        CancelFilter {
            filter: filter_quiet_err::<T, E>,
            target,
            _ignore: Default::default(),
        }
    }

    pub fn on_none<T>(target: Entity) -> CancelFilter<Option<T>, T, fn(Option<T>) -> FilterResult<T>> {
        CancelFilter {
            filter: filter_none::<T>,
            target,
            _ignore: Default::default(),
        }
    }
}

impl<InputT, OutputT, F> Operation for CancelFilter<InputT, OutputT, F>
where
    InputT: 'static + Send + Sync,
    OutputT: 'static + Send + Sync,
    F: Copy + Fn(InputT) -> FilterResult<OutputT> + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<InputT>::new(),
            SingleTargetStorage::new(self.target),
            FilterStorage(self.filter),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: input } = source_mut.take_input::<InputT>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let FilterStorage::<F>(filter) = *source_mut.get().or_broken()?;

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
        world.get_entity_mut(target).or_broken()?
            .give_input(session, output, roster)
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

pub struct CreateDisposalFilter;

impl CreateDisposalFilter {
    pub fn on_err<T, E>(target: Entity) -> DisposalFilter<Result<T, E>, T, fn(Result<T, E>) -> FilterResult<T>>
    where
        T: 'static + Send + Sync,
        E: Error + 'static + Send + Sync,
    {
        DisposalFilter {
            filter: filter_err::<T, E>,
            target,
            _ignore: Default::default(),
        }
    }

    pub fn on_quiet_err<T, E>(target: Entity) -> DisposalFilter<Result<T, E>, T, fn(Result<T, E>) -> FilterResult<T>>
    where
        T: 'static + Send + Sync,
        E: 'static + Send + Sync,
    {
        DisposalFilter {
            filter: filter_quiet_err::<T, E>,
            target,
            _ignore: Default::default(),
        }
    }

    pub fn on_ok<T, E>(target: Entity) -> DisposalFilter<Result<T, E>, E, fn(Result<T, E>) -> FilterResult<E>>
    where
        T: 'static + Send + Sync,
        E: 'static + Send + Sync,
    {
        DisposalFilter {
            filter: filter_ok::<T, E>,
            target,
            _ignore: Default::default(),
        }
    }

    pub fn on_none<T>(target: Entity) -> DisposalFilter<Option<T>, T, fn(Option<T>) -> FilterResult<T>>
    where
        T: 'static + Send + Sync,
    {
        DisposalFilter {
            filter: filter_none::<T>,
            target,
            _ignore: Default::default(),
        }
    }

    pub fn on_some<T>(target: Entity) -> DisposalFilter<Option<T>, (), fn(Option<T>) -> FilterResult<()>>
    where
        T: 'static + Send + Sync,
    {
        DisposalFilter {
            filter: filter_some::<T>,
            target,
            _ignore: Default::default(),
        }
    }
}

pub struct DisposalFilter<InputT, OutputT, F> {
    filter: F,
    target: Entity,
    _ignore: std::marker::PhantomData<(InputT, OutputT)>,
}

impl<InputT, OutputT, F> Operation for DisposalFilter<InputT, OutputT, F>
where
    InputT: 'static + Send + Sync,
    OutputT: 'static + Send + Sync,
    F: Copy + Fn(InputT) -> FilterResult<OutputT> + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<InputT>::new(),
            SingleTargetStorage::new(self.target),
            FilterStorage(self.filter),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: input } = source_mut.take_input::<InputT>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let FilterStorage::<F>(filter) = *source_mut.get().or_broken()?;

        let output = match filter(input) {
            Ok(output) => output,
            Err(reason) => {
                let disposal = Disposal::filtered(source, reason);
                source_mut.emit_disposal(session, disposal, roster);
                return Ok(());
            }
        };

        world.get_entity_mut(target).or_broken()?
            .give_input(session, output, roster)
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<InputT>()?;
        clean.cleanup_disposals()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<InputT>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}
