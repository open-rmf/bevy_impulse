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

use bevy::prelude::{Entity, World, Component};

use crate::{
    InputStorage, InputBundle, FunnelInputStatus, FunnelSourceStorage,
    SingleTargetStorage, Operation, OperationStatus, OperationRoster, Unzippable,
    ForkTargetStorage, SingleSourceStorage, Cancel, RaceCancelled, ForkTargetStatus,
};

use smallvec::SmallVec;

pub(crate) struct RaceInput<T> {
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> RaceInput<T> {
    pub(crate) fn new(target: Entity) -> Self {
        Self { target, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Operation for RaceInput<T> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        // NOTE: The target will be informed aobut its source by the ZipRace or
        // BundleRace operation.
        world.entity_mut(entity).insert((
            SingleTargetStorage(self.target),
            FunnelInputStatus::Pending,
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        source_mut.get_mut::<FunnelInputStatus>().ok_or(())?.ready();

        let target = source_mut.get::<SingleTargetStorage>().ok_or(())?.0;
        world.get_mut::<RaceWinner>(target).ok_or(())?.propose(source);
        roster.queue(target);

        // We can't let this link be cleaned up automatically. Its cleanup needs
        // to be handled by the race that it belongs to.
        Ok(OperationStatus::Unfinished)
    }
}

pub(crate) struct ZipRace<Values> {
    sources: FunnelSourceStorage,
    targets: ForkTargetStorage,
    _ignore: std::marker::PhantomData<Values>,
}

impl<Values> ZipRace<Values> {
    pub(crate) fn new(
        sources: FunnelSourceStorage,
        targets: ForkTargetStorage
    ) -> Self {
        Self { sources, targets, _ignore: Default::default() }
    }
}

impl<Values: Unzippable> Operation for ZipRace<Values> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        for target in &self.targets.0 {
            if let Some(mut target_mut) = world.get_entity_mut(*target) {
                target_mut.insert((
                    SingleSourceStorage(entity),
                    ForkTargetStatus::Active,
                ));
            }
        }
        world.entity_mut(entity).insert((
            self.sources,
            self.targets,
            RaceWinner::Pending,
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        manage_race_delivery(source, world, roster, Values::race_values)
    }
}

pub(crate) struct BundleRace<T> {
    sources: FunnelSourceStorage,
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<T> BundleRace<T> {
    pub(crate) fn new(
        sources: FunnelSourceStorage,
        target: Entity,
    ) -> Self {
        Self { sources, target, _ignore: Default::default() }
    }
}

impl<T: 'static + Send + Sync> Operation for BundleRace<T> {
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleSourceStorage(entity));
        }
        world.entity_mut(entity).insert((
            self.sources,
            SingleTargetStorage(self.target),
            RaceWinner::Pending,
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        manage_race_delivery(source, world, roster, deliver_bundle_race_winner::<T>)
    }
}

fn manage_race_delivery(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
    deliver: fn(Entity, Entity, &mut World, &mut OperationRoster) -> Result<OperationStatus, ()>,
) -> Result<OperationStatus, ()> {
    if let Some(winner) = world.get_mut::<RaceWinner>(source).ok_or(())?.take_ready() {
        deliver(source, winner, world, roster)?;
        let inputs = world.get::<FunnelSourceStorage>(source).ok_or(())?;
        for input in &inputs.0 {
            let input_status = world.get::<FunnelInputStatus>(*input).ok_or(())?;
            if input_status.is_pending() {
                roster.drop_dependency(Cancel::race_lost(source, *input, input_status.clone()));
            }
        }
    }

    inspect_race_targets(source, world, roster)?;

    // Note that we need to keep the race link alive for as long as any input
    // source still has a Pending status. Otherwise we will break the chain in
    // ways that could lead to errors. Unlike most Operations, we expect execute
    // to be called on a race many times (each time that there is a change to at
    // least one input source). Each time execute is called, we inspect the
    // sources until we identify that everything can be closed down.
    inspect_race_inputs(source, world, roster)
}

fn inspect_race_inputs(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) -> Result<OperationStatus, ()> {
    let source_ref = world.entity(source);
    let inputs = source_ref.get::<FunnelSourceStorage>().ok_or(())?;

    let mut dispose_race = true;
    if let Some(fork_targets) = source_ref.get::<ForkTargetStorage>() {
        // Check if any input chains in the race have cancelled or disposed,
        // and pass that along to their associated endpoints.
        let mut close_input: SmallVec<[Entity; 16]> = SmallVec::new();

        for (input, target) in inputs.0.iter().zip(fork_targets.0.iter()) {
            let input_status = world.get::<FunnelInputStatus>(*input).ok_or(())?;
            if input_status.is_pending() {
                dispose_race = false;
            } else if let Some(cause) = input_status.cancelled() {
                roster.cancel(Cancel { apply_to: *target, cause });
                close_input.push(*input);
            } else if input_status.is_disposed() {
                roster.dispose_chain_from(*target);
                close_input.push(*input);
            }
        }

        for e in close_input {
            world.get_mut::<FunnelInputStatus>(e).ok_or(())?.close();
        }

    } else if let Some(single_target) = source_ref.get::<SingleTargetStorage>() {
        // Check if all input chains are undeliverable, and cancel the race
        // target if they are.
        let mut input_statuses: SmallVec<[(Entity, FunnelInputStatus); 16]> = SmallVec::new();
        for candidate in &inputs.0 {
            let status = world.get::<FunnelInputStatus>(*candidate).ok_or(())?;
            if !status.undeliverable() {
                dispose_race = false;
                break;
            }
            input_statuses.push((*candidate, status.clone()));
        }

        if dispose_race {
            roster.cancel(Cancel::new(
                single_target.0,
                RaceCancelled {
                    race: source,
                    input_statuses: input_statuses.into_vec()
                }.into(),
            ));
        }
    } else {
        return Err(());
    }

    if dispose_race {
        return Ok(OperationStatus::Finished);
    } else {
        return Ok(OperationStatus::Unfinished);
    }
}

fn inspect_race_targets(
    source: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) -> Result<OperationStatus, ()> {
    let source_ref = world.entity(source);
    if let Some(fork_targets) = source_ref.get::<ForkTargetStorage>() {
        // Check whether any targets have dropped, and propagate that upwards
        let inputs = source_ref.get::<FunnelSourceStorage>().ok_or(())?;
        let mut close_target: SmallVec<[Entity; 16]> = SmallVec::new();
        for (input, target) in inputs.0.iter().zip(fork_targets.0.iter()) {
            let target_status = world.get::<ForkTargetStatus>(*target).ok_or(())?;
            if let Some(cause) = target_status.dropped() {
                roster.drop_dependency(Cancel { apply_to: *input, cause });
                close_target.push(*target);
            }
        }

        for e in close_target {
            world.get_mut::<ForkTargetStatus>(e).ok_or(())?.close();
        }
    }

    Ok(OperationStatus::Unfinished)
}

fn deliver_bundle_race_winner<T: 'static + Send + Sync>(
    source: Entity,
    winner: Entity,
    world: &mut World,
    roster: &mut OperationRoster,
) -> Result<OperationStatus, ()> {
    let target = world.get::<SingleTargetStorage>(source).ok_or(())?.0;
    let input = world
        .get_entity_mut(winner).ok_or(())?
        .take::<InputStorage<T>>().ok_or(())?.take();

    world.get_entity_mut(target).ok_or(())?.insert(InputBundle::new(input));
    roster.queue(target);
    Ok(OperationStatus::Finished)
}

#[derive(Component)]
enum RaceWinner {
    Pending,
    Ready(Entity),
    Closed,
}

impl RaceWinner {
    fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    fn propose(&mut self, winner: Entity) {
        if self.is_pending() {
            *self = Self::Ready(winner);
        }
    }

    fn take_ready(&mut self) -> Option<Entity> {
        match self {
            Self::Ready(input) => {
                let result = Some(*input);
                *self = Self::Closed;
                result
            }
            _ => None,
        }
    }
}
