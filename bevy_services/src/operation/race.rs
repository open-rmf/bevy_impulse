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

#[derive(Component)]
struct RaceWinner(Option<Entity>);

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

        let mut target_mut = world.get_entity_mut(target).ok_or(())?;
        let mut winner = target_mut.get_mut::<RaceWinner>().ok_or(())?;
        if winner.0.is_none() {
            winner.0 = Some(source);
            roster.queue(target);
        }

        Ok(OperationStatus::Disregard)
    }
}

pub(crate) struct ZipRace<Values> {
    sources: FunnelSourceStorage,
    targets: ForkTargetStorage,
    _ignore: std::marker::PhantomData<Values>,
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
    let source_ref = world.get_entity(source).ok_or(())?;
    let Some(winner) = source_ref.get::<RaceWinner>().ok_or(())?.0 else {
        // There's no winner for the race, so let's see if we are in a situation
        // that needs attention.
        if inspect_race_inputs(source, world, roster)? == OperationStatus::Finished {
            return Ok(OperationStatus::Finished);
        }

        return inspect_race_targets(source, world, roster);
    };

    deliver(source, winner, world, roster)
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
            if !input_status.undeliverable() {
                dispose_race = false;
            } else if let Some(cause) = input_status.cancelled() {
                roster.cancel(Cancel { apply_to: *target, cause });
                close_input.push(*input);
            } else if input_status.is_disposed() {
                roster.dispose_chain(*target);
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
        return Ok(OperationStatus::Disregard);
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

    Ok(OperationStatus::Disregard)
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
