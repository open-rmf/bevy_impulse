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

use bevy_ecs::{
    prelude::{Entity, World},
    world::Command,
};
use bevy_hierarchy::prelude::DespawnRecursiveExt;

use backtrace::Backtrace;

use crate::{
    Broken, ConnectionFailure, EntryForScope, ForkTargetStorage, OperationError, OperationResult,
    OrBroken, ScopeEntryStorage, SingleInputStorage, SingleTargetStorage, StreamTargetMap,
    UnhandledErrors,
};

/// If two nodes have been created, they will each have a unique source and a
/// target entity allocated to them. If we want to connect them, then we want
/// the target of one to no longer be unique - we instead want it to be the
/// source entity of the other. This [`Command`] redirects the target information
/// of the sending node to target the source entity of the receiving node.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Connect {
    pub(crate) original_target: Entity,
    pub(crate) new_target: Entity,
}

impl Command for Connect {
    fn apply(self, world: &mut World) {
        if let Err(OperationError::Broken(backtrace)) = try_connect(self, world) {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .connections
                .push(ConnectionFailure {
                    original_target: self.original_target,
                    new_target: self.new_target,
                    backtrace: backtrace.unwrap_or_else(Backtrace::new),
                })
        }
    }
}

fn try_connect(connect: Connect, world: &mut World) -> OperationResult {
    if let Some(EntryForScope(scope)) = world.get(connect.original_target) {
        // The original target was the entry point of a scope, so we need to
        // handle it a bit differently. Instead of modifying target and input
        // storage components, we need to modify EntryForScope and
        // ScopeEntryStorage components.
        let scope = *scope;
        world
            .get_entity_mut(connect.new_target)
            .or_broken()?
            .insert(EntryForScope(scope));
        world
            .get_entity_mut(scope)
            .or_broken()?
            .insert(ScopeEntryStorage(connect.new_target));

        world
            .get_entity_mut(connect.original_target)
            .or_broken()?
            .despawn_recursive();
        return Ok(());
    }

    let old_inputs = world
        .get_entity_mut(connect.original_target)
        .or_broken()?
        .take::<SingleInputStorage>()
        .or_broken()?
        .take();

    for input in old_inputs.into_iter() {
        let mut input_mut = world.get_entity_mut(input).or_broken()?;

        let mut connection_happened = false;
        if let Some(mut target) = input_mut.get_mut::<SingleTargetStorage>() {
            // If the old target is different from the original target, then that
            // means we are connecting to a stream instead, so we need to first
            // check if they are equal.
            if target.get() == connect.original_target {
                connection_happened = true;
                target.set(connect.new_target);
            }
        }

        if let Some(mut targets) = input_mut.get_mut::<ForkTargetStorage>() {
            for target in &mut targets.0 {
                if *target == connect.original_target {
                    connection_happened = true;
                    *target = connect.new_target;
                }
            }
        }

        if let Some(mut targets) = input_mut.get_mut::<StreamTargetMap>() {
            for target in &mut targets.map {
                if *target == connect.original_target {
                    connection_happened = true;
                    *target = connect.new_target;
                }
            }
        }

        if !connection_happened {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .broken
                .push(Broken {
                    node: input,
                    backtrace: Some(Backtrace::new()),
                });
        }

        if let Some(mut new_inputs_mut) = world.get_mut::<SingleInputStorage>(connect.new_target) {
            new_inputs_mut.add(input);
        } else {
            world
                .get_entity_mut(connect.new_target)
                .or_broken()?
                .insert(SingleInputStorage::new(input));
        }
    }

    world
        .get_entity_mut(connect.original_target)
        .or_broken()?
        .despawn_recursive();

    Ok(())
}
