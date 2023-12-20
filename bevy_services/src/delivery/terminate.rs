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
    ResponseStorage, cancel,
    promise::private::{Sender, Expectation}
};

use bevy::{
    prelude::{Entity, Component, World},
    ecs::system::Command,
};

use std::collections::VecDeque;

#[derive(Component)]
pub(crate) struct Detached;

#[derive(Component)]
pub(crate) struct Expected(pub(crate) Expectation);

#[derive(Component)]
pub(crate) struct Termination<T>(pub(crate) Sender<T>);

pub(crate) struct Terminate<Response: 'static + Send + Sync + Clone> {
    storage: Entity,
    sender: Sender<Response>,
}

impl<Response: 'static + Send + Sync + Clone> Command for Terminate<Response> {
    fn apply(self, world: &mut bevy::prelude::World) {
        let mut storage_mut = world.entity_mut(self.storage);
        if let Some(ResponseStorage(Some(response))) = storage_mut.take::<ResponseStorage<Response>>() {
            self.sender.send(response);
            return;
        }

        // The response was not ready yet, so we need to put in a Pending
        // component to keep polling for the next response.
        storage_mut
            .insert(Expected(self.sender.expectation()))
            .insert()
    }
}

fn pending_termination<Response: 'static + Send + Sync>(
    world: &mut World,
    queue: &mut VecDeque<Entity>,
    storage: Entity,
) {
    let Some(mut storage_mut) = world.get_entity_mut(storage) else {
        return;
    };

    let Some(ResponseStorage(Some(response))) = storage_mut.take::<ResponseStorage<Response>>() else {
        if !storage_mut.contains::<Detached>() {
            let Some(termination) = storage_mut.get::<Termination<Response>>() else {
                cancel(world, storage);
                return;
            };

            termination.0.expectation().
        }
    };
}
