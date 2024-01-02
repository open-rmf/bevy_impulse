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

use bevy::{
    prelude::{Entity, World, Component},
    ecs::system::Command,
};
use arrayvec::ArrayVec;
use std::collections::VecDeque;

use crate::{
    DispatchCommand, Dispatch, UnusedTarget, InputStorage, InputBundle,
    cancel,
};

pub(crate) struct MakeFork<Response: 'static + Send + Sync + Clone> {
    provider: Entity,
    branches: [Entity; 2],
    _ignore: std::marker::PhantomData<Response>,
}

impl<Response: 'static + Send + Sync + Clone> MakeFork<Response> {
    pub(crate) fn new(provider: Entity, branches: [Entity; 2]) -> Self {
        Self { provider, branches, _ignore: Default::default() }
    }
}

impl<Response: 'static + Send + Sync + Clone> Command for MakeFork<Response> {
    fn apply(self, world: &mut World) {
        world
            .entity_mut(self.provider)
            .insert(Fork {
                targets: ArrayVec::from(self.branches)
            })
            .insert(Dispatch::new_fork::<Response>());
    }
}

impl Dispatch {
    fn new_fork<Response: 'static + Send + Sync + Clone>() -> Self {
        Dispatch(dispatch_fork::<Response>)
    }
}

fn dispatch_fork<Response: 'static + Send + Sync + Clone>(
    world: &mut World,
    cmd: DispatchCommand,
) {
    let Some(mut provider_mut) = world.get_entity_mut(cmd.provider) else {
        cancel(world, cmd.target);
        return;
    };

    provider_mut.remove::<UnusedTarget>();
    if let Some(response) = {
        if let Some(InputStorage(response)) = provider_mut.take::<InputStorage<Response>>() {
            if let Some(mut fork_mut) = provider_mut.get_mut::<Fork>() {
                fork_mut.targets.retain(|t| *t != cmd.target);
                if fork_mut.targets.is_empty() {
                    // We can safely despawn this fork entity because it has
                    // provided to all the targets that it's supposed to.
                    provider_mut.despawn();
                } else {
                    // If there is still another fork expecting the response to
                    // arrive, then we should clone the response and re-insert
                    // it into the storage.
                    provider_mut.insert(InputBundle::new(response.clone()));
                }
            }
            Some(response)
        } else {
            provider_mut.insert(Pending(pending_fork::<Response>));
            None
        }
    } {
        // We have a response that we can provide to the target immediately
        let Some(mut target_mut) = world.get_entity_mut(cmd.target) else {
            cancel(world, cmd.target);
            return;
        };
        target_mut.insert(InputBundle::new(response));
    }
}

fn pending_fork<Response: 'static + Send + Sync + Clone>(
    world: &mut World,
    queue: &mut VecDeque<Entity>,
    provider: Entity,
) {
    let Some(mut provider_mut) = world.get_entity_mut(provider) else {
        return
    };
    let Some(InputStorage(response)) = provider_mut.take::<InputStorage<Response>>() else {
        // Maybe panic here instead? Why wasn't the response available when the
        // pending was triggered?
        let fork = provider_mut.take::<Fork>().unwrap();
        provider_mut.despawn();
        for target in fork.targets {
            cancel(world, target);
        }
        return
    };

    let mut fork = provider_mut.take::<Fork>().unwrap();
    let mut response_storage = Some(response);
    // This somewhat complex loop helps to prevent us from making an unnecessary
    // clone of the response data.
    while let Some(next_target) = fork.targets.pop() {
        let response = if !fork.targets.is_empty() {
            response_storage.as_ref().unwrap().clone()
        } else {
            response_storage.take().unwrap()
        };
        let Some(mut target_mut) = world.get_entity_mut(next_target) else {
            continue
        };
        target_mut.insert(InputBundle::new(response));
        queue.push_back(next_target);
    }
}

#[derive(Component)]
struct Fork {
    targets: ArrayVec<Entity, 2>,
}
