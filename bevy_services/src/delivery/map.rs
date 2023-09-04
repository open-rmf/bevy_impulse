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
use std::collections::VecDeque;

use crate::{
    UnusedTarget, Target, Dispatch, DispatchCommand, ResponseStorage, Pending,
    cancel,
};

pub(crate) struct MakeMap<Response, U> {
    provider: Entity,
    f: Box<dyn FnOnce(Response) -> U + Send + Sync>,
}

impl<Response, U> MakeMap<Response, U> {
    pub(crate) fn new(
        provider: Entity,
        f: Box<dyn FnOnce(Response) -> U + Send + Sync>,
    ) -> Self {
        MakeMap { provider, f }
    }
}

impl<Response: 'static + Send + Sync, U: 'static + Send + Sync> Command for MakeMap<Response, U> {
    fn apply(self, world: &mut World) {
        world
            .entity_mut(self.provider)
            .insert(Map(self.f))
            .insert(Dispatch::new_map::<Response, U>());
    }
}

impl Dispatch {
    fn new_map<Response: 'static + Send + Sync, U: 'static + Send + Sync>() -> Self {
        Dispatch(dispatch_map::<Response, U>)
    }
}

fn dispatch_map<Response: 'static + Send + Sync, U: 'static + Send + Sync>(
    world: &mut World,
    cmd: DispatchCommand,
) {
    let Some(mut provider_mut) = world.get_entity_mut(cmd.provider) else {
        cancel(world, cmd.target);
        return;
    };

    provider_mut.remove::<UnusedTarget>();
    let Some(ResponseStorage(Some(response))) = provider_mut.take::<ResponseStorage<Response>>() else {
        provider_mut
            .insert(Target(cmd.target))
            .insert(Pending(pending_map::<Response, U>));
        return;
    };

    let map = provider_mut.take::<Map<Response, U>>().unwrap();
    provider_mut.despawn();
    let u = (map.0)(response);
    let Some(mut target_mut) = world.get_entity_mut(cmd.target) else {
        cancel(world, cmd.target);
        return;
    };

    target_mut.insert(ResponseStorage(Some(u)));
}

fn pending_map<Response: 'static + Send + Sync, U: 'static + Send + Sync>(
    world: &mut World,
    queue: &mut VecDeque<Entity>,
    provider: Entity,
) {
    let mut provider_mut = world.entity_mut(provider);
    let map = provider_mut.take::<Map<Response, U>>().unwrap();
    let target = provider_mut.get::<Target>().unwrap().0;
    let Some(ResponseStorage(Some(response))) = provider_mut.take::<ResponseStorage<Response>>() else {
        cancel(world, target);
        return;
    };
    let u = (map.0)(response);

    let Some(mut target_mut) = world.get_entity_mut(target) else {
        return
    };
    target_mut.insert(ResponseStorage(Some(u)));
    queue.push_back(target);
}

#[derive(Component)]
struct Map<Response, U>(Box<dyn FnOnce(Response) -> U + Send + Sync>);
