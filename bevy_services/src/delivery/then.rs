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
    prelude::{Entity, Component, World},
    ecs::system::Command,
};
use std::collections::VecDeque;

use crate::{
    DispatchCommand, Target, ResponseStorage, UnusedTarget, Pending,
    RequestStorage, Dispatch, cancel
};

pub(crate) struct MakeThen<Input> {
    provider: Entity,
    service: Entity,
    _ignore: std::marker::PhantomData<Input>,
}

impl<Input> MakeThen<Input> {
    pub(crate) fn new(provider: Entity, service: Entity) -> Self {
        Self { provider, service, _ignore: Default::default() }
    }
}

impl<Input: 'static + Send + Sync> Command for MakeThen<Input> {
    fn apply(self, world: &mut World) {
        world
            .entity_mut(self.provider)
            .insert(Then{ service: self.service })
            .insert(Dispatch::new_then::<Input>());
    }
}

impl Dispatch {
    fn new_then<Input: 'static + Send + Sync>() -> Self {
        Dispatch(dispatch_then::<Input>)
    }
}

fn dispatch_then<Input: 'static + Send + Sync>(
    world: &mut World,
    cmd: DispatchCommand,
) {
    let Some(mut provider_mut) = world.get_entity_mut(cmd.provider) else {
        cancel(world, cmd.target);
        return;
    };

    provider_mut.remove::<UnusedTarget>();
    let Some(ResponseStorage(Some(response))) = provider_mut.take::<ResponseStorage<Input>>() else {
        provider_mut
            .insert(Target(cmd.target))
            .insert(Pending(pending_then::<Input>));
        return;
    };

    let then = provider_mut.take::<Then>().unwrap();
    provider_mut.despawn();
    let Some(mut target_mut) = world.get_entity_mut(cmd.target) else {
        cancel(world, cmd.target);
        return;
    };
    target_mut.insert(RequestStorage(Some(response)));
    let Some(service_ref) = world.get_entity(then.service) else {
        cancel(world, cmd.target);
        return;
    };
    let Some(service_dispatch) = service_ref.get::<Dispatch>() else {
        cancel(world, cmd.target);
        return;
    };

    (service_dispatch.0)(world, DispatchCommand::new(then.service, cmd.target));
}

fn pending_then<Input: 'static + Send + Sync>(
    world: &mut World,
    queue: &mut VecDeque<Entity>,
    provider: Entity,
) {
    let mut provider_mut = world.entity_mut(provider);
    let service = provider_mut.get::<Then>().unwrap().service;
    let target = provider_mut.get::<Target>().unwrap().0;
    let Some(ResponseStorage(Some(response))) = provider_mut.take::<ResponseStorage<Input>>() else {
        provider_mut.despawn();
        cancel(world, target);
        return;
    };
    provider_mut.despawn();
    let Some(mut target_mut) = world.get_entity_mut(target) else {
        return;
    };
    target_mut.insert(RequestStorage(Some(response)));
    let Some(service_ref) = world.get_entity(service) else {
        cancel(world, target);
        return;
    };
    let Some(service_dispatch) = service_ref.get::<Dispatch>() else {
        cancel(world, target);
        return;
    };

    (service_dispatch.0)(world, DispatchCommand::new(service, target));
    queue.push_back(target);
}

#[derive(Component)]
struct Then {
    service: Entity,
}
