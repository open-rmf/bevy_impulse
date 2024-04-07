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

use bevy::prelude::{Entity, World, QueryState, Added};

use smallvec::SmallVec;

use crate::{ChannelQueue, PollTask, WakeQueue, OperationRoster};

pub fn flush_services(
    world: &mut World,
    mut poll_task_query: QueryState<(Entity, &PollTask), Added<PollTask>>,
) {
    let async_receiver = world.get_resource_or_insert_with(|| ChannelQueue::new()).receiver.clone();
    while let Ok(mut command_queue) = async_receiver.try_recv() {
        command_queue.apply(world);
    }

    let pollables: SmallVec<[_; 8]> = poll_task_query
        .iter(world)
        .map(|(e, f)| (e, f.0))
        .collect();

    let mut roster = OperationRoster::new();
    for (e, f) in pollables {
        f(e, world, &mut roster);
    }

    let wakeables: SmallVec<[_; 8]> = world
        .get_resource_or_insert_with(|| WakeQueue::new())
        .receiver
        .try_iter()
        .collect();

    for e in wakeables {
        let Some(f) = world.get::<PollTask>(e).map(|x| x.0) else {
            roster.cancel(e);
            continue;
        };

        f(e, world, &mut roster);
    }

    while !roster.is_empty() {
        for e in roster.cancel.drain(..) {
            cancel_service(e, world);
        }

        for e in roster.dispose.drain(..) {
            world.despawn(e);
        }
    }
}

fn cancel_service(entity: Entity, world: &mut World) {

}
