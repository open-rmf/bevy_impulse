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

use bevy::{
    prelude::{Component, Entity, World, Resource, Bundle},
    tasks::Task as BevyTask,
};

use std::{
    task::Poll,
    future::Future,
    pin::Pin,
    task::Context,
    sync::Arc,
};

use futures::task::{waker_ref, ArcWake};

use crossbeam::channel::{unbounded, Sender as CbSender, Receiver as CbReceiver};

use crate::{
    SingleTargetStorage, OperationRoster, Blocker, ManageInput,
    OperationSetup, OperationRequest, OperationResult, OperationStatus, Operation,
    OrBroken, BlockerStorage,
};

struct JobWaker {
    sender: CbSender<Entity>,
    entity: Entity,
}

impl ArcWake for JobWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.sender.send(arc_self.entity).ok();
    }
}

#[derive(Component)]
struct JobWakerStorage(Arc<JobWaker>);

#[derive(Resource)]
pub(crate) struct WakeQueue {
    sender: CbSender<Entity>,
    pub(crate) receiver: CbReceiver<Entity>,
}

impl WakeQueue {
    pub(crate) fn new() -> WakeQueue {
        let (sender, receiver) = unbounded();
        WakeQueue { sender, receiver }
    }
}

#[derive(Component)]
struct TaskStorage<Response>(BevyTask<Response>);

#[derive(Component)]
struct TaskRequesterStorage(Entity);

#[derive(Component)]
pub(crate) struct PollTask(pub(crate) fn(Entity, &mut World, &mut OperationRoster));

#[derive(Bundle)]
pub(crate) struct OperateTask<Response: 'static + Send + Sync> {
    requester: TaskRequesterStorage,
    target: SingleTargetStorage,
    task: TaskStorage<Response>,
    blocker: BlockerStorage,
}

impl<Response: 'static + Send + Sync> OperateTask<Response> {
    pub(crate) fn new(
        requester: Entity,
        target: Entity,
        task: BevyTask<Response>,
        blocker: Option<Blocker>,
    ) -> Self {
        Self {
            requester: TaskRequesterStorage(requester),
            target: SingleTargetStorage(target),
            task: TaskStorage(task),
            blocker: BlockerStorage(blocker),
        }
    }
}

impl<Response: 'static + Send + Sync> Operation for OperateTask<Response> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        let wake_queue = world.get_resource_or_insert_with(|| WakeQueue::new());
        let waker = Arc::new(JobWaker {
            sender: wake_queue.sender.clone(),
            entity: source,
        });

        world.entity_mut(source).insert((self, JobWakerStorage(waker)));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let mut task = source_mut.take::<TaskStorage<Response>>().or_broken()?.0;

        let waker = if let Some(waker) = source_mut.take::<JobWakerStorage>() {
            waker.0.clone()
        } else {
            let wake_queue = world.get_resource_or_insert_with(|| WakeQueue::new());
            let waker = Arc::new(JobWaker {
                sender: wake_queue.sender.clone(),
                entity: source,
            });
            waker
        };

        match Pin::new(&mut task).poll(
            &mut Context::from_waker(&waker_ref(&waker))
        ) {
            Poll::Ready(result) => {
                // Task has finished
                let mut source_mut = world.entity_mut(source);
                let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
                let requester = source_mut.get::<TaskRequesterStorage>().or_broken()?.0;
                let unblock = source_mut.take::<BlockerStorage>().or_broken()?;
                if let Some(unblock) = unblock.0 {
                    roster.unblock(unblock);
                }

                world.entity_mut(target).give_input(requester, result, roster);
                world.despawn(source);
                return Ok(OperationStatus::Finished);
            }
            Poll::Pending => {
                // Task is still running
                world.entity_mut(source).insert((
                    TaskStorage(task),
                    JobWakerStorage(waker),
                ));
                return Ok(OperationStatus::Unfinished);
            }
        }
    }
}
