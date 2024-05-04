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
    tasks::{Task as BevyTask, AsyncComputeTaskPool},
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

use smallvec::SmallVec;

use crate::{
    SingleTargetStorage, OperationRoster, Blocker, ManageInput, Scope,
    OperationSetup, OperationRequest, OperationResult, Operation, Cleanup,
    OrBroken, BlockerStorage, OperationCleanup, ChannelQueue, cleanup_operation,
    OperationReachability, ReachabilityResult,
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
struct TaskOwnerStorage(Entity);

#[derive(Component)]
pub(crate) struct PollTask(pub(crate) fn(Entity, &mut World, &mut OperationRoster));

#[derive(Bundle)]
pub(crate) struct OperateTask<Response: 'static + Send + Sync> {
    requester: TaskRequesterStorage,
    owner: TaskOwnerStorage,
    target: SingleTargetStorage,
    task: TaskStorage<Response>,
    blocker: BlockerStorage,
}

impl<Response: 'static + Send + Sync> OperateTask<Response> {
    pub(crate) fn new(
        requester: Entity,
        owner: Entity,
        target: Entity,
        task: BevyTask<Response>,
        blocker: Option<Blocker>,
    ) -> Self {
        Self {
            requester: TaskRequesterStorage(requester),
            owner: TaskOwnerStorage(owner),
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

        let mut source_mut = world.entity_mut(source);
        source_mut.insert((self, JobWakerStorage(waker)));

        let Some(mut owner_mut) = world.get_entity_mut(self.owner.0) else {
            return;
        };
        let Some(mut tasks) = owner_mut.get_mut::<ActiveTasksStorage>() else {
            return;
        };
        tasks.list.push(ActiveTask { id: source, requester: self.requester.0 });
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
            }
            Poll::Pending => {
                // Task is still running
                world.entity_mut(source).insert((
                    TaskStorage(task),
                    JobWakerStorage(waker),
                ));
            }
        }

        Ok(())
    }

    fn cleanup(clean: OperationCleanup) -> OperationResult {
        let requester = clean.requester;
        let source = clean.source;
        let mut source_mut = clean.world.get_entity_mut(source).or_broken()?;
        let owner = source_mut.get::<TaskOwnerStorage>().or_broken()?.0;
        let task = source_mut.take::<TaskStorage<Response>>().or_broken()?.0;
        let sender = clean.world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        AsyncComputeTaskPool::get().spawn(async move {
            task.cancel().await;
            sender.send(Box::new(move |world: &mut World, roster: &mut OperationRoster| {
                let Some(mut owner_mut) = world.get_entity_mut(owner) else {
                    world.despawn(source);
                    return;
                };

                let Some(mut active_tasks) = owner_mut.get_mut::<ActiveTasksStorage>() else {
                    world.despawn(source);
                    return;
                };

                let mut cleanup_ready = true;
                active_tasks.list.retain(|ActiveTask { id, requester: r }| {
                    if *id == source {
                        return false;
                    }

                    if *r == requester {
                        // The owner has another active task related to this
                        // requester so its cleanup is not finished yet.
                        cleanup_ready = false;
                    }
                    true
                });

                if cleanup_ready {
                    OperationCleanup { source: owner, requester, world, roster }
                        .notify_cleaned();
                }

                world.despawn(source);
            }));
        }).detach();


        Ok(())
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        let requester = reachability.world
            .get_entity(reachability.source).or_broken()?
            .get::<TaskRequesterStorage>().or_broken()?.0;
        Ok(requester == reachability.requester)
    }
}

#[derive(Component, Default)]
pub struct ActiveTasksStorage {
    pub list: SmallVec<[ActiveTask; 16]>,
}

pub struct ActiveTask {
    id: Entity,
    requester: Entity,
}

impl ActiveTasksStorage {
    pub fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        let source_ref = clean.world.get_entity(clean.source).or_broken()?;
        let active_tasks = source_ref.get::<Self>().or_broken()?;
        let mut to_cleanup: SmallVec<[Entity; 16]> = SmallVec::new();
        let mut cleanup_ready = true;
        for ActiveTask { id, requester } in &active_tasks.list {
            if *requester == clean.requester {
                to_cleanup.push(*id);
                cleanup_ready = false;
            }
        }

        for task_id in to_cleanup {
            cleanup_operation(OperationCleanup {
                source: task_id,
                requester: clean.requester,
                world: clean.world,
                roster: clean.roster
            });
        }

        if cleanup_ready {
            clean.notify_cleaned();
        }

        Ok(())
    }

    pub fn contains_requester(r: OperationReachability) -> ReachabilityResult {
        let active_tasks = &r.world.get_entity(r.source).or_broken()?
            .get::<Self>().or_broken()?.list;

        Ok(active_tasks.iter().find(
            |task| task.requester == r.requester
        ).is_some())
    }
}
