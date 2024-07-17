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
    prelude::{Component, Entity, World, Resource, BuildWorldChildren},
    tasks::{Task as BevyTask, AsyncComputeTaskPool},
    ecs::system::Command,
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
    OperationRoster, Blocker, ManageInput, ChannelQueue, UnhandledErrors,
    OperationSetup, OperationRequest, OperationResult, Operation, AddOperation,
    OrBroken, OperationCleanup, ChannelItem, OperationError, Broken, ScopeStorage,
    OperationReachability, ReachabilityResult, emit_disposal, Disposal, StreamPack,
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
pub(crate) struct OperateTask<Response: 'static + Send + Sync, Streams: StreamPack> {
    source: Entity,
    session: Entity,
    node: Entity,
    target: Entity,
    task: Option<BevyTask<Response>>,
    blocker: Option<Blocker>,
    sender: CbSender<ChannelItem>,
    disposal: Option<Disposal>,
    being_cleaned: bool,
    finished_normally: bool,
    _ignore: std::marker::PhantomData<Streams>,
}

impl<Response: 'static + Send + Sync, Streams: StreamPack> OperateTask<Response, Streams> {
    pub(crate) fn new(
        source: Entity,
        session: Entity,
        node: Entity,
        target: Entity,
        task: BevyTask<Response>,
        blocker: Option<Blocker>,
        sender: CbSender<ChannelItem>,
    ) -> Self {
        Self {
            source,
            session,
            node,
            target,
            task: Some(task),
            blocker,
            sender,
            disposal: None,
            being_cleaned: false,
            finished_normally: false,
            _ignore: Default::default(),
        }
    }

    pub(crate) fn add(self, world: &mut World, roster: &mut OperationRoster) {
        let source = self.source;
        let scope = world.get::<ScopeStorage>(source).map(|s| s.get());
        let mut source_mut = world.entity_mut(source);
        source_mut.set_parent(self.node);
        if let Some(scope) = scope {
            source_mut.insert(ScopeStorage::new(scope));
        }

        AddOperation::new(None, source, self).apply(world);
        roster.queue(source);
    }
}

impl<Response, Streams> Drop for OperateTask<Response, Streams>
where
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn drop(&mut self) {
        if self.finished_normally {
            // The task finished normally so no special action needs to be taken
            return;
        }

        let source = self.source;
        let session = self.session;
        let node = self.node;
        let task = self.task.take();
        let unblock = self.blocker.take();
        let sender = self.sender.clone();
        let disposal = self.disposal.take();
        let being_cleaned = self.being_cleaned;

        AsyncComputeTaskPool::get().spawn(async move {
            let mut disposed = false;
            if let Some(task) = task {
                disposed = true;
                task.cancel().await;
            }
            sender.send(Box::new(move |world: &mut World, roster: &mut OperationRoster| {
                cleanup_task::<Response>(session, source, node, unblock, being_cleaned, world, roster);

                if disposed {
                    let disposal = disposal.unwrap_or_else(|| {
                        Disposal::task_despawned(source, node)
                    });
                    emit_disposal(node, session, disposal, world, roster);
                }
            }))
        }).detach();
    }
}

impl<Response, Streams> Operation for OperateTask<Response, Streams>
where
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        let wake_queue = world.get_resource_or_insert_with(|| WakeQueue::new());
        let waker = Arc::new(JobWaker {
            sender: wake_queue.sender.clone(),
            entity: source,
        });

        let mut source_mut = world.entity_mut(source);
        let node = self.node;
        let session = self.session;
        source_mut
            .insert((
                self,
                JobWakerStorage(waker),
                StopTask(stop_task::<Response, Streams>),
            ))
            .set_parent(node);

        let mut node_mut = world.get_entity_mut(node).or_broken()?;
        let mut tasks = node_mut.get_mut::<ActiveTasksStorage>().or_broken()?;
        tasks.list.push(ActiveTask { task_id: source, session });
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        // It's possible for a task to get into the roster after it has despawned
        // so we'll just exit early when that happens. However this should not
        // actually be possible because the OperationExecuteStorage must still be
        // accessible in order to be inside this function.
        let mut source_mut = world.get_entity_mut(source).or_not_ready()?;
        // If the task has been stopped / cancelled then OperateTask will have
        // been removed, even if it has not despawned yet.
        let mut operation = source_mut.get_mut::<OperateTask<Response, Streams>>().or_not_ready()?;
        if operation.being_cleaned {
            // The operation is being cleaned up, so the task will not be
            // available and there will be nothing for us to do here. We should
            // simply return immediately.
            return Ok(());
        }
        let mut task = operation.task.take().or_broken()?;
        let target = operation.target;
        let session = operation.session;
        let node = operation.node;
        let being_cleaned = operation.being_cleaned;
        // We take out unblock here just in case the entity gets despawned and/or
        // the OperateTask component gets dropped before we reach the end of the
        // function.
        let unblock = operation.blocker.take();

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
                // Task has finished. We will defer its input until after the
                // ChannelQueue has been processed so that any streams from this
                // task will be delivered before the final output.
                let r = world.entity_mut(target).defer_input(session, result, roster);
                world.get_mut::<OperateTask<Response, Streams>>(source).or_broken()?.finished_normally = true;
                cleanup_task::<Response>(session, source, node, unblock, being_cleaned, world, roster);

                if Streams::has_streams() {
                    if let Some(scope) = world.get::<ScopeStorage>(node) {
                        // When an async task with any number of streams >= 1 is
                        // finished, we should always do a disposal notification
                        // to force a reachability check. Normally there are
                        // specific events that prompt us to check reachability,
                        // but if a reachability test occurred while the async
                        // node was running and the reachability depends on a
                        // stream which may or may not have been emitted, then
                        // the reachability test may have concluded with a false
                        // positive, and it needs to be rechecked now that the
                        // async node has finished.
                        //
                        // TODO(@mxgrey): Make this more efficient, e.g. only
                        // trigger this disposal if we detected that a
                        // reachability test happened while this task was
                        // running.
                        roster.disposed(scope.get(), session);
                    }
                }

                r?;
            }
            Poll::Pending => {
                // Task is still running
                if let Some(mut operation) = world.get_mut::<OperateTask<Response, Streams>>(source) {
                    operation.task = Some(task);
                    operation.blocker = unblock;
                    world.entity_mut(source).insert(JobWakerStorage(waker));
                } else {
                    if unblock.is_some() {
                        // Somehow the task entity and/or the OperateTask
                        // component has dropped while the task information was
                        // outside of it. Since this task was blocking a service
                        // we should recreate the OperateTask object with
                        // everything filled in, and then drop it according.
                        let sender = world.get_resource_or_insert_with(
                            || ChannelQueue::default()
                        ).sender.clone();

                        let operation = OperateTask::<_, Streams>::new(
                            source, session, node, target, task, unblock, sender,
                        );

                        // Dropping this operation will trigger the task cancellation
                        drop(operation);
                    }
                }
            }
        }

        Ok(())
    }

    fn cleanup(clean: OperationCleanup) -> OperationResult {
        let session = clean.session;
        let source = clean.source;
        let mut source_mut = clean.world.get_entity_mut(source).or_broken()?;
        let mut operation = source_mut.get_mut::<OperateTask<Response, Streams>>().or_broken()?;
        operation.being_cleaned = true;
        let node = operation.node;
        let task = operation.task.take();
        let unblock = operation.blocker.take();
        let sender = operation.sender.clone();
        if let Some(task) = task {
            AsyncComputeTaskPool::get().spawn(async move {
                task.cancel().await;
                if let Err(err) = sender.send(Box::new(move |world: &mut World, roster: &mut OperationRoster| {
                    cleanup_task::<Response>(session, source, node, unblock, true, world, roster);
                })) {
                    eprintln!("Failed to send a command to cleanup a task: {err}");
                }
            }).detach();
        } else {
            cleanup_task::<Response>(session, source, node, unblock, true, clean.world, clean.roster);
        }

        Ok(())
    }

    fn is_reachable(reachability: OperationReachability) -> ReachabilityResult {
        let session = reachability.world
            .get_entity(reachability.source).or_broken()?
            .get::<OperateTask<Response, Streams>>().or_broken()?.session;
        Ok(session == reachability.session)
    }
}

fn cleanup_task<Response>(
    session: Entity,
    source: Entity,
    node: Entity,
    unblock: Option<Blocker>,
    being_cleaned: bool,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    if let Some(unblock) = unblock {
        roster.unblock(unblock);
    }

    if let Some(mut node_mut) = world.get_entity_mut(node) {
        if let Some(mut active_tasks) = node_mut.get_mut::<ActiveTasksStorage>() {
            let mut cleanup_ready = true;
            active_tasks.list.retain(|ActiveTask { task_id: id, session: r }| {
                if *id == source {
                    return false;
                }

                if *r == session {
                    // The node has another active task related to this
                    // session so its cleanup is not finished yet.
                    cleanup_ready = false;
                }
                true
            });

            if being_cleaned && cleanup_ready {
                // We are notifying about the cleanup on behalf of the node that
                // created this task, so we set initialize as source: node
                let mut cleanup = OperationCleanup {
                    source: node, session, world, roster
                };
                if let Err(OperationError::Broken(backtrace)) = cleanup.notify_cleaned() {
                    world.get_resource_or_insert_with(|| UnhandledErrors::default())
                        .broken
                        .push(Broken { node, backtrace });
                }
            }
        };
    };

    if world.get_entity(source).is_some() {
        world.despawn(source);
    }

    roster.purge(source);
}

#[derive(Component, Clone, Copy)]
pub(crate) struct StopTask(pub(crate) fn(OperationRequest, Disposal) -> OperationResult);

fn stop_task<Response: 'static + Send + Sync, Streams: StreamPack>(
    OperationRequest { source, world, .. }: OperationRequest,
    disposal: Disposal,
) -> OperationResult {
    let mut operation = world
        .get_entity_mut(source).or_broken()?
        .take::<OperateTask<Response, Streams>>().or_broken()?;

    operation.disposal = Some(disposal);
    drop(operation);
    Ok(())
}

#[derive(Component, Default)]
pub struct ActiveTasksStorage {
    pub list: SmallVec<[ActiveTask; 16]>,
}

pub struct ActiveTask {
    task_id: Entity,
    session: Entity,
}

impl ActiveTasksStorage {
    pub fn cleanup(mut cleaner: OperationCleanup) -> OperationResult {
        let source = cleaner.source;
        let source_ref = cleaner.world.get_entity(source).or_broken()?;
        let active_tasks = source_ref.get::<Self>().or_broken()?;
        let mut to_cleanup: SmallVec<[Entity; 16]> = SmallVec::new();
        let mut cleanup_ready = true;
        for ActiveTask { task_id: id, session } in &active_tasks.list {
            if *session == cleaner.session {
                to_cleanup.push(*id);
                cleanup_ready = false;
            }
        }

        for task_id in to_cleanup {
            cleaner = cleaner.for_node(task_id);
            cleaner.clean();
        }

        if cleanup_ready {
            cleaner.notify_cleaned()?;
        }

        Ok(())
    }

    pub fn contains_session(r: &OperationReachability) -> ReachabilityResult {
        let active_tasks = &r.world.get_entity(r.source).or_broken()?
            .get::<Self>().or_broken()?.list;

        Ok(active_tasks.iter().find(
            |task| task.session == r.session
        ).is_some())
    }
}
