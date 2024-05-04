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

use crate::{
    BlockingMap, AsyncMap, Operation, ChannelQueue, InnerChannel,
    SingleTargetStorage, Stream, Input, ManageInput, OperationCleanup,
    CallBlockingMap, CallAsyncMap, SingleSourceStorage, OperationResult,
    OrBroken, OperationSetup, OperationRequest, OperateTask, ActiveTasksStorage,
};

use bevy::{
    prelude::{Component, Entity, Bundle},
    tasks::AsyncComputeTaskPool,
};

use std::future::Future;

#[derive(Bundle)]
pub(crate) struct OperateBlockingMap<F, Request, Response>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    storage: BlockingMapStorage<F, Request, Response>,
    target: SingleTargetStorage,
}

impl<F, Request, Response> OperateBlockingMap<F, Request, Response>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    pub(crate) fn new(target: Entity, f: F) -> Self {
        Self {
            storage: BlockingMapStorage {
                f,
                _ignore: Default::default(),
            },
            target: SingleTargetStorage(target),
        }
    }
}

#[derive(Component)]
struct BlockingMapStorage<F, Request, Response> {
    f: F,
    _ignore: std::marker::PhantomData<(Request, Response)>,
}

impl<F, Request, Response> Operation for OperateBlockingMap<F, Request, Response>
where
    F: CallBlockingMap<Request, Response> + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target.0) {
            target_mut.insert(SingleSourceStorage(source));
        }
        world.entity_mut(source).insert(self);
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let Input { requester, data: request } = source_mut.take_input::<Request>()?;
        let map = source_mut.take::<BlockingMapStorage<F, Request, Response>>().or_broken()?.f;
        let mut target_mut = world.get_entity_mut(target).or_broken()?;

        let response = map.call(BlockingMap { request });
        target_mut.give_input(requester, response, roster)?;
        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<Request>()?;
        clean.notify_cleaned()
    }
}

#[derive(Bundle)]
pub(crate) struct OperateAsyncMap<F, Request, Task, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: 'static + Send + Sync,
    Streams: Stream,
{
    storage: AsyncMapStorage<F, Request, Task, Streams>,
    target: SingleTargetStorage,
}

impl<F, Request, Task, Streams> OperateAsyncMap<F, Request, Task, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: 'static + Send + Sync,
    Streams: Stream,
{
    pub(crate) fn new(target: Entity, f: F) -> Self {
        Self {
            storage: AsyncMapStorage {
                f,
                _ignore: Default::default(),
            },
            target: SingleTargetStorage(target),
        }
    }
}

#[derive(Component)]
struct AsyncMapStorage<F, Request, Task, Streams> {
    f: F,
    _ignore: std::marker::PhantomData<(Request, Task, Streams)>,
}

impl<F, Request, Task, Streams> Operation for OperateAsyncMap<F, Request, Task, Streams>
where
    F: CallAsyncMap<Request, Task, Streams> + 'static + Send + Sync,
    Task: Future + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        world.entity_mut(source).insert((
            self, ActiveTasksStorage::default(),
        ));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { requester, data: request } = source_mut.take_input::<Request>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let map = source_mut.take::<AsyncMapStorage<F, Request, Task, Streams>>().or_broken()?.f;

        let channel = InnerChannel::new(source, sender);
        let channel = channel.into_specific();

        let task = AsyncComputeTaskPool::get().spawn(map.call(AsyncMap { request, channel }));

        let task_source = world.spawn(()).id();
        OperateTask::new(requester, source, target, task, None)
            .setup(OperationSetup { source: task_source, world });
        roster.queue(task_source);
        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<Request>()?;
        ActiveTasksStorage::cleanup(clean)
    }
}
