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
    BlockingMap, AsyncMap, Operation,
    OperationRoster, OperationStatus, ChannelQueue, InnerChannel,
    TargetStorage, InputStorage, InputBundle, Stream, TaskBundle,
    CallBlockingMap, CallAsyncMap, SourceStorage,
};

use bevy::{
    prelude::{World, Component, Entity, Bundle},
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
    target: TargetStorage,
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
            target: TargetStorage(target),
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
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target.0) {
            target_mut.insert(SourceStorage(entity));
        }
        world.entity_mut(entity).insert(self);
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let target = source_mut.get::<TargetStorage>().ok_or(())?.0;
        let request = source_mut.take::<InputStorage<Request>>().ok_or(())?.0;
        let map = source_mut.take::<BlockingMapStorage<F, Request, Response>>().ok_or(())?.f;
        let mut target_mut = world.get_entity_mut(target).ok_or(())?;

        let response = map.call(BlockingMap { request });
        target_mut.insert(InputBundle::new(response));
        roster.queue(target);
        Ok(OperationStatus::Finished)
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
    target: TargetStorage,
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
            target: TargetStorage(target),
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
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut World,
    ) {
        world.entity_mut(entity).insert(self);
    }

    fn execute(
        source: Entity,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let target = source_mut.get::<TargetStorage>().ok_or(())?.0;
        let request = source_mut.take::<InputStorage<Request>>().ok_or(())?.0;
        let map = source_mut.take::<AsyncMapStorage<F, Request, Task, Streams>>().ok_or(())?.f;

        let channel = InnerChannel::new(source, sender);
        let channel = channel.into_specific();

        let task = AsyncComputeTaskPool::get().spawn(map.call(AsyncMap { request, channel }));
        source_mut.insert(TaskBundle::new(task));
        Ok(OperationStatus::Queued { provider: source })
    }
}
