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
    prelude::{Component, Entity, Bundle},
    tasks::AsyncComputeTaskPool,
};

use std::future::Future;

use crate::{
    BlockingMap, AsyncMap, Operation, ChannelQueue, Channel,
    SingleTargetStorage, StreamPack, Input, ManageInput, OperationCleanup,
    CallBlockingMap, CallAsyncMap, SingleInputStorage, OperationResult,
    OrBroken, OperationSetup, OperationRequest, OperateTask, ActiveTasksStorage,
    OperationReachability, ReachabilityResult, InputBundle,
};

#[derive(Bundle)]
pub(crate) struct OperateBlockingMap<F, Request, Response, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    storage: BlockingMapStorage<F>,
    target: SingleTargetStorage,
    #[bundle(ignore)]
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<F, Request, Response, Streams> OperateBlockingMap<F, Request, Response, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    pub(crate) fn new(target: Entity, f: F) -> Self {
        Self {
            storage: BlockingMapStorage { f: Some(f) },
            target: SingleTargetStorage::new(target),
            _ignore: Default::default(),
        }
    }
}

#[derive(Component)]
struct BlockingMapStorage<F> {
    f: Option<F>,
}

impl<F, Request, Response, Streams> Operation for OperateBlockingMap<F, Request, Response, Streams>
where
    F: CallBlockingMap<Request, Response, Streams> + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target.0).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            self,
            InputBundle::<Request>::new(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let streams = Streams::make_buffer(source, world);
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let Input { session, data: request } = source_mut.take_input::<Request>()?;
        let mut map = source_mut.get_mut::<BlockingMapStorage<F>>().or_broken()?;
        let mut f = map.f.take().or_broken()?;

        let response = f.call(BlockingMap { request, streams: streams.clone(), source, session });
        map.f = Some(f);

        Streams::process_buffer(streams, source, session, world, roster)?;

        world.get_entity_mut(target).or_broken()?.give_input(session, response, roster)?;
        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<Request>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<Request>()? {
            return Ok(true);
        }
        SingleInputStorage::is_reachable(&mut reachability)
    }
}

#[derive(Bundle)]
pub(crate) struct OperateAsyncMap<F, Request, Task, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: 'static + Send + Sync,
    Streams: StreamPack,
{
    storage: AsyncMapStorage<F>,
    target: SingleTargetStorage,
    #[bundle(ignore)]
    _ignore: std::marker::PhantomData<(Request, Task, Streams)>,
}

impl<F, Request, Task, Streams> OperateAsyncMap<F, Request, Task, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: 'static + Send + Sync,
    Streams: StreamPack,
{
    pub(crate) fn new(target: Entity, f: F) -> Self {
        Self {
            storage: AsyncMapStorage { f: Some(f) },
            target: SingleTargetStorage::new(target),
            _ignore: Default::default(),
        }
    }
}

#[derive(Component)]
struct AsyncMapStorage<F> {
    f: Option<F>,
}

impl<F, Request, Task, Streams> Operation for OperateAsyncMap<F, Request, Task, Streams>
where
    F: CallAsyncMap<Request, Task, Streams> + 'static + Send + Sync,
    Task: Future + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target.0).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            self,
            ActiveTasksStorage::default(),
            InputBundle::<Request>::new(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: request } = source_mut.take_input::<Request>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let mut f = source_mut.get_mut::<AsyncMapStorage<F>>().or_broken()?
            .f.take().or_broken()?;

        let channel = Channel::new(source, session, sender.clone());
        let streams = channel.for_streams::<Streams>(&world)?;

        let task = AsyncComputeTaskPool::get().spawn(f.call(AsyncMap {
            request, streams, channel, source, session,
        }));
        world.get_entity_mut(source).or_broken()?
            .get_mut::<AsyncMapStorage<F>>().or_broken()?
            .f = Some(f);

        let task_source = world.spawn(()).id();
        OperateTask::new(
            task_source, session, source, target, task, None, sender,
        ).add(world, roster);
        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<Request>()?;
        ActiveTasksStorage::cleanup(clean)
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<Request>()? {
            return Ok(true);
        }
        if ActiveTasksStorage::contains_session(&mut reachability)? {
            return Ok(true);
        }
        SingleInputStorage::is_reachable(&mut reachability)
    }
}
