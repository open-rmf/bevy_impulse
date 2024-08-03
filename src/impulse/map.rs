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

use bevy_ecs::prelude::{Entity, Component, Bundle};

use std::future::Future;

use crate::{
    Impulsive, OperationSetup, OperationRequest, SingleTargetStorage, StreamPack,
    InputBundle, OperationResult, OrBroken, Input, ManageInput, Sendish,
    ChannelQueue, BlockingMap, AsyncMap, Channel, OperateTask, ActiveTasksStorage,
    CallBlockingMapOnce, CallAsyncMapOnce, UnusedStreams, make_stream_buffer_from_world,
    async_execution::{spawn_task, task_cancel_sender}
};

/// The key difference between this and [`crate::OperateBlockingMap`] is that
/// this supports FnOnce since it's used for impulse chains which are not
/// reusable, whereas [`crate::OperateBlockingMap`] is used in workflows which
/// need to be reusable, so it can only support FnMut.
#[derive(Bundle)]
pub(crate) struct ImpulseBlockingMap<F, Request, Response, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    f: BlockingMapOnceStorage<F>,
    target: SingleTargetStorage,
    #[bundle(ignore)]
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<F, Request, Response, Streams> ImpulseBlockingMap<F, Request, Response, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    pub(crate) fn new(target: Entity, f: F) -> Self {
        Self {
            f: BlockingMapOnceStorage { f },
            target: SingleTargetStorage::new(target),
            _ignore: Default::default(),
        }
    }
}

#[derive(Component)]
struct BlockingMapOnceStorage<F> {
    f: F,
}

impl<F, Request, Response, Streams> Impulsive for ImpulseBlockingMap<F, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
    F: CallBlockingMapOnce<Request, Response, Streams> + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            self,
            InputBundle::<Request>::new(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let streams = make_stream_buffer_from_world::<Streams>(source, world)?;
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let Input { session, data: request } = source_mut.take_input::<Request>()?;
        let f = source_mut.take::<BlockingMapOnceStorage<F>>().or_broken()?.f;

        let response = f.call(BlockingMap { request, streams: streams.clone(), source, session });

        let mut unused_streams = UnusedStreams::new(source);
        Streams::process_buffer(
            streams, source, session, &mut unused_streams, world, roster
        )?;
        // Note: We do not need to emit a disposal for any unused streams since
        // this is only used for impulses, not workflows.

        world.get_entity_mut(target).or_broken()?.give_input(session, response, roster)?;
        Ok(())
    }
}

/// The key difference between this and [`crate::OperateAsyncMap`] is that
/// this supports FnOnce since it's used for impulse chains which are not
/// reusable, whereas [`crate::OperateAsyncMap`] is used in workflows which
/// need to be reusable, so it can only support FnMut.
pub(crate) struct ImpulseAsyncMap<F, Request, Task, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: 'static + Sendish,
    Streams: 'static + Send + Sync,
{
    f: AsyncMapOnceStorage<F>,
    target: SingleTargetStorage,
    _ignore: std::marker::PhantomData<fn(Request, Task, Streams)>,
}

impl<F, Request, Task, Streams> ImpulseAsyncMap<F, Request, Task, Streams>
where
    F: 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: 'static + Sendish,
    Streams: 'static + Send + Sync,
{
    pub(crate) fn new(target: Entity, f: F) -> Self {
        Self {
            f: AsyncMapOnceStorage { f },
            target: SingleTargetStorage::new(target),
            _ignore: Default::default(),
        }
    }
}

#[derive(Component)]
struct AsyncMapOnceStorage<F> {
    f: F,
}

impl<F, Request, Task, Streams> Impulsive for ImpulseAsyncMap<F, Request, Task, Streams>
where
    Request: 'static + Send + Sync,
    Task: Future + 'static + Sendish,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
    F: CallAsyncMapOnce<Request, Task, Streams> + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            self.f,
            self.target,
            InputBundle::<Request>::new(),
            ActiveTasksStorage::default(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest,
    ) -> OperationResult {
        let sender = world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: request } = source_mut.take_input::<Request>()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let f = source_mut.take::<AsyncMapOnceStorage<F>>().or_broken()?.f;

        let channel = Channel::new(source, session, sender.clone());
        let streams = channel.for_streams::<Streams>(&world)?;

        let task = spawn_task(f.call(AsyncMap {
            request, streams, channel, source, session,
        }), world);
        let cancel_sender = task_cancel_sender(world);

        let task_source = world.spawn(()).id();
        OperateTask::<_, Streams>::new(
            task_source, session, source, target, task, cancel_sender, None, sender,
        ).add(world, roster);
        Ok(())
    }
}
