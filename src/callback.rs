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
    BlockingCallback, AsyncCallback, Channel, InnerChannel, ChannelQueue,
    OperationRoster, StreamPack, Input, Provider, ProvideOnce,
    AddOperation, OperateCallback, ManageInput, OperationError,
    OrBroken, OperateTask,
};

use bevy::{
    prelude::{World, Entity, In},
    ecs::system::{IntoSystem, BoxedSystem},
    tasks::AsyncComputeTaskPool,
};

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    future::Future,
};

/// A Callback is similar to a [`Service`](crate::Service) except it is not
/// associated with an [`Entity`]. Instead it can be passed around and shared as an
/// object. Cloning the Callback will produce a new reference to the same underlying
/// instance. If the Callback has any internal state (e.g. [`Local`](bevy::prelude::Local)
/// parameters, change trackers, or mutable captured variables), that internal state will
/// be shared among all its clones.
//
// TODO(@mxgrey): Explain the different ways to instantiate a Callback.
pub struct Callback<Request, Response, Streams = ()> {
    pub(crate) inner: Arc<Mutex<InnerCallback<Request, Response, Streams>>>,
}

impl<Request, Response, Streams> Clone for Callback<Request, Response, Streams> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<Request, Response, Streams> Callback<Request, Response, Streams> {
    pub fn new(callback: impl CallbackTrait<Request, Response, Streams> + 'static + Send) -> Self {
        Self {
            inner: Arc::new(Mutex::new(
                InnerCallback {
                    queue: VecDeque::new(),
                    callback: Some(Box::new(callback)),
                }
            ))
        }
    }
}

pub(crate) struct InnerCallback<Request, Response, Streams> {
    pub(crate) queue: VecDeque<PendingCallbackRequest>,
    pub(crate) callback: Option<Box<dyn CallbackTrait<Request, Response, Streams> + 'static + Send>>,
}

pub struct CallbackRequest<'a> {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
    pub(crate) world: &'a mut World,
    pub(crate) roster: &'a mut OperationRoster,
}

impl<'a> CallbackRequest<'a> {
    fn get_request<Request: 'static + Send + Sync>(
        &mut self
    ) -> Result<Input<Request>, OperationError> {
        self.world
            .get_entity_mut(self.source).or_broken()?
            .take_input()
    }

    fn give_response<Response: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        response: Response,
    ) -> Result<(), OperationError> {
        self.world
            .get_entity_mut(self.target).or_broken()?
            .give_input(session, response, self.roster)?;
        Ok(())
    }

    fn give_task<Task: Future + 'static + Send>(
        &mut self,
        session: Entity,
        task: Task,
    ) -> Result<(), OperationError>
    where
        Task::Output: Send + Sync,
    {
        let sender = self.world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let task = AsyncComputeTaskPool::get().spawn(task);
        let task_id = self.world.spawn(()).id();
        OperateTask::new(task_id, session, self.source, self.target, task, None, sender)
            .add(self.world, self.roster);
        Ok(())
    }

    fn get_channel<Streams: StreamPack>(
        &mut self,
        session: Entity,
    ) -> Result<Channel<Streams>, OperationError> {
        let sender = self.world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let channel = InnerChannel::new(self.source, session, sender);
        channel.into_specific(&self.world)
    }
}

pub struct PendingCallbackRequest {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
}

impl PendingCallbackRequest {
    pub(crate) fn activate<'a>(
        self,
        world: &'a mut World,
        roster: &'a mut OperationRoster,
    ) -> CallbackRequest<'a> {
        CallbackRequest {
            source: self.source,
            target: self.target,
            world,
            roster,
        }
    }
}

pub trait CallbackTrait<Request, Response, Streams> {
    fn call(&mut self, request: CallbackRequest) -> Result<(), OperationError>;
}

pub struct BlockingCallbackMarker<M>(std::marker::PhantomData<M>);

struct BlockingCallbackSystem<Request, Response, Streams: StreamPack> {
    system: BoxedSystem<BlockingCallback<Request, Streams>, Response>,
    initialized: bool,
}

impl<Request, Response, Streams> CallbackTrait<Request, Response, ()> for BlockingCallbackSystem<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn call(&mut self, mut input: CallbackRequest) -> Result<(), OperationError> {
        let Input { session, data: request } = input.get_request()?;

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let streams = Streams::make_buffer(input.source, input.world);

        let response = self.system.run(BlockingCallback {
            request, streams: streams.clone(), source: input.source, session,
        }, input.world);
        self.system.apply_deferred(&mut input.world);

        Streams::process_buffer(streams, input.source, session, input.world, input.roster)?;

        input.give_response(session, response)
    }
}

pub struct AsyncCallbackMarker<M>(std::marker::PhantomData<M>);

struct AsyncCallbackSystem<Request, Task, Streams: StreamPack> {
    system: BoxedSystem<AsyncCallback<Request, Streams>, Task>,
    initialized: bool,
}

impl<Request, Task, Streams> CallbackTrait<Request, Task::Output, Streams> for AsyncCallbackSystem<Request, Task, Streams>
where
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn call(&mut self, mut input: CallbackRequest) -> Result<(), OperationError> {
        let Input { session, data: request } = input.get_request()?;

        let channel = input.get_channel(session)?;

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let task = self.system.run(AsyncCallback {
            request, channel, source: input.source, session,
        }, &mut input.world);
        self.system.apply_deferred(&mut input.world);

        input.give_task(session, task)
    }
}

struct MapCallback<F: 'static + Send> {
    callback: F,
}

impl<Request, Response, Streams, F> CallbackTrait<Request, Response, Streams> for MapCallback<F>
where
    F: FnMut(CallbackRequest) -> Result<(), OperationError> + 'static + Send,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: 'static + Send + Sync,
{
    fn call(&mut self, request: CallbackRequest) -> Result<(), OperationError> {
        (self.callback)(request)
    }
}

pub struct BlockingMapCallbackMarker<M>(std::marker::PhantomData<M>);
pub struct AsyncMapCallbackMarker<M>(std::marker::PhantomData<M>);

pub trait AsCallback<M> {
    type Request;
    type Response;
    type Streams;
    fn as_callback(self) -> Callback<Self::Request, Self::Response, Self::Streams>;
}

impl<Request, Response, M, Sys> AsCallback<BlockingCallbackMarker<(Request, Response, M)>> for Sys
where
    Sys: IntoSystem<BlockingCallback<Request>, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();

    fn as_callback(self) -> Callback<Self::Request, Self::Response, Self::Streams> {
        Callback::new(BlockingCallbackSystem {
            system: Box::new(IntoSystem::into_system(self)),
            initialized: false,
        })
    }
}

impl<Request, Task, Streams, M, Sys> AsCallback<AsyncCallbackMarker<(Request, Task, Streams, M)>> for Sys
where
    Sys: IntoSystem<AsyncCallback<Request, Streams>, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;

    fn as_callback(self) -> Callback<Self::Request, Self::Response, Self::Streams> {
        Callback::new(AsyncCallbackSystem {
            system: Box::new(IntoSystem::into_system(self)),
            initialized: false,
        })
    }
}

impl<Request, Response, Streams, F> AsCallback<BlockingMapCallbackMarker<(Request, Response, Streams)>> for F
where
    F: FnMut(BlockingCallback<Request, Streams>) -> Response + 'static + Send,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

    fn as_callback(mut self) -> Callback<Self::Request, Self::Response, Self::Streams> {
        let callback = move |mut input: CallbackRequest| {
            let Input { session, data: request } = input.get_request::<Self::Request>()?;
            let streams = Streams::make_buffer(input.source, input.world);
            let response = (self)(BlockingCallback {
                request, streams: streams.clone(), source: input.source, session,
            });
            Streams::process_buffer(streams, input.source, session, input.world, input.roster)?;
            input.give_response(session, response)
        };
        Callback::new(MapCallback { callback })
    }
}

impl<Request, Task, Streams, F> AsCallback<AsyncMapCallbackMarker<(Request, Task, Streams)>> for F
where
    F: FnMut(AsyncCallback<Request, Streams>) -> Task + 'static + Send,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;

    fn as_callback(mut self) -> Callback<Self::Request, Self::Response, Self::Streams> {
        let callback = move |mut input: CallbackRequest| {
            let Input { session, data: request } = input.get_request::<Self::Request>()?;
            let channel = input.get_channel(session)?;
            let task = (self)(AsyncCallback {
                request, channel, source: input.source, session,
            });
            input.give_task(session, task)
        };
        Callback::new(MapCallback { callback })
    }
}

pub trait IntoBlockingCallback<M> {
    type Request;
    type Response;
    fn into_blocking_callback(self) -> Callback<Self::Request, Self::Response, ()>;
}

impl<Request, Response, M, Sys> IntoBlockingCallback<BlockingCallbackMarker<(Request, Response, M)>> for Sys
where
    Sys: IntoSystem<Request, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    fn into_blocking_callback(self) -> Callback<Self::Request, Self::Response, ()> {
        peel_blocking.pipe(self).as_callback()
    }
}

fn peel_blocking<Request>(In(BlockingCallback { request, .. }): In<BlockingCallback<Request>>) -> Request {
    request
}

impl<Request, Response, F> IntoBlockingCallback<BlockingMapCallbackMarker<(Request, Response)>> for F
where
    F: FnMut(Request) -> Response + 'static + Send,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    fn into_blocking_callback(mut self) -> Callback<Self::Request, Self::Response, ()> {
        let f = move |BlockingCallback { request, .. }| {
            (self)(request)
        };

        f.as_callback()
    }
}

pub trait IntoAsyncCallback<M> {
    type Request;
    type Response;
    fn into_async_callback(self) -> Callback<Self::Request, Self::Response, ()>;
}

impl<Request, Task, M, Sys> IntoAsyncCallback<AsyncCallbackMarker<(Request, Task, (), M)>> for Sys
where
    Sys: IntoSystem<Request, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Task::Output;
    fn into_async_callback(self) -> Callback<Self::Request, Self::Response, ()> {
        peel_async.pipe(self).as_callback()
    }
}

fn peel_async<Request>(In(AsyncCallback { request, .. }): In<AsyncCallback<Request, ()>>) -> Request {
    request
}

impl<Request, Task, F> IntoAsyncCallback<AsyncMapCallbackMarker<(Request, Task, ())>> for F
where
    F: FnMut(Request) -> Task + 'static + Send,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Task::Output;
    fn into_async_callback(mut self) -> Callback<Self::Request, Self::Response, ()> {
        let f = move |AsyncCallback { request, .. }| {
            (self)(request)
        };

        f.as_callback()
    }
}

impl<Request, Response, Streams> ProvideOnce for Callback<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

    fn connect(self, scope: Option<Entity>, source: Entity, target: Entity, commands: &mut bevy::prelude::Commands) {
        commands.add(AddOperation::new(scope, source, OperateCallback::new(self, target)));
    }
}

impl<Request, Response, Streams> Provider for Callback<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{

}
