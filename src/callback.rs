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
    BlockingCallback, AsyncCallback, Channel, ChannelQueue,
    OperationRoster, StreamPack, Input, Provider, ProvideOnce,
    AddOperation, OperateCallback, ManageInput, OperationError,
    OrBroken, OperateTask, UnusedStreams, ManageDisposal,
    make_stream_buffer_from_world, async_execution::spawn_task,
};

use bevy_ecs::{
    prelude::{World, Entity, In, Commands},
    system::{IntoSystem, BoxedSystem},
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
///
/// There are three ways to instantiate a callback:
///
/// ### [`.as_callback()`](AsCallback)
///
/// If you have a Bevy system with an input parameter of `In<`[`AsyncCallback`]`>`
/// or `In<`[`BlockingCallback`]`>` then you can convert it into a callback
/// object by applying `.as_callback()`.
///
/// ### [`.into_async_callback()`](IntoAsyncCallback)
///
/// If you have a Bevy system whose return type implements the [`Future`] trait,
/// it can be converted into an async callback object by applying
/// `.into_async_callback()` to it. The `Response` type of the callback will be
/// `Future::Output` rather than the return type of the system. The return value
/// will be polled in an async compute task pool.
///
/// ### [`.into_blocking_callback()`](IntoBlockingCallback)
///
/// Any Bevy system can be converted into a blocking callback by applying
/// `.into_blocking_callback()` to it. The `Request` type of the callback will
/// be whatever the input type of the system is (the `T` inside of `In<T>`). The
/// `Response` type of the callback will be whatever the return value of the
/// callback is.
///
/// A blocking callback is always an exclusive system, so it will block all
/// other systems from running until it is finished.
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
        unused_streams: UnusedStreams,
    ) -> Result<(), OperationError> {
        if !unused_streams.streams.is_empty() {
            self.world.get_entity_mut(self.source).or_broken()?
                .emit_disposal(session, unused_streams.into(), self.roster);
        }

        self.world
            .get_entity_mut(self.target).or_broken()?
            .give_input(session, response, self.roster)?;

        Ok(())
    }

    fn give_task<Task: Future + 'static + Send, Streams: StreamPack>(
        &mut self,
        session: Entity,
        task: Task,
    ) -> Result<(), OperationError>
    where
        Task::Output: Send + Sync,
    {
        let sender = self.world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let task = spawn_task(task);
        let task_id = self.world.spawn(()).id();
        OperateTask::<_, Streams>::new(task_id, session, self.source, self.target, task, None, sender)
            .add(self.world, self.roster);
        Ok(())
    }

    fn get_channel<Streams: StreamPack>(
        &mut self,
        session: Entity,
    ) -> Result<(Channel, Streams::Channel), OperationError> {
        let sender = self.world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let channel = Channel::new(self.source, session, sender);
        let streams = channel.for_streams::<Streams>(&self.world)?;
        Ok((channel, streams))
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

impl<Request, Response, Streams> CallbackTrait<Request, Response, Streams> for BlockingCallbackSystem<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn call(&mut self, mut input: CallbackRequest) -> Result<(), OperationError> {
        let Input { session, data: request } = input.get_request()?;

        if !self.initialized {
            self.system.initialize(&mut input.world);
            self.initialized = true;
        }

        let streams = make_stream_buffer_from_world::<Streams>(input.source, input.world)?;

        let response = self.system.run(BlockingCallback {
            request, streams: streams.clone(), source: input.source, session,
        }, input.world);
        self.system.apply_deferred(&mut input.world);

        let mut unused_streams = UnusedStreams::new(input.source);
        Streams::process_buffer(
            streams, input.source, session, &mut unused_streams, input.world, input.roster
        )?;

        input.give_response(session, response, unused_streams)
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

        let (channel, streams) = input.get_channel::<Streams>(session)?;

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let task = self.system.run(AsyncCallback {
            request, streams, channel, source: input.source, session,
        }, &mut input.world);
        self.system.apply_deferred(&mut input.world);

        input.give_task::<_, Streams>(session, task)
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

impl<Request, Response, Streams, M, Sys> AsCallback<BlockingCallbackMarker<(Request, Response, Streams, M)>> for Sys
where
    Sys: IntoSystem<BlockingCallback<Request, Streams>, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

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
            let streams = make_stream_buffer_from_world::<Streams>(input.source, input.world)?;
            let response = (self)(BlockingCallback {
                request, streams: streams.clone(), source: input.source, session,
            });

            let mut unused_streams = UnusedStreams::new(input.source);
            Streams::process_buffer(
                streams, input.source, session, &mut unused_streams, input.world, input.roster
            )?;
            input.give_response(session, response, unused_streams)
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
            let (channel, streams) = input.get_channel::<Streams>(session)?;
            let task = (self)(AsyncCallback {
                request, streams, channel, source: input.source, session,
            });
            input.give_task::<_, Streams>(session, task)
        };
        Callback::new(MapCallback { callback })
    }
}

/// Use this to convert any Bevy system into a blocking callback.
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

    fn connect(self, scope: Option<Entity>, source: Entity, target: Entity, commands: &mut Commands) {
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
