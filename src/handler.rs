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
    BlockingHandler, AsyncHandler, Channel, InnerChannel, ChannelQueue,
    OperationRoster, StreamPack, Input, Provider, ProvideOnce, UnhandledErrors,
    AddOperation, OperateHandler, ManageInput, OperationError, SetupFailure,
    OrBroken, OperateTask, Operation, OperationSetup,
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

/// A handler is similar to a [`Service`](crate::Service) except it is not
/// associated with an [`Entity`]. Instead it can be passed around and shared as an
/// object. Cloning the Handler will produce a new reference to the same underlying
/// instance. If the handler has any internal state (e.g. [`Local`](bevy::prelude::Local)
/// parameters, change trackers, or mutable captured variables), that internal state will
/// be shared among all its clones.
///
/// TODO(@mxgrey): Explain the different ways to instantiate a Handler.
pub struct Handler<Request, Response, Streams = ()> {
    pub(crate) inner: Arc<Mutex<InnerHandler<Request, Response, Streams>>>,
}

impl<Request, Response, Streams> Clone for Handler<Request, Response, Streams> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<Request, Response, Streams> Handler<Request, Response, Streams> {
    pub fn new(handler: impl HandlerTrait<Request, Response, Streams> + 'static + Send) -> Self {
        Self {
            inner: Arc::new(Mutex::new(
                InnerHandler {
                    queue: VecDeque::new(),
                    handler: Some(Box::new(handler)),
                }
            ))
        }
    }
}

pub(crate) struct InnerHandler<Request, Response, Streams> {
    pub(crate) queue: VecDeque<PendingHandleRequest>,
    pub(crate) handler: Option<Box<dyn HandlerTrait<Request, Response, Streams> + 'static + Send>>,
}

pub struct HandleRequest<'a> {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
    pub(crate) world: &'a mut World,
    pub(crate) roster: &'a mut OperationRoster,
}

impl<'a> HandleRequest<'a> {
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
        let operation = OperateTask::new(task_id, session, self.source, self.target, task, None, sender);
        let setup = OperationSetup { source: task_id, world: self.world };
        if let Err(error) = operation.setup(setup) {
            self.world.get_resource_or_insert_with(|| UnhandledErrors::default())
                .setup
                .push(SetupFailure { broken_node: self.source, error });
        }
        self.roster.queue(task_id);
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

pub struct PendingHandleRequest {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
}

impl PendingHandleRequest {
    pub(crate) fn activate<'a>(
        self,
        world: &'a mut World,
        roster: &'a mut OperationRoster,
    ) -> HandleRequest<'a> {
        HandleRequest {
            source: self.source,
            target: self.target,
            world,
            roster,
        }
    }
}

pub trait HandlerTrait<Request, Response, Streams> {
    fn handle(&mut self, request: HandleRequest) -> Result<(), OperationError>;
}

pub struct BlockingHandlerMarker<M>(std::marker::PhantomData<M>);

struct BlockingHandlerSystem<Request, Response, Streams: StreamPack> {
    system: BoxedSystem<BlockingHandler<Request, Streams>, Response>,
    initialized: bool,
}

impl<Request, Response, Streams> HandlerTrait<Request, Response, ()> for BlockingHandlerSystem<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn handle(&mut self, mut input: HandleRequest) -> Result<(), OperationError> {
        let Input { session, data: request } = input.get_request()?;

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let streams = Streams::make_buffer(input.source, input.world);

        let response = self.system.run(BlockingHandler { request, streams: streams.clone() }, input.world);
        self.system.apply_deferred(&mut input.world);

        Streams::process_buffer(streams, input.source, session, input.world, input.roster)?;

        input.give_response(session, response)
    }
}

pub struct AsyncHandlerMarker<M>(std::marker::PhantomData<M>);

struct AsyncHandlerSystem<Request, Task, Streams: StreamPack> {
    system: BoxedSystem<AsyncHandler<Request, Streams>, Task>,
    initialized: bool,
}

impl<Request, Task, Streams> HandlerTrait<Request, Task::Output, Streams> for AsyncHandlerSystem<Request, Task, Streams>
where
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn handle(&mut self, mut input: HandleRequest) -> Result<(), OperationError> {
        let Input { session, data: request } = input.get_request()?;

        let channel = input.get_channel(session)?;

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let task = self.system.run(AsyncHandler { request, channel }, &mut input.world);
        self.system.apply_deferred(&mut input.world);

        input.give_task(session, task)
    }
}

struct CallbackHandler<F: 'static + Send> {
    callback: F,
}

impl<Request, Response, Streams, F> HandlerTrait<Request, Response, Streams> for CallbackHandler<F>
where
    F: FnMut(HandleRequest) -> Result<(), OperationError> + 'static + Send,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: 'static + Send + Sync,
{
    fn handle(&mut self, request: HandleRequest) -> Result<(), OperationError> {
        (self.callback)(request)
    }
}

pub struct BlockingCallbackMarker<M>(std::marker::PhantomData<M>);
pub struct AsyncCallbackMarker<M>(std::marker::PhantomData<M>);

pub trait AsHandler<M> {
    type Request;
    type Response;
    type Streams;
    fn as_handler(self) -> Handler<Self::Request, Self::Response, Self::Streams>;
}

impl<Request, Response, M, Sys> AsHandler<BlockingHandlerMarker<(Request, Response, M)>> for Sys
where
    Sys: IntoSystem<BlockingHandler<Request>, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();

    fn as_handler(self) -> Handler<Self::Request, Self::Response, Self::Streams> {
        Handler::new(BlockingHandlerSystem {
            system: Box::new(IntoSystem::into_system(self)),
            initialized: false,
        })
    }
}

impl<Request, Task, Streams, M, Sys> AsHandler<AsyncHandlerMarker<(Request, Task, Streams, M)>> for Sys
where
    Sys: IntoSystem<AsyncHandler<Request, Streams>, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;

    fn as_handler(self) -> Handler<Self::Request, Self::Response, Self::Streams> {
        Handler::new(AsyncHandlerSystem {
            system: Box::new(IntoSystem::into_system(self)),
            initialized: false,
        })
    }
}

impl<Request, Response, Streams, F> AsHandler<BlockingCallbackMarker<(Request, Response, Streams)>> for F
where
    F: FnMut(BlockingHandler<Request, Streams>) -> Response + 'static + Send,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

    fn as_handler(mut self) -> Handler<Self::Request, Self::Response, Self::Streams> {
        let callback = move |mut input: HandleRequest| {
            let Input { session, data: request } = input.get_request::<Self::Request>()?;
            let streams = Streams::make_buffer(input.source, input.world);
            let response = (self)(BlockingHandler { request, streams: streams.clone() });
            Streams::process_buffer(streams, input.source, session, input.world, input.roster)?;
            input.give_response(session, response)
        };
        Handler::new(CallbackHandler { callback })
    }
}

impl<Request, Task, Streams, F> AsHandler<AsyncCallbackMarker<(Request, Task, Streams)>> for F
where
    F: FnMut(AsyncHandler<Request, Streams>) -> Task + 'static + Send,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;

    fn as_handler(mut self) -> Handler<Self::Request, Self::Response, Self::Streams> {
        let callback = move |mut input: HandleRequest| {
            let Input { session, data: request } = input.get_request::<Self::Request>()?;
            let channel = input.get_channel(session)?;
            let task = (self)(AsyncHandler { request, channel });
            input.give_task(session, task)
        };
        Handler::new(CallbackHandler { callback })
    }
}

pub trait IntoBlockingHandler<M> {
    type Request;
    type Response;
    fn into_blocking_handler(self) -> Handler<Self::Request, Self::Response, ()>;
}

impl<Request, Response, M, Sys> IntoBlockingHandler<BlockingHandlerMarker<(Request, Response, M)>> for Sys
where
    Sys: IntoSystem<Request, Response, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    fn into_blocking_handler(self) -> Handler<Self::Request, Self::Response, ()> {
        peel_blocking.pipe(self).as_handler()
    }
}

fn peel_blocking<Request>(In(BlockingHandler { request, .. }): In<BlockingHandler<Request>>) -> Request {
    request
}

impl<Request, Response, F> IntoBlockingHandler<BlockingCallbackMarker<(Request, Response)>> for F
where
    F: FnMut(Request) -> Response + 'static + Send,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    fn into_blocking_handler(mut self) -> Handler<Self::Request, Self::Response, ()> {
        let f = move |BlockingHandler { request, .. }| {
            (self)(request)
        };

        f.as_handler()
    }
}

pub trait IntoAsyncHandler<M> {
    type Request;
    type Response;
    fn into_async_handler(self) -> Handler<Self::Request, Self::Response, ()>;
}

impl<Request, Task, M, Sys> IntoAsyncHandler<AsyncHandlerMarker<(Request, Task, (), M)>> for Sys
where
    Sys: IntoSystem<Request, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Task::Output;
    fn into_async_handler(self) -> Handler<Self::Request, Self::Response, ()> {
        peel_async.pipe(self).as_handler()
    }
}

fn peel_async<Request>(In(AsyncHandler { request, .. }): In<AsyncHandler<Request, ()>>) -> Request {
    request
}

impl<Request, Task, F> IntoAsyncHandler<AsyncCallbackMarker<(Request, Task, ())>> for F
where
    F: FnMut(Request) -> Task + 'static + Send,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Task::Output;
    fn into_async_handler(mut self) -> Handler<Self::Request, Self::Response, ()> {
        let f = move |AsyncHandler { request, .. }| {
            (self)(request)
        };

        f.as_handler()
    }
}

impl<Request, Response, Streams> ProvideOnce for Handler<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

    fn connect(self, source: Entity, target: Entity, commands: &mut bevy::prelude::Commands) {
        commands.add(AddOperation::new(source, OperateHandler::new(self, target)));
    }
}

impl<Request, Response, Streams> Provider for Handler<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{

}
