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
    OperationRoster, Stream, Input, TaskBundle, Provider,
    PerformOperation, OperateHandler, Cancel, ManageInput, OperationError,
    OrBroken,
    private,
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
        requester: Entity,
        response: Response,
    ) -> Result<(), OperationError> {
        self.world
            .get_entity_mut(self.target).or_broken()?
            .give_input(requester, response, self.roster)?;
        Ok(())
    }

    fn give_task<Task: Future + 'static + Send>(
        &mut self,
        task: Task
    ) -> Result<(), OperationError>
    where
        Task::Output: Send + Sync,
    {
        let mut source_mut = self.world.get_entity_mut(self.source).or_broken()?;

        let task = AsyncComputeTaskPool::get().spawn(task);
        /*  */
        source_mut.insert(TaskBundle::new(task));
        self.roster.poll(self.source);
        Ok(())
    }

    fn get_channel<Streams: Stream>(&mut self) -> Channel<Streams> {
        let sender = self.world.get_resource_or_insert_with(|| ChannelQueue::new()).sender.clone();
        let channel = InnerChannel::new(self.source, sender);
        channel.into_specific()
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

struct BlockingHandlerSystem<Request, Response> {
    system: BoxedSystem<BlockingHandler<Request>, Response>,
    initialized: bool,
}

impl<Request, Response> HandlerTrait<Request, Response, ()> for BlockingHandlerSystem<Request, Response>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    fn handle(&mut self, mut input: HandleRequest) -> Result<(), OperationError> {
        let Input { requester, data: request } = input.get_request()?;

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let response = self.system.run(BlockingHandler { request }, &mut input.world);
        self.system.apply_deferred(&mut input.world);

        input.give_response(requester, response);
        Ok(())
    }
}

pub struct AsyncHandlerMarker<M>(std::marker::PhantomData<M>);

struct AsyncHandlerSystem<Request, Task, Streams> {
    system: BoxedSystem<AsyncHandler<Request, Streams>, Task>,
    initialized: bool,
}

impl<Request, Task, Streams> HandlerTrait<Request, Task::Output, Streams> for AsyncHandlerSystem<Request, Task, Streams>
where
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    fn handle(&mut self, mut input: HandleRequest) -> Result<(), OperationError> {
        let Input { requester, data: request } = input.get_request()?;

        let channel = input.get_channel();

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let task = self.system.run(AsyncHandler { request, channel }, &mut input.world);
        self.system.apply_deferred(&mut input.world);

        input.give_task(task)
    }
}

struct CallbackHandler<F: 'static + Send> {
    callback: F,
}

impl<Request, Response, Streams, F> HandlerTrait<Request, Response, Streams> for CallbackHandler<F>
where
    F: FnMut(HandleRequest) + 'static + Send,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: 'static + Send + Sync,
{
    fn handle(&mut self, request: HandleRequest) {
        (self.callback)(request);
    }
}

pub struct BlockingCallbackMarker<M>(std::marker::PhantomData<M>);
pub struct AsyncCallbackMarker<M>(std::marker::PhantomData<M>);

pub trait AsHandler<M>: private::Sealed<M> {
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

impl<Request, Response, M, Sys> private::Sealed<BlockingHandlerMarker<(Request, Response, M)>> for Sys { }


impl<Request, Task, Streams, M, Sys> AsHandler<AsyncHandlerMarker<(Request, Task, Streams, M)>> for Sys
where
    Sys: IntoSystem<AsyncHandler<Request, Streams>, Task, M>,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
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

impl<Request, Task, Streams, M, Sys> private::Sealed<AsyncHandlerMarker<(Request, Task, Streams, M)>> for Sys { }


impl<Request, Response, F> AsHandler<BlockingCallbackMarker<(Request, Response)>> for F
where
    F: FnMut(BlockingHandler<Request>) -> Response + 'static + Send,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();

    fn as_handler(mut self) -> Handler<Self::Request, Self::Response, Self::Streams> {
        let callback = move |mut input: HandleRequest| {
            let Some(request) = input.get_request::<Self::Request>() else {
                return;
            };
            let response = (self)(BlockingHandler { request });
            input.give_response(response);
        };
        Handler::new(CallbackHandler { callback })
    }
}

impl<Request, Response, F> private::Sealed<BlockingCallbackMarker<(Request, Response)>> for F { }

impl<Request, Task, Streams, F> AsHandler<AsyncCallbackMarker<(Request, Task, Streams)>> for F
where
    F: FnMut(AsyncHandler<Request, Streams>) -> Task + 'static + Send,
    Task: Future + 'static + Send,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;

    fn as_handler(mut self) -> Handler<Self::Request, Self::Response, Self::Streams> {
        let callback = move |mut input: HandleRequest| {
            let Some(request) = input.get_request::<Self::Request>() else {
                return;
            };
            let channel = input.get_channel();
            let task = (self)(AsyncHandler { request, channel });
            input.give_task(task);
        };
        Handler::new(CallbackHandler { callback })
    }
}

impl<Request, Task, Streams, F> private::Sealed<AsyncCallbackMarker<(Request, Task, Streams)>> for F { }

pub trait IntoBlockingHandler<M>: private::Sealed<M> {
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

fn peel_blocking<Request>(In(BlockingHandler { request }): In<BlockingHandler<Request>>) -> Request {
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
        let f = move |BlockingHandler { request }| {
            (self)(request)
        };

        f.as_handler()
    }
}

pub trait IntoAsyncHandler<M>: private::Sealed<M> {
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

impl<Request, Response, Streams> Provider for Handler<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: Stream,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

    fn provide(self, source: Entity, target: Entity, commands: &mut bevy::prelude::Commands) {
        commands.add(PerformOperation::new(source, OperateHandler::new(self, target)));
    }
}
