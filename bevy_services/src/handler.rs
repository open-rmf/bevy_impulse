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
    OperationRoster, Stream, InputStorage, InputBundle, TaskBundle,
    private,
};

use bevy::{
    prelude::{World, Entity, In, Component},
    ecs::system::{IntoSystem, BoxedSystem},
    tasks::AsyncComputeTaskPool,
};

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    future::Future,
};

pub struct Handler<Request, Response, Streams = ()> {
    inner: Arc<Mutex<InnerHandler<Request, Response, Streams>>>,
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

struct InnerHandler<Request, Response, Streams> {
    queue: VecDeque<PendingHandleRequest>,
    handler: Option<Box<dyn HandlerTrait<Request, Response, Streams> + 'static + Send>>,
}

pub struct HandleRequest<'a> {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
    pub(crate) world: &'a mut World,
    pub(crate) roster: &'a mut OperationRoster,
}

impl<'a> HandleRequest<'a> {
    fn get_request<Request: 'static + Send + Sync>(&mut self) -> Option<Request> {
        let Some(mut source_mut) = self.world.get_entity_mut(self.source) else {
            self.roster.cancel(self.source);
            return None;
        };

        source_mut.take::<InputStorage<Request>>().map(|s| s.0)
    }

    fn give_response<Response: 'static + Send + Sync>(&mut self, response: Response) {
        let Some(mut target_mut) = self.world.get_entity_mut(self.target) else {
            self.roster.cancel(self.target);
            return;
        };

        target_mut.insert(InputBundle::new(response));
    }

    fn give_task<Task: Future + 'static + Send>(&mut self, task: Task)
    where
        Task::Output: Send + Sync,
    {
        let Some(mut source_mut) = self.world.get_entity_mut(self.source) else {
            self.roster.cancel(self.source);
            return;
        };

        let task = AsyncComputeTaskPool::get().spawn(task);
        source_mut.insert(TaskBundle::new(task));
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

pub trait HandlerTrait<Request, Response, Streams> {
    fn handle(&mut self, request: HandleRequest);
}

pub struct BlockingHandlerMarker<M>(std::marker::PhantomData<M>);

#[derive(Component)]
struct BlockingHandlerSystem<Request, Response> {
    system: BoxedSystem<BlockingHandler<Request>, Response>,
    initialized: bool,
}

impl<Request, Response> HandlerTrait<Request, Response, ()> for BlockingHandlerSystem<Request, Response>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    fn handle(&mut self, mut input: HandleRequest) {
        let Some(request) = input.get_request() else {
            return;
        };

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let response = self.system.run(BlockingHandler { request }, &mut input.world);
        self.system.apply_deferred(&mut input.world);

        input.give_response(response);
    }
}

pub struct AsyncHandlerMarker<M>(std::marker::PhantomData<M>);

#[derive(Component)]
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
    fn handle(&mut self, mut input: HandleRequest) {
        let Some(request) = input.get_request() else {
            return;
        };

        let channel = input.get_channel();

        if !self.initialized {
            self.system.initialize(&mut input.world);
        }

        let task = self.system.run(AsyncHandler { request, channel }, &mut input.world);
        self.system.apply_deferred(&mut input.world);

        input.give_task(task);
    }
}

#[derive(Component)]
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
