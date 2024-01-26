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
    BlockingHandler, AsyncHandler, Channel, OperationRoster, Stream,
    private,
};

use bevy::{
    prelude::{World, Entity},
    ecs::system::{IntoSystem, BoxedSystem},
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
    pub fn new(sys: impl HandlerTrait<Request, Response, Streams> + 'static) -> Self {
        Self {
            inner: Arc::new(Mutex::new(
                InnerHandler {
                    queue: VecDeque::new(),
                    handler: Some(Box::new(sys)),
                }
            ))
        }
    }
}

struct InnerHandler<Request, Response, Streams> {
    queue: VecDeque<PendingHandleRequest>,
    handler: Option<Box<dyn HandlerTrait<Request, Response, Streams>>>,
}

pub struct HandleRequest<'a> {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
    pub(crate) world: &'a mut World,
    pub(crate) roster: &'a mut OperationRoster,
}

pub struct PendingHandleRequest {
    pub(crate) source: Entity,
    pub(crate) target: Entity,
}

pub trait HandlerTrait<Request, Response, Streams> {
    fn handle(&mut self, request: HandleRequest);
}

pub struct BlockingHandlerMarker<M>(std::marker::PhantomData<M>);

struct BlockingHandlerSystem<Request, Response> {
    system: BoxedSystem<BlockingHandler<Request>, Response>,
}

impl<Request, Response> HandlerTrait<Request, Response, ()> for BlockingHandlerSystem<Request, Response>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    fn handle(&mut self, request: HandleRequest) {

    }
}

pub struct AsyncHandlerMarker<M>(std::marker::PhantomData<M>);

struct AsyncHandlerSystem<Request, Task, Streams> {
    system: BoxedSystem<AsyncHandler<Request, Streams>, Task>,
}

impl<Request, Task, Streams> HandlerTrait<Request, Task::Output, Streams> for AsyncHandlerSystem<Request, Task, Streams>
where
    Task: Future,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    fn handle(&mut self, request: HandleRequest) {

    }
}

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
        })
    }
}

impl<Request, Response, M, Sys> private::Sealed<BlockingHandlerMarker<(Request, Response, M)>> for Sys { }


impl<Request, Task, Streams, M, Sys> AsHandler<AsyncHandlerMarker<(Request, Task, Streams, M)>> for Sys
where
    Sys: IntoSystem<AsyncHandler<Request, Streams>, Task, M>,
    Task: Future + 'static,
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
        })
    }
}

impl<Request, Task, Streams, M, Sys> private::Sealed<AsyncHandlerMarker<(Request, Task, Streams, M)>> for Sys { }



pub trait IntoBlockingHandler<M>: private::Sealed<M> {
    type Request;
    type Response;
    fn into_blocking_handler(self) -> Handler<Self::Request, Self::Response, ()>;
}

impl<Request, Response, M, Sys> IntoBlockingHandler<BlockingHandlerMarker<M>> for Sys
where
    Sys: IntoSystem<

pub trait IntoAsyncHandler<M>: private::Sealed<M> {
    type Request;
    type Response;
    type Streams;
    fn into_async_handler(self) -> Handler<Self::Request, Self::Response, Self::Streams>;
}
