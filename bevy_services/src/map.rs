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
    Provider, BlockingMap, AsyncMap, PerformOperation, OperateBlockingMap,
    OperateAsyncMap, Stream,
};

use bevy::prelude::{Entity, Commands};

use std::future::Future;

/// A newtype to indicate that the map definition is given directly by F.
pub struct MapDef<F>(F);

/// Convert an FnOnce that takes in a [`BlockingMap`] or an [`AsyncMap`] into a
/// recognized map type.
pub trait AsMap<M> {
    type MapType;
    fn as_map(self) -> Self::MapType;
}

/// A trait that all different ways of defining a Blocking Map must funnel into.
pub(crate) trait CallBlockingMap<Request, Response> {
    fn call(self, input: BlockingMap<Request>) -> Response;
}

impl<F, Request, Response> CallBlockingMap<Request, Response> for MapDef<F>
where
    F: FnOnce(BlockingMap<Request>) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    fn call(self, request: BlockingMap<Request>) -> Response {
        (self.0)(request)
    }
}


/// A newtype to mark the definition of a BlockingMap.
///
/// Maps cannot contain Bevy Systems; they can only contain objects that
/// implement [`FnOnce`].
pub struct BlockingMapDef<Def, Request, Response> {
    def: Def,
    _ignore: std::marker::PhantomData<(Request, Response)>,
}

impl<Def, Request, Response> Provider for BlockingMapDef<Def, Request, Response>
where
    Def: CallBlockingMap<Request, Response> + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();

    fn provide(self, source: Entity, target: Entity, commands: &mut Commands) {
        commands.add(PerformOperation::new(source, OperateBlockingMap::new(target, self.def)));
    }
}

pub struct BlockingMapMarker;

impl<F, Request, Response> AsMap<(Request, Response, BlockingMapMarker)> for F
where
    F: FnOnce(BlockingMap<Request>) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type MapType = BlockingMapDef<MapDef<F>, Request, Response>;
    fn as_map(self) -> Self::MapType {
        BlockingMapDef { def: MapDef(self), _ignore: Default::default() }
    }
}

/// Convert any [`FnOnce`] into a [`BlockingMapDef`].
pub trait IntoBlockingMap<M> {
    type MapType;
    fn into_blocking_map(self) -> Self::MapType;
}

impl<F, Request, Response> IntoBlockingMap<(Request, Response)> for F
where
    F: FnOnce(Request) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type MapType = BlockingMapDef<BlockingMapAdapter<F>, Request, Response>;
    fn into_blocking_map(self) -> Self::MapType {
        BlockingMapDef { def: BlockingMapAdapter(self), _ignore: Default::default() }
    }
}

pub struct BlockingMapAdapter<F>(F);

impl<F, Request, Response> CallBlockingMap<Request, Response> for BlockingMapAdapter<F>
where
    F: FnOnce(Request) -> Response,
{
    fn call(self, BlockingMap{ request }: BlockingMap<Request>) -> Response {
        (self.0)(request)
    }
}

pub(crate) trait CallAsyncMap<Request, Task, Streams> {
    fn call(self, input: AsyncMap<Request, Streams>) -> Task;
}

impl<F, Request, Task, Streams> CallAsyncMap<Request, Task, Streams> for MapDef<F>
where
    F: FnOnce(AsyncMap<Request, Streams>) -> Task + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: 'static + Send + Sync,
    Streams: Stream,
{
    fn call(self, input: AsyncMap<Request, Streams>) -> Task {
        (self.0)(input)
    }
}

pub struct AsyncMapMarker;

impl<F, Request, Task, Streams> AsMap<(Request, Task, Streams, AsyncMapMarker)> for F
where
    F: FnOnce(AsyncMap<Request, Streams>) -> Task + 'static + Send + Sync,
    Task: Future + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    type MapType = AsyncMapDef<MapDef<F>, Request, Task, Streams>;
    fn as_map(self) -> Self::MapType {
        AsyncMapDef { def: MapDef(self), _ignore: Default::default() }
    }
}

/// A newtype to mark the definition of an AsyncMap.
///
/// Maps cannot contain Bevy Systems; they can only contain objects that
/// implement [`FnOnce`].
pub struct AsyncMapDef<Def, Request, Task, Streams> {
    def: Def,
    _ignore: std::marker::PhantomData<(Request, Task, Streams)>,
}

impl<Def, Request, Task, Streams> Provider for AsyncMapDef<Def, Request, Task, Streams>
where
    Def: CallAsyncMap<Request, Task, Streams> + 'static + Send + Sync,
    Task: Future + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: Stream,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;

    fn provide(self, source: Entity, target: Entity, commands: &mut Commands) {
        commands.add(PerformOperation::new(source, OperateAsyncMap::new(target, self.def)));
    }
}

pub trait IntoAsyncMap<M> {
    type MapType;
    fn into_async_map(self) -> Self::MapType;
}

impl<F, Request, Task> IntoAsyncMap<(Request, Task)> for F
where
    F: FnOnce(Request) -> Task + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: Future + 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
{
    type MapType = AsyncMapDef<AsyncMapAdapter<F>, Request, Task, ()>;
    fn into_async_map(self) -> Self::MapType {
        AsyncMapDef { def: AsyncMapAdapter(self), _ignore: Default::default() }
    }
}

pub struct AsyncMapAdapter<F>(F);

impl<F, Request, Task> CallAsyncMap<Request, Task, ()> for AsyncMapAdapter<F>
where
    F: FnOnce(Request) -> Task + 'static + Send + Sync,
    Task: Future + 'static + Send + Sync,
{
    fn call(self, AsyncMap{ request, .. }: AsyncMap<Request, ()>) -> Task {
        (self.0)(request)
    }
}
