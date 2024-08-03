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
    BlockingMap, AsyncMap, AddImpulse, ImpulseBlockingMap, ImpulseAsyncMap,
    StreamPack, ProvideOnce, BlockingMapMarker, AsyncMapMarker, Sendish,
};

use bevy_ecs::prelude::{Entity, Commands};

use std::future::Future;

/// A newtype to indicate that the map definition is given directly by F.
pub struct MapOnceDef<F>(F);

/// Convert an [`FnOnce`] that takes in a [`BlockingMap`] or an [`AsyncMap`]
/// into a recognized map type.
pub trait AsMapOnce<M> {
    type MapType;
    fn as_map_once(self) -> Self::MapType;
}

pub(crate) trait CallBlockingMapOnce<Request, Response, Streams: StreamPack> {
    fn call(self, input: BlockingMap<Request, Streams>) -> Response;
}

impl<F, Request, Response, Streams> CallBlockingMapOnce<Request, Response, Streams> for MapOnceDef<F>
where
    F: FnOnce(BlockingMap<Request, Streams>) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn call(self, input: BlockingMap<Request, Streams>) -> Response {
        (self.0)(input)
    }
}

/// A newtype to mark the definition of a BlockingMap.
///
/// Maps cannot contain Bevy Systems; they can only contain objects that
/// implement [`FnOnce`].
pub struct BlockingMapOnceDef<Def, Request, Response, Streams> {
    def: Def,
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<Def, Request, Response, Streams> ProvideOnce for BlockingMapOnceDef<Def, Request, Response, Streams>
where
    Def: CallBlockingMapOnce<Request, Response, Streams> + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();

    fn connect(self, _: Option<Entity>, source: Entity, target: Entity, commands: &mut Commands) {
        commands.add(AddImpulse::new(source, ImpulseBlockingMap::new(target, self.def)));
    }
}

impl<F, Request, Response, Streams> AsMapOnce<(Request, Response, Streams, BlockingMapMarker)> for F
where
    F: FnOnce(BlockingMap<Request, Streams>) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type MapType = BlockingMapOnceDef<MapOnceDef<F>, Request, Response, Streams>;
    fn as_map_once(self) -> Self::MapType {
        BlockingMapOnceDef { def: MapOnceDef(self), _ignore: Default::default() }
    }
}

/// Convert any [`FnOnce`] into a [`BlockingMapOnceDef`].
pub trait IntoBlockingMapOnce<M> {
    type MapType;
    fn into_blocking_map_once(self) -> Self::MapType;
}

impl<F, Request, Response> IntoBlockingMapOnce<(Request, Response)> for F
where
    F: FnOnce(Request) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type MapType = BlockingMapOnceDef<BlockingMapOnceAdapter<F>, Request, Response, ()>;
    fn into_blocking_map_once(self) -> Self::MapType {
        BlockingMapOnceDef { def: BlockingMapOnceAdapter(self), _ignore: Default::default() }
    }
}

pub struct BlockingMapOnceAdapter<F>(F);

impl<F, Request, Response> CallBlockingMapOnce<Request, Response, ()> for BlockingMapOnceAdapter<F>
where
    F: FnOnce(Request) -> Response,
{
    fn call(self, BlockingMap { request, .. }: BlockingMap<Request, ()>) -> Response {
        (self.0)(request)
    }
}

pub(crate) trait CallAsyncMapOnce<Request, Task, Streams: StreamPack> {
    fn call(self, input: AsyncMap<Request, Streams>) -> Task;
}

impl<F, Request, Task, Streams> CallAsyncMapOnce<Request, Task, Streams> for MapOnceDef<F>
where
    F: FnOnce(AsyncMap<Request, Streams>) -> Task + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn call(self, input: AsyncMap<Request, Streams>) -> Task {
        (self.0)(input)
    }
}

impl<F, Request, Task, Streams> AsMapOnce<(Request, Task, Streams, AsyncMapMarker)> for F
where
    F: FnOnce(AsyncMap<Request, Streams>) -> Task + 'static + Send + Sync,
    Task: Future + 'static + Sendish,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    type MapType = AsyncMapOnceDef<MapOnceDef<F>, Request, Task, Streams>;
    fn as_map_once(self) -> Self::MapType {
        AsyncMapOnceDef { def: MapOnceDef(self), _ignore: Default::default() }
    }
}

/// A newtype to mark the definition of an AsyncMap.
///
/// Maps cannot contain Bevy Systems; they can only contain objects that
/// implement [`FnOnce`].
pub struct AsyncMapOnceDef<Def, Request, Task, Streams> {
    def: Def,
    _ignore: std::marker::PhantomData<(Request, Task, Streams)>,
}

impl<Def, Request, Task, Streams> ProvideOnce for AsyncMapOnceDef<Def, Request, Task, Streams>
where
    Def: CallAsyncMapOnce<Request, Task, Streams> + 'static + Send + Sync,
    Task: Future + 'static + Sendish,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;

    fn connect(self, _: Option<Entity>, source: Entity, target: Entity, commands: &mut Commands) {
        commands.add(AddImpulse::new(source, ImpulseAsyncMap::new(target, self.def)));
    }
}

pub trait IntoAsyncMapOnce<M> {
    type MapType;
    fn into_async_map_once(self) -> Self::MapType;
}

impl<F, Request, Task> IntoAsyncMapOnce<(Request, Task)> for F
where
    F: FnOnce(Request) -> Task + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: Future + 'static + Sendish,
    Task::Output: 'static + Send + Sync,
{
    type MapType = AsyncMapOnceDef<AsyncMapOnceAdapter<F>, Request, Task, ()>;
    fn into_async_map_once(self) -> Self::MapType {
        AsyncMapOnceDef { def: AsyncMapOnceAdapter(self), _ignore: Default::default() }
    }
}

pub struct AsyncMapOnceAdapter<F>(F);

impl<F, Request, Task> CallAsyncMapOnce<Request, Task, ()> for AsyncMapOnceAdapter<F>
where
    F: FnOnce(Request) -> Task + 'static + Send + Sync,
    Task: Future + 'static + Sendish,
{
    fn call(self, AsyncMap { request, .. }: AsyncMap<Request, ()>) -> Task {
        (self.0)(request)
    }
}
