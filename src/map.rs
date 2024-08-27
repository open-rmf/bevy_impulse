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
    AddOperation, AsyncMap, BlockingMap, OperateAsyncMap, OperateBlockingMap, ProvideOnce,
    Provider, Sendish, StreamPack,
};

use bevy_ecs::prelude::{Commands, Entity};

use std::future::Future;

/// A newtype to indicate that the map definition is given directly by F.
#[derive(Clone, Copy)]
pub struct MapDef<F>(F);

/// Maps are used to perform simple transformations of data which do not require
/// any direct [`World`][4] access or any persistent system state. They are more
/// efficient than [`Service`][5] or [`Callback`][6] when suitable.
///
/// There are two kinds of functions that can be used as maps:
/// * **Blocking**: A regular function with a single input and any output.
///   All system execution will be blocked while this is running, similar to flushing [`Commands`].
/// * **Async**: A function that takes a single input and returns something that implements the [`Future`] trait.
///   The [`Future`] will be executed in the [`AsyncComputeTaskPool`][7] unless the `single_threaded_async` feature is active.
///
/// If you want to insert a map into a workflow or impulse chain, you can pass your function into one of the following,
/// depending on whether you want blocking or async:
/// * **Blocking**
///   * [`Chain::map_block`](crate::Chain::map_block)
///   * [`Chain::map_block_node`](crate::Chain::map_block_node)
///   * [`Builder::create_map_block`](crate::Builder::create_map_block)
///   * [`Impulse::map_block`](crate::Impulse::map_block)
/// * **Async**
///   * [`Chain::map_async`](crate::Chain::map_async)
///   * [`Chain::map_async_node`](crate::Chain::map_async_node)
///   * [`Builder::create_map_async`](crate::Builder::create_map_async)
///   * [`Impulse::map_async`](crate::Impulse::map_async)
///
/// If you want your map to emit streams, you will need your input argument to be
/// [`BlockingMap`] or [`AsyncMap`]. In that case you need to use one of the following:
///
/// * [`Chain::map`](crate::Chain::map)
/// * [`Builder::create_map`](crate::Builder::create_map)
/// * [`Impulse::map`](crate::Impulse::map)
///
/// You can also use [`.as_map`][1], [`.into_blocking_map`][2], [`.into_async_map`][3]
/// to convert a suitable function into a [`Provider`] that can be passed into
/// any function that accepts a [`Provider`].
///
/// [1]: AsMap::as_map
/// [2]: IntoBlockingMap::into_blocking_map
/// [3]: IntoAsyncMap::into_async_map
/// [4]: bevy_ecs::prelude::World
/// [5]: crate::Service
/// [6]: crate::Callback
/// [7]: bevy_tasks::AsyncComputeTaskPool
#[allow(clippy::wrong_self_convention)]
pub trait AsMap<M> {
    type MapType;
    /// Convert an [`FnMut`] that takes a single input of [`BlockingMap`] or
    /// [`AsyncMap`] into a [`Provider`].
    fn as_map(self) -> Self::MapType;
}

pub type RequestOfMap<M, F> = <<F as AsMap<M>>::MapType as ProvideOnce>::Request;
pub type ResponseOfMap<M, F> = <<F as AsMap<M>>::MapType as ProvideOnce>::Response;
pub type StreamsOfMap<M, F> = <<F as AsMap<M>>::MapType as ProvideOnce>::Streams;

/// A trait that all different ways of defining a Blocking Map must funnel into.
pub(crate) trait CallBlockingMap<Request, Response, Streams: StreamPack> {
    fn call(&mut self, input: BlockingMap<Request, Streams>) -> Response;
}

impl<F, Request, Response, Streams> CallBlockingMap<Request, Response, Streams> for MapDef<F>
where
    F: FnMut(BlockingMap<Request, Streams>) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn call(&mut self, request: BlockingMap<Request, Streams>) -> Response {
        (self.0)(request)
    }
}

/// A newtype to mark the definition of a BlockingMap.
///
/// Maps cannot contain Bevy Systems; they can only contain objects that
/// implement [`FnMut`].
pub struct BlockingMapDef<Def, Request, Response, Streams> {
    def: Def,
    _ignore: std::marker::PhantomData<fn(Request, Response, Streams)>,
}

impl<Def: Clone, Request, Response, Streams> Clone
    for BlockingMapDef<Def, Request, Response, Streams>
{
    fn clone(&self) -> Self {
        Self {
            def: self.def.clone(),
            _ignore: Default::default(),
        }
    }
}

impl<Def, Request, Response, Streams> ProvideOnce
    for BlockingMapDef<Def, Request, Response, Streams>
where
    Def: CallBlockingMap<Request, Response, Streams> + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

    fn connect(
        self,
        scope: Option<Entity>,
        source: Entity,
        target: Entity,
        commands: &mut Commands,
    ) {
        commands.add(AddOperation::new(
            scope,
            source,
            OperateBlockingMap::new(target, self.def),
        ));
    }
}

impl<Def, Request, Response, Streams> Provider for BlockingMapDef<Def, Request, Response, Streams>
where
    Def: CallBlockingMap<Request, Response, Streams> + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
}

pub struct BlockingMapMarker;

impl<F, Request, Response, Streams> AsMap<(Request, Response, Streams, BlockingMapMarker)> for F
where
    F: FnMut(BlockingMap<Request, Streams>) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type MapType = BlockingMapDef<MapDef<F>, Request, Response, Streams>;
    fn as_map(self) -> Self::MapType {
        BlockingMapDef {
            def: MapDef(self),
            _ignore: Default::default(),
        }
    }
}

/// Convert any [`FnMut`] into a [`BlockingMapDef`].
pub trait IntoBlockingMap<M> {
    type MapType;
    fn into_blocking_map(self) -> Self::MapType;
}

impl<F, Request, Response, Streams> IntoBlockingMap<(Request, Response, Streams)> for F
where
    F: FnMut(Request) -> Response + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type MapType = BlockingMapDef<BlockingMapAdapter<F>, Request, Response, Streams>;
    /// Convert any single input function into a [`Provider`] whose request type
    /// is the single input type of the function and whose response type is the
    /// return type of the function.
    fn into_blocking_map(self) -> Self::MapType {
        BlockingMapDef {
            def: BlockingMapAdapter(self),
            _ignore: Default::default(),
        }
    }
}

pub struct BlockingMapAdapter<F>(F);

impl<F, Request, Response> CallBlockingMap<Request, Response, ()> for BlockingMapAdapter<F>
where
    F: FnMut(Request) -> Response,
{
    fn call(&mut self, BlockingMap { request, .. }: BlockingMap<Request, ()>) -> Response {
        (self.0)(request)
    }
}

pub(crate) trait CallAsyncMap<Request, Task, Streams: StreamPack> {
    fn call(&mut self, input: AsyncMap<Request, Streams>) -> Task;
}

impl<F, Request, Task, Streams> CallAsyncMap<Request, Task, Streams> for MapDef<F>
where
    F: FnMut(AsyncMap<Request, Streams>) -> Task + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: 'static + Send,
    Streams: StreamPack,
{
    fn call(&mut self, input: AsyncMap<Request, Streams>) -> Task {
        (self.0)(input)
    }
}

pub struct AsyncMapMarker;

impl<F, Request, Task, Streams> AsMap<(Request, Task, Streams, AsyncMapMarker)> for F
where
    F: FnMut(AsyncMap<Request, Streams>) -> Task + 'static + Send + Sync,
    Task: Future + 'static + Sendish,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    type MapType = AsyncMapDef<MapDef<F>, Request, Task, Streams>;
    fn as_map(self) -> Self::MapType {
        AsyncMapDef {
            def: MapDef(self),
            _ignore: Default::default(),
        }
    }
}

/// A newtype to mark the definition of an AsyncMap.
///
/// Maps cannot contain Bevy Systems; they can only contain objects that
/// implement [`FnMut`].
pub struct AsyncMapDef<Def, Request, Task, Streams> {
    def: Def,
    _ignore: std::marker::PhantomData<fn(Request, Task, Streams)>,
}

impl<Def: Clone, Request, Task, Streams> Clone for AsyncMapDef<Def, Request, Task, Streams> {
    fn clone(&self) -> Self {
        Self {
            def: self.def.clone(),
            _ignore: Default::default(),
        }
    }
}

impl<Def, Request, Task, Streams> ProvideOnce for AsyncMapDef<Def, Request, Task, Streams>
where
    Def: CallAsyncMap<Request, Task, Streams> + 'static + Send + Sync,
    Task: Future + 'static + Sendish,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Task::Output;
    type Streams = Streams;

    fn connect(
        self,
        scope: Option<Entity>,
        source: Entity,
        target: Entity,
        commands: &mut Commands,
    ) {
        commands.add(AddOperation::new(
            scope,
            source,
            OperateAsyncMap::new(target, self.def),
        ));
    }
}

impl<Def, Request, Task, Streams> Provider for AsyncMapDef<Def, Request, Task, Streams>
where
    Def: CallAsyncMap<Request, Task, Streams> + 'static + Send + Sync,
    Task: Future + 'static + Sendish,
    Request: 'static + Send + Sync,
    Task::Output: 'static + Send + Sync,
    Streams: StreamPack,
{
}

pub trait IntoAsyncMap<M> {
    type MapType;
    fn into_async_map(self) -> Self::MapType;
}

impl<F, Request, Task> IntoAsyncMap<(Request, Task)> for F
where
    F: FnMut(Request) -> Task + 'static + Send + Sync,
    Request: 'static + Send + Sync,
    Task: Future + 'static + Sendish,
    Task::Output: 'static + Send + Sync,
{
    type MapType = AsyncMapDef<AsyncMapAdapter<F>, Request, Task, ()>;
    /// Convert any
    fn into_async_map(self) -> Self::MapType {
        AsyncMapDef {
            def: AsyncMapAdapter(self),
            _ignore: Default::default(),
        }
    }
}

pub struct AsyncMapAdapter<F>(F);

impl<F, Request, Task> CallAsyncMap<Request, Task, ()> for AsyncMapAdapter<F>
where
    F: FnMut(Request) -> Task + 'static + Send + Sync,
    Task: Future + 'static + Sendish,
{
    fn call(&mut self, AsyncMap { request, .. }: AsyncMap<Request, ()>) -> Task {
        (self.0)(request)
    }
}
