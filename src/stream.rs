/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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

use bevy::{
    prelude::{
        Component, Bundle, Entity, Commands, World, BuildChildren, Deref,
        DerefMut, With,
    },
    ecs::query::ReadOnlyWorldQuery,
};

use crossbeam::channel::{Receiver, unbounded};

use std::{rc::Rc, cell::RefCell, sync::Arc};

use smallvec::SmallVec;

use crate::{
    InputSlot, Output, UnusedTarget, RedirectWorkflowStream, RedirectScopeStream,
    AddOperation, AddImpulse, OperationRoster, OperationResult, OrBroken, ManageInput,
    InnerChannel, TakenStream, StreamChannel, Push, Builder, UnusedStreams,
};

pub trait Stream: 'static + Send + Sync + Sized {
    type Container: IntoIterator<Item = Self> + Extend<Self> + Default;

    fn send(
        self,
        StreamRequest { session, target, world, roster, .. }: StreamRequest
    ) -> OperationResult {
        if let Some(target) = target {
            world.get_entity_mut(target)
                .or_broken()?
                .give_input(session, self, roster)?;
        }

        Ok(())
    }

    fn spawn_scope_stream(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (
        InputSlot<Self>,
        Output<Self>,
    ) {
        let source = commands.spawn(()).id();
        let target = commands.spawn(UnusedTarget).id();
        commands.add(AddOperation::new(Some(in_scope), source, RedirectScopeStream::<Self>::new(target)));
        (
            InputSlot::new(in_scope, source),
            Output::new(out_scope, target),
        )
    }

    fn spawn_workflow_stream(
        builder: &mut Builder,
    ) -> InputSlot<Self> {
        let source = builder.commands.spawn(()).id();
        builder.commands.add(AddOperation::new(
            Some(builder.scope()), source, RedirectWorkflowStream::<Self>::new()),
        );
        InputSlot::new(builder.scope, source)
    }

    fn spawn_node_stream(
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (
        StreamTargetStorage<Self>,
        Output<Self>,
    ) {
        let target = builder.commands.spawn(UnusedTarget).id();
        let index = map.add(target);
        (
            StreamTargetStorage::new(index),
            Output::new(builder.scope, target),
        )
    }

    fn take_stream(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> (
        StreamTargetStorage<Self>,
        Receiver<Self>,
    ) {
        let (sender, receiver) = unbounded::<Self>();
        let target = commands
            .spawn(())
            // Set the parent of this stream to be the session so it can be
            // recursively despawned together.
            .set_parent(source)
            .id();

        let index = map.add(target);

        commands.add(AddImpulse::new(target, TakenStream::new(sender)));

        (
            StreamTargetStorage::new(index),
            receiver,
        )
    }

    fn collect_stream(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> StreamTargetStorage<Self> {
        let redirect = commands.spawn(()).set_parent(source).id();
        commands.add(AddImpulse::new(
            redirect,
            Push::<Self>::new(target, true),
        ));
        let index = map.add(redirect);
        StreamTargetStorage::new(index)
    }
}

/// A simple newtype wrapper that turns any suitable data structure
/// (`'static + Send + Sync`) into a stream.
#[derive(Clone, Copy, Debug, Deref, DerefMut)]
pub struct StreamOf<T>(pub T);

impl<T: 'static + Send + Sync> Stream for StreamOf<T> {
    type Container = SmallVec<[StreamOf<T>; 16]>;
}

pub struct StreamBuffer<T: Stream> {
    // TODO(@mxgrey): Consider replacing the Rc with an unsafe pointer so that
    // no heap allocation is needed each time a stream is used in a blocking
    // function.
    container: Rc<RefCell<T::Container>>,
    target: Option<Entity>,
}

impl<T: Stream> Clone for StreamBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            container: Rc::clone(&self.container),
            target: self.target.clone(),
        }
    }
}

impl<T: Stream + std::fmt::Debug> StreamBuffer<T> {
    pub fn send(&self, data: T) {
        self.container.borrow_mut().extend([data]);
    }
}

pub struct StreamRequest<'a> {
    /// The node that emitted the stream
    pub source: Entity,
    /// The session of the stream
    pub session: Entity,
    /// The target of the stream, if a specific target exists.
    pub target: Option<Entity>,
    /// The world that the stream exists inside
    pub world: &'a mut World,
    /// The roster of the stream
    pub roster: &'a mut OperationRoster,
}

/// [`StreamAvailable`] is a marker component that indicates what streams are offered by
/// a service.
#[derive(Component)]
pub struct StreamAvailable<T: Stream> {
    _ignore: std::marker::PhantomData<T>,
}

impl<T: Stream> Default for StreamAvailable<T> {
    fn default() -> Self {
        Self { _ignore: Default::default() }
    }
}

/// [`StreamTargetStorage`] keeps track of the target for each stream for a source.
#[derive(Component)]
pub struct StreamTargetStorage<T: Stream> {
    index: usize,
    _ignore: std::marker::PhantomData<T>,
}

impl<T: Stream> StreamTargetStorage<T> {
    fn new(index: usize) -> Self {
        Self { index, _ignore: Default::default() }
    }

    pub fn get(&self) -> usize {
        self.index
    }
}

/// The actual entity target of the stream is held in this component which does
/// not have any generic parameters. This means it is possible to lookup the
/// targets of the streams coming out of the node without knowing the concrete
/// type of the streams. This is crucial for being able to redirect the stream
/// targets.
//
// TODO(@mxgrey): Consider whether we should store stream type information in here
#[derive(Component, Default)]
pub struct StreamTargetMap {
    pub(crate) map: SmallVec<[Entity; 8]>,
}

impl StreamTargetMap {
    fn add(&mut self, target: Entity) -> usize {
        let index = self.map.len();
        self.map.push(target);
        index
    }

    pub fn map(&self) -> &SmallVec<[Entity; 8]> {
        &self.map
    }

    pub fn get(&self, index: usize) -> Option<Entity> {
        self.map.get(index).copied()
    }
}

pub trait StreamPack: 'static + Send + Sync {
    type StreamAvailableBundle: Bundle + Default;
    type StreamFilter: ReadOnlyWorldQuery;
    type StreamStorageBundle: Bundle;
    type StreamInputPack;
    type StreamOutputPack: std::fmt::Debug;
    type Receiver;
    type Channel;
    type Buffer: Clone;

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (
        Self::StreamInputPack,
        Self::StreamOutputPack,
    );

    fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack;

    fn spawn_node_streams(
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (
        Self::StreamStorageBundle,
        Self::StreamOutputPack,
    );

    fn take_streams(source: Entity, map: &mut StreamTargetMap, builder: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::Receiver,
    );

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Self::StreamStorageBundle;

    fn make_channel(inner: &Arc<InnerChannel>, world: &World) -> Self::Channel;

    fn make_buffer(source: Entity, world: &World) -> Self::Buffer;

    fn process_buffer(
        buffer: Self::Buffer,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;
}

impl<T: Stream> StreamPack for T {
    type StreamAvailableBundle = StreamAvailable<Self>;
    type StreamFilter = With<StreamAvailable<Self>>;
    type StreamStorageBundle = StreamTargetStorage<Self>;
    type StreamInputPack = InputSlot<Self>;
    type StreamOutputPack = Output<Self>;
    type Receiver = Receiver<Self>;
    type Channel = StreamChannel<Self>;
    type Buffer = StreamBuffer<Self>;

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (
        Self::StreamInputPack,
        Self::StreamOutputPack,
    ) {
        T::spawn_scope_stream(in_scope, out_scope, commands)
    }

    fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack {
        T::spawn_workflow_stream(builder)
    }

    fn spawn_node_streams(
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (
        Self::StreamStorageBundle,
        Self::StreamOutputPack,
    ) {
        T::spawn_node_stream(map, builder)
    }

    fn take_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> (
        Self::StreamStorageBundle,
        Self::Receiver,
    ) {
        Self::take_stream(source, map, commands)
    }

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Self::StreamStorageBundle {
        Self::collect_stream(source, target, map, commands)
    }

    fn make_channel(
        inner: &Arc<InnerChannel>,
        world: &World,
    ) -> Self::Channel {
        let index = world.get::<StreamTargetStorage<Self>>(inner.source())
            .map(|t| t.index);
        let target = index.map(
            |index| world
                .get::<StreamTargetMap>(inner.source())
                .map(|t| t.get(index))
                .flatten()
        ).flatten();
        StreamChannel::new(target, Arc::clone(inner))
    }

    fn make_buffer(source: Entity, world: &World) -> Self::Buffer {
        let index = world.get::<StreamTargetStorage<Self>>(source)
            .map(|t| t.index);
        let target = index.map(
            |index| world
                .get::<StreamTargetMap>(source)
                .map(|t| t.get(index))
                .flatten()
        ).flatten();
        StreamBuffer { container: Default::default(), target }
    }

    fn process_buffer(
        buffer: Self::Buffer,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let target = buffer.target;
        let mut was_unused = true;
        for data in Rc::into_inner(buffer.container).or_broken()?.into_inner().into_iter() {
            was_unused = false;
            data.send(StreamRequest { source, session, target, world, roster })?;
        }

        if was_unused {
            unused.streams.push(std::any::type_name::<Self>());
        }

        Ok(())
    }
}

impl StreamPack for () {
    type StreamAvailableBundle = ();
    type StreamFilter = ();
    type StreamStorageBundle = ();
    type StreamInputPack = ();
    type StreamOutputPack = ();
    type Receiver = ();
    type Channel = ();
    type Buffer = ();

    fn spawn_scope_streams(
        _: Entity,
        _: Entity,
        _: &mut Commands,
    ) -> (
        Self::StreamInputPack,
        Self::StreamOutputPack,
    ) {
        ((), ())
    }

    fn spawn_workflow_streams(_: &mut Builder) -> Self::StreamInputPack {
        ()
    }

    fn spawn_node_streams(
        _: &mut StreamTargetMap,
        _: &mut Builder,
    ) -> (
        Self::StreamStorageBundle,
        Self::StreamOutputPack,
    ) {
        ((), ())
    }

    fn take_streams(_: Entity, _: &mut StreamTargetMap, _: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::Receiver,
    ) {
        ((), ())
    }

    fn collect_streams(
        _: Entity,
        _: Entity,
        _: &mut StreamTargetMap,
        _: &mut Commands,
    ) -> Self::StreamStorageBundle {
        ()
    }

    fn make_channel(
        _: &Arc<InnerChannel>,
        _: &World,
    ) -> Self::Channel {
        ()
    }

    fn make_buffer(_: Entity, _: &World) -> Self::Buffer {
        ()
    }

    fn process_buffer(
        _: Self::Buffer,
        _: Entity,
        _: Entity,
        _: &mut UnusedStreams,
        _: &mut World,
        _: &mut OperationRoster,
    ) -> OperationResult {
        Ok(())
    }
}

impl<T1: StreamPack> StreamPack for (T1,) {
    type StreamAvailableBundle = T1::StreamAvailableBundle;
    type StreamFilter = T1::StreamFilter;
    type StreamStorageBundle = T1::StreamStorageBundle;
    type StreamInputPack = T1::StreamInputPack;
    type StreamOutputPack = T1::StreamOutputPack;
    type Receiver = T1::Receiver;
    type Channel = T1::Channel;
    type Buffer = T1::Buffer;

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (
        Self::StreamInputPack,
        Self::StreamOutputPack,
    ) {
        T1::spawn_scope_streams(in_scope, out_scope, commands)
    }

    fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack {
        T1::spawn_workflow_streams(builder)
    }

    fn spawn_node_streams(
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (
        Self::StreamStorageBundle,
        Self::StreamOutputPack,
    ) {
        T1::spawn_node_streams(map, builder)
    }

    fn take_streams(source: Entity, map: &mut StreamTargetMap, builder: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::Receiver,
    ) {
        T1::take_streams(source, map, builder)
    }

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Self::StreamStorageBundle {
        T1::collect_streams(source, target, map, commands)
    }

    fn make_channel(
        inner: &Arc<InnerChannel>,
        world: &World,
    ) -> Self::Channel {
        T1::make_channel(inner, world)
    }

    fn make_buffer(source: Entity, world: &World) -> Self::Buffer {
        T1::make_buffer(source, world)
    }

    fn process_buffer(
        buffer: Self::Buffer,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        T1::process_buffer(buffer, source, session, unused, world, roster)?;
        Ok(())
    }
}

impl<T1: StreamPack, T2: StreamPack> StreamPack for (T1, T2) {
    type StreamAvailableBundle = (T1::StreamAvailableBundle, T2::StreamAvailableBundle);
    type StreamFilter = (T1::StreamFilter, T2::StreamFilter);
    type StreamStorageBundle = (T1::StreamStorageBundle, T2::StreamStorageBundle);
    type StreamInputPack = (T1::StreamInputPack, T2::StreamInputPack);
    type StreamOutputPack = (T1::StreamOutputPack, T2::StreamOutputPack);
    type Receiver = (T1::Receiver, T2::Receiver);
    type Channel = (T1::Channel, T2::Channel);
    type Buffer = (T1::Buffer, T2::Buffer);

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (
        Self::StreamInputPack,
        Self::StreamOutputPack,
    ) {
        let t1 = T1::spawn_scope_streams(in_scope, out_scope, commands);
        let t2 = T2::spawn_scope_streams(in_scope, out_scope, commands);
        ((t1.0, t2.0), (t1.1, t2.1))
    }

    fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack {
        let t1 = T1::spawn_workflow_streams(builder);
        let t2 = T2::spawn_workflow_streams(builder);
        (t1, t2)
    }

    fn spawn_node_streams(
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (
        Self::StreamStorageBundle,
        Self::StreamOutputPack,
    ) {
        let t1 = T1::spawn_node_streams(map, builder);
        let t2 = T2::spawn_node_streams(map, builder);
        ((t1.0, t2.0), (t1.1, t2.1))
    }

    fn take_streams(source: Entity, map: &mut StreamTargetMap, builder: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::Receiver,
    ) {
        let t1 = T1::take_streams(source, map, builder);
        let t2 = T2::take_streams(source, map, builder);
        ((t1.0, t2.0), (t1.1, t2.1))
    }

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Self::StreamStorageBundle {
        let t1 = T1::collect_streams(source, target, map, commands);
        let t2 = T2::collect_streams(source, target, map, commands);
        (t1, t2)
    }

    fn make_channel(
        inner: &Arc<InnerChannel>,
        world: &World,
    ) -> Self::Channel {
        let t1 = T1::make_channel(inner, world);
        let t2 = T2::make_channel(inner, world);
        (t1, t2)
    }

    fn make_buffer(source: Entity, world: &World) -> Self::Buffer {
        let t1 = T1::make_buffer(source, world);
        let t2 = T2::make_buffer(source, world);
        (t1, t2)
    }

    fn process_buffer(
        buffer: Self::Buffer,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        T1::process_buffer(buffer.0, source, session, unused, world, roster)?;
        T2::process_buffer(buffer.1, source, session, unused, world, roster)?;
        Ok(())
    }
}

impl<T1: StreamPack, T2: StreamPack, T3: StreamPack> StreamPack for (T1, T2, T3) {
    type StreamAvailableBundle = (T1::StreamAvailableBundle, T2::StreamAvailableBundle, T3::StreamAvailableBundle);
    type StreamFilter = (T1::StreamFilter, T2::StreamFilter, T3::StreamFilter);
    type StreamStorageBundle = (T1::StreamStorageBundle, T2::StreamStorageBundle, T3::StreamStorageBundle);
    type StreamInputPack = (T1::StreamInputPack, T2::StreamInputPack, T3::StreamInputPack);
    type StreamOutputPack = (T1::StreamOutputPack, T2::StreamOutputPack, T3::StreamOutputPack);
    type Receiver = (T1::Receiver, T2::Receiver, T3::Receiver);
    type Channel = (T1::Channel, T2::Channel, T3::Channel);
    type Buffer = (T1::Buffer, T2::Buffer, T3::Buffer);

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (
        Self::StreamInputPack,
        Self::StreamOutputPack,
    ) {
        let t1 = T1::spawn_scope_streams(in_scope, out_scope, commands);
        let t2 = T2::spawn_scope_streams(in_scope, out_scope, commands);
        let t3 = T3::spawn_scope_streams(in_scope, out_scope, commands);
        ((t1.0, t2.0, t3.0), (t1.1, t2.1, t3.1))
    }

    fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack {
        let t1 = T1::spawn_workflow_streams(builder);
        let t2 = T2::spawn_workflow_streams(builder);
        let t3 = T3::spawn_workflow_streams(builder);
        (t1, t2, t3)
    }

    fn spawn_node_streams(
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (
        Self::StreamStorageBundle,
        Self::StreamOutputPack,
    ) {
        let t1 = T1::spawn_node_streams(map, builder);
        let t2 = T2::spawn_node_streams(map, builder);
        let t3 = T3::spawn_node_streams(map, builder);
        ((t1.0, t2.0, t3.0), (t1.1, t2.1, t3.1))
    }

    fn take_streams(source: Entity, map: &mut StreamTargetMap, builder: &mut Commands) -> (
        Self::StreamStorageBundle,
        Self::Receiver,
    ) {
        let t1 = T1::take_streams(source, map, builder);
        let t2 = T2::take_streams(source, map, builder);
        let t3 = T3::take_streams(source, map, builder);
        ((t1.0, t2.0, t3.0), (t1.1, t2.1, t3.1))
    }

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Self::StreamStorageBundle {
        let t1 = T1::collect_streams(source, target, map, commands);
        let t2 = T2::collect_streams(source, target, map, commands);
        let t3 = T3::collect_streams(source, target, map, commands);
        (t1, t2, t3)
    }

    fn make_channel(
        inner: &Arc<InnerChannel>,
        world: &World,
    ) -> Self::Channel {
        let t1 = T1::make_channel(inner, world);
        let t2 = T2::make_channel(inner, world);
        let t3 = T3::make_channel(inner, world);
        (t1, t2, t3)
    }

    fn make_buffer(source: Entity, world: &World) -> Self::Buffer {
        let t1 = T1::make_buffer(source, world);
        let t2 = T2::make_buffer(source, world);
        let t3 = T3::make_buffer(source, world);
        (t1, t2, t3)
    }

    fn process_buffer(
        buffer: Self::Buffer,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        T1::process_buffer(buffer.0, source, session, unused, world, roster)?;
        T2::process_buffer(buffer.1, source, session, unused, world, roster)?;
        T3::process_buffer(buffer.2, source, session, unused, world, roster)?;
        Ok(())
    }
}

/// Used by [`ServiceDiscovery`](crate::ServiceDiscovery) to filter services
/// based on what streams they provide. If a stream is required, you should wrap
/// the stream type in [`Require`]. If a stream is optional, then wrap it in
/// [`Option`].
///
/// The service you receive will appear as though it provides all the stream
/// types wrapped in both your `Require` and `Option` filters, but it might not
/// actually provide any of the streams that were wrapped in `Option`. A service
/// that does not actually provide the optional stream can be treated as if it
/// does provide the stream, except it will never actually send out any of that
/// optional stream data.
///
/// ```
/// use bevy_impulse::{*, testing::*};
///
/// fn service_discovery_system(
///     discover: ServiceDiscovery<
///         f32,
///         f32,
///         (
///             Require<(StreamOf<f32>, StreamOf<u32>)>,
///             Option<(StreamOf<String>, StreamOf<u8>)>,
///         )
///     >
/// ) {
///     let service: Service<
///         f32,
///         f32,
///         (
///             (StreamOf<f32>, StreamOf<u32>),
///             (StreamOf<String>, StreamOf<u8>),
///         )
///     > = discover.iter().next().unwrap();
/// }
/// ```
pub trait StreamFilter {
    type Filter: ReadOnlyWorldQuery;
    type Pack: StreamPack;
}

/// Used by [`ServiceDiscovery`](crate::ServiceDiscovery) to indicate that a
/// certain pack of streams is required.
///
/// For streams that are optional, wrap them in [`Option`] instead.
///
/// See [`StreamFilter`] for a usage example.
pub struct Require<T> {
    _ignore: std::marker::PhantomData<T>,
}

impl StreamFilter for () {
    type Filter = ();
    type Pack = ();
}

impl<T: StreamPack> StreamFilter for Require<T> {
    type Filter = T::StreamFilter;
    type Pack = T;
}

impl<T: StreamPack> StreamFilter for Option<T> {
    type Filter = ();
    type Pack = T;
}

impl<T0: StreamFilter, T1: StreamFilter> StreamFilter for (T0, T1) {
    type Filter = (T0::Filter, T1::Filter);
    type Pack = (T0::Pack, T1::Pack);
}

#[cfg(test)]
mod tests {
    use crate::{*, testing::*};

    #[test]
    fn test_single_stream() {
        let mut context = TestingContext::minimal_plugins();

        let count_blocking_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): BlockingServiceInput<u32, StreamOf<u32>>| {
                    for i in 0..input.request {
                        input.streams.send(StreamOf(i));
                    }
                    return input.request;
                }
            )
        });

        test_counting_stream(count_blocking_srv, &mut context);

        let count_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<u32, StreamOf<u32>>| {
                    async move {
                        for i in 0..input.request {
                            input.streams.send(StreamOf(i));
                        }
                        return input.request;
                    }
                }
            )
        });

        test_counting_stream(count_async_srv, &mut context);

        let count_blocking_callback = (
            |In(input): BlockingCallbackInput<u32, StreamOf<u32>>| {
                for i in 0..input.request {
                    input.streams.send(StreamOf(i));
                }
                return input.request;
            }
        ).as_callback();

        test_counting_stream(count_blocking_callback, &mut context);

        let count_async_callback = (
            |In(input): AsyncCallbackInput<u32, StreamOf<u32>>| {
                async move {
                    for i in 0..input.request {
                        input.streams.send(StreamOf(i));
                    }
                    return input.request;
                }
            }
        ).as_callback();

        test_counting_stream(count_async_callback, &mut context);

        let count_blocking_map = (
            |input: BlockingMap<u32, StreamOf<u32>>| {
                for i in 0..input.request {
                    input.streams.send(StreamOf(i));
                }
                return input.request;
            }
        ).as_map();

        test_counting_stream(count_blocking_map, &mut context);

        let count_async_map = (
            |input: AsyncMap<u32, StreamOf<u32>>| {
                async move {
                    for i in 0..input.request {
                        input.streams.send(StreamOf(i));
                    }
                    return input.request;
                }
            }
        ).as_map();

        test_counting_stream(count_async_map, &mut context);
    }

    fn test_counting_stream(
        provider: impl Provider<Request = u32, Response = u32, Streams = StreamOf<u32>>,
        context: &mut TestingContext,
    ) {
        let mut recipient = context.command(|commands| {
            commands.request(10, provider).take()
        });

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some_and(|v| v == 10));
        let stream: Vec<u32> = recipient.streams.into_iter().map(|v| v.0).collect();
        assert_eq!(stream, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(context.no_unhandled_errors());
    }

    type FormatStreams = (StreamOf<u32>, StreamOf<i32>, StreamOf<f32>);
    #[test]
    fn test_tuple_stream() {
        let mut context = TestingContext::minimal_plugins();

        let parse_blocking_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): BlockingServiceInput<String, FormatStreams>| {
                    impl_formatting_streams_blocking(input.request, input.streams);
                }
            )
        });

        test_formatting_stream(parse_blocking_srv, &mut context);

        let parse_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<String, FormatStreams>| {
                    async move {
                        impl_formatting_streams_async(input.request, input.streams);
                    }
                }
            )
        });

        test_formatting_stream(parse_async_srv, &mut context);

        let parse_blocking_callback = (
            |In(input): BlockingCallbackInput<String, FormatStreams>| {
                impl_formatting_streams_blocking(input.request, input.streams);
            }
        ).as_callback();

        test_formatting_stream(parse_blocking_callback, &mut context);

        let parse_async_callback = (
            |In(input): AsyncCallbackInput<String, FormatStreams>| {
                async move {
                    impl_formatting_streams_async(input.request, input.streams);
                }
            }
        ).as_callback();

        test_formatting_stream(parse_async_callback, &mut context);

        let parse_blocking_map = (
            |input: BlockingMap<String, FormatStreams>| {
                impl_formatting_streams_blocking(input.request, input.streams);
            }
        ).as_map();

        test_formatting_stream(parse_blocking_map, &mut context);

        let parse_async_map = (
            |input: AsyncMap<String, FormatStreams>| {
                async move {
                    impl_formatting_streams_async(input.request, input.streams);
                }
            }
        ).as_map();

        test_formatting_stream(parse_async_map, &mut context);
    }

    fn impl_formatting_streams_blocking(
        request: String,
        streams: <FormatStreams as StreamPack>::Buffer,
    ) {
        if let Ok(value) = request.parse::<u32>() {
            streams.0.send(StreamOf(value));
        }

        if let Ok(value) = request.parse::<i32>() {
            streams.1.send(StreamOf(value));
        }

        if let Ok(value) = request.parse::<f32>() {
            streams.2.send(StreamOf(value));
        }
    }

    fn impl_formatting_streams_async(
        request: String,
        streams: <FormatStreams as StreamPack>::Channel,
    ) {
        if let Ok(value) = request.parse::<u32>() {
            streams.0.send(StreamOf(value));
        }

        if let Ok(value) = request.parse::<i32>() {
            streams.1.send(StreamOf(value));
        }

        if let Ok(value) = request.parse::<f32>() {
            streams.2.send(StreamOf(value));
        }
    }

    fn test_formatting_stream(
        provider: impl Provider<Request = String, Response = (), Streams = FormatStreams> + Clone,
        context: &mut TestingContext,
    ) {
        let mut recipient = context.command(|commands| {
            commands.request("5".to_owned(), provider.clone()).take()
        });

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, [5]);
        assert_eq!(outcome.stream_i32, [5]);
        assert_eq!(outcome.stream_f32, [5.0]);

        let mut recipient = context.command(|commands| {
            commands.request("-2".to_owned(), provider.clone()).take()
        });

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert_eq!(outcome.stream_i32, [-2]);
        assert_eq!(outcome.stream_f32, [-2.0]);

        let mut recipient = context.command(|commands| {
            commands.request("6.7".to_owned(), provider.clone()).take()
        });

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert!(outcome.stream_i32.is_empty());
        assert_eq!(outcome.stream_f32, [6.7]);

        let mut recipient = context.command(|commands| {
            commands.request("hello".to_owned(), provider.clone()).take()
        });

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert!(outcome.stream_i32.is_empty());
        assert!(outcome.stream_f32.is_empty());
    }

    struct FormatOutcome {
        stream_u32: Vec<u32>,
        stream_i32: Vec<i32>,
        stream_f32: Vec<f32>,
    }

    impl From<Recipient<(), FormatStreams>> for FormatOutcome {
        fn from(recipient: Recipient<(), FormatStreams>) -> Self {
            Self {
                stream_u32: recipient.streams.0.into_iter().map(|v| v.0).collect(),
                stream_i32: recipient.streams.1.into_iter().map(|v| v.0).collect(),
                stream_f32: recipient.streams.2.into_iter().map(|v| v.0).collect()
            }
        }
    }
}
