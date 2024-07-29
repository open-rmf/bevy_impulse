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
    ecs::{
        system::Command,
        query::{ReadOnlyWorldQuery, WorldQuery},
    },
    utils::all_tuples,
};

use crossbeam::channel::{Receiver, unbounded};

use std::{rc::Rc, cell::RefCell, sync::Arc};

use smallvec::SmallVec;

use crate::{
    InputSlot, Output, UnusedTarget, RedirectWorkflowStream, RedirectScopeStream,
    AddOperation, AddImpulse, OperationRoster, OperationResult, OrBroken, ManageInput,
    InnerChannel, TakenStream, StreamChannel, Push, Builder, UnusedStreams,
    OperationError, DeferredRoster, UnhandledErrors, Broken,
};

pub trait Stream: 'static + Send + Sync + Sized {
    type Container: IntoIterator<Item = Self> + Extend<Self> + Default + 'static + Send + Sync;

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

/// The `StreamPack` trait defines the interface for a pack of streams. Each
/// [`Provider`](crate::Provider) can provide zero, one, or more streams of data
/// that may be sent out while it's running. The `StreamPack` allows those
/// streams to be packed together as one generic argument.
pub trait StreamPack: 'static + Send + Sync {
    type StreamAvailableBundle: Bundle + Default;
    type StreamFilter: ReadOnlyWorldQuery;
    type StreamStorageBundle: Bundle;
    type StreamInputPack;
    type StreamOutputPack: std::fmt::Debug;
    type Receiver;
    type Channel;
    type Buffer: Clone;
    type TargetIndexQuery: ReadOnlyWorldQuery;

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

    fn make_buffer<'a>(
        target_index: <Self::TargetIndexQuery as WorldQuery>::Item<'a>,
        target_map: Option<&StreamTargetMap>,
    ) -> Self::Buffer;

    fn process_buffer(
        buffer: Self::Buffer,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;

    fn defer_buffer(
        buffer: Self::Buffer,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    );

    /// Are there actually any streams in the pack?
    fn has_streams() -> bool;
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
    type TargetIndexQuery = Option<&'static StreamTargetStorage<Self>>;

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

    fn make_buffer<'a>(
        target_index: Option<&'a StreamTargetStorage<Self>>,
        target_map: Option<&StreamTargetMap>,
    ) -> Self::Buffer {
        let target = target_index
            .map(|s| target_map.map(|t| t.map.get(s.index))
            ).flatten().flatten().copied();

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

    fn defer_buffer(
        buffer: Self::Buffer,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    ) {
        commands.add(SendStreams::<Self> {
            source,
            session,
            container: buffer.container.take(),
            target: buffer.target,
        });
    }

    fn has_streams() -> bool {
        true
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
    type TargetIndexQuery = ();

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

    fn make_buffer(
        _: Self::TargetIndexQuery,
        _: Option<&StreamTargetMap>,
    ) -> Self::Buffer {
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

    fn defer_buffer(
        _: Self::Buffer,
        _: Entity,
        _: Entity,
        _: &mut Commands,
    ) {

    }

    fn has_streams() -> bool {
        false
    }
}

macro_rules! impl_streampack_for_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: StreamPack),*> StreamPack for ($($T,)*) {
            type StreamAvailableBundle = ($($T::StreamAvailableBundle,)*);
            type StreamFilter = ($($T::StreamFilter,)*);
            type StreamStorageBundle = ($($T::StreamStorageBundle,)*);
            type StreamInputPack = ($($T::StreamInputPack,)*);
            type StreamOutputPack = ($($T::StreamOutputPack,)*);
            type Receiver = ($($T::Receiver,)*);
            type Channel = ($($T::Channel,)*);
            type Buffer = ($($T::Buffer,)*);
            type TargetIndexQuery = ($($T::TargetIndexQuery,)*);

            fn spawn_scope_streams(
                in_scope: Entity,
                out_scope: Entity,
                commands: &mut Commands,
            ) -> (
                Self::StreamInputPack,
                Self::StreamOutputPack,
            ) {
                let ($($T,)*) = (
                    $(
                        $T::spawn_scope_streams(in_scope, out_scope, commands),
                    )*
                );
                // Now unpack the tuples
                (
                    (
                        $(
                            $T.0,
                        )*
                    ),
                    (
                        $(
                            $T.1,
                        )*
                    )
                )
            }

            fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack {
                (
                    $($T::spawn_workflow_streams(builder),
                    )*
                 )
            }

            fn spawn_node_streams(
                map: &mut StreamTargetMap,
                builder: &mut Builder,
            ) -> (
                Self::StreamStorageBundle,
                Self::StreamOutputPack,
            ) {
                let ($($T,)*) = (
                    $(
                        $T::spawn_node_streams(map, builder),
                    )*
                );
                // Now unpack the tuples
                (
                    (
                        $(
                            $T.0,
                        )*
                    ),
                    (
                        $(
                            $T.1,
                        )*
                    )
                )
            }

            fn take_streams(source: Entity, map: &mut StreamTargetMap, builder: &mut Commands) -> (
                Self::StreamStorageBundle,
                Self::Receiver,
            ) {
                let ($($T,)*) = (
                    $(
                        $T::take_streams(source, map, builder),
                    )*
                );
                // Now unpack the tuples
                (
                    (
                        $(
                            $T.0,
                        )*
                    ),
                    (
                        $(
                            $T.1,
                        )*
                    )
                )
            }

            fn collect_streams(
                source: Entity,
                target: Entity,
                map: &mut StreamTargetMap,
                commands: &mut Commands,
            ) -> Self::StreamStorageBundle {
                (
                    $(
                        $T::collect_streams(source, target, map, commands),
                    )*
                )
            }

            fn make_channel(
                inner: &Arc<InnerChannel>,
                world: &World,
            ) -> Self::Channel {
                (
                    $(
                        $T::make_channel(inner, world),
                    )*
                )
            }

            fn make_buffer<'a>(
                target_index: <Self::TargetIndexQuery as WorldQuery>::Item<'a>,
                target_map: Option<&StreamTargetMap>,
            ) -> Self::Buffer {
                let ($($T,)*) = target_index;
                (
                    $(
                        $T::make_buffer($T, target_map),
                    )*
                )
            }

            fn process_buffer(
                buffer: Self::Buffer,
                source: Entity,
                session: Entity,
                unused: &mut UnusedStreams,
                world: &mut World,
                roster: &mut OperationRoster,
            ) -> OperationResult {
                let ($($T,)*) = buffer;
                $(
                    $T::process_buffer($T, source, session, unused, world, roster)?;
                )*
                Ok(())
            }

            fn defer_buffer(
                buffer: Self::Buffer,
                source: Entity,
                session: Entity,
                commands: &mut Commands,
            ) {
                let ($($T,)*) = buffer;
                $(
                    $T::defer_buffer($T, source, session, commands);
                )*
            }

            fn has_streams() -> bool {
                let mut has_streams = false;
                $(
                    has_streams = has_streams || $T::has_streams();
                )*
                has_streams
            }
        }
    }
}

// Implements the `StreamPack` trait for all tuples between size 1 and 12
// (inclusive) made of types that implement `StreamPack`
all_tuples!(impl_streampack_for_tuple, 1, 12, T);

pub(crate) fn make_stream_buffer_from_world<Streams: StreamPack>(
    source: Entity,
    world: &mut World,
) -> Result<Streams::Buffer, OperationError> {
    let mut stream_query = world.query::<(
        Streams::TargetIndexQuery,
        Option<&'static StreamTargetMap>,
    )>();
    let (target_indices, target_map) = stream_query.get(world, source).or_broken()?;
    Ok(Streams::make_buffer(target_indices, target_map))
}

struct SendStreams<S: Stream> {
    container: S::Container,
    source: Entity,
    session: Entity,
    target: Option<Entity>,
}

impl<S: Stream> Command for SendStreams<S> {
    fn apply(self, world: &mut World) {
        world.get_resource_or_insert_with(|| DeferredRoster::default());
        world.resource_scope::<DeferredRoster, _>(|world, mut deferred| {
            for data in self.container {
                let r = data.send(StreamRequest {
                    source: self.source,
                    session: self.session,
                    target: self.target,
                    world,
                    roster: &mut *deferred,
                });

                if let Err(OperationError::Broken(backtrace)) = r {
                    world.get_resource_or_insert_with(|| UnhandledErrors::default())
                        .broken
                        .push(Broken { node: self.source, backtrace });
                }
            }
        });
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

impl<T: StreamPack> StreamFilter for Require<T> {
    type Filter = T::StreamFilter;
    type Pack = T;
}

impl<T: StreamPack> StreamFilter for Option<T> {
    type Filter = ();
    type Pack = T;
}

macro_rules! impl_streamfilter_for_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: StreamFilter),*> StreamFilter for ($($T,)*) {
            type Filter = ($($T::Filter,)*);
            type Pack = ($($T::Pack,)*);
        }
    }
}

// Implements the `StreamFilter` trait for all tuples between size 0 and 12
// (inclusive) made of types that implement `StreamFilter`
all_tuples!(impl_streamfilter_for_tuple, 0, 12, T);

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
