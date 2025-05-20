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

use bevy_derive::{Deref, DerefMut};
use bevy_ecs::{
    hierarchy::ChildOf,
    prelude::{Bundle, Command, Commands, Component, Entity, With, World},
    query::{QueryData, QueryFilter, ReadOnlyQueryData},
};
pub use bevy_impulse_derive::Stream;
use futures::{future::BoxFuture, join};
use variadics_please::all_tuples;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver as Receiver};

use std::{
    any::TypeId,
    cell::RefCell,
    collections::HashSet,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};

use smallvec::SmallVec;

use crate::{
    AddImpulse, AddOperation, Broken, Builder, DeferredRoster, InnerChannel, InputSlot,
    ManageInput, OperationError, OperationResult, OperationRoster, OrBroken, Output, Push,
    RedirectScopeStream, RedirectWorkflowStream, SingleInputStorage, StreamChannel, TakenStream,
    UnhandledErrors, UnusedStreams, UnusedTarget,
};

pub trait Stream: 'static + Send + Sync + Sized {
    type Container: IntoIterator<Item = Self> + Extend<Self> + Default + 'static + Send + Sync;

    fn send(
        self,
        StreamRequest {
            session,
            target,
            world,
            roster,
            ..
        }: StreamRequest,
    ) -> OperationResult {
        if let Some(target) = target {
            world
                .get_entity_mut(target)
                .or_broken()?
                .give_input(session, self, roster)?;
        }

        Ok(())
    }

    fn spawn_scope_stream(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (InputSlot<Self>, Output<Self>) {
        let source = commands.spawn(()).id();
        let target = commands.spawn(UnusedTarget).id();
        commands.queue(AddOperation::new(
            Some(in_scope),
            source,
            RedirectScopeStream::<Self>::new(target),
        ));

        (
            InputSlot::new(in_scope, source),
            Output::new(out_scope, target),
        )
    }

    fn spawn_workflow_stream(builder: &mut Builder) -> InputSlot<Self> {
        let source = builder.commands.spawn(()).id();
        builder.commands.queue(AddOperation::new(
            Some(builder.scope()),
            source,
            RedirectWorkflowStream::<Self>::new(),
        ));
        InputSlot::new(builder.scope, source)
    }

    fn spawn_node_stream(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (StreamTargetStorage<Self>, Output<Self>) {
        let target = builder
            .commands
            .spawn((SingleInputStorage::new(source), UnusedTarget))
            .id();

        let index = map.add(target);
        (
            StreamTargetStorage::new(index),
            Output::new(builder.scope, target),
        )
    }

    fn extract_target_storage(
        source: Entity,
        world: &World,
    ) -> Result<StreamTargetStorage<Self>, OperationError> {
        world
            .get::<StreamTargetStorage<Self>>(source)
            .or_broken()
            .cloned()
    }

    fn take_stream(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> (StreamTargetStorage<Self>, Receiver<Self>) {
        let (sender, receiver) = unbounded_channel::<Self>();
        let target = commands
            .spawn(())
            // Set the parent of this stream to be the session so it can be
            // recursively despawned together.
            .insert(ChildOf(source))
            .id();

        let index = map.add(target);

        commands.queue(AddImpulse::new(target, TakenStream::new(sender)));

        (StreamTargetStorage::new(index), receiver)
    }

    fn collect_stream(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> StreamTargetStorage<Self> {
        let redirect = commands.spawn(()).insert(ChildOf(source)).id();
        commands.queue(AddImpulse::new(redirect, Push::<Self>::new(target, true)));
        let index = map.add(redirect);
        StreamTargetStorage::new(index)
    }
}

/// A simple newtype wrapper that turns any suitable data structure
/// (`'static + Send + Sync`) into a stream.
#[derive(Clone, Copy, Debug, Deref, DerefMut)]
pub struct StreamOf<T: 'static + Send + Sync>(pub T);

pub type DefaultStreamContainer<T> = SmallVec<[T; 16]>;

impl<T: 'static + Send + Sync> Stream for StreamOf<T> {
    type Container = DefaultStreamContainer<Self>;
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
            target: self.target,
        }
    }
}

impl<T: Stream> StreamBuffer<T> {
    pub fn send(&self, data: T) {
        self.container.borrow_mut().extend([data]);
    }

    pub fn target(&self) -> Option<Entity> {
        self.target
    }
}

/// Used to forward the data from a stream receiver into a different
/// [`StreamChannel`].
pub struct StreamForward<T: Stream> {
    receiver: Receiver<T>,
    channel: StreamChannel<T>,
}

impl<T: Stream + Unpin> Future for StreamForward<T> {
    /// This will keep forwarding the streams until the upstream sender
    /// disconnects, so there is nothing of interest to return from the future.
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_mut = self.get_mut();
        loop {
            // We loop because even if there are multiple values available, the
            // poll will only be triggered once.
            match self_mut.receiver.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(result)) => {
                    self_mut.channel.send(result);
                }
                Poll::Ready(None) => {
                    // This channel has expired so we should indicate that we are
                    // finished here
                    return Poll::Ready(());
                }
            }
        }
    }
}

/// Used by [`StreamPack`] to implement the `StreamPack::forward_channels` method
/// for an empty tuple.
pub struct NoopForward;

impl Future for NoopForward {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(())
    }
}

/// Used by [`StreamPack`] to implement the `StreamPack::forward_channels` method
/// for a non-empty tuple.
pub struct StreamPackForward {
    inner: BoxFuture<'static, ()>,
}

impl Future for StreamPackForward {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.as_mut().poll(cx)
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
    _ignore: std::marker::PhantomData<fn(T)>,
}

impl<T: Stream> Default for StreamAvailable<T> {
    fn default() -> Self {
        Self {
            _ignore: Default::default(),
        }
    }
}

/// [`StreamTargetStorage`] keeps track of the target for each stream for a source.
#[derive(Component)]
pub struct StreamTargetStorage<T: Stream> {
    index: usize,
    _ignore: std::marker::PhantomData<fn(T)>,
}

impl<T: Stream> Clone for StreamTargetStorage<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Stream> Copy for StreamTargetStorage<T> {}

impl<T: Stream> StreamTargetStorage<T> {
    fn new(index: usize) -> Self {
        Self {
            index,
            _ignore: Default::default(),
        }
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
#[derive(Component, Default, Clone, Debug)]
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
    type StreamFilter: QueryFilter;
    type StreamStorageBundle: Bundle + Clone;
    type StreamInputPack;
    type StreamOutputPack;
    type Receiver: Send + Sync;
    type Channel: Send;
    type Forward: Future<Output = ()> + Send;
    type Buffer: Clone;
    type TargetIndexQuery: ReadOnlyQueryData;

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (Self::StreamInputPack, Self::StreamOutputPack);

    fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack;

    fn spawn_node_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (Self::StreamStorageBundle, Self::StreamOutputPack);

    fn extract_target_storage(
        source: Entity,
        world: &World,
    ) -> Result<Self::StreamStorageBundle, OperationError>;

    fn take_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Commands,
    ) -> (Self::StreamStorageBundle, Self::Receiver);

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Self::StreamStorageBundle;

    fn make_channel(inner: &Arc<InnerChannel>, world: &World) -> Self::Channel;

    fn make_buffer(
        target_index: <Self::TargetIndexQuery as QueryData>::Item<'_>,
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

    fn defer_buffer(buffer: Self::Buffer, source: Entity, session: Entity, commands: &mut Commands);

    fn forward_channels(receiver: Self::Receiver, channel: Self::Channel) -> Self::Forward;

    /// Are there actually any streams in the pack?
    fn has_streams() -> bool;

    fn insert_types(types: &mut HashSet<TypeId>);
}

impl<T: Stream + Unpin> StreamPack for T {
    type StreamAvailableBundle = StreamAvailable<Self>;
    type StreamFilter = With<StreamAvailable<Self>>;
    type StreamStorageBundle = StreamTargetStorage<Self>;
    type StreamInputPack = InputSlot<Self>;
    type StreamOutputPack = Output<Self>;
    type Receiver = Receiver<Self>;
    type Channel = StreamChannel<Self>;
    type Forward = StreamForward<Self>;
    type Buffer = StreamBuffer<Self>;
    type TargetIndexQuery = Option<&'static StreamTargetStorage<Self>>;

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (Self::StreamInputPack, Self::StreamOutputPack) {
        T::spawn_scope_stream(in_scope, out_scope, commands)
    }

    fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack {
        T::spawn_workflow_stream(builder)
    }

    fn spawn_node_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> (Self::StreamStorageBundle, Self::StreamOutputPack) {
        T::spawn_node_stream(source, map, builder)
    }

    fn extract_target_storage(
        source: Entity,
        world: &World,
    ) -> Result<Self::StreamStorageBundle, OperationError> {
        T::extract_target_storage(source, world)
    }

    fn take_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> (Self::StreamStorageBundle, Self::Receiver) {
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

    fn make_channel(inner: &Arc<InnerChannel>, world: &World) -> Self::Channel {
        let index = world
            .get::<StreamTargetStorage<Self>>(inner.source())
            .map(|t| t.index);
        let target = index.and_then(|index| {
            world
                .get::<StreamTargetMap>(inner.source())
                .and_then(|t| t.get(index))
        });
        StreamChannel::new(target, Arc::clone(inner))
    }

    fn make_buffer(
        target_index: Option<&StreamTargetStorage<Self>>,
        target_map: Option<&StreamTargetMap>,
    ) -> Self::Buffer {
        let target = target_index
            .and_then(|s| target_map.map(|t| t.map.get(s.index)))
            .flatten()
            .copied();

        StreamBuffer {
            container: Default::default(),
            target,
        }
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
        for data in Rc::into_inner(buffer.container)
            .or_broken()?
            .into_inner()
            .into_iter()
        {
            was_unused = false;
            data.send(StreamRequest {
                source,
                session,
                target,
                world,
                roster,
            })?;
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
        commands.queue(SendStreams::<Self> {
            source,
            session,
            container: buffer.container.take(),
            target: buffer.target,
        });
    }

    fn forward_channels(receiver: Self::Receiver, channel: Self::Channel) -> Self::Forward {
        StreamForward { receiver, channel }
    }

    fn has_streams() -> bool {
        true
    }

    fn insert_types(types: &mut HashSet<TypeId>) {
        types.insert(TypeId::of::<T>());
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
    type Forward = NoopForward;
    type Buffer = ();
    type TargetIndexQuery = ();

    fn spawn_scope_streams(
        _: Entity,
        _: Entity,
        _: &mut Commands,
    ) -> (Self::StreamInputPack, Self::StreamOutputPack) {
        ((), ())
    }

    fn spawn_workflow_streams(_: &mut Builder) -> Self::StreamInputPack {
        // Just return ()
    }

    fn spawn_node_streams(
        _: Entity,
        _: &mut StreamTargetMap,
        _: &mut Builder,
    ) -> (Self::StreamStorageBundle, Self::StreamOutputPack) {
        ((), ())
    }

    fn extract_target_storage(
        _: Entity,
        _: &World,
    ) -> Result<Self::StreamStorageBundle, OperationError> {
        Ok(())
    }

    fn take_streams(
        _: Entity,
        _: &mut StreamTargetMap,
        _: &mut Commands,
    ) -> (Self::StreamStorageBundle, Self::Receiver) {
        ((), ())
    }

    fn collect_streams(
        _: Entity,
        _: Entity,
        _: &mut StreamTargetMap,
        _: &mut Commands,
    ) -> Self::StreamStorageBundle {
        // Just return ()
    }

    fn make_channel(_: &Arc<InnerChannel>, _: &World) -> Self::Channel {
        // Just return ()
    }

    fn make_buffer(_: Self::TargetIndexQuery, _: Option<&StreamTargetMap>) -> Self::Buffer {
        // Just return ()
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

    fn defer_buffer(_: Self::Buffer, _: Entity, _: Entity, _: &mut Commands) {}

    fn forward_channels(_: Self::Receiver, _: Self::Channel) -> Self::Forward {
        NoopForward
    }

    fn has_streams() -> bool {
        false
    }

    fn insert_types(_: &mut HashSet<TypeId>) {
        // Do nothing
    }
}

macro_rules! impl_streampack_for_tuple {
    ($(($T:ident, $U:ident)),*) => {
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
            type Forward = StreamPackForward;
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
                source: Entity,
                map: &mut StreamTargetMap,
                builder: &mut Builder,
            ) -> (
                Self::StreamStorageBundle,
                Self::StreamOutputPack,
            ) {
                let ($($T,)*) = (
                    $(
                        $T::spawn_node_streams(source, map, builder),
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

            fn extract_target_storage(
                source: Entity,
                world: &World,
            ) -> Result<Self::StreamStorageBundle, OperationError> {
                Ok(($(
                    $T::extract_target_storage(source, world)?,
                )*))
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

            fn make_buffer(
                target_index: <Self::TargetIndexQuery as QueryData>::Item<'_>,
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

            fn forward_channels(
                receiver: Self::Receiver,
                channel: Self::Channel,
            ) -> Self::Forward {
                let inner = async move {
                    let ($($T,)*) = receiver;
                    let ($($U,)*) = channel;
                    join!($(
                        $T::forward_channels($T, $U),
                    )*);
                };

                StreamPackForward{ inner: Box::pin(inner) }
            }

            fn has_streams() -> bool {
                let mut has_streams = false;
                $(
                    has_streams = has_streams || $T::has_streams();
                )*
                has_streams
            }

            fn insert_types(types: &mut HashSet<TypeId>) {
                $(
                    $T::insert_types(types);
                )*
            }
        }
    }
}

// Implements the `StreamPack` trait for all tuples between size 1 and 12
// (inclusive) made of types that implement `StreamPack`
all_tuples!(impl_streampack_for_tuple, 1, 12, T, U);

pub(crate) fn make_stream_buffer_from_world<Streams: StreamPack>(
    source: Entity,
    world: &mut World,
) -> Result<Streams::Buffer, OperationError> {
    let mut stream_query =
        world.query::<(Streams::TargetIndexQuery, Option<&'static StreamTargetMap>)>();
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
        world.get_resource_or_insert_with(DeferredRoster::default);
        world.resource_scope::<DeferredRoster, _>(|world, mut deferred| {
            for data in self.container {
                let r = data.send(StreamRequest {
                    source: self.source,
                    session: self.session,
                    target: self.target,
                    world,
                    roster: &mut deferred,
                });

                if let Err(OperationError::Broken(backtrace)) = r {
                    world
                        .get_resource_or_insert_with(UnhandledErrors::default)
                        .broken
                        .push(Broken {
                            node: self.source,
                            backtrace,
                        });
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
/// use bevy_impulse::{Require, prelude::*, testing::*};
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
    type Filter: QueryFilter;
    type Pack: StreamPack;
}

/// Used by [`ServiceDiscovery`](crate::ServiceDiscovery) to indicate that a
/// certain pack of streams is required.
///
/// For streams that are optional, wrap them in [`Option`] instead.
///
/// See [`StreamFilter`] for a usage example.
pub struct Require<T> {
    _ignore: std::marker::PhantomData<fn(T)>,
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
    use crate::{prelude::*, testing::*};

    #[test]
    fn test_single_stream() {
        let mut context = TestingContext::minimal_plugins();

        let count_blocking_srv = context.command(|commands| {
            commands.spawn_service(|In(input): BlockingServiceInput<u32, StreamOf<u32>>| {
                for i in 0..input.request {
                    input.streams.send(StreamOf(i));
                }
                return input.request;
            })
        });

        test_counting_stream(count_blocking_srv, &mut context);

        let count_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<u32, StreamOf<u32>>| async move {
                    for i in 0..input.request {
                        input.streams.send(StreamOf(i));
                    }
                    return input.request;
                },
            )
        });

        test_counting_stream(count_async_srv, &mut context);

        let count_blocking_callback = (|In(input): BlockingCallbackInput<u32, StreamOf<u32>>| {
            for i in 0..input.request {
                input.streams.send(StreamOf(i));
            }
            return input.request;
        })
        .as_callback();

        test_counting_stream(count_blocking_callback, &mut context);

        let count_async_callback =
            (|In(input): AsyncCallbackInput<u32, StreamOf<u32>>| async move {
                for i in 0..input.request {
                    input.streams.send(StreamOf(i));
                }
                return input.request;
            })
            .as_callback();

        test_counting_stream(count_async_callback, &mut context);

        let count_blocking_map = (|input: BlockingMap<u32, StreamOf<u32>>| {
            for i in 0..input.request {
                input.streams.send(StreamOf(i));
            }
            return input.request;
        })
        .as_map();

        test_counting_stream(count_blocking_map, &mut context);

        let count_async_map = (|input: AsyncMap<u32, StreamOf<u32>>| async move {
            for i in 0..input.request {
                input.streams.send(StreamOf(i));
            }
            return input.request;
        })
        .as_map();

        test_counting_stream(count_async_map, &mut context);
    }

    fn test_counting_stream(
        provider: impl Provider<Request = u32, Response = u32, Streams = StreamOf<u32>>,
        context: &mut TestingContext,
    ) {
        let mut recipient = context.command(|commands| commands.request(10, provider).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient
            .response
            .take()
            .available()
            .is_some_and(|v| v == 10));

        let mut stream: Vec<u32> = Vec::new();
        while let Ok(r) = recipient.streams.try_recv() {
            stream.push(r.0);
        }
        assert_eq!(stream, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(context.no_unhandled_errors());
    }

    type FormatStreams = (StreamOf<u32>, StreamOf<i32>, StreamOf<f32>);
    #[test]
    fn test_tuple_stream() {
        let mut context = TestingContext::minimal_plugins();

        let parse_blocking_srv = context.command(|commands| {
            commands.spawn_service(|In(input): BlockingServiceInput<String, FormatStreams>| {
                impl_formatting_streams_blocking(input.request, input.streams);
            })
        });

        test_formatting_stream(parse_blocking_srv, &mut context);

        let parse_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<String, FormatStreams>| async move {
                    impl_formatting_streams_async(input.request, input.streams);
                },
            )
        });

        test_formatting_stream(parse_async_srv, &mut context);

        let parse_continuous_srv = context
            .app
            .spawn_continuous_service(Update, impl_formatting_streams_continuous);

        test_formatting_stream(parse_continuous_srv, &mut context);

        let parse_blocking_callback =
            (|In(input): BlockingCallbackInput<String, FormatStreams>| {
                impl_formatting_streams_blocking(input.request, input.streams);
            })
            .as_callback();

        test_formatting_stream(parse_blocking_callback, &mut context);

        let parse_async_callback =
            (|In(input): AsyncCallbackInput<String, FormatStreams>| async move {
                impl_formatting_streams_async(input.request, input.streams);
            })
            .as_callback();

        test_formatting_stream(parse_async_callback, &mut context);

        let parse_blocking_map = (|input: BlockingMap<String, FormatStreams>| {
            impl_formatting_streams_blocking(input.request, input.streams);
        })
        .as_map();

        test_formatting_stream(parse_blocking_map, &mut context);

        let parse_async_map = (|input: AsyncMap<String, FormatStreams>| async move {
            impl_formatting_streams_async(input.request, input.streams);
        })
        .as_map();

        test_formatting_stream(parse_async_map, &mut context);

        let make_workflow = |service: Service<String, (), FormatStreams>| {
            move |scope: Scope<String, (), FormatStreams>, builder: &mut Builder| {
                let node = scope
                    .input
                    .chain(builder)
                    .map_block(move |value| (value, service))
                    .then_injection_node();

                builder.connect(node.streams.0, scope.streams.0);
                builder.connect(node.streams.1, scope.streams.1);
                builder.connect(node.streams.2, scope.streams.2);

                node.output.chain(builder).connect(scope.terminate);
            }
        };

        let blocking_injection_workflow = context.spawn_workflow(make_workflow(parse_blocking_srv));
        test_formatting_stream(blocking_injection_workflow, &mut context);

        let async_injection_workflow = context.spawn_workflow(make_workflow(parse_async_srv));
        test_formatting_stream(async_injection_workflow, &mut context);

        let continuous_injection_workflow =
            context.spawn_workflow(make_workflow(parse_continuous_srv));
        test_formatting_stream(continuous_injection_workflow, &mut context);

        let nested_workflow = context.spawn_workflow::<_, _, FormatStreams, _>(|scope, builder| {
            let inner_node = scope.input.chain(builder).then_node(parse_continuous_srv);
            builder.connect(inner_node.streams.0, scope.streams.0);
            builder.connect(inner_node.streams.1, scope.streams.1);
            builder.connect(inner_node.streams.2, scope.streams.2);
            builder.connect(inner_node.output, scope.terminate);
        });
        test_formatting_stream(nested_workflow, &mut context);

        let nested_workflow = context.spawn_workflow::<_, _, FormatStreams, _>(|scope, builder| {
            let inner_node = builder.create_node(parse_continuous_srv);
            builder.connect(scope.input, inner_node.input);
            builder.connect(inner_node.streams.0, scope.streams.0);
            builder.connect(inner_node.streams.1, scope.streams.1);
            builder.connect(inner_node.streams.2, scope.streams.2);
            builder.connect(inner_node.output, scope.terminate);
        });
        test_formatting_stream(nested_workflow, &mut context);
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

    fn impl_formatting_streams_continuous(
        In(ContinuousService { key }): In<ContinuousService<String, (), FormatStreams>>,
        mut param: ContinuousQuery<String, (), FormatStreams>,
    ) {
        param.get_mut(&key).unwrap().for_each(|order| {
            if let Ok(value) = order.request().parse::<u32>() {
                order.streams().0.send(StreamOf(value));
            }

            if let Ok(value) = order.request().parse::<i32>() {
                order.streams().1.send(StreamOf(value));
            }

            if let Ok(value) = order.request().parse::<f32>() {
                order.streams().2.send(StreamOf(value));
            }

            order.respond(());
        });
    }

    fn test_formatting_stream(
        provider: impl Provider<Request = String, Response = (), Streams = FormatStreams> + Clone,
        context: &mut TestingContext,
    ) {
        let mut recipient =
            context.command(|commands| commands.request("5".to_owned(), provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, [5]);
        assert_eq!(outcome.stream_i32, [5]);
        assert_eq!(outcome.stream_f32, [5.0]);

        let mut recipient =
            context.command(|commands| commands.request("-2".to_owned(), provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert_eq!(outcome.stream_i32, [-2]);
        assert_eq!(outcome.stream_f32, [-2.0]);

        let mut recipient =
            context.command(|commands| commands.request("6.7".to_owned(), provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert!(outcome.stream_i32.is_empty());
        assert_eq!(outcome.stream_f32, [6.7]);

        let mut recipient = context.command(|commands| {
            commands
                .request("hello".to_owned(), provider.clone())
                .take()
        });

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: FormatOutcome = recipient.into();
        assert!(outcome.stream_u32.is_empty());
        assert!(outcome.stream_i32.is_empty());
        assert!(outcome.stream_f32.is_empty());
    }

    #[derive(Default)]
    struct FormatOutcome {
        stream_u32: Vec<u32>,
        stream_i32: Vec<i32>,
        stream_f32: Vec<f32>,
    }

    impl From<Recipient<(), FormatStreams>> for FormatOutcome {
        fn from(mut recipient: Recipient<(), FormatStreams>) -> Self {
            let mut result = Self::default();
            while let Ok(r) = recipient.streams.0.try_recv() {
                result.stream_u32.push(r.0);
            }

            while let Ok(r) = recipient.streams.1.try_recv() {
                result.stream_i32.push(r.0);
            }

            while let Ok(r) = recipient.streams.2.try_recv() {
                result.stream_f32.push(r.0);
            }

            result
        }
    }
}
