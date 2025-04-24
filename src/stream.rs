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

use bevy_ecs::{
    prelude::{Commands, Component, Entity, World},
    system::Command,
};
use bevy_hierarchy::BuildChildren;
pub use bevy_impulse_derive::Stream;
use bevy_utils::all_tuples;

use tokio::sync::mpsc::unbounded_channel;
pub use tokio::sync::mpsc::UnboundedReceiver as Receiver;

use std::{
    any::TypeId,
    borrow::Cow,
    cell::RefCell,
    collections::{hash_map::Entry, HashMap, HashSet},
    rc::Rc,
    sync::{Arc, OnceLock},
};

use smallvec::SmallVec;

use crate::{
    AddImpulse, AddOperation, AnonymousStreamRedirect, Broken, Builder, DeferredRoster,
    DuplicateStream, InnerChannel, InputSlot, ManageInput, OperationError, OperationResult,
    OperationRoster, OrBroken, Output, Push, RedirectScopeStream, RedirectWorkflowStream,
    ReportUnhandled, SingleInputStorage, TakenStream, UnhandledErrors, UnusedStreams, UnusedTarget,
};

mod dynamically_named_stream;
pub use dynamically_named_stream::*;

mod named_stream;
pub use named_stream::*;

mod stream_channel;
pub use stream_channel::*;

pub trait StreamEffect: 'static + Send + Sync + Sized {
    type Input: 'static + Send + Sync;
    type Output: 'static + Send + Sync;

    /// Specify a side effect that is meant to happen whenever a stream value is
    /// sent.
    fn side_effect(
        input: Self::Input,
        request: &mut StreamRequest,
    ) -> Result<Self::Output, OperationError>;
}

pub trait Stream: StreamEffect {
    type StreamChannel: Send;
    type StreamBuffer: Clone;

    fn spawn_scope_stream(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (InputSlot<Self::Input>, Output<Self::Output>) {
        let source = commands.spawn(()).id();
        let target = commands.spawn(UnusedTarget).id();
        commands.add(AddOperation::new(
            Some(in_scope),
            source,
            RedirectScopeStream::<Self>::new(target),
        ));

        (
            InputSlot::new(in_scope, source),
            Output::new(out_scope, target),
        )
    }

    fn spawn_workflow_stream(builder: &mut Builder) -> InputSlot<Self::Input>;

    fn spawn_node_stream(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> Output<Self::Output>;

    fn take_stream(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Receiver<Self::Output> {
        let (sender, receiver) = unbounded_channel::<Self::Output>();
        let target = commands
            .spawn(())
            // Set the parent of this stream to be the impulse so it can be
            // recursively despawned together.
            .set_parent(source)
            .id();

        map.add_anonymous::<Self::Output>(target, commands);
        commands.add(AddImpulse::new(target, TakenStream::new(sender)));

        receiver
    }

    fn collect_stream(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) {
        let redirect = commands.spawn(()).set_parent(source).id();
        commands.add(AddImpulse::new(redirect, Push::<Self>::new(target, true)));
        map.add_anonymous::<Self::Output>(redirect, commands);
    }

    fn make_stream_channel(inner: &Arc<InnerChannel>, world: &World) -> Self::StreamChannel;

    fn make_stream_buffer(target_map: Option<&StreamTargetMap>) -> Self::StreamBuffer;

    fn process_stream_buffer(
        buffer: Self::StreamBuffer,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;

    fn defer_buffer(
        buffer: Self::StreamBuffer,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    );
}

/// A wrapper to make an anonymous (unnamed) stream for any type `T` that
/// implements `'static + Send + Sync`. This simply transmits the `T` with no
/// side-effects.
pub struct StreamOf<T: 'static + Send + Sync>(std::marker::PhantomData<fn(T)>);

impl<T: 'static + Send + Sync> Clone for StreamOf<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: 'static + Send + Sync> Copy for StreamOf<T> {}

impl<T: 'static + Send + Sync> std::fmt::Debug for StreamOf<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        static NAME: OnceLock<String> = OnceLock::new();
        let name = NAME.get_or_init(|| format!("StreamOf<{}>", std::any::type_name::<T>(),));

        f.debug_struct(name.as_str()).finish()
    }
}

pub type DefaultStreamContainer<T> = SmallVec<[T; 16]>;

impl<T: 'static + Send + Sync> StreamEffect for StreamOf<T> {
    type Input = T;
    type Output = T;

    fn side_effect(
        input: Self::Input,
        _: &mut StreamRequest,
    ) -> Result<Self::Output, OperationError> {
        Ok(input)
    }
}

impl<T: 'static + Send + Sync> Stream for StreamOf<T> {
    type StreamChannel = StreamChannel<Self>;
    type StreamBuffer = StreamBuffer<T>;

    fn spawn_workflow_stream(builder: &mut Builder) -> InputSlot<Self::Input> {
        AnonymousStream::<StreamOf<T>>::spawn_workflow_stream(builder)
    }

    fn spawn_node_stream(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> Output<Self::Output> {
        AnonymousStream::<StreamOf<T>>::spawn_node_stream(source, map, builder)
    }

    fn make_stream_channel(inner: &Arc<InnerChannel>, world: &World) -> Self::StreamChannel {
        AnonymousStream::<StreamOf<T>>::make_stream_channel(inner, world)
    }

    fn make_stream_buffer(target_map: Option<&StreamTargetMap>) -> Self::StreamBuffer {
        AnonymousStream::<StreamOf<T>>::make_stream_buffer(target_map)
    }

    fn process_stream_buffer(
        buffer: Self::StreamBuffer,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        AnonymousStream::<StreamOf<T>>::process_stream_buffer(
            buffer, source, session, unused, world, roster,
        )
    }

    fn defer_buffer(
        buffer: Self::StreamBuffer,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    ) {
        AnonymousStream::<StreamOf<T>>::defer_buffer(buffer, source, session, commands);
    }
}

/// A wrapper to turn any [`StreamEffect`] into an anonymous (unnamed) stream.
/// This should be used if you want a stream with the same behavior as [`StreamOf`]
/// but with some additional side effect. The input and output data types of the
/// stream may be different.
pub struct AnonymousStream<S: StreamEffect>(std::marker::PhantomData<fn(S)>);

impl<S: StreamEffect> StreamEffect for AnonymousStream<S> {
    type Input = S::Input;
    type Output = S::Output;
    fn side_effect(
        input: Self::Input,
        request: &mut StreamRequest,
    ) -> Result<Self::Output, OperationError> {
        S::side_effect(input, request)
    }
}

impl<S: StreamEffect> Stream for AnonymousStream<S> {
    type StreamChannel = StreamChannel<S>;
    type StreamBuffer = StreamBuffer<S::Input>;

    fn spawn_workflow_stream(builder: &mut Builder) -> InputSlot<Self::Input> {
        let source = builder.commands.spawn(()).id();
        builder.commands.add(AddOperation::new(
            Some(builder.scope()),
            source,
            RedirectWorkflowStream::new(AnonymousStreamRedirect::<Self>::new(None)),
        ));
        InputSlot::new(builder.scope, source)
    }

    fn spawn_node_stream(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> Output<Self::Output> {
        let target = builder
            .commands
            .spawn((SingleInputStorage::new(source), UnusedTarget))
            .id();

        map.add_anonymous::<Self::Output>(target, builder.commands());
        Output::new(builder.scope, target)
    }

    fn make_stream_channel(inner: &Arc<InnerChannel>, world: &World) -> Self::StreamChannel {
        let target = world
            .get::<StreamTargetMap>(inner.source())
            .and_then(|t| t.get_anonymous::<S::Output>());
        StreamChannel::new(target, Arc::clone(inner))
    }

    fn make_stream_buffer(target_map: Option<&StreamTargetMap>) -> Self::StreamBuffer {
        let target = target_map.and_then(|map| map.get_anonymous::<S::Output>());

        StreamBuffer {
            container: Default::default(),
            target,
        }
    }

    fn process_stream_buffer(
        buffer: Self::StreamBuffer,
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
            let mut request = StreamRequest {
                source,
                session,
                target,
                world,
                roster,
            };

            Self::side_effect(data, &mut request)
                .and_then(|output| request.send_output(output))
                .report_unhandled(source, world);
        }

        if was_unused {
            unused.streams.push(std::any::type_name::<Self>());
        }

        Ok(())
    }

    fn defer_buffer(
        buffer: Self::StreamBuffer,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    ) {
        commands.add(
            SendAnonymousStreams::<S, DefaultStreamContainer<S::Input>>::new(
                buffer.container.take(),
                source,
                session,
                buffer.target,
            ),
        );
    }
}

pub struct StreamBuffer<T> {
    // TODO(@mxgrey): Consider replacing the Rc with an unsafe pointer so that
    // no heap allocation is needed each time a stream is used in a blocking
    // function.
    container: Rc<RefCell<DefaultStreamContainer<T>>>,
    target: Option<Entity>,
}

impl<Container> Clone for StreamBuffer<Container> {
    fn clone(&self) -> Self {
        Self {
            container: Rc::clone(&self.container),
            target: self.target,
        }
    }
}

impl<T> StreamBuffer<T> {
    pub fn send(&self, input: T) {
        self.container.borrow_mut().push(input);
    }

    pub fn target(&self) -> Option<Entity> {
        self.target
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

impl<'a> StreamRequest<'a> {
    pub fn send_output<T: 'static + Send + Sync>(self, output: T) -> OperationResult {
        let Self {
            session,
            target,
            world,
            roster,
            ..
        } = self;
        if let Some(target) = target {
            world
                .get_entity_mut(target)
                .or_broken()?
                .give_input(session, output, roster)?;
        }

        Ok(())
    }
}

/// [`StreamAvailability`] is a component that indicates what streams are offered by
/// a service.
#[derive(Component, Default)]
pub struct StreamAvailability {
    anonymous: HashSet<TypeId>,
    named: HashMap<Cow<'static, str>, TypeId>,
}

impl StreamAvailability {
    pub fn add_anonymous<T: 'static + Send + Sync>(&mut self) {
        self.anonymous.insert(TypeId::of::<T>());
    }

    pub fn add_named<T: 'static + Send + Sync>(
        &mut self,
        name: Cow<'static, str>,
    ) -> Result<(), TypeId> {
        match self.named.entry(name) {
            Entry::Vacant(vacant) => {
                vacant.insert(TypeId::of::<T>());
                Ok(())
            }
            Entry::Occupied(occupied) => Err(*occupied.get()),
        }
    }

    pub fn has_anonymous<T: 'static + Send + Sync>(&self) -> bool {
        self.dyn_has_anonymous(&TypeId::of::<T>())
    }

    pub fn dyn_has_anonymous(&self, target_type: &TypeId) -> bool {
        self.anonymous.contains(target_type)
    }

    pub fn has_named<T: 'static + Send + Sync>(&self, name: &str) -> bool {
        self.dyn_has_named(name, &TypeId::of::<T>())
    }

    pub fn dyn_has_named(&self, name: &str, target_type: &TypeId) -> bool {
        self.named.get(name).is_some_and(|ty| *ty == *target_type)
    }
}

/// The actual entity target of the stream is held in this component which does
/// not have any generic parameters. This means it is possible to lookup the
/// targets of the streams coming out of the node without knowing the concrete
/// type of the streams. This is crucial for being able to redirect the stream
/// targets.
#[derive(Component, Default, Clone, Debug)]
pub struct StreamTargetMap {
    pub(crate) anonymous: HashMap<TypeId, Entity>,
    pub(crate) named: HashMap<Cow<'static, str>, (TypeId, Entity)>,
}

impl StreamTargetMap {
    pub fn add_anonymous<T: 'static + Send + Sync>(
        &mut self,
        target: Entity,
        commands: &mut Commands,
    ) {
        match self.anonymous.entry(TypeId::of::<T>()) {
            Entry::Vacant(vacant) => {
                vacant.insert(target);
            }
            Entry::Occupied(_) => {
                commands.add(move |world: &mut World| {
                    world
                        .get_resource_or_insert_with(|| UnhandledErrors::default())
                        .duplicate_streams
                        .push(DuplicateStream {
                            target,
                            type_name: std::any::type_name::<T>(),
                            stream_name: None,
                        })
                });
            }
        }
    }

    pub fn add_named<T: 'static + Send + Sync>(
        &mut self,
        name: Cow<'static, str>,
        target: Entity,
        commands: &mut Commands,
    ) {
        match self.named.entry(name.clone()) {
            Entry::Vacant(vacant) => {
                vacant.insert((TypeId::of::<T>(), target));
            }
            Entry::Occupied(_) => {
                commands.add(move |world: &mut World| {
                    world
                        .get_resource_or_insert_with(|| UnhandledErrors::default())
                        .duplicate_streams
                        .push(DuplicateStream {
                            target,
                            type_name: std::any::type_name::<T>(),
                            stream_name: Some(name.clone()),
                        });
                });
            }
        }
    }

    pub fn anonymous(&self) -> &HashMap<TypeId, Entity> {
        &self.anonymous
    }

    pub fn get_anonymous<T: 'static + Send + Sync>(&self) -> Option<Entity> {
        self.anonymous.get(&TypeId::of::<T>()).copied()
    }

    pub fn get_named<T: 'static + Send + Sync>(&self, name: &str) -> Option<Entity> {
        let target_type = TypeId::of::<T>();
        self.named
            .get(name)
            .filter(|(ty, _)| *ty == target_type)
            .map(|(_, target)| *target)
    }

    pub fn get_named_or_anonymous<T: 'static + Send + Sync>(
        &self,
        name: &str,
    ) -> Option<NamedTarget> {
        self.get_named::<T>(name)
            .map(NamedTarget::Value)
            .or_else(|| {
                self.get_anonymous::<NamedValue<T>>()
                    .map(NamedTarget::NamedValue)
            })
            .or_else(|| self.get_anonymous::<T>().map(NamedTarget::Value))
    }
}

/// The `StreamPack` trait defines the interface for a pack of streams. Each
/// [`Provider`](crate::Provider) can provide zero, one, or more streams of data
/// that may be sent out while it's running. The `StreamPack` allows those
/// streams to be packed together as one generic argument.
pub trait StreamPack: 'static + Send + Sync {
    type StreamInputPack;
    type StreamOutputPack;
    type StreamReceivers: Send + Sync;
    type StreamChannels: Send;
    type StreamBuffers: Clone;

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
    ) -> Self::StreamOutputPack;

    fn take_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Self::StreamReceivers;

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    );

    fn make_stream_channels(inner: &Arc<InnerChannel>, world: &World) -> Self::StreamChannels;

    fn make_stream_buffers(target_map: Option<&StreamTargetMap>) -> Self::StreamBuffers;

    fn process_stream_buffers(
        buffer: Self::StreamBuffers,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;

    fn defer_buffers(
        buffer: Self::StreamBuffers,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    );

    fn set_stream_availability(availability: &mut StreamAvailability);

    fn are_streams_available(availability: &StreamAvailability) -> bool;

    /// Are there actually any streams in the pack?
    fn has_streams() -> bool;

    fn insert_types(types: &mut HashSet<TypeId>);
}

impl<T: Stream + Unpin> StreamPack for T {
    type StreamInputPack = InputSlot<<Self as StreamEffect>::Input>;
    type StreamOutputPack = Output<<Self as StreamEffect>::Output>;
    type StreamReceivers = Receiver<<Self as StreamEffect>::Output>;
    type StreamChannels = <Self as Stream>::StreamChannel;
    type StreamBuffers = <Self as Stream>::StreamBuffer;

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
    ) -> Self::StreamOutputPack {
        T::spawn_node_stream(source, map, builder)
    }

    fn take_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Self::StreamReceivers {
        Self::take_stream(source, map, commands)
    }

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) {
        Self::collect_stream(source, target, map, commands)
    }

    fn make_stream_channels(inner: &Arc<InnerChannel>, world: &World) -> Self::StreamChannels {
        Self::make_stream_channel(inner, world)
    }

    fn make_stream_buffers(target_map: Option<&StreamTargetMap>) -> Self::StreamBuffers {
        Self::make_stream_buffer(target_map)
    }

    fn process_stream_buffers(
        buffer: Self::StreamBuffers,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        Self::process_stream_buffer(buffer, source, session, unused, world, roster)
    }

    fn defer_buffers(
        buffer: Self::StreamBuffers,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    ) {
        Self::defer_buffer(buffer, source, session, commands);
    }

    fn set_stream_availability(availability: &mut StreamAvailability) {
        availability.add_anonymous::<<Self as StreamEffect>::Output>();
    }

    fn are_streams_available(availability: &StreamAvailability) -> bool {
        availability.has_anonymous::<<Self as StreamEffect>::Output>()
    }

    fn has_streams() -> bool {
        true
    }

    fn insert_types(types: &mut HashSet<TypeId>) {
        types.insert(TypeId::of::<T>());
    }
}

impl StreamPack for () {
    type StreamInputPack = ();
    type StreamOutputPack = ();
    type StreamReceivers = ();
    type StreamChannels = ();
    type StreamBuffers = ();

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
    ) -> Self::StreamOutputPack {
        // Just return ()
    }

    fn take_streams(_: Entity, _: &mut StreamTargetMap, _: &mut Commands) -> Self::StreamReceivers {
        // Just return ()
    }

    fn collect_streams(_: Entity, _: Entity, _: &mut StreamTargetMap, _: &mut Commands) {
        // Do nothing
    }

    fn make_stream_channels(_: &Arc<InnerChannel>, _: &World) -> Self::StreamChannels {
        // Just return ()
    }

    fn make_stream_buffers(_: Option<&StreamTargetMap>) -> Self::StreamBuffers {
        // Just return ()
    }

    fn process_stream_buffers(
        _: Self::StreamBuffers,
        _: Entity,
        _: Entity,
        _: &mut UnusedStreams,
        _: &mut World,
        _: &mut OperationRoster,
    ) -> OperationResult {
        Ok(())
    }

    fn defer_buffers(_: Self::StreamBuffers, _: Entity, _: Entity, _: &mut Commands) {}

    fn set_stream_availability(_: &mut StreamAvailability) {
        // Do nothing
    }

    fn are_streams_available(_: &StreamAvailability) -> bool {
        true
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
            type StreamInputPack = ($($T::StreamInputPack,)*);
            type StreamOutputPack = ($($T::StreamOutputPack,)*);
            type StreamReceivers = ($($T::StreamReceivers,)*);
            type StreamChannels = ($($T::StreamChannels,)*);
            type StreamBuffers = ($($T::StreamBuffers,)*);

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
                    $(
                        $T::spawn_workflow_streams(builder),
                    )*
                 )
            }

            fn spawn_node_streams(
                source: Entity,
                map: &mut StreamTargetMap,
                builder: &mut Builder,
            ) -> Self::StreamOutputPack {
                (
                    $(
                        $T::spawn_node_streams(source, map, builder),
                    )*
                )
            }

            fn take_streams(source: Entity, map: &mut StreamTargetMap, builder: &mut Commands) -> Self::StreamReceivers {
                (
                    $(
                        $T::take_streams(source, map, builder),
                    )*
                )
            }

            fn collect_streams(
                source: Entity,
                target: Entity,
                map: &mut StreamTargetMap,
                commands: &mut Commands,
            ) {
                $(
                    $T::collect_streams(source, target, map, commands);
                )*
            }

            fn make_stream_channels(
                inner: &Arc<InnerChannel>,
                world: &World,
            ) -> Self::StreamChannels {
                (
                    $(
                        $T::make_stream_channels(inner, world),
                    )*
                )
            }

            fn make_stream_buffers(
                target_map: Option<&StreamTargetMap>,
            ) -> Self::StreamBuffers {
                (
                    $(
                        $T::make_stream_buffers(target_map),
                    )*
                )
            }

            fn process_stream_buffers(
                buffer: Self::StreamBuffers,
                source: Entity,
                session: Entity,
                unused: &mut UnusedStreams,
                world: &mut World,
                roster: &mut OperationRoster,
            ) -> OperationResult {
                let ($($T,)*) = buffer;
                $(
                    $T::process_stream_buffers($T, source, session, unused, world, roster)?;
                )*
                Ok(())
            }

            fn defer_buffers(
                buffer: Self::StreamBuffers,
                source: Entity,
                session: Entity,
                commands: &mut Commands,
            ) {
                let ($($T,)*) = buffer;
                $(
                    $T::defer_buffers($T, source, session, commands);
                )*
            }

            fn set_stream_availability(availability: &mut StreamAvailability) {
                $(
                    $T::set_stream_availability(availability);
                )*
            }

            fn are_streams_available(availability: &StreamAvailability) -> bool {
                true
                $(
                    && $T::are_streams_available(availability)
                )*
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

pub(crate) fn make_stream_buffers_from_world<Streams: StreamPack>(
    source: Entity,
    world: &mut World,
) -> Result<Streams::StreamBuffers, OperationError> {
    let target_map = world.get::<StreamTargetMap>(source);
    Ok(Streams::make_stream_buffers(target_map))
}

pub struct SendAnonymousStreams<S, Container> {
    container: Container,
    source: Entity,
    session: Entity,
    target: Option<Entity>,
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S, Container> SendAnonymousStreams<S, Container> {
    pub fn new(
        container: Container,
        source: Entity,
        session: Entity,
        target: Option<Entity>,
    ) -> Self {
        Self {
            container,
            source,
            session,
            target,
            _ignore: Default::default(),
        }
    }
}

impl<S, Container> Command for SendAnonymousStreams<S, Container>
where
    S: StreamEffect,
    Container: 'static + Send + Sync + IntoIterator<Item = S::Input>,
{
    fn apply(self, world: &mut World) {
        world.get_resource_or_insert_with(DeferredRoster::default);
        world.resource_scope::<DeferredRoster, _>(|world, mut deferred| {
            for data in self.container {
                let mut request = StreamRequest {
                    source: self.source,
                    session: self.session,
                    target: self.target,
                    world,
                    roster: &mut deferred,
                };

                S::side_effect(data, &mut request)
                    .and_then(move |output| request.send_output(output))
                    .report_unhandled(self.source, world);
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
    type Pack: StreamPack;
    fn are_required_streams_available(availability: Option<&StreamAvailability>) -> bool;
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
    type Pack = T;
    fn are_required_streams_available(availability: Option<&StreamAvailability>) -> bool {
        let Some(availability) = availability else {
            return false;
        };
        T::are_streams_available(availability)
    }
}

impl<T: StreamPack> StreamFilter for Option<T> {
    type Pack = T;

    fn are_required_streams_available(_: Option<&StreamAvailability>) -> bool {
        true
    }
}

macro_rules! impl_streamfilter_for_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: StreamFilter),*> StreamFilter for ($($T,)*) {
            type Pack = ($($T::Pack,)*);

            fn are_required_streams_available(_availability: Option<&StreamAvailability>) -> bool {
                true
                $(
                    && $T::are_required_streams_available(_availability)
                )*
            }
        }
    }
}

// Implements the `StreamFilter` trait for all tuples between size 0 and 12
// (inclusive) made of types that implement `StreamFilter`
all_tuples!(impl_streamfilter_for_tuple, 0, 12, T);

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*, StreamBuffer};

    #[test]
    fn test_single_stream() {
        let mut context = TestingContext::minimal_plugins();

        let count_blocking_srv = context.command(|commands| {
            commands.spawn_service(|In(input): BlockingServiceInput<u32, StreamOf<u32>>| {
                for i in 0..input.request {
                    input.streams.send(i);
                }
                return input.request;
            })
        });

        test_counting_stream(count_blocking_srv, &mut context);

        let count_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<u32, StreamOf<u32>>| async move {
                    for i in 0..input.request {
                        input.streams.send(i);
                    }
                    return input.request;
                },
            )
        });

        test_counting_stream(count_async_srv, &mut context);

        let count_blocking_callback = (|In(input): BlockingCallbackInput<u32, StreamOf<u32>>| {
            for i in 0..input.request {
                input.streams.send(i);
            }
            return input.request;
        })
        .as_callback();

        test_counting_stream(count_blocking_callback, &mut context);

        let count_async_callback =
            (|In(input): AsyncCallbackInput<u32, StreamOf<u32>>| async move {
                for i in 0..input.request {
                    input.streams.send(i);
                }
                return input.request;
            })
            .as_callback();

        test_counting_stream(count_async_callback, &mut context);

        let count_blocking_map = (|input: BlockingMap<u32, StreamOf<u32>>| {
            for i in 0..input.request {
                input.streams.send(i);
            }
            return input.request;
        })
        .as_map();

        test_counting_stream(count_blocking_map, &mut context);

        let count_async_map = (|input: AsyncMap<u32, StreamOf<u32>>| async move {
            for i in 0..input.request {
                input.streams.send(i);
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
            stream.push(r);
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
        streams: <FormatStreams as StreamPack>::StreamBuffers,
    ) {
        if let Ok(value) = request.parse::<u32>() {
            streams.0.send(value);
        }

        if let Ok(value) = request.parse::<i32>() {
            streams.1.send(value);
        }

        if let Ok(value) = request.parse::<f32>() {
            streams.2.send(value);
        }
    }

    fn impl_formatting_streams_async(
        request: String,
        streams: <FormatStreams as StreamPack>::StreamChannels,
    ) {
        if let Ok(value) = request.parse::<u32>() {
            streams.0.send(value);
        }

        if let Ok(value) = request.parse::<i32>() {
            streams.1.send(value);
        }

        if let Ok(value) = request.parse::<f32>() {
            streams.2.send(value);
        }
    }

    fn impl_formatting_streams_continuous(
        In(ContinuousService { key }): In<ContinuousService<String, (), FormatStreams>>,
        mut param: ContinuousQuery<String, (), FormatStreams>,
    ) {
        param.get_mut(&key).unwrap().for_each(|order| {
            if let Ok(value) = order.request().parse::<u32>() {
                order.streams().0.send(value);
            }

            if let Ok(value) = order.request().parse::<i32>() {
                order.streams().1.send(value);
            }

            if let Ok(value) = order.request().parse::<f32>() {
                order.streams().2.send(value);
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
                result.stream_u32.push(r);
            }

            while let Ok(r) = recipient.streams.1.try_recv() {
                result.stream_i32.push(r);
            }

            while let Ok(r) = recipient.streams.2.try_recv() {
                result.stream_f32.push(r);
            }

            result
        }
    }

    use crate::{Receiver, StreamAvailability, StreamChannel};

    struct TestStreamMap {
        stream_u32: StreamOf<u32>,
        stream_i32: StreamOf<i32>,
        stream_string: StreamOf<String>,
    }

    impl StreamPack for TestStreamMap {
        type StreamInputPack = TestStreamMapInputs;
        type StreamOutputPack = TestStreamMapOutputs;
        type StreamReceivers = TestStreamMapReceivers;
        type StreamChannels = TestStreamMapChannels;
        type StreamBuffers = TestStreamMapBuffers;

        fn spawn_scope_streams(
            in_scope: bevy_impulse::re_exports::Entity,
            out_scope: bevy_impulse::re_exports::Entity,
            commands: &mut bevy_impulse::re_exports::Commands,
        ) -> (Self::StreamInputPack, Self::StreamOutputPack) {
            let (input_stream_u32, output_stream_u32) = StreamOf::<u32>::spawn_scope_stream(in_scope, out_scope, commands);
            let (input_stream_i32, output_stream_i32) = StreamOf::<i32>::spawn_scope_stream(in_scope, out_scope, commands);
            let (input_stream_string, output_stream_string) = StreamOf::<String>::spawn_scope_stream(in_scope, out_scope, commands);

            (
                TestStreamMapInputs {
                    stream_u32: input_stream_u32,
                    stream_i32: input_stream_i32,
                    stream_string: input_stream_string,
                },
                TestStreamMapOutputs {
                    stream_u32: output_stream_u32,
                    stream_i32: output_stream_i32,
                    stream_string: output_stream_string,
                }
            )
        }

        fn spawn_workflow_streams(builder: &mut bevy_impulse::Builder) -> Self::StreamInputPack {
            TestStreamMapInputs {
                stream_u32: StreamOf::<u32>::spawn_workflow_streams(builder),
                stream_i32: StreamOf::<i32>::spawn_workflow_streams(builder),
                stream_string: StreamOf::<String>::spawn_workflow_streams(builder),
            }
        }

        fn spawn_node_streams(
            source: bevy_impulse::re_exports::Entity,
            map: &mut bevy_impulse::StreamTargetMap,
            builder: &mut bevy_impulse::Builder,
        ) -> Self::StreamOutputPack {
            TestStreamMapOutputs {
                stream_u32: StreamOf::<u32>::spawn_node_stream(source, map, builder),
                stream_i32: StreamOf::<i32>::spawn_node_stream(source, map, builder),
                stream_string: StreamOf::<String>::spawn_node_stream(source, map, builder),
            }
        }
    }

    struct TestStreamMapInputs {
        stream_u32: InputSlot<u32>,
        stream_i32: InputSlot<i32>,
        stream_string: InputSlot<String>,
    }

    struct TestStreamMapOutputs {
        stream_u32: Output<u32>,
        stream_i32: Output<i32>,
        stream_string: Output<String>,
    }

    struct TestStreamMapChannels {
        stream_u32: StreamChannel<StreamOf<u32>>,
        stream_i32: StreamChannel<StreamOf<i32>>,
        stream_string: StreamChannel<StreamOf<String>>,
    }

    struct TestStreamMapReceivers {
        stream_u32: Receiver<u32>,
        stream_i32: Receiver<i32>,
        stream_string: Receiver<String>,
    }

    #[derive(Clone)]
     struct TestStreamMapBuffers {
        stream_u32: StreamBuffer<u32>,
        stream_i32: StreamBuffer<i32>,
        stream_string: StreamBuffer<String>,
     }
}
