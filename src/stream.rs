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
    dyn_node::DynStreamPack,
    AddImpulse, AddOperation, AnonymousStreamRedirect, Builder, DeferredRoster,
    DuplicateStream, InnerChannel, InputSlot, ManageInput, OperationError, OperationResult,
    OperationRoster, OrBroken, Output, Push, RedirectScopeStream, RedirectWorkflowStream,
    ReportUnhandled, SingleInputStorage, TakenStream, UnhandledErrors, UnusedStreams, UnusedTarget,
    MissingStreamsError,
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
        commands.add(AddImpulse::new(redirect, Push::<Self::Output>::new(target, true)));
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
    named: HashMap<Cow<'static, str>, NamedAvailability>,
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
                vacant.insert(NamedAvailability::new::<T>());
                Ok(())
            }
            Entry::Occupied(occupied) => Err(occupied.get().value),
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
        self.named.get(name).is_some_and(|ty| ty.value == *target_type)
    }

    pub fn can_cast_to(&self, target: &Self) -> Result<(), MissingStreamsError> {
        let mut missing = MissingStreamsError::default();
        for anon in &self.anonymous {
            if !target.anonymous.contains(anon) {
                missing.anonymous.insert(*anon);
            }
        }

        for (name, avail) in &self.named {
            if let Some(target_avail) = target.named.get(name) {
                if avail.value != target_avail.value {
                    missing.named.insert(name.clone(), avail.value);
                }
            } else if !target.anonymous.contains(&avail.named_value) {
                missing.named.insert(name.clone(), avail.value);
            }
        }

        missing.into_result()
    }
}

#[derive(Clone, Copy)]
struct NamedAvailability {
    value: TypeId,
    named_value: TypeId,
}

impl NamedAvailability {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self {
            value: TypeId::of::<T>(),
            named_value: TypeId::of::<NamedValue<T>>(),
        }
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

    fn into_dyn_stream_pack(
        pack: &mut DynStreamPack,
        outputs: Self::StreamOutputPack
    );

    /// Are there actually any streams in the pack?
    fn has_streams() -> bool;
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

    fn into_dyn_stream_pack(
        pack: &mut DynStreamPack,
        outputs: Self::StreamOutputPack
    ) {
        pack.add_anonymous(outputs);
    }

    fn has_streams() -> bool {
        true
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

    fn into_dyn_stream_pack(
        _: &mut DynStreamPack,
        _: Self::StreamOutputPack
    ) {
        // Do nothing
    }

    fn has_streams() -> bool {
        false
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

            fn into_dyn_stream_pack(
                pack: &mut DynStreamPack,
                outputs: Self::StreamOutputPack,
            ) {
                let ($($T,)*) = outputs;
                $(
                    $T::into_dyn_stream_pack(pack, $T);
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
    use crate::{
        prelude::*,
        testing::*,
        dyn_node::*,
    };
    use std::borrow::Cow;

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

        validate_formatting_stream(parse_blocking_srv, &mut context);

        let parse_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<String, FormatStreams>| async move {
                    impl_formatting_streams_async(input.request, input.streams);
                },
            )
        });

        validate_formatting_stream(parse_async_srv, &mut context);

        let parse_continuous_srv = context
            .app
            .spawn_continuous_service(Update, impl_formatting_streams_continuous);

        validate_formatting_stream(parse_continuous_srv, &mut context);

        let parse_blocking_callback =
            (|In(input): BlockingCallbackInput<String, FormatStreams>| {
                impl_formatting_streams_blocking(input.request, input.streams);
            })
            .as_callback();

        validate_formatting_stream(parse_blocking_callback, &mut context);

        let parse_async_callback =
            (|In(input): AsyncCallbackInput<String, FormatStreams>| async move {
                impl_formatting_streams_async(input.request, input.streams);
            })
            .as_callback();

        validate_formatting_stream(parse_async_callback, &mut context);

        let parse_blocking_map = (|input: BlockingMap<String, FormatStreams>| {
            impl_formatting_streams_blocking(input.request, input.streams);
        })
        .as_map();

        validate_formatting_stream(parse_blocking_map, &mut context);

        let parse_async_map = (|input: AsyncMap<String, FormatStreams>| async move {
            impl_formatting_streams_async(input.request, input.streams);
        })
        .as_map();

        validate_formatting_stream(parse_async_map, &mut context);

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

                builder.connect(node.output, scope.terminate);
            }
        };

        let blocking_injection_workflow = context.spawn_workflow(make_workflow(parse_blocking_srv));
        validate_formatting_stream(blocking_injection_workflow, &mut context);

        let async_injection_workflow = context.spawn_workflow(make_workflow(parse_async_srv));
        validate_formatting_stream(async_injection_workflow, &mut context);

        let continuous_injection_workflow =
            context.spawn_workflow(make_workflow(parse_continuous_srv));
        validate_formatting_stream(continuous_injection_workflow, &mut context);

        let nested_workflow = context.spawn_workflow::<_, _, FormatStreams, _>(|scope, builder| {
            let inner_node = scope.input.chain(builder).then_node(continuous_injection_workflow);
            builder.connect(inner_node.streams.0, scope.streams.0);
            builder.connect(inner_node.streams.1, scope.streams.1);
            builder.connect(inner_node.streams.2, scope.streams.2);
            builder.connect(inner_node.output, scope.terminate);
        });
        validate_formatting_stream(nested_workflow, &mut context);

        let double_nested_workflow = context.spawn_workflow::<_, _, FormatStreams, _>(|scope, builder| {
            let inner_node = builder.create_node(nested_workflow);
            builder.connect(scope.input, inner_node.input);
            builder.connect(inner_node.streams.0, scope.streams.0);
            builder.connect(inner_node.streams.1, scope.streams.1);
            builder.connect(inner_node.streams.2, scope.streams.2);
            builder.connect(inner_node.output, scope.terminate);
        });
        validate_formatting_stream(double_nested_workflow, &mut context);
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

    fn validate_formatting_stream(
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

    #[test]
    fn test_stream_pack() {
        let mut context = TestingContext::minimal_plugins();

        let parse_blocking_srv = context.command(|commands| {
            commands.spawn_service(|In(input): BlockingServiceInput<Vec<String>, TestStreamPack>| {
                impl_stream_pack_test_blocking(input.request, input.streams);
            })
        });

        validate_stream_pack(parse_blocking_srv, &mut context);

        let parse_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<Vec<String>, TestStreamPack>| async move {
                    impl_stream_pack_test_async(input.request, input.streams);
                },
            )
        });

        validate_stream_pack(parse_async_srv, &mut context);

        let parse_continuous_srv = context
            .app
            .spawn_continuous_service(Update, impl_stream_pack_test_continuous);

        validate_stream_pack(parse_continuous_srv, &mut context);

        let parse_blocking_callback =
            (|In(input): BlockingCallbackInput<Vec<String>, TestStreamPack>| {
                impl_stream_pack_test_blocking(input.request, input.streams);
            })
            .as_callback();

        validate_stream_pack(parse_blocking_callback, &mut context);

        let parse_async_callback =
            (|In(input): AsyncCallbackInput<Vec<String>, TestStreamPack>| async move {
                impl_stream_pack_test_async(input.request, input.streams);
            })
            .as_callback();

        validate_stream_pack(parse_async_callback, &mut context);

        let parse_blocking_map = (|input: BlockingMap<Vec<String>, TestStreamPack>| {
            impl_stream_pack_test_blocking(input.request, input.streams);
        })
        .as_map();

        validate_stream_pack(parse_blocking_map, &mut context);

        let parse_async_map = (|input: AsyncMap<Vec<String>, TestStreamPack>| async move {
            impl_stream_pack_test_async(input.request, input.streams);
        })
        .as_map();

        validate_stream_pack(parse_async_map, &mut context);

        let make_workflow = |service: Service<Vec<String>, (), TestStreamPack>| {
            move |scope: Scope<Vec<String>, (), TestStreamPack>, builder: &mut Builder| {
                let node = scope
                    .input
                    .chain(builder)
                    .map_block(move |value| (value, service))
                    .then_injection_node();

                builder.connect(node.streams.stream_u32, scope.streams.stream_u32);
                builder.connect(node.streams.stream_i32, scope.streams.stream_i32);
                builder.connect(node.streams.stream_string, scope.streams.stream_string);

                builder.connect(node.output, scope.terminate);
            }
        };

        let blocking_injection_workflow = context.spawn_workflow(make_workflow(parse_blocking_srv));
        validate_stream_pack(blocking_injection_workflow, &mut context);

        let async_injection_workflow = context.spawn_workflow(make_workflow(parse_async_srv));
        validate_stream_pack(async_injection_workflow, &mut context);

        let continuous_injection_workflow =
            context.spawn_workflow(make_workflow(parse_continuous_srv));
        validate_stream_pack(continuous_injection_workflow, &mut context);

        let nested_workflow = context.spawn_workflow::<_, _, TestStreamPack, _>(|scope, builder| {
            let node = scope.input.chain(builder).then_node(parse_continuous_srv);

            builder.connect(node.streams.stream_u32, scope.streams.stream_u32);
            builder.connect(node.streams.stream_i32, scope.streams.stream_i32);
            builder.connect(node.streams.stream_string, scope.streams.stream_string);

            builder.connect(node.output, scope.terminate);
        });
        validate_stream_pack(nested_workflow, &mut context);

        let double_nested_workflow = context.spawn_workflow::<_, _, TestStreamPack, _>(|scope, builder| {
            let node = scope.input.chain(builder).then_node(nested_workflow);

            builder.connect(node.streams.stream_u32, scope.streams.stream_u32);
            builder.connect(node.streams.stream_i32, scope.streams.stream_i32);
            builder.connect(node.streams.stream_string, scope.streams.stream_string);

            builder.connect(node.output, scope.terminate);
        });
        validate_stream_pack(double_nested_workflow, &mut context);

        let scoped_workflow = context.spawn_workflow::<_, _, TestStreamPack, _>(|scope, builder| {
            let inner_scope = builder.create_scope::<_, _, TestStreamPack, _>(|scope, builder| {
                let node = scope.input.chain(builder).then_node(parse_continuous_srv);

                builder.connect(node.streams.stream_u32, scope.streams.stream_u32);
                builder.connect(node.streams.stream_i32, scope.streams.stream_i32);
                builder.connect(node.streams.stream_string, scope.streams.stream_string);

                builder.connect(node.output, scope.terminate);
            });

            builder.connect(scope.input, inner_scope.input);

            builder.connect(inner_scope.streams.stream_u32, scope.streams.stream_u32);
            builder.connect(inner_scope.streams.stream_i32, scope.streams.stream_i32);
            builder.connect(inner_scope.streams.stream_string, scope.streams.stream_string);

            builder.connect(inner_scope.output, scope.terminate);
        });
        validate_stream_pack(scoped_workflow, &mut context);

        let dyn_stream_workflow = context.spawn_workflow::<Vec<String>, (), TestStreamPack, _>(|scope, builder| {
            let dyn_scope_input: DynOutput = scope.input.into();

            let node = builder.create_node(parse_continuous_srv);
            let mut dyn_node: DynNode = node.into();

            dyn_scope_input.connect_to(&dyn_node.input, builder).unwrap();

            let dyn_scope_stream_u32: DynInputSlot = scope.streams.stream_u32.into();
            let dyn_node_stream_u32 = dyn_node.streams.take_named("stream_u32").unwrap();
            dyn_node_stream_u32.connect_to(&dyn_scope_stream_u32, builder).unwrap();

            let dyn_scope_stream_i32: DynInputSlot = scope.streams.stream_i32.into();
            let dyn_node_stream_i32 = dyn_node.streams.take_named("stream_i32").unwrap();
            dyn_node_stream_i32.connect_to(&dyn_scope_stream_i32, builder).unwrap();

            let dyn_scope_stream_string: DynInputSlot = scope.streams.stream_string.into();
            let dyn_node_stream_string = dyn_node.streams.take_named("stream_string").unwrap();
            dyn_node_stream_string.connect_to(&dyn_scope_stream_string, builder).unwrap();

            let terminate: DynInputSlot = scope.terminate.into();
            dyn_node.output.connect_to(&terminate, builder).unwrap();
        });
        validate_stream_pack(dyn_stream_workflow, &mut context);

        // We can do a stream cast for the service-type providers but not for
        // the callbacks or maps.
        validate_dynamically_named_stream_receiver(parse_blocking_srv, &mut context);
        validate_dynamically_named_stream_receiver(parse_async_srv, &mut context);
        validate_dynamically_named_stream_receiver(parse_continuous_srv, &mut context);
        validate_dynamically_named_stream_receiver(blocking_injection_workflow, &mut context);
        validate_dynamically_named_stream_receiver(async_injection_workflow, &mut context);
        validate_dynamically_named_stream_receiver(continuous_injection_workflow, &mut context);
        validate_dynamically_named_stream_receiver(nested_workflow, &mut context);
        validate_dynamically_named_stream_receiver(double_nested_workflow, &mut context);
    }

    fn validate_stream_pack(
        provider: impl Provider<Request = Vec<String>, Response = (), Streams = TestStreamPack> + Clone,
        context: &mut TestingContext,
    ) {
        let request = vec![
            "5".to_owned(),
            "10".to_owned(),
            "-3".to_owned(),
            "-27".to_owned(),
            "hello".to_owned(),
        ];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors(), "{:#?}", context.get_unhandled_errors());

        let outcome: StreamMapOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, [5, 10]);
        assert_eq!(outcome.stream_i32, [5, 10, -3, -27]);
        assert_eq!(outcome.stream_string, ["5", "10", "-3", "-27", "hello"]);

        let request = vec![];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors(), "{:#?}", context.get_unhandled_errors());

        let outcome: StreamMapOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, Vec::<u32>::new());
        assert_eq!(outcome.stream_i32, Vec::<i32>::new());
        assert_eq!(outcome.stream_string, Vec::<String>::new());

        let request = vec![
            "foo".to_string(),
            "bar".to_string(),
            "1.32".to_string(),
            "-8".to_string(),
        ];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());

        let outcome: StreamMapOutcome = recipient.into();
        assert_eq!(outcome.stream_u32, Vec::<u32>::new());
        assert_eq!(outcome.stream_i32, [-8]);
        assert_eq!(outcome.stream_string, ["foo", "bar", "1.32", "-8"]);
    }

    fn validate_dynamically_named_stream_receiver(
        provider: Service<Vec<String>, (), TestStreamPack>,
        context: &mut TestingContext,
    ) {
        let provider: Service<Vec<String>, (), TestDynamicNamedStreams> = provider.optional_stream_cast();

        let request = vec![
            "5".to_owned(),
            "10".to_owned(),
            "-3".to_owned(),
            "-27".to_owned(),
            "hello".to_owned(),
        ];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors(), "{:#?}", context.get_unhandled_errors());

        let outcome: StreamMapOutcome = recipient.try_into().unwrap();
        assert_eq!(outcome.stream_u32, [5, 10]);
        assert_eq!(outcome.stream_i32, [5, 10, -3, -27]);
        assert_eq!(outcome.stream_string, ["5", "10", "-3", "-27", "hello"]);

        let request = vec![];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors(), "{:#?}", context.get_unhandled_errors());

        let outcome: StreamMapOutcome = recipient.try_into().unwrap();
        assert_eq!(outcome.stream_u32, Vec::<u32>::new());
        assert_eq!(outcome.stream_i32, Vec::<i32>::new());
        assert_eq!(outcome.stream_string, Vec::<String>::new());

        let request = vec![
            "foo".to_string(),
            "bar".to_string(),
            "1.32".to_string(),
            "-8".to_string(),
        ];

        let mut recipient =
            context.command(|commands| commands.request(request, provider.clone()).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());

        let outcome: StreamMapOutcome = recipient.try_into().unwrap();
        assert_eq!(outcome.stream_u32, Vec::<u32>::new());
        assert_eq!(outcome.stream_i32, [-8]);
        assert_eq!(outcome.stream_string, ["foo", "bar", "1.32", "-8"]);
    }

    fn impl_stream_pack_test_blocking(
        request: Vec<String>,
        streams: <TestStreamPack as StreamPack>::StreamBuffers,
    ) {
        for r in request {
            if let Ok(value) = r.parse::<u32>() {
                streams.stream_u32.send(value);
            }

            if let Ok(value) = r.parse::<i32>() {
                streams.stream_i32.send(value);
            }

            streams.stream_string.send(r);
        }
    }

    fn impl_stream_pack_test_async(
        request: Vec<String>,
        streams: <TestStreamPack as StreamPack>::StreamChannels,
    ) {
        for r in request {
            if let Ok(value) = r.parse::<u32>() {
                streams.stream_u32.send(value);
            }

            if let Ok(value) = r.parse::<i32>() {
                streams.stream_i32.send(value);
            }

            streams.stream_string.send(r);
        }
    }

    fn impl_stream_pack_test_continuous(
        In(ContinuousService { key }): In<ContinuousService<Vec<String>, (), TestStreamPack>>,
        mut param: ContinuousQuery<Vec<String>, (), TestStreamPack>,
    ) {
        param.get_mut(&key).unwrap().for_each(|order| {
            for r in order.request().clone() {
                if let Ok(value) = r.parse::<u32>() {
                    order.streams().stream_u32.send(value);
                }

                if let Ok(value) = r.parse::<i32>() {
                    order.streams().stream_i32.send(value);
                }

                order.streams().stream_string.send(r);
            }

            order.respond(());
        });
    }

    #[derive(Default)]
    struct StreamMapOutcome {
        stream_u32: Vec<u32>,
        stream_i32: Vec<i32>,
        stream_string: Vec<String>,
    }

    impl From<Recipient<(), TestStreamPack>> for StreamMapOutcome {
        fn from(mut recipient: Recipient<(), TestStreamPack>) -> Self {
            let mut result = Self::default();
            while let Ok(r) = recipient.streams.stream_u32.try_recv() {
                result.stream_u32.push(r);
            }

            while let Ok(r) = recipient.streams.stream_i32.try_recv() {
                result.stream_i32.push(r);
            }

            while let Ok(r) = recipient.streams.stream_string.try_recv() {
                result.stream_string.push(r);
            }

            result
        }
    }

    type TestDynamicNamedStreams = (
        DynamicallyNamedStream<StreamOf<u32>>,
        DynamicallyNamedStream<StreamOf<i32>>,
        DynamicallyNamedStream<StreamOf<String>>,
    );

    impl TryFrom<Recipient<(), TestDynamicNamedStreams>> for StreamMapOutcome {
        type Error = UnknownName;
        fn try_from(mut recipient: Recipient<(), TestDynamicNamedStreams>) -> Result<Self, Self::Error> {
            let mut result = Self::default();
            while let Ok(NamedValue { name, value }) = recipient.streams.0.try_recv() {
                if name == "stream_u32" {
                    result.stream_u32.push(value);
                } else {
                    return Err(UnknownName { name })
                }
            }

            while let Ok(NamedValue { name, value }) = recipient.streams.1.try_recv() {
                if name == "stream_i32" {
                    result.stream_i32.push(value);
                } else {
                    return Err(UnknownName { name })
                }
            }

            while let Ok(NamedValue { name, value }) = recipient.streams.2.try_recv() {
                if name == "stream_string" {
                    result.stream_string.push(value);
                } else {
                    return Err(UnknownName { name })
                }
            }

            Ok(result)
        }
    }

    #[test]
    fn test_dynamically_named_streams() {
        let mut context = TestingContext::minimal_plugins();

        let parse_blocking_srv = context.command(|commands| {
            commands.spawn_service(|In(input): BlockingServiceInput<NamedInputs, TestDynamicNamedStreams>| {
                impl_dynamically_named_streams_blocking(input.request, input.streams);
            })
        });

        validate_dynamically_named_streams(parse_blocking_srv, &mut context);

        let parse_async_srv = context.command(|commands| {
            commands.spawn_service(
                |In(input): AsyncServiceInput<NamedInputs, TestDynamicNamedStreams>| async move {
                    impl_dynamically_named_streams_async(input.request, input.streams);
                },
            )
        });

        validate_dynamically_named_streams(parse_async_srv, &mut context);

        let parse_continuous_srv = context
            .app
            .spawn_continuous_service(Update, impl_dynamically_named_streams_continuous);

        validate_dynamically_named_streams(parse_continuous_srv, &mut context);

        let parse_blocking_callback =
            (|In(input): BlockingCallbackInput<NamedInputs, TestDynamicNamedStreams>| {
                impl_dynamically_named_streams_blocking(input.request, input.streams);
            })
            .as_callback();

        validate_dynamically_named_streams(parse_blocking_callback, &mut context);

        let parse_async_callback =
            (|In(input): AsyncCallbackInput<NamedInputs, TestDynamicNamedStreams>| async move {
                impl_dynamically_named_streams_async(input.request, input.streams);
            })
            .as_callback();

        validate_dynamically_named_streams(parse_async_callback, &mut context);

        let parse_blocking_map = (|input: BlockingMap<NamedInputs, TestDynamicNamedStreams>| {
            impl_dynamically_named_streams_blocking(input.request, input.streams);
        })
        .as_map();

        validate_dynamically_named_streams(parse_blocking_map, &mut context);

        let parse_async_map = (|input: AsyncMap<NamedInputs, TestDynamicNamedStreams>| async move {
            impl_dynamically_named_streams_async(input.request, input.streams);
        })
        .as_map();

        validate_dynamically_named_streams(parse_async_map, &mut context);

        let make_workflow = |service: Service<NamedInputs, (), TestDynamicNamedStreams>| {
            move |scope: Scope<NamedInputs, (), TestDynamicNamedStreams>, builder: &mut Builder| {
                let node = scope
                    .input
                    .chain(builder)
                    .map_block(move |value| (value, service))
                    .then_injection_node();

                builder.connect(node.streams.0, scope.streams.0);
                builder.connect(node.streams.1, scope.streams.1);
                builder.connect(node.streams.2, scope.streams.2);

                builder.connect(node.output, scope.terminate);
            }
        };

        let blocking_injection_workflow = context.spawn_workflow(make_workflow(parse_blocking_srv));
        validate_dynamically_named_streams(blocking_injection_workflow, &mut context);

        let async_injection_workflow = context.spawn_workflow(make_workflow(parse_async_srv));
        validate_dynamically_named_streams(async_injection_workflow, &mut context);

        let continuous_injection_workflow =
            context.spawn_workflow(make_workflow(parse_continuous_srv));
        validate_dynamically_named_streams(continuous_injection_workflow, &mut context);

        let nested_workflow = context.spawn_workflow::<_, _, TestDynamicNamedStreams, _>(|scope, builder| {
            let node = scope.input.chain(builder).then_node(parse_continuous_srv);

            builder.connect(node.streams.0, scope.streams.0);
            builder.connect(node.streams.1, scope.streams.1);
            builder.connect(node.streams.2, scope.streams.2);

            builder.connect(node.output, scope.terminate);
        });
        validate_dynamically_named_streams(nested_workflow, &mut context);

        let double_nested_workflow = context.spawn_workflow::<_, _, TestDynamicNamedStreams, _>(|scope, builder| {
            let node = scope.input.chain(builder).then_node(nested_workflow);

            builder.connect(node.streams.0, scope.streams.0);
            builder.connect(node.streams.1, scope.streams.1);
            builder.connect(node.streams.2, scope.streams.2);

            builder.connect(node.output, scope.terminate);
        });
        validate_dynamically_named_streams(double_nested_workflow, &mut context);

        let scoped_workflow = context.spawn_workflow::<_, _, TestDynamicNamedStreams, _>(|scope, builder| {
            let inner_scope = builder.create_scope::<_, _, TestDynamicNamedStreams, _>(|scope, builder| {
                let node = scope.input.chain(builder).then_node(parse_continuous_srv);

                builder.connect(node.streams.0, scope.streams.0);
                builder.connect(node.streams.1, scope.streams.1);
                builder.connect(node.streams.2, scope.streams.2);

                builder.connect(node.output, scope.terminate);
            });

            builder.connect(scope.input, inner_scope.input);

            builder.connect(inner_scope.streams.0, scope.streams.0);
            builder.connect(inner_scope.streams.1, scope.streams.1);
            builder.connect(inner_scope.streams.2, scope.streams.2);

            builder.connect(inner_scope.output, scope.terminate);
        });
        validate_dynamically_named_streams(scoped_workflow, &mut context);

        // We can do a stream cast for the service-type providers but not for
        // the callbacks or maps.
        validate_dynamically_named_streams_into_stream_pack(parse_blocking_srv, &mut context);
        validate_dynamically_named_streams_into_stream_pack(parse_async_srv, &mut context);
        validate_dynamically_named_streams_into_stream_pack(parse_continuous_srv, &mut context);
        validate_dynamically_named_streams_into_stream_pack(blocking_injection_workflow, &mut context);
        validate_dynamically_named_streams_into_stream_pack(async_injection_workflow, &mut context);
        validate_dynamically_named_streams_into_stream_pack(continuous_injection_workflow, &mut context);
        validate_dynamically_named_streams_into_stream_pack(nested_workflow, &mut context);
        validate_dynamically_named_streams_into_stream_pack(double_nested_workflow, &mut context);
    }

    fn validate_dynamically_named_streams(
        provider: impl Provider<Request = NamedInputs, Response = (), Streams = TestDynamicNamedStreams> + Clone,
        context: &mut TestingContext,
    ) {
        let expected_values_u32 = vec![
            NamedValue::new("stream_u32", 5),
            NamedValue::new("stream_u32", 10),
            NamedValue::new("stream_i32", 12),
        ];

        let expected_values_i32 = vec![
            NamedValue::new("stream_i32", 2),
            NamedValue::new("stream_i32", -5),
            NamedValue::new("stream_u32", 7),
        ];

        let expected_values_string = vec![
            NamedValue::new("stream_string", "hello".to_owned()),
            NamedValue::new("stream_string", "8".to_owned()),
            NamedValue::new("stream_u32", "22".to_owned()),
        ];

        let request = NamedInputs {
            values_u32: expected_values_u32.clone(),
            values_i32: expected_values_i32.clone(),
            values_string: expected_values_string.clone(),
        };

        let mut recipient =
            context.command(|commands| commands.request(request, provider).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let received_values_u32 = collect_received_values(recipient.streams.0);
        assert_eq!(expected_values_u32, received_values_u32);

        let received_values_i32 = collect_received_values(recipient.streams.1);
        assert_eq!(expected_values_i32, received_values_i32);

        let received_values_string = collect_received_values(recipient.streams.2);
        assert_eq!(expected_values_string, received_values_string);
    }

    fn collect_received_values<T>(mut receiver: crate::Receiver<T>) -> Vec<T> {
        let mut result = Vec::new();
        while let Ok(value) = receiver.try_recv() {
            result.push(value);
        }
        result
    }

    fn validate_dynamically_named_streams_into_stream_pack(
        provider: Service<NamedInputs, (), TestDynamicNamedStreams>,
        context: &mut TestingContext,
    ) {
        let provider: Service<NamedInputs, (), TestStreamPack> = provider.optional_stream_cast();

        let request = NamedInputs {
            values_u32: vec![
                NamedValue::new("stream_u32", 5),
                NamedValue::new("stream_u32", 10),

                // This won't appear because its name isn't being listened for
                // for this value type
                NamedValue::new("stream_i32", 12),
            ],
            values_i32: vec![
                NamedValue::new("stream_i32", 2),
                NamedValue::new("stream_i32", -5),

                // This won't appear because its name isn't being listened for
                // for this value type
                NamedValue::new("stream_u32", 7),
            ],
            values_string: vec![
                NamedValue::new("stream_string", "hello".to_owned()),
                NamedValue::new("stream_string", "8".to_owned()),

                // This won't appear because its named isn't being listened for
                // for this value type
                NamedValue::new("stream_u32", "22".to_owned()),
            ],
        };

        let mut recipient =
            context.command(|commands| commands.request(request, provider).take());

        context.run_with_conditions(&mut recipient.response, Duration::from_secs(2));
        assert!(recipient.response.take().available().is_some());
        assert!(context.no_unhandled_errors());

        let outcome: StreamMapOutcome = recipient.try_into().unwrap();
        assert_eq!(outcome.stream_u32, [5, 10]);
        assert_eq!(outcome.stream_i32, [2, -5]);
        assert_eq!(outcome.stream_string, ["hello", "8"]);
    }

    fn impl_dynamically_named_streams_blocking(
        request: NamedInputs,
        streams: <TestDynamicNamedStreams as StreamPack>::StreamBuffers,
    ) {
        for nv in request.values_u32 {
            streams.0.send(nv);
        }

        for nv in request.values_i32 {
            streams.1.send(nv);
        }

        for nv in request.values_string {
            streams.2.send(nv);
        }
    }

    fn impl_dynamically_named_streams_async(
        request: NamedInputs,
        streams: <TestDynamicNamedStreams as StreamPack>::StreamChannels,
    ) {
        for nv in request.values_u32 {
            streams.0.send(nv);
        }

        for nv in request.values_i32 {
            streams.1.send(nv);
        }

        for nv in request.values_string {
            streams.2.send(nv);
        }
    }

    fn impl_dynamically_named_streams_continuous(
        In(ContinuousService { key }): In<ContinuousService<NamedInputs, (), TestDynamicNamedStreams>>,
        mut param: ContinuousQuery<NamedInputs, (), TestDynamicNamedStreams>,
    ) {
        param.get_mut(&key).unwrap().for_each(|order| {
            for nv in order.request().values_u32.iter() {
                order.streams().0.send(nv.clone());
            }

            for nv in order.request().values_i32.iter() {
                order.streams().1.send(nv.clone());
            }

            for nv in order.request().values_string.iter() {
                order.streams().2.send(nv.clone());
            }

            order.respond(());
        });
    }

    struct NamedInputs {
        values_u32: Vec<NamedValue<u32>>,
        values_i32: Vec<NamedValue<i32>>,
        values_string: Vec<NamedValue<String>>,
    }

    #[derive(thiserror::Error, Debug)]
    #[error("received unknown name: {name}")]
    struct UnknownName {
        name: Cow<'static, str>,
    }

    use crate::StreamAvailability;

    struct TestStreamPack {
        stream_u32: StreamOf<u32>,
        stream_i32: StreamOf<i32>,
        stream_string: StreamOf<String>,
    }

    impl TestStreamPack {
        #[allow(unused)]
        fn __bevy_impulse_allow_unused_fields(&self) {
            println!("{:#?}", [
                std::any::type_name_of_val(&self.stream_u32),
                std::any::type_name_of_val(&self.stream_i32),
                std::any::type_name_of_val(&self.stream_string),
            ]);
        }
    }

    impl StreamPack for TestStreamPack {
        type StreamInputPack = TestStreamPackInputs;
        type StreamOutputPack = TestStreamPackOutputs;
        type StreamReceivers = TestStreamPackReceivers;
        type StreamChannels = TestStreamPackChannels;
        type StreamBuffers = TestStreamPackBuffers;

        fn spawn_scope_streams(
            in_scope: ::bevy_impulse::re_exports::Entity,
            out_scope: ::bevy_impulse::re_exports::Entity,
            commands: &mut ::bevy_impulse::re_exports::Commands,
        ) -> (Self::StreamInputPack, Self::StreamOutputPack) {
            let (input_stream_u32, output_stream_u32) = ::bevy_impulse::NamedStream::<StreamOf::<u32>>::spawn_scope_stream(
                in_scope, out_scope, commands,
            );
            let (input_stream_i32, output_stream_i32) = ::bevy_impulse::NamedStream::<StreamOf::<i32>>::spawn_scope_stream(
                in_scope, out_scope, commands,
            );
            let (input_stream_string, output_stream_string) = ::bevy_impulse::NamedStream::<StreamOf::<String>>::spawn_scope_stream(
                in_scope, out_scope, commands,
            );

            (
                TestStreamPackInputs {
                    stream_u32: input_stream_u32,
                    stream_i32: input_stream_i32,
                    stream_string: input_stream_string,
                },
                TestStreamPackOutputs {
                    stream_u32: output_stream_u32,
                    stream_i32: output_stream_i32,
                    stream_string: output_stream_string,
                }
            )
        }

        fn spawn_workflow_streams(builder: &mut ::bevy_impulse::Builder) -> Self::StreamInputPack {
            TestStreamPackInputs {
                stream_u32: ::bevy_impulse::NamedStream::<StreamOf::<u32>>::spawn_workflow_stream(
                    ::std::borrow::Cow::Borrowed("stream_u32"), builder,
                ),
                stream_i32: ::bevy_impulse::NamedStream::<StreamOf::<i32>>::spawn_workflow_stream(
                    ::std::borrow::Cow::Borrowed("stream_i32"), builder,
                ),
                stream_string: ::bevy_impulse::NamedStream::<StreamOf::<String>>::spawn_workflow_stream(
                    ::std::borrow::Cow::Borrowed("stream_string"), builder,
                ),
            }
        }

        fn spawn_node_streams(
            source: ::bevy_impulse::re_exports::Entity,
            map: &mut ::bevy_impulse::StreamTargetMap,
            builder: &mut ::bevy_impulse::Builder,
        ) -> Self::StreamOutputPack {
            TestStreamPackOutputs {
                stream_u32: ::bevy_impulse::NamedStream::<StreamOf::<u32>>::spawn_node_stream(
                    ::std::borrow::Cow::Borrowed("stream_u32"), source, map, builder,
                ),
                stream_i32: ::bevy_impulse::NamedStream::<StreamOf::<i32>>::spawn_node_stream(
                    ::std::borrow::Cow::Borrowed("stream_i32"), source, map, builder,
                ),
                stream_string: ::bevy_impulse::NamedStream::<StreamOf::<String>>::spawn_node_stream(
                    ::std::borrow::Cow::Borrowed("stream_string"), source, map, builder,
                ),
            }
        }

        fn take_streams(
            source: ::bevy_impulse::re_exports::Entity,
            map: &mut ::bevy_impulse::StreamTargetMap,
            commands: &mut ::bevy_impulse::re_exports::Commands,
        ) -> Self::StreamReceivers {
            TestStreamPackReceivers {
                stream_u32: ::bevy_impulse::NamedStream::<StreamOf::<u32>>::take_stream(
                    ::std::borrow::Cow::Borrowed("stream_u32"), source, map, commands,
                ),
                stream_i32: ::bevy_impulse::NamedStream::<StreamOf::<i32>>::take_stream(
                    ::std::borrow::Cow::Borrowed("stream_i32"), source, map, commands,
                ),
                stream_string: ::bevy_impulse::NamedStream::<StreamOf::<String>>::take_stream(
                    ::std::borrow::Cow::Borrowed("stream_string"), source, map, commands,
                ),
            }
        }

        fn collect_streams(
            source: ::bevy_impulse::re_exports::Entity,
            target: ::bevy_impulse::re_exports::Entity,
            map: &mut ::bevy_impulse::StreamTargetMap,
            commands: &mut ::bevy_impulse::re_exports::Commands,
        ) {
            ::bevy_impulse::NamedStream::<StreamOf::<u32>>::collect_stream(
                ::std::borrow::Cow::Borrowed("stream_u32"), source, target, map, commands,
            );
            ::bevy_impulse::NamedStream::<StreamOf::<i32>>::collect_stream(
                ::std::borrow::Cow::Borrowed("stream_i32"), source, target, map, commands,
            );
            ::bevy_impulse::NamedStream::<StreamOf::<String>>::collect_stream(
                ::std::borrow::Cow::Borrowed("stream_string"), source, target ,map, commands,
            );
        }

        fn make_stream_channels(
            inner: &::std::sync::Arc<::bevy_impulse::InnerChannel>,
            world: &::bevy_impulse::re_exports::World,
        ) -> Self::StreamChannels {
            TestStreamPackChannels {
                stream_u32: ::bevy_impulse::NamedStream::<StreamOf::<u32>>::make_stream_channel(
                    ::std::borrow::Cow::Borrowed("stream_u32"), inner, world,
                ),
                stream_i32: ::bevy_impulse::NamedStream::<StreamOf::<i32>>::make_stream_channel(
                    ::std::borrow::Cow::Borrowed("stream_i32"), inner, world,
                ),
                stream_string: ::bevy_impulse::NamedStream::<StreamOf::<String>>::make_stream_channel(
                    ::std::borrow::Cow::Borrowed("stream_string"), inner, world,
                ),
            }
        }

        fn make_stream_buffers(
            target_map: Option<&::bevy_impulse::StreamTargetMap>,
        ) -> Self::StreamBuffers {
            TestStreamPackBuffers {
                stream_u32: ::bevy_impulse::NamedStream::<StreamOf::<u32>>::make_stream_buffer(target_map),
                stream_i32: ::bevy_impulse::NamedStream::<StreamOf::<i32>>::make_stream_buffer(target_map),
                stream_string: ::bevy_impulse::NamedStream::<StreamOf::<String>>::make_stream_buffer(target_map),
            }
        }

        fn process_stream_buffers(
            buffer: Self::StreamBuffers,
            source: ::bevy_impulse::re_exports::Entity,
            session: ::bevy_impulse::re_exports::Entity,
            unused: &mut ::bevy_impulse::UnusedStreams,
            world: &mut ::bevy_impulse::re_exports::World,
            roster: &mut ::bevy_impulse::OperationRoster,
        ) -> ::bevy_impulse::OperationResult {
            ::bevy_impulse::NamedStream::<StreamOf::<u32>>::process_stream_buffer(
                &::std::borrow::Cow::Borrowed("stream_u32"), buffer.stream_u32, source, session, unused, world, roster,
            )?;
            ::bevy_impulse::NamedStream::<StreamOf::<i32>>::process_stream_buffer(
                &::std::borrow::Cow::Borrowed("stream_i32"), buffer.stream_i32, source, session, unused, world, roster,
            )?;
            ::bevy_impulse::NamedStream::<StreamOf<String>>::process_stream_buffer(
                &::std::borrow::Cow::Borrowed("stream_string"), buffer.stream_string, source, session, unused, world, roster,
            )?;
            Ok(())
        }

        fn defer_buffers(
            buffer: Self::StreamBuffers,
            source: ::bevy_impulse::re_exports::Entity,
            session: ::bevy_impulse::re_exports::Entity,
            commands: &mut ::bevy_impulse::re_exports::Commands,
        ) {
            ::bevy_impulse::NamedStream::<StreamOf::<u32>>::defer_buffer(
                &::std::borrow::Cow::Borrowed("stream_u32"), buffer.stream_u32, source, session, commands,
            );
            ::bevy_impulse::NamedStream::<StreamOf::<i32>>::defer_buffer(
                &::std::borrow::Cow::Borrowed("stream_i32"), buffer.stream_i32, source, session, commands,
            );
            ::bevy_impulse::NamedStream::<StreamOf::<String>>::defer_buffer(
                &::std::borrow::Cow::Borrowed("stream_string"), buffer.stream_string, source, session, commands,
            );
        }

        fn set_stream_availability(availability: &mut StreamAvailability) {
            let _ = availability.add_named::<<StreamOf::<u32> as ::bevy_impulse::StreamEffect>::Output>(
                ::std::borrow::Cow::Borrowed("stream_u32"),
            );
            let _ = availability.add_named::<<StreamOf::<i32> as ::bevy_impulse::StreamEffect>::Output>(
                ::std::borrow::Cow::Borrowed("stream_i32"),
            );
            let _ = availability.add_named::<<StreamOf::<String> as ::bevy_impulse::StreamEffect>::Output>(
                ::std::borrow::Cow::Borrowed("stream_string"),
            );
        }

        fn are_streams_available(availability: &StreamAvailability) -> bool {
            availability.has_named::<<StreamOf::<u32> as ::bevy_impulse::StreamEffect>::Output>("stream_u32")
            && availability.has_named::<<StreamOf::<i32> as ::bevy_impulse::StreamEffect>::Output>("stream_i32")
            && availability.has_named::<<StreamOf::<String> as ::bevy_impulse::StreamEffect>::Output>("stream_string")
        }

        fn into_dyn_stream_pack(
            pack: &mut ::bevy_impulse::dyn_node::DynStreamPack,
            outputs: Self::StreamOutputPack
        ) {
            pack.add_named("stream_u32", outputs.stream_u32);
            pack.add_named("stream_i32", outputs.stream_i32);
            pack.add_named("stream_string", outputs.stream_string);
        }

        fn has_streams() -> bool {
            true
        }
    }

    struct TestStreamPackInputs {
        stream_u32: ::bevy_impulse::InputSlot<u32>,
        stream_i32: ::bevy_impulse::InputSlot<i32>,
        stream_string: ::bevy_impulse::InputSlot<String>,
    }

    struct TestStreamPackOutputs {
        stream_u32: ::bevy_impulse::Output<u32>,
        stream_i32: ::bevy_impulse::Output<i32>,
        stream_string: ::bevy_impulse::Output<String>,
    }

    struct TestStreamPackChannels {
        stream_u32: ::bevy_impulse::NamedStreamChannel<StreamOf<u32>>,
        stream_i32: ::bevy_impulse::NamedStreamChannel<StreamOf<i32>>,
        stream_string: ::bevy_impulse::NamedStreamChannel<StreamOf<String>>,
    }

    struct TestStreamPackReceivers {
        stream_u32: ::bevy_impulse::Receiver<u32>,
        stream_i32: ::bevy_impulse::Receiver<i32>,
        stream_string: ::bevy_impulse::Receiver<String>,
    }

    #[derive(Clone)]
     struct TestStreamPackBuffers {
        stream_u32: ::bevy_impulse::NamedStreamBuffer<u32>,
        stream_i32: ::bevy_impulse::NamedStreamBuffer<i32>,
        stream_string: ::bevy_impulse::NamedStreamBuffer<String>,
     }
}
