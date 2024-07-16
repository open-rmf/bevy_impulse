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

use bevy::prelude::{Component, Bundle, Entity, Commands, World, BuildChildren};
use bevy::utils::all_tuples;

use crossbeam::channel::{Receiver, unbounded};

use std::{rc::Rc, cell::RefCell, sync::Arc};

use smallvec::SmallVec;

use crate::{
    InputSlot, Output, UnusedTarget, RedirectWorkflowStream, RedirectScopeStream,
    AddOperation, AddImpulse, OperationRoster, OperationResult, OrBroken, ManageInput,
    InnerChannel, TakenStream, StreamChannel, Push, Builder,
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

impl<T: Stream> StreamBuffer<T> {
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
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult;
}

impl<T: Stream> StreamPack for T {
    type StreamAvailableBundle = StreamAvailable<Self>;
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
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let target = buffer.target;
        for data in Rc::into_inner(buffer.container).or_broken()?.into_inner().into_iter() {
            data.send(StreamRequest { source, session, target, world, roster })?;
        }

        Ok(())
    }
}

impl StreamPack for () {
    type StreamAvailableBundle = ();
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
        _: &mut World,
        _: &mut OperationRoster,
    ) -> OperationResult {
        Ok(())
    }
}

impl<T1: StreamPack> StreamPack for (T1,) {
    type StreamAvailableBundle = T1::StreamAvailableBundle;
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
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        T1::process_buffer(buffer, source, session, world, roster)?;
        Ok(())
    }
}


macro_rules! impl_streampack_for_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: StreamPack),*> StreamPack for ($($T,)*) {
            type StreamAvailableBundle = ($($T::StreamAvailableBundle,)*);
            type StreamStorageBundle = ($($T::StreamStorageBundle,)*);
            type StreamInputPack = ($($T::StreamInputPack,)*);
            type StreamOutputPack = ($($T::StreamOutputPack,)*);
            type Receiver = ($($T::Receiver,)*);
            type Channel = ($($T::Channel,)*);
            type Buffer = ($($T::Buffer,)*);

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

            fn make_buffer(source: Entity, world: &World) -> Self::Buffer {
                (
                    $(
                        $T::make_buffer(source, world),
                    )*
                )
            }

            fn process_buffer(
                buffer: Self::Buffer,
                source: Entity,
                session: Entity,
                world: &mut World,
                roster: &mut OperationRoster,
            ) -> OperationResult {
                let ($($T,)*) = buffer;
                $(
                    $T::process_buffer($T, source, session, world, roster)?;
                )*
                Ok(())
            }
        }
    }
}

// Implements the `StreamPack` trait for all tuples between size 2 and 12
// (inclusive) made of types that implement `StreamPack`
all_tuples!(impl_streampack_for_tuple, 2, 12, T);
