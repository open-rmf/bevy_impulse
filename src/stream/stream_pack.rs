/*
 * Copyright (C) 2025 Open Source Robotics Foundation
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

use bevy_ecs::prelude::{Commands, Entity, World};
use bevy_utils::all_tuples;

use std::sync::Arc;

use crate::{
    dyn_node::{DynStreamInputPack, DynStreamOutputPack},
    Builder, InnerChannel, OperationError, OperationResult, OperationRoster, StreamAvailability,
    StreamTargetMap, UnusedStreams,
};

/// The `StreamPack` trait defines the interface for a pack of one or more streams.
/// Each [`Provider`](crate::Provider) can provide zero, one, or more streams of data
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

    fn into_dyn_stream_input_pack(pack: &mut DynStreamInputPack, inputs: Self::StreamInputPack);

    fn into_dyn_stream_output_pack(pack: &mut DynStreamOutputPack, outputs: Self::StreamOutputPack);

    /// Are there actually any streams in the pack?
    fn has_streams() -> bool;
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

    fn into_dyn_stream_input_pack(_: &mut DynStreamInputPack, _: Self::StreamInputPack) {
        // Do nothing
    }

    fn into_dyn_stream_output_pack(_: &mut DynStreamOutputPack, _: Self::StreamOutputPack) {
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

            fn into_dyn_stream_input_pack(
                pack: &mut DynStreamInputPack,
                inputs: Self::StreamInputPack,
            ) {
                let ($($T,)*) = inputs;
                $(
                    $T::into_dyn_stream_input_pack(pack, $T);
                )*
            }

            fn into_dyn_stream_output_pack(
                pack: &mut DynStreamOutputPack,
                outputs: Self::StreamOutputPack,
            ) {
                let ($($T,)*) = outputs;
                $(
                    $T::into_dyn_stream_output_pack(pack, $T);
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
