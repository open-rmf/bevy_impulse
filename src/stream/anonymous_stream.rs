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

use bevy_ecs::{
    prelude::{Commands, Entity, World},
    system::Command,
};
use bevy_hierarchy::BuildChildren;

use tokio::sync::mpsc::unbounded_channel;
pub use tokio::sync::mpsc::UnboundedReceiver as Receiver;

use std::{rc::Rc, sync::Arc};

use crate::{
    dyn_node::{DynStreamInputPack, DynStreamOutputPack},
    AddImpulse, AddOperation, AnonymousStreamRedirect, Builder, DefaultStreamBufferContainer,
    DeferredRoster, InnerChannel, InputSlot, OperationError, OperationResult, OperationRoster,
    OrBroken, Output, Push, RedirectScopeStream, RedirectWorkflowStream, ReportUnhandled,
    SingleInputStorage, StreamAvailability, StreamBuffer, StreamChannel, StreamEffect, StreamPack,
    StreamRequest, StreamTargetMap, TakenStream, UnusedStreams, UnusedTarget,
};

/// A wrapper to turn any [`StreamEffect`] into an anonymous (unnamed) stream.
/// This should be used if you want a stream with the same behavior as [`StreamOf`][1]
/// but with some additional side effect. The input and output data types of the
/// stream may be different.
///
/// [1]: crate::StreamOf
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

impl<S: StreamEffect> StreamPack for AnonymousStream<S> {
    type StreamInputPack = InputSlot<S::Input>;
    type StreamOutputPack = Output<S::Output>;
    type StreamReceivers = Receiver<S::Output>;
    type StreamChannels = StreamChannel<S>;
    type StreamBuffers = StreamBuffer<S::Input>;

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (InputSlot<S::Input>, Output<S::Output>) {
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

    fn spawn_workflow_streams(builder: &mut Builder) -> InputSlot<S::Input> {
        let source = builder.commands.spawn(()).id();
        builder.commands.add(AddOperation::new(
            Some(builder.scope()),
            source,
            RedirectWorkflowStream::new(AnonymousStreamRedirect::<S>::new(None)),
        ));
        InputSlot::new(builder.scope(), source)
    }

    fn spawn_node_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> Output<S::Output> {
        let target = builder
            .commands
            .spawn((SingleInputStorage::new(source), UnusedTarget))
            .id();

        map.add_anonymous::<S::Output>(target, builder.commands());
        Output::new(builder.scope(), target)
    }

    fn take_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Receiver<S::Output> {
        let (sender, receiver) = unbounded_channel::<S::Output>();
        let target = commands
            .spawn(())
            // Set the parent of this stream to be the impulse so it can be
            // recursively despawned together.
            .set_parent(source)
            .id();

        map.add_anonymous::<S::Output>(target, commands);
        commands.add(AddImpulse::new(None, target, TakenStream::new(sender)));

        receiver
    }

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) {
        let redirect = commands.spawn(()).set_parent(source).id();
        commands.add(AddImpulse::new(
            None,
            redirect,
            Push::<S::Output>::new(target, true),
        ));
        map.add_anonymous::<S::Output>(redirect, commands);
    }

    fn make_stream_channels(inner: &Arc<InnerChannel>, world: &World) -> Self::StreamChannels {
        let target = world
            .get::<StreamTargetMap>(inner.source())
            .and_then(|t| t.get_anonymous::<S::Output>());
        StreamChannel::new(target, Arc::clone(inner))
    }

    fn make_stream_buffers(target_map: Option<&StreamTargetMap>) -> StreamBuffer<S::Input> {
        let target = target_map.and_then(|map| map.get_anonymous::<S::Output>());

        StreamBuffer {
            container: Default::default(),
            target,
        }
    }

    fn process_stream_buffers(
        buffer: Self::StreamBuffers,
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

    fn defer_buffers(
        buffer: Self::StreamBuffers,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    ) {
        commands.add(SendAnonymousStreams::<
            S,
            DefaultStreamBufferContainer<S::Input>,
        >::new(
            buffer.container.take(), source, session, buffer.target
        ));
    }

    fn set_stream_availability(availability: &mut StreamAvailability) {
        availability.add_anonymous::<S::Output>();
    }

    fn are_streams_available(availability: &StreamAvailability) -> bool {
        availability.has_anonymous::<S::Output>()
    }

    fn into_dyn_stream_input_pack(pack: &mut DynStreamInputPack, inputs: Self::StreamInputPack) {
        pack.add_anonymous(inputs);
    }

    fn into_dyn_stream_output_pack(
        pack: &mut DynStreamOutputPack,
        outputs: Self::StreamOutputPack,
    ) {
        pack.add_anonymous(outputs);
    }

    fn has_streams() -> bool {
        true
    }
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
