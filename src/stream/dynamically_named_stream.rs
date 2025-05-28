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
use bevy_hierarchy::BuildChildren;

use std::{cell::RefCell, rc::Rc, sync::Arc};
use tokio::sync::mpsc::unbounded_channel;

use crate::{
    dyn_node::{DynStreamInputPack, DynStreamOutputPack},
    AddImpulse, AddOperation, Builder, DefaultStreamContainer,
    InnerChannel, InputSlot, NamedStreamTargets, NamedStreamRedirect,
    NamedTarget, NamedValue, OperationResult, OperationRoster,
    OrBroken, Output, Push, Receiver, RedirectScopeStream, RedirectWorkflowStream,
    ReportUnhandled, SendNamedStreams, SingleInputStorage, StreamAvailability, StreamEffect,
    StreamPack, StreamRequest, StreamTargetMap, TakenStream, UnusedStreams, UnusedTarget,
    send_named_stream,
};

/// A wrapper to turn any stream type into a named stream. Each item that moves
/// through the stream can have its own name, determined at runtime.
pub struct DynamicallyNamedStream<S: StreamEffect>(std::marker::PhantomData<fn(S)>);

impl<S: StreamEffect> StreamEffect for DynamicallyNamedStream<S> {
    type Input = NamedValue<S::Input>;
    type Output = NamedValue<S::Output>;
    fn side_effect(
        input: Self::Input,
        request: &mut StreamRequest,
    ) -> Result<Self::Output, crate::OperationError> {
        let NamedValue { name, value } = input;
        S::side_effect(value, request).map(|value| NamedValue { name, value })
    }
}

impl<S: StreamEffect> StreamPack for DynamicallyNamedStream<S> {
    type StreamInputPack = InputSlot<<Self as StreamEffect>::Input>;
    type StreamOutputPack = Output<<Self as StreamEffect>::Output>;
    type StreamReceivers = Receiver<<Self as StreamEffect>::Output>;
    type StreamChannels = DynamicallyNamedStreamChannel<S>;
    type StreamBuffers = DynamicallyNamedStreamBuffer<S::Input>;

    fn spawn_scope_streams(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (InputSlot<NamedValue<S::Input>>, Output<NamedValue<S::Output>>) {
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

    fn spawn_workflow_streams(builder: &mut Builder) -> Self::StreamInputPack {
        let source = builder.commands.spawn(()).id();
        builder.commands.add(AddOperation::new(
            Some(builder.scope()),
            source,
            RedirectWorkflowStream::new(NamedStreamRedirect::<S>::dynamic()),
        ));
        InputSlot::new(builder.scope, source)
    }

    fn spawn_node_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> Output<NamedValue<S::Output>> {
        let target = builder
            .commands
            .spawn((SingleInputStorage::new(source), UnusedTarget))
            .id();

        map.add_anonymous::<NamedValue<S::Output>>(target, builder.commands());
        Output::new(builder.scope, target)
    }

    fn take_streams(
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Receiver<NamedValue<S::Output>> {
        let (sender, receiver) = unbounded_channel::<NamedValue<S::Output>>();
        let target = commands
            .spawn(())
            // Set the parent of this stream to be the impulse so it can be
            // recursively despawned together.
            .set_parent(source)
            .id();

        map.add_anonymous::<NamedValue<S::Output>>(target, commands);
        commands.add(AddImpulse::new(target, TakenStream::new(sender)));

        receiver
    }

    fn collect_streams(
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) {
        let redirect = commands.spawn(()).set_parent(source).id();
        commands.add(AddImpulse::new(redirect, Push::<NamedValue<S::Output>>::new(target, true)));
        map.add_anonymous::<NamedValue<S::Output>>(redirect, commands);
    }

    fn make_stream_channels(inner: &Arc<InnerChannel>, world: &World) -> Self::StreamChannels {
        let targets =
            NamedStreamTargets::new::<S::Output>(world.get::<StreamTargetMap>(inner.source()));
        DynamicallyNamedStreamChannel::new(Arc::new(targets), Arc::clone(&inner))
    }

    fn make_stream_buffers(target_map: Option<&StreamTargetMap>) -> Self::StreamBuffers {
        let targets = NamedStreamTargets::new::<S::Output>(target_map);
        DynamicallyNamedStreamBuffer {
            targets: Arc::new(targets),
            container: Default::default(),
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
        let targets = buffer.targets;
        let mut was_unused = true;
        for NamedValue { name, value } in Rc::into_inner(buffer.container)
            .or_broken()?
            .into_inner()
            .into_iter()
        {
            was_unused = false;
            let target = targets.get(name.as_ref());
            let mut request = StreamRequest {
                source,
                session,
                target: target.map(NamedTarget::as_entity),
                world,
                roster,
            };

            S::side_effect(value, &mut request)
                .and_then(|value| {
                    target
                        .map(|t| t.send_output(NamedValue { name, value }, request))
                        .unwrap_or(Ok(()))
                })
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
        commands.add(SendNamedStreams::<
            S,
            DefaultStreamContainer<NamedValue<S::Input>>,
        >::new(
            buffer.container.take(), source, session, buffer.targets
        ));
    }

    fn set_stream_availability(availability: &mut StreamAvailability) {
        availability.add_anonymous::<NamedValue<S::Output>>();
    }

    fn are_streams_available(availability: &StreamAvailability) -> bool {
        availability.has_anonymous::<S::Output>()
    }

    fn into_dyn_stream_input_pack(
        pack: &mut DynStreamInputPack,
        inputs: Self::StreamInputPack,
    ) {
        pack.add_anonymous(inputs);
    }

    fn into_dyn_stream_output_pack(
        pack: &mut DynStreamOutputPack,
        outputs: Self::StreamOutputPack
    ) {
        pack.add_anonymous(outputs);
    }

    fn has_streams() -> bool {
        true
    }
}

pub struct DynamicallyNamedStreamChannel<S> {
    targets: Arc<NamedStreamTargets>,
    inner: Arc<InnerChannel>,
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S: StreamEffect> DynamicallyNamedStreamChannel<S> {
    pub fn send(&self, data: NamedValue<S::Input>) {
        let NamedValue { name, value } = data;
        let f = send_named_stream::<S>(
            self.inner.source,
            self.inner.session,
            Arc::clone(&self.targets),
            name,
            value,
        );

        self.inner.sender.send(Box::new(f)).ok();
    }

    fn new(targets: Arc<NamedStreamTargets>, inner: Arc<InnerChannel>) -> Self {
        Self {
            targets,
            inner,
            _ignore: Default::default(),
        }
    }
}

pub struct DynamicallyNamedStreamBuffer<T: 'static + Send + Sync> {
    targets: Arc<NamedStreamTargets>,
    container: Rc<RefCell<DefaultStreamContainer<NamedValue<T>>>>,
}

impl<T: 'static + Send + Sync> Clone for DynamicallyNamedStreamBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            targets: Arc::clone(&self.targets),
            container: Rc::clone(&self.container),
        }
    }
}

impl<T: 'static + Send + Sync> DynamicallyNamedStreamBuffer<T> {
    pub fn send(&self, input: NamedValue<T>) {
        self.container.borrow_mut().push(input);
    }
}
