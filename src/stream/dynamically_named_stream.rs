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

use std::{borrow::Cow, cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use crate::{
    AddOperation, Builder, DefaultStreamContainer, DeferredRoster, ExitTargetStorage, InnerChannel,
    Input, InputBundle, InputSlot, ManageInput, OperationRequest, OperationResult, OperationRoster,
    OperationSetup, OrBroken, Output, RedirectWorkflowStream, ReportUnhandled, ScopeStorage,
    SingleInputStorage, Stream, StreamEffect, StreamRedirect, StreamRequest, StreamTargetMap,
    UnusedStreams, UnusedTarget,
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

impl<S: StreamEffect> Stream for DynamicallyNamedStream<S> {
    type StreamChannel = NamedStreamChannel<S>;
    type StreamBuffer = NamedStreamBuffer<S::Input>;

    fn spawn_workflow_stream(builder: &mut Builder) -> InputSlot<Self::Input> {
        let source = builder.commands.spawn(()).id();
        builder.commands.add(AddOperation::new(
            Some(builder.scope()),
            source,
            RedirectWorkflowStream::new(NamedStreamRedirect::<S>::default()),
        ));
        InputSlot::new(builder.scope, source)
    }

    fn spawn_node_stream(
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut crate::Builder,
    ) -> crate::Output<Self::Output> {
        let target = builder
            .commands
            .spawn((SingleInputStorage::new(source), UnusedTarget))
            .id();

        map.add_anonymous::<NamedValue<S::Output>>(target, builder.commands());
        Output::new(builder.scope, target)
    }

    fn make_stream_channel(inner: &Arc<InnerChannel>, world: &World) -> Self::StreamChannel {
        let targets =
            NamedStreamTargets::new::<S::Output>(world.get::<StreamTargetMap>(inner.source()));
        NamedStreamChannel::new(Arc::new(targets), Arc::clone(&inner))
    }

    fn make_stream_buffer(target_map: Option<&StreamTargetMap>) -> Self::StreamBuffer {
        let targets = NamedStreamTargets::new::<S::Output>(target_map);
        NamedStreamBuffer {
            targets: Arc::new(targets),
            container: Default::default(),
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

    fn defer_buffer(
        buffer: Self::StreamBuffer,
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
}

/// A container that can tie together a name with a value.
#[derive(Debug, Clone)]
pub struct NamedValue<T: 'static + Send + Sync> {
    pub name: Cow<'static, str>,
    pub value: T,
}

#[derive(Default)]
pub struct NamedStreamTargets {
    /// Targets that connected to a specific stream name
    specific: HashMap<Cow<'static, str>, Entity>,
    /// A target that connected to a general NamedStream
    general: Option<Entity>,
    /// A target that accepts the base stream output
    anonymous: Option<Entity>,
}

impl NamedStreamTargets {
    fn new<T: 'static + Send + Sync>(targets: Option<&StreamTargetMap>) -> Self {
        let Some(targets) = targets else {
            return Self::default();
        };

        let output_type = std::any::TypeId::of::<T>();

        let mut specific = HashMap::new();
        for (name, (ty, target)) in &targets.named {
            if *ty == output_type {
                specific.insert(name.clone(), *target);
            }
        }

        let general = targets.get_anonymous::<NamedValue<T>>();
        let anonymous = targets.get_anonymous::<T>();

        Self {
            specific,
            general,
            anonymous,
        }
    }

    fn get(&self, name: &str) -> Option<NamedTarget> {
        self
            // First check the specifically named connections
            .specific
            .get(name)
            .copied()
            .map(NamedTarget::Value)
            // If there is no specifically named connection, then use a target
            // that accepts all named streams
            .or_else(|| self.general.map(NamedTarget::NamedValue))
            // If there is no target accepting all naemd streams then use one
            // that accepts the output type without any name
            .or_else(|| self.anonymous.map(NamedTarget::Value))
    }
}

#[derive(Clone, Copy)]
pub enum NamedTarget {
    /// Send a named value
    NamedValue(Entity),
    /// Send only the value
    Value(Entity),
}

impl NamedTarget {
    pub fn as_entity(self) -> Entity {
        match self {
            Self::NamedValue(target) => target,
            Self::Value(target) => target,
        }
    }

    pub fn send_output<T: 'static + Send + Sync>(
        self,
        NamedValue { name, value }: NamedValue<T>,
        request: StreamRequest,
    ) -> OperationResult {
        match self {
            NamedTarget::NamedValue(_) => request.send_output(NamedValue { name, value }),
            NamedTarget::Value(_) => request.send_output(value),
        }
    }
}

pub struct NamedStreamChannel<S> {
    targets: Arc<NamedStreamTargets>,
    inner: Arc<InnerChannel>,
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S: StreamEffect> NamedStreamChannel<S> {
    pub fn send(&self, data: NamedValue<S::Input>) {
        let source = self.inner.source;
        let session = self.inner.session;
        let targets = Arc::clone(&self.targets);
        self.inner
            .sender
            .send(Box::new(
                move |world: &mut World, roster: &mut OperationRoster| {
                    let NamedValue { name, value } = data;
                    let target = targets.get(&name);
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
                },
            ))
            .ok();
    }

    fn new(targets: Arc<NamedStreamTargets>, inner: Arc<InnerChannel>) -> Self {
        Self {
            targets,
            inner,
            _ignore: Default::default(),
        }
    }
}

pub struct NamedStreamBuffer<T: 'static + Send + Sync> {
    targets: Arc<NamedStreamTargets>,
    container: Rc<RefCell<DefaultStreamContainer<NamedValue<T>>>>,
}

impl<T: 'static + Send + Sync> Clone for NamedStreamBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            targets: Arc::clone(&self.targets),
            container: Rc::clone(&self.container),
        }
    }
}

pub struct SendNamedStreams<S, Container> {
    container: Container,
    source: Entity,
    session: Entity,
    targets: Arc<NamedStreamTargets>,
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S, Container> SendNamedStreams<S, Container> {
    pub fn new(
        container: Container,
        source: Entity,
        session: Entity,
        targets: Arc<NamedStreamTargets>,
    ) -> Self {
        Self {
            container,
            source,
            session,
            targets,
            _ignore: Default::default(),
        }
    }
}

impl<S, Container> Command for SendNamedStreams<S, Container>
where
    S: StreamEffect,
    Container: 'static + Send + Sync + IntoIterator<Item = NamedValue<S::Input>>,
{
    fn apply(self, world: &mut World) {
        world.get_resource_or_insert_with(DeferredRoster::default);
        world.resource_scope::<DeferredRoster, _>(|world, mut deferred| {
            for data in self.container {
                let NamedValue { name, value } = data;
                let target = self.targets.get(&name);
                let mut request = StreamRequest {
                    source: self.source,
                    session: self.session,
                    target: target.map(NamedTarget::as_entity),
                    world,
                    roster: &mut deferred,
                };

                S::side_effect(value, &mut request)
                    .and_then(move |value| {
                        target
                            .map(|t| t.send_output(NamedValue { name, value }, request))
                            .unwrap_or(Ok(()))
                    })
                    .report_unhandled(self.source, world);
            }
        });
    }
}

pub struct NamedStreamRedirect<S: StreamEffect> {
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S: StreamEffect> Default for NamedStreamRedirect<S> {
    fn default() -> Self {
        Self {
            _ignore: Default::default(),
        }
    }
}

impl<S: StreamEffect> StreamRedirect for NamedStreamRedirect<S> {
    type Input = NamedValue<S::Input>;

    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .entity_mut(source)
            .insert(InputBundle::<NamedValue<S::Input>>::new());
        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input {
            session: scoped_session,
            data: NamedValue { name, value },
        } = source_mut.take_input::<NamedValue<S::Input>>()?;
        let scope = world.get::<ScopeStorage>(source).or_broken()?.get();

        let exit = world
            .get::<ExitTargetStorage>(scope)
            .or_broken()?
            .map
            .get(&scoped_session)
            .or_not_ready()?;

        let exit_source = exit.source;
        let parent_session = exit.parent_session;

        let stream_targets = world.get::<StreamTargetMap>(exit_source).or_broken()?;

        let target = stream_targets.get_named_or_anonymous::<S::Output>(&name);
        let mut request = StreamRequest {
            source,
            session: parent_session,
            target: target.map(NamedTarget::as_entity),
            world,
            roster,
        };

        S::side_effect(value, &mut request).and_then(|value| {
            target
                .map(|t| t.send_output(NamedValue { name, value }, request))
                .unwrap_or(Ok(()))
        })?;

        Ok(())
    }
}
