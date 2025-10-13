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
    prelude::{ChildOf, Commands, Component, Entity, World},
    system::Command,
};

use std::{borrow::Cow, cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use tokio::sync::mpsc::unbounded_channel;

use crate::{
    AddImpulse, AddOperation, Builder, DefaultStreamBufferContainer, DeferredRoster,
    ExitTargetStorage, InnerChannel, Input, InputBundle, InputSlot, ManageInput, OperationRequest,
    OperationResult, OperationRoster, OperationSetup, OrBroken, Output, Push, Receiver,
    RedirectScopeStream, RedirectWorkflowStream, ReportUnhandled, ScopeStorage, SingleInputStorage,
    StreamEffect, StreamRedirect, StreamRequest, StreamTargetMap, TakenStream, UnusedStreams,
    UnusedTarget,
};

pub struct NamedStream<S: StreamEffect>(std::marker::PhantomData<fn(S)>);

impl<S: StreamEffect> NamedStream<S> {
    pub fn spawn_scope_stream(
        in_scope: Entity,
        out_scope: Entity,
        commands: &mut Commands,
    ) -> (InputSlot<S::Input>, Output<S::Output>) {
        let source = commands.spawn(()).id();
        let target = commands.spawn(UnusedTarget).id();
        commands.queue(AddOperation::new(
            Some(in_scope),
            source,
            RedirectScopeStream::<S>::new(target),
        ));

        (
            InputSlot::new(in_scope, source),
            Output::new(out_scope, target),
        )
    }

    pub fn spawn_workflow_stream(
        name: impl Into<Cow<'static, str>>,
        builder: &mut Builder,
    ) -> InputSlot<S::Input> {
        let source = builder.commands.spawn(()).id();
        builder.commands.queue(AddOperation::new(
            Some(builder.scope()),
            source,
            RedirectWorkflowStream::new(NamedStreamRedirect::<S>::static_name(name.into())),
        ));
        InputSlot::new(builder.scope(), source)
    }

    pub fn spawn_node_stream(
        name: impl Into<Cow<'static, str>>,
        source: Entity,
        map: &mut StreamTargetMap,
        builder: &mut Builder,
    ) -> Output<S::Output> {
        let target = builder
            .commands
            .spawn((SingleInputStorage::new(source), UnusedTarget))
            .id();

        map.add_named::<S::Output>(name.into(), target, builder.commands());
        Output::new(builder.scope(), target)
    }

    pub fn take_stream(
        name: impl Into<Cow<'static, str>>,
        source: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) -> Receiver<S::Output> {
        let (sender, receiver) = unbounded_channel::<S::Output>();
        let target = commands.spawn(()).insert(ChildOf(source)).id();

        map.add_named::<S::Output>(name.into(), target, commands);
        commands.queue(AddImpulse::new(None, target, TakenStream::new(sender)));

        receiver
    }

    pub fn collect_stream(
        name: impl Into<Cow<'static, str>>,
        source: Entity,
        target: Entity,
        map: &mut StreamTargetMap,
        commands: &mut Commands,
    ) {
        let name = name.into();
        let redirect = commands.spawn(()).insert(ChildOf(source)).id();
        commands.queue(AddImpulse::new(
            None,
            redirect,
            Push::<S::Output>::new(target, true).with_name(name.clone()),
        ));
        map.add_named::<S::Output>(name, redirect, commands);
    }

    pub fn make_stream_channel(
        name: impl Into<Cow<'static, str>>,
        inner: &Arc<InnerChannel>,
        world: &World,
    ) -> NamedStreamChannel<S> {
        let targets =
            NamedStreamTargets::new::<S::Output>(world.get::<StreamTargetMap>(inner.source()));
        NamedStreamChannel::new(name.into(), Arc::new(targets), Arc::clone(&inner))
    }

    pub fn make_stream_buffer(target_map: Option<&StreamTargetMap>) -> NamedStreamBuffer<S::Input> {
        let targets = NamedStreamTargets::new::<S::Output>(target_map);
        NamedStreamBuffer {
            targets: Arc::new(targets),
            container: Default::default(),
        }
    }

    pub fn process_stream_buffer(
        name: impl Into<Cow<'static, str>>,
        buffer: NamedStreamBuffer<S::Input>,
        source: Entity,
        session: Entity,
        unused: &mut UnusedStreams,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let name = name.into();
        let targets = buffer.targets;
        let mut was_unused = true;
        for value in Rc::into_inner(buffer.container)
            .or_broken()?
            .into_inner()
            .into_iter()
        {
            was_unused = false;
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
                        .map(|t| {
                            t.send_output(
                                NamedValue {
                                    name: name.clone(),
                                    value,
                                },
                                request,
                            )
                        })
                        .unwrap_or(Ok(()))
                })
                .report_unhandled(source, world);
        }

        if was_unused {
            unused.streams.push(std::any::type_name::<Self>());
        }

        Ok(())
    }

    pub fn defer_buffer(
        name: impl Into<Cow<'static, str>>,
        buffer: NamedStreamBuffer<S::Input>,
        source: Entity,
        session: Entity,
        commands: &mut Commands,
    ) {
        let name = name.into();
        let container: DefaultStreamBufferContainer<_> = buffer
            .container
            .take()
            .into_iter()
            .map(|value| NamedValue {
                name: name.clone(),
                value,
            })
            .collect();

        commands.queue(SendNamedStreams::<
            S,
            DefaultStreamBufferContainer<NamedValue<S::Input>>,
        >::new(container, source, session, buffer.targets));
    }
}

/// A container that can tie together a name with a value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NamedValue<T: 'static + Send + Sync> {
    pub name: Cow<'static, str>,
    pub value: T,
}

impl<T: 'static + Send + Sync> NamedValue<T> {
    pub fn new(name: impl Into<Cow<'static, str>>, value: T) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
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
    pub fn new<T: 'static + Send + Sync>(targets: Option<&StreamTargetMap>) -> Self {
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

    pub fn get(&self, name: &str) -> Option<NamedTarget> {
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
    static_name: Option<Cow<'static, str>>,
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S: StreamEffect> NamedStreamRedirect<S> {
    pub fn dynamic() -> Self {
        Self {
            static_name: None,
            _ignore: Default::default(),
        }
    }

    pub fn static_name(static_name: Cow<'static, str>) -> Self {
        Self {
            static_name: Some(static_name),
            _ignore: Default::default(),
        }
    }
}

#[derive(Component)]
struct StaticStreamName(Option<Cow<'static, str>>);

impl<S: StreamEffect> StreamRedirect for NamedStreamRedirect<S> {
    type Input = NamedValue<S::Input>;

    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        let mut entity_mut = world.entity_mut(source);
        if self.static_name.is_some() {
            // We have a static name for this redirect then we expect to receive
            // a plain S::Input
            entity_mut.insert(InputBundle::<S::Input>::new());
        } else {
            // If there is no static name then this is meant to receive any
            // named input.
            entity_mut.insert(InputBundle::<NamedValue<S::Input>>::new());
        }

        entity_mut.insert(StaticStreamName(self.static_name));

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

        let static_name = source_mut.get::<StaticStreamName>().or_broken()?.0.clone();
        let (scoped_session, name, value) = match static_name {
            Some(name) => {
                // If this named stream has a static name then we expect to receive
                // a plain S::Input and use the stream's static name.
                let Input {
                    session: scoped_session,
                    data: value,
                } = source_mut.take_input::<S::Input>()?;
                (scoped_session, name, value)
            }
            None => {
                // f this named stream does not have a static name then it's for
                // a dynamically named stream, so we expect to receive a named
                // value.
                let Input {
                    session: scoped_session,
                    data: NamedValue { name, value },
                } = source_mut.take_input::<NamedValue<S::Input>>()?;
                (scoped_session, name, value)
            }
        };

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

pub struct NamedStreamChannel<S> {
    name: Cow<'static, str>,
    targets: Arc<NamedStreamTargets>,
    inner: Arc<InnerChannel>,
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S: StreamEffect> NamedStreamChannel<S> {
    pub fn send(&self, data: S::Input) {
        let f = send_named_stream::<S>(
            self.inner.source,
            self.inner.session,
            Arc::clone(&self.targets),
            self.name.clone(),
            data,
        );

        self.inner.sender.send(Box::new(f)).ok();
    }

    fn new(
        name: Cow<'static, str>,
        targets: Arc<NamedStreamTargets>,
        inner: Arc<InnerChannel>,
    ) -> Self {
        Self {
            name,
            targets,
            inner,
            _ignore: Default::default(),
        }
    }
}

impl<S> Clone for NamedStreamChannel<S> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            targets: Arc::clone(&self.targets),
            inner: Arc::clone(&self.inner),
            _ignore: Default::default(),
        }
    }
}

pub(crate) fn send_named_stream<S: StreamEffect>(
    source: Entity,
    session: Entity,
    targets: Arc<NamedStreamTargets>,
    name: Cow<'static, str>,
    value: S::Input,
) -> impl FnOnce(&mut World, &mut OperationRoster) {
    move |world: &mut World, roster: &mut OperationRoster| {
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
    }
}

pub struct NamedStreamBuffer<T: 'static + Send + Sync> {
    targets: Arc<NamedStreamTargets>,
    container: Rc<RefCell<DefaultStreamBufferContainer<T>>>,
}

impl<T: 'static + Send + Sync> Clone for NamedStreamBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            targets: Arc::clone(&self.targets),
            container: Rc::clone(&self.container),
        }
    }
}

impl<T: 'static + Send + Sync> NamedStreamBuffer<T> {
    pub fn send(&self, input: T) {
        self.container.borrow_mut().push(input);
    }
}
