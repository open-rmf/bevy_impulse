/*
 * Copyright (C) 2024 Open Source Robotics Foundation
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
    prelude::{Component, Entity, Resource, World},
    world::Command,
};
use bevy_hierarchy::DespawnRecursiveExt;

use backtrace::Backtrace;

use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as TokioReceiver, UnboundedSender as TokioSender,
};

use smallvec::SmallVec;

use anyhow::anyhow;

use std::sync::Arc;

use crate::{
    Broken, Cancel, CancelFailure, Cancellable, ManageCancellation, MiscellaneousFailure,
    OperationCancel, OperationError, OperationExecuteStorage, OperationRequest, OperationResult,
    OperationSetup, SetupFailure, SingleTargetStorage, UnhandledErrors, UnusedTarget,
};

pub(crate) trait Impulsive {
    fn setup(self, info: OperationSetup) -> OperationResult;
    fn execute(request: OperationRequest) -> OperationResult;
}

#[derive(Component)]
pub(crate) struct ImpulseMarker;

pub(crate) struct AddImpulse<I: Impulsive> {
    source: Entity,
    impulse: I,
}

impl<I: Impulsive> AddImpulse<I> {
    pub(crate) fn new(source: Entity, impulse: I) -> Self {
        Self { source, impulse }
    }
}

impl<I: Impulsive + 'static + Sync + Send> Command for AddImpulse<I> {
    fn apply(self, world: &mut World) {
        if let Err(error) = self.impulse.setup(OperationSetup {
            source: self.source,
            world,
        }) {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .setup
                .push(SetupFailure {
                    broken_node: self.source,
                    error,
                });
        }
        world
            .entity_mut(self.source)
            .insert((
                OperationExecuteStorage(perform_impulse::<I>),
                Cancellable::new(cancel_impulse),
                ImpulseMarker,
            ))
            .remove::<UnusedTarget>();
    }
}

fn perform_impulse<I: Impulsive>(
    OperationRequest {
        source,
        world,
        roster,
    }: OperationRequest,
) {
    match I::execute(OperationRequest {
        source,
        world,
        roster,
    }) {
        Ok(()) => {
            // Do nothing
        }
        Err(OperationError::NotReady) => {
            // Do nothing
        }
        Err(OperationError::Broken(backtrace)) => {
            if let Some(mut source_mut) = world.get_entity_mut(source) {
                source_mut.emit_broken(backtrace, roster);
            } else {
                world
                    .get_resource_or_insert_with(UnhandledErrors::default)
                    .cancellations
                    .push(CancelFailure {
                        error: OperationError::Broken(Some(Backtrace::new())),
                        cancel: Cancel {
                            origin: source,
                            target: source,
                            session: None,
                            cancellation: Broken {
                                node: source,
                                backtrace,
                            }
                            .into(),
                        },
                    });
            }
        }
    }
}

pub(crate) fn cancel_impulse(
    OperationCancel {
        cancel,
        world,
        roster,
    }: OperationCancel,
) -> OperationResult {
    // We cancel an impulse by travelling to its terminal and
    let mut terminal = cancel.target;
    loop {
        let Some(target) = world.get::<SingleTargetStorage>(terminal) else {
            break;
        };
        terminal = target.get();
    }

    if let Some(on_cancel) = world.get::<OnTerminalCancelled>(terminal) {
        let on_cancel = on_cancel.0;
        let cancel = cancel.for_target(terminal);
        match on_cancel(OperationCancel {
            cancel: cancel.clone(),
            world,
            roster,
        }) {
            Ok(()) | Err(OperationError::NotReady) => {
                // Do nothing
            }
            Err(OperationError::Broken(backtrace)) => {
                world
                    .get_resource_or_insert_with(UnhandledErrors::default)
                    .cancellations
                    .push(CancelFailure {
                        error: OperationError::Broken(backtrace),
                        cancel,
                    });
            }
        }
    }

    if let Some(terminal_mut) = world.get_entity_mut(terminal) {
        terminal_mut.despawn_recursive();
    }

    Ok(())
}

#[derive(Component)]
pub(crate) struct OnTerminalCancelled(pub(crate) fn(OperationCancel) -> OperationResult);

#[derive(Resource)]
pub(crate) struct ImpulseLifecycleChannel {
    pub(crate) sender: TokioSender<Entity>,
    pub(crate) receiver: TokioReceiver<Entity>,
}

impl Default for ImpulseLifecycleChannel {
    fn default() -> Self {
        let (sender, receiver) = unbounded_channel();
        Self { sender, receiver }
    }
}

/// This component tracks the lifecycle of an entity that is the terminal
/// target of an impulse chain. When this component gets dropped, the upstream
/// chain will be notified.
#[derive(Component)]
pub(crate) struct ImpulseLifecycle {
    /// The impulse sources that are feeding into the entity which holds this
    /// component.
    sources: SmallVec<[Entity; 8]>,
    /// Used to notify the flusher that the target of the sources has been dropped
    sender: TokioSender<Entity>,
}

impl ImpulseLifecycle {
    fn new(source: Entity, sender: TokioSender<Entity>) -> Self {
        Self {
            sources: SmallVec::from_iter([source]),
            sender,
        }
    }
}

impl Drop for ImpulseLifecycle {
    fn drop(&mut self) {
        for source in &self.sources {
            if let Err(err) = self.sender.send(*source) {
                eprintln!(
                    "Failed to notify that an impulse was dropped: {err}\nBacktrace:\n{:#?}",
                    Backtrace::new(),
                );
            }
        }
    }
}

pub(crate) fn add_lifecycle_dependency(source: Entity, target: Entity, world: &mut World) {
    let sender = world
        .get_resource_or_insert_with(ImpulseLifecycleChannel::default)
        .sender
        .clone();

    if let Some(mut lifecycle) = world.get_mut::<ImpulseLifecycle>(target) {
        lifecycle.sources.push(source);
    } else if let Some(mut target_mut) = world.get_entity_mut(target) {
        target_mut.insert(ImpulseLifecycle::new(source, sender));
    } else {
        // The target is already despawned
        if let Err(err) = sender.send(source) {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: Arc::new(anyhow!(
                        "Failed to notify that a target is already despawned: {err}"
                    )),
                    backtrace: Some(Backtrace::new()),
                })
        }
    }
}
