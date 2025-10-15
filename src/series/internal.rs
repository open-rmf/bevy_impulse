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

use bevy_ecs::prelude::{ChildOf, Command, Component, Entity, Resource, World};

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

pub(crate) trait Executable {
    fn setup(self, info: OperationSetup) -> OperationResult;
    fn execute(request: OperationRequest) -> OperationResult;
}

#[derive(Component)]
pub(crate) struct SeriesMarker;

pub(crate) struct AddExecution<E: Executable> {
    source: Option<Entity>,
    target: Entity,
    execution: E,
}

impl<E: Executable> AddExecution<E> {
    pub(crate) fn new(source: Option<Entity>, target: Entity, execution: E) -> Self {
        Self {
            source,
            target,
            execution,
        }
    }
}

impl<I: Executable + 'static + Sync + Send> Command for AddExecution<I> {
    fn apply(self, world: &mut World) {
        if let Err(error) = self.execution.setup(OperationSetup {
            source: self.target,
            world,
        }) {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .setup
                .push(SetupFailure {
                    broken_node: self.target,
                    error,
                });
        }
        world
            .entity_mut(self.target)
            .insert((
                OperationExecuteStorage(perform_execution::<I>),
                Cancellable::new(cancel_execution),
                SeriesMarker,
            ))
            .remove::<UnusedTarget>();

        if let Some(source) = self.source {
            world.entity_mut(source).insert(ChildOf(self.target));
        }
    }
}

fn perform_execution<I: Executable>(
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
            if let Ok(mut source_mut) = world.get_entity_mut(source) {
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

pub(crate) fn cancel_execution(
    OperationCancel {
        cancel,
        world,
        roster,
    }: OperationCancel,
) -> OperationResult {
    // We cancel a series by travelling to its terminal and
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

    if let Ok(terminal_mut) = world.get_entity_mut(terminal) {
        terminal_mut.despawn();
    }

    Ok(())
}

#[derive(Component)]
pub(crate) struct OnTerminalCancelled(pub(crate) fn(OperationCancel) -> OperationResult);

#[derive(Resource)]
pub(crate) struct SeriesLifecycleChannel {
    pub(crate) sender: TokioSender<Entity>,
    pub(crate) receiver: TokioReceiver<Entity>,
}

impl Default for SeriesLifecycleChannel {
    fn default() -> Self {
        let (sender, receiver) = unbounded_channel();
        Self { sender, receiver }
    }
}

/// This component tracks the lifecycle of an entity that is the terminal
/// target of a series. When this component gets dropped, the upstream
/// chain will be notified.
#[derive(Component)]
pub(crate) struct SeriesLifecycle {
    /// The series sources that are feeding into the entity which holds this
    /// component.
    sources: SmallVec<[Entity; 8]>,
    /// Used to notify the flusher that the target of the sources has been dropped
    sender: TokioSender<Entity>,
}

impl SeriesLifecycle {
    fn new(source: Entity, sender: TokioSender<Entity>) -> Self {
        Self {
            sources: SmallVec::from_iter([source]),
            sender,
        }
    }
}

impl Drop for SeriesLifecycle {
    fn drop(&mut self) {
        for source in &self.sources {
            if let Err(err) = self.sender.send(*source) {
                eprintln!(
                    "Failed to notify that a series was dropped: {err}\nBacktrace:\n{:#?}",
                    Backtrace::new(),
                );
            }
        }
    }
}

pub(crate) fn add_lifecycle_dependency(source: Entity, target: Entity, world: &mut World) {
    let sender = world
        .get_resource_or_insert_with(SeriesLifecycleChannel::default)
        .sender
        .clone();

    if let Some(mut lifecycle) = world.get_mut::<SeriesLifecycle>(target) {
        lifecycle.sources.push(source);
    } else if let Ok(mut target_mut) = world.get_entity_mut(target) {
        target_mut.insert(SeriesLifecycle::new(source, sender));
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
