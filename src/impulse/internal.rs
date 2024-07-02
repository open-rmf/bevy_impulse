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

use bevy::{
    prelude::{World, Entity, Component, Resource, DespawnRecursiveExt},
    ecs::system::Command,
};

use backtrace::Backtrace;

use crossbeam::channel::{unbounded, Sender as CbSender, Receiver as CbReceiver};

use smallvec::SmallVec;

use crate::{
    OperationSetup, OperationRequest, OperationResult, OperationCancel,
    OperationExecuteStorage, OperationError, Cancellable, ManageCancellation,
    UnhandledErrors, CancelFailure, Cancel, Broken, SingleTargetStorage,
    UnusedTarget,
};

pub(crate) trait Impulsive {
    fn setup(self, info: OperationSetup) -> OperationResult;
    fn execute(request: OperationRequest) -> OperationResult;
}

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
        self.impulse.setup(OperationSetup { source: self.source, world });
        world.entity_mut(self.source)
            .insert((
                OperationExecuteStorage(perform_impulse::<I>),
                Cancellable::new(cancel_impulse),
            ))
            .remove::<UnusedTarget>();
    }
}

fn perform_impulse<I: Impulsive>(
    OperationRequest { source, world, roster }: OperationRequest,
) {
    match I::execute(OperationRequest { source, world, roster }) {
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
                .get_resource_or_insert_with(|| UnhandledErrors::default())
                .cancellations
                .push(CancelFailure {
                    error: OperationError::Broken(Some(Backtrace::new())),
                    cancel: Cancel {
                        source,
                        target: source,
                        session: None,
                        cancellation: Broken { node: source, backtrace }.into(),
                    }
                });
            }
        }
    }
}

fn cancel_impulse(
    OperationCancel { cancel, world, roster }: OperationCancel,
) -> OperationResult {
    // We cancel an impulse by travelling to its terminal and
    let mut terminal = cancel.source;
    loop {
        let Some(target) = world.get::<SingleTargetStorage>(terminal) else {
            break;
        };
        terminal = target.get();
    }

    if let Some(on_cancel) = world.get::<OnTerminalCancelled>(terminal) {
        let on_cancel = on_cancel.0;
        match on_cancel(OperationCancel { cancel: cancel.clone(), world, roster }) {
            Ok(()) | Err(OperationError::NotReady) => {
                // Do nothing
            }
            Err(OperationError::Broken(backtrace)) => {
                world
                .get_resource_or_insert_with(|| UnhandledErrors::default())
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

#[derive(Resource, Clone)]
pub(crate) struct ImpulseLifecycleChannel {
    pub(crate) sender: CbSender<Entity>,
    pub(crate) receiver: CbReceiver<Entity>,
}

impl Default for ImpulseLifecycleChannel {
    fn default() -> Self {
        let (sender, receiver) = unbounded();
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
    sender: CbSender<Entity>,
}

impl ImpulseLifecycle {
    fn new(source: Entity, sender: CbSender<Entity>) -> Self {
        Self {
            sources: SmallVec::from_iter([source]),
            sender,
        }
    }
}

impl Drop for ImpulseLifecycle {
    fn drop(&mut self) {
        for source in &self.sources {
            self.sender.send(*source);
        }
    }
}

pub(crate) fn add_lifecycle_dependency<'a>(
    source: Entity,
    target: Entity,
    world: &'a mut World,
) {
    let sender = world
        .get_resource_or_insert_with(|| ImpulseLifecycleChannel::default())
        .sender
        .clone();

    if let Some(mut lifecycle) = world.get_mut::<ImpulseLifecycle>(target) {
        lifecycle.sources.push(source);
    } else if let Some(mut target_mut) = world.get_entity_mut(target) {
        target_mut.insert(ImpulseLifecycle::new(source, sender));
    } else {
        // The target is already despawned
        sender.send(source);
    }
}
