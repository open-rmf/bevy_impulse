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
    prelude::{Entity, Component, Bundle, Resource, World},
    ecs::world::EntityMut,
};

use backtrace::Backtrace;

use smallvec::SmallVec;

use std::sync::Arc;

use crate::{
    Disposal, Filtered, OperationError, ScopeStorage, OrBroken,
    OperationCleanup, OperationResult, SingleTargetStorage, OperationRoster,
};

/// Information about the cancellation that occurred.
#[derive(Debug, Clone)]
pub struct Cancellation {
    /// The cause of a cancellation
    pub cause: Arc<CancellationCause>,
    /// Cancellations that occurred within cancellation workflows that were
    /// triggered by this cancellation.
    pub while_cancelling: Vec<Cancellation>,
}

impl Cancellation {
    pub fn from_cause(cause: CancellationCause) -> Self {
        Self { cause: Arc::new(cause), while_cancelling: Default::default() }
    }
}

impl<T: Into<CancellationCause>> From<T> for Cancellation {
    fn from(value: T) -> Self {
        Cancellation { cause: Arc::new(value.into()), while_cancelling: Default::default() }
    }
}

/// Get an explanation for why a cancellation occurred.
#[derive(Debug)]
pub enum CancellationCause {
    /// The promise taken by the requester was dropped without being detached.
    TargetDropped(Entity),

    /// There are no terminating nodes for the workflow that can be reached
    /// anymore.
    Unreachable(Unreachability),

    /// A filtering node has triggered a cancellation.
    Filtered(Filtered),

    /// A node in the workflow was broken, for example despawned or missing a
    /// component. This type of cancellation indicates that you are modifying
    /// the entities in a workflow in an unsupported way. If you believe that
    /// you are not doing anything unsupported then this could indicate a bug in
    /// `bevy_impulse` itself, and you encouraged to open an issue with a minimal
    /// reproducible example.
    ///
    /// The entity provided in [`BrokenLink`] is the link where the breakage was
    /// detected.
    Broken(Broken),
}

#[derive(Debug, Clone)]
pub struct Broken {
    pub node: Entity,
    pub backtrace: Option<Backtrace>,
}

impl From<Broken> for CancellationCause {
    fn from(value: Broken) -> Self {
        CancellationCause::Broken(value)
    }
}

/// Passed into the [`OperationRoster`](crate::OperationRoster) to pass a cancel
/// signal into the target.
#[derive(Debug, Clone)]
pub(crate) struct Cancel {
    /// The entity that triggered the cancellation
    pub(crate) source: Entity,
    /// The target of the cancellation
    pub(crate) target: Entity,
    /// The session which is being cancelled for the target
    pub(crate) session: Entity,
    /// Information about why a cancellation is happening
    pub(crate) cancellation: Cancellation,
}

impl Cancel {
    pub(crate) fn trigger(
        self,
        world: &mut World,
        roster: &mut OperationRoster,
    ) {
        if let Err(failure) = self.try_trigger(world, roster) {
            // We were unable to deliver the cancellation to the intended target.
            // We should move this into the unhandled errors resource so that it
            // does not get lost.
            world
            .get_resource_or_insert_with(|| UnhandledErrors::default())
            .cancellations.push(failure);
        }
    }

    fn try_trigger(
        self,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> Result<(), CancelFailure> {
        if let Some(cancel) = world.get::<OperationCancel>(self.target) {
            let cancel = cancel.0;
            (cancel)(
                OperationCleanup {
                    source: self.target,
                    session: self.session,
                    world,
                    roster
                },
                self.cancellation,
            );
        } else {
            return Err(CancelFailure::new(
                OperationError::Broken(Some(Backtrace::new())),
                self,
            ));
        }

        Ok(())
    }
}

/// A variant of [`CancellationCause`]
#[derive(Debug)]
pub struct Unreachability {
    /// The ID of the scope whose termination became unreachable.
    pub scope: Entity,
    /// The ID of the session whose termination became unreachable.
    pub session: Entity,
    /// A list of the disposals that occurred for this session.
    pub disposals: Vec<Disposal>,
}

impl Unreachability {
    pub fn new(scope: Entity, session: Entity, disposals: Vec<Disposal>) -> Self {
        Self { scope, session, disposals }
    }
}

impl From<Unreachability> for CancellationCause {
    fn from(value: Unreachability) -> Self {
        CancellationCause::Unreachable(value)
    }
}

/// Signals that a cancellation has occurred. This can be read by receivers
/// using [`try_take_cancel()`](ManageInput).
pub struct CancelSignal {
    pub session: Entity,
    pub cancellation: Cancellation,
}

#[derive(Component, Default)]
struct CancelSignalStorage {
    reverse_queue: SmallVec<[CancelSignal; 8]>,
}

pub trait ManageCancellation {
    /// Have this node emit a signal to cancel the current scope.
    fn emit_cancel(
        &mut self,
        session: Entity,
        cancellation: Cancellation,
        roster: &mut OperationRoster,
    );

    fn try_receive_cancel(&mut self) -> Result<Option<CancelSignal>, OperationError>;
}

impl<'w> ManageCancellation for EntityMut<'w> {
    fn emit_cancel(
        &mut self,
        session: Entity,
        cancellation: Cancellation,
        roster: &mut OperationRoster,
    ) {
        if let Err(failure) = try_emit_cancel(self, session, cancellation, roster) {
            // We were unable to emit the cancel according to the normal
            // procedure. We should move this into the unhandled errors resource
            // so that it does not get lost.
            self.world_scope(move |world| {
                world
                .get_resource_or_insert_with(|| UnhandledErrors::default())
                .cancellations.push(failure);
            });
        }
    }

    fn try_receive_cancel(&mut self) -> Result<Option<CancelSignal>, OperationError> {
        let mut storage = self.get_mut::<CancelSignalStorage>().or_broken()?;
        Ok(storage.reverse_queue.pop())
    }
}

fn try_emit_cancel(
    source_mut: &mut EntityMut,
    session: Entity,
    cancellation: Cancellation,
    roster: &mut OperationRoster,
) -> Result<(), CancelFailure> {
    let source = source_mut.id();
    if let Some(scope) = source_mut.get::<ScopeStorage>() {
        // The cancellation is happening inside a scope, so we should cancel
        // the scope
        let scope = scope.get();
        roster.cancel(Cancel { source, target: scope, session, cancellation });
    } else if let Some(target) = source_mut.get::<SingleTargetStorage>() {
        let target = target.get();
        roster.cancel(Cancel { source, target, session, cancellation });
    } else {
        return Err(CancelFailure::new(
            OperationError::Broken(Some(Backtrace::new())),
            Cancel {
                source,
                target: source,
                session,
                cancellation,
            }
        ));
    }

    Ok(())
}

pub struct CancelFailure {
    /// The error produced while the cancellation was happening
    pub error: OperationError,
    /// The cancellation that was being emitted
    pub cancel: Cancel,
}

impl CancelFailure {
    fn new(
        error: OperationError,
        cancel: Cancel,
    ) -> Self {
        Self { error, cancel }
    }
}

#[derive(Resource, Default)]
pub struct UnhandledErrors {
    pub cancellations: Vec<CancelFailure>,
}

#[derive(Component)]
struct OperationCancel(fn(OperationCleanup, Cancellation) -> OperationResult);

#[derive(Bundle)]
pub struct CancellableBundle {
    storage: CancelSignalStorage,
    cancel: OperationCancel,
}

impl CancellableBundle {
    pub fn new(cancel: fn(OperationCleanup, Cancellation) -> OperationResult) -> Self {
        CancellableBundle { storage: Default::default(), cancel: OperationCancel(cancel) }
    }
}
