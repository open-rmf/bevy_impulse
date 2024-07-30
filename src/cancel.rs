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
    prelude::{Entity, Component, Bundle, World},
    ecs::world::EntityWorldMut,
};

use backtrace::Backtrace;

use std::sync::Arc;

use crate::{
    Disposal, Filtered, OperationError, ScopeStorage, CancelFailure,
    OperationResult, OperationRoster, Supplanted, UnhandledErrors,
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

    pub fn filtered(filtered_at_node: Entity, reason: Option<anyhow::Error>) -> Self {
        Filtered { filtered_at_node, reason }.into()
    }

    pub fn supplanted(
        supplanted_at_node: Entity,
        supplanted_by_node: Entity,
        supplanting_session: Entity,
    ) -> Self {
        Supplanted { supplanted_at_node, supplanted_by_node, supplanting_session }.into()
    }

    pub fn invalid_span(
        from_point: Entity,
        to_point: Option<Entity>,
    ) -> Self {
        InvalidSpan { from_point, to_point }.into()
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

    /// Some workflows will queue up requests to deliver them one at a time.
    /// Depending on the label of the incoming requests, a new request might
    /// supplant an earlier one, causing the earlier request to be cancelled.
    Supplanted(Supplanted),

    /// An operation that acts on nodes within a workflow was given an invalid
    /// span to operate on.
    InvalidSpan(InvalidSpan),

    /// A promise can never be delivered because the mutex inside of a [`Promise`][1]
    /// was poisoned.
    ///
    /// [1]: crate::Promise
    PoisonedMutexInPromise,

    /// A node in the workflow was broken, for example despawned or missing a
    /// component. This type of cancellation indicates that you are modifying
    /// the entities in a workflow in an unsupported way. If you believe that
    /// you are not doing anything unsupported then this could indicate a bug in
    /// `bevy_impulse` itself, and you encouraged to open an issue with a minimal
    /// reproducible example.
    ///
    /// The entity provided in [`Broken`] is the link where the breakage was
    /// detected.
    Broken(Broken),
}

impl From<Filtered> for CancellationCause {
    fn from(value: Filtered) -> Self {
        CancellationCause::Filtered(value)
    }
}

impl From<Supplanted> for CancellationCause {
    fn from(value: Supplanted) -> Self {
        CancellationCause::Supplanted(value)
    }
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

/// Passed into the [`OperationRoster`] to pass a cancel  signal into the target.
#[derive(Debug, Clone)]
pub struct Cancel {
    /// The entity that triggered the cancellation
    pub(crate) origin: Entity,
    /// The target of the cancellation
    pub(crate) target: Entity,
    /// The session which is being cancelled for the target
    pub(crate) session: Option<Entity>,
    /// Information about why a cancellation is happening
    pub(crate) cancellation: Cancellation,
}

impl Cancel {
    pub(crate) fn for_target(mut self, target: Entity) -> Self {
        self.target = target;
        self
    }

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
        if let Some(cancel) = world.get::<OperationCancelStorage>(self.target) {
            let cancel = cancel.0;
            // TODO(@mxgrey): Figure out a way to structure this so we don't
            // need to always clone self.
            return (cancel)(OperationCancel { cancel: self.clone(), world, roster })
                .map_err(|error| {
                    CancelFailure::new(error, self)
                });
        } else {
            return Err(CancelFailure::new(
                OperationError::Broken(Some(Backtrace::new())),
                self,
            ));
        }
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

/// A variant of [`CancellationCause`]
#[derive(Debug)]
pub struct InvalidSpan {
    /// The starting point of the span
    pub from_point: Entity,
    /// The ending point of the span
    pub to_point: Option<Entity>,
}

impl From<InvalidSpan> for CancellationCause {
    fn from(value: InvalidSpan) -> Self {
        CancellationCause::InvalidSpan(value)
    }
}

pub trait ManageCancellation {
    /// Have this node emit a signal to cancel the current scope.
    fn emit_cancel(
        &mut self,
        session: Entity,
        cancellation: Cancellation,
        roster: &mut OperationRoster,
    );

    fn emit_broken(
        &mut self,
        backtrace: Option<Backtrace>,
        roster: &mut OperationRoster,
    );
}

impl<'w> ManageCancellation for EntityWorldMut<'w> {
    fn emit_cancel(
        &mut self,
        session: Entity,
        cancellation: Cancellation,
        roster: &mut OperationRoster,
    ) {
        if let Err(failure) = try_emit_cancel(self, Some(session), cancellation, roster) {
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

    fn emit_broken(
        &mut self,
        backtrace: Option<Backtrace>,
        roster: &mut OperationRoster,
    ) {
        let cause = Broken { node: self.id(), backtrace };
        if let Err(failure) = try_emit_cancel(self, None, cause.into(), roster) {
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
}

pub fn try_emit_broken(
    source: Entity,
    backtrace: Option<Backtrace>,
    world: &mut World,
    roster: &mut OperationRoster,
) {
    if let Some(mut source_mut) = world.get_entity_mut(source) {
        source_mut.emit_broken(backtrace, roster);
    } else {
        world
        .get_resource_or_insert_with(|| UnhandledErrors::default())
        .cancellations
        .push(CancelFailure {
            error: OperationError::Broken(Some(Backtrace::new())),
            cancel: Cancel {
                origin: source,
                target: source,
                session: None,
                cancellation: Broken { node: source, backtrace }.into(),
            }
        });
    }
}

fn try_emit_cancel(
    source_mut: &mut EntityWorldMut,
    session: Option<Entity>,
    cancellation: Cancellation,
    roster: &mut OperationRoster,
) -> Result<(), CancelFailure> {
    let source = source_mut.id();
    if let Some(scope) = source_mut.get::<ScopeStorage>() {
        // The cancellation is happening inside a scope, so we should cancel
        // the scope
        let scope = scope.get();
        roster.cancel(Cancel { origin: source, target: scope, session, cancellation });
    } else if let Some(session) = session {
        // The cancellation is not happening inside a scope, so we should tell
        // the session itself to cancel.
        roster.cancel(Cancel { origin: source, target: session, session: Some(session), cancellation });
    } else {
        return Err(CancelFailure::new(
            OperationError::Broken(Some(Backtrace::new())),
            Cancel {
                origin: source,
                target: source,
                session,
                cancellation,
            }
        ));
    }

    Ok(())
}

pub struct OperationCancel<'a> {
    pub cancel: Cancel,
    pub world: &'a mut World,
    pub roster: &'a mut OperationRoster,
}

#[derive(Component)]
struct OperationCancelStorage(fn(OperationCancel) -> OperationResult);

#[derive(Bundle)]
pub struct Cancellable {
    cancel: OperationCancelStorage,
}

impl Cancellable {
    pub fn new(cancel: fn(OperationCancel) -> OperationResult) -> Self {
        Cancellable { cancel: OperationCancelStorage(cancel) }
    }
}
