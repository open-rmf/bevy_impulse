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
    prelude::{Bundle, Component, Entity, World},
    world::EntityWorldMut,
};

use backtrace::Backtrace;

use thiserror::Error as ThisError;

use std::{fmt::Display, sync::Arc};

use crate::{
    CancelFailure, Disposal, Filtered, OperationError, OperationResult, OperationRoster,
    ScopeStorage, Supplanted, UnhandledErrors, DisplayDebugSlice,
};

/// Information about the cancellation that occurred.
#[derive(ThisError, Debug, Clone)]
#[error("A workflow or a request was cancelled")]
pub struct Cancellation {
    /// The cause of a cancellation
    pub cause: Arc<CancellationCause>,
    /// Cancellations that occurred within cancellation workflows that were
    /// triggered by this cancellation.
    pub while_cancelling: Vec<Cancellation>,
}

impl Cancellation {
    pub fn from_cause(cause: CancellationCause) -> Self {
        Self {
            cause: Arc::new(cause),
            while_cancelling: Default::default(),
        }
    }

    pub fn unreachable(scope: Entity, session: Entity, disposals: Vec<Disposal>) -> Self {
        Unreachability {
            scope,
            session,
            disposals,
        }
        .into()
    }

    pub fn filtered(filtered_at_node: Entity, reason: Option<anyhow::Error>) -> Self {
        Filtered {
            filtered_at_node,
            reason,
        }
        .into()
    }

    pub fn triggered(cancelled_at_node: Entity, value: Option<String>) -> Self {
        TriggeredCancellation {
            cancelled_at_node,
            value,
        }
        .into()
    }

    pub fn supplanted(
        supplanted_at_node: Entity,
        supplanted_by_node: Entity,
        supplanting_session: Entity,
    ) -> Self {
        Supplanted {
            supplanted_at_node,
            supplanted_by_node,
            supplanting_session,
        }
        .into()
    }

    pub fn invalid_span(from_point: Entity, to_point: Option<Entity>) -> Self {
        InvalidSpan {
            from_point,
            to_point,
        }
        .into()
    }

    pub fn circular_collect(conflicts: Vec<[Entity; 2]>) -> Self {
        CircularCollect { conflicts }.into()
    }

    pub fn undeliverable() -> Self {
        CancellationCause::Undeliverable.into()
    }
}

impl<T: Into<CancellationCause>> From<T> for Cancellation {
    fn from(value: T) -> Self {
        Cancellation {
            cause: Arc::new(value.into()),
            while_cancelling: Default::default(),
        }
    }
}

/// Get an explanation for why a cancellation occurred.
#[derive(ThisError, Debug)]

pub enum CancellationCause {
    /// The promise taken by the requester was dropped without being detached.
    #[error("the promise taken by the requester was dropped without being detached: {:?}", .0)]
    TargetDropped(Entity),

    /// There are no terminating nodes for the workflow that can be reached
    /// anymore.
    #[error("{}", .0)]
    Unreachable(Unreachability),

    /// A filtering node has triggered a cancellation.
    #[error("{}", .0)]
    Filtered(Filtered),

    /// The workflow triggered its own cancellation.
    #[error("{}", .0)]
    Triggered(TriggeredCancellation),

    /// Some workflows will queue up requests to deliver them one at a time.
    /// Depending on the label of the incoming requests, a new request might
    /// supplant an earlier one, causing the earlier request to be cancelled.
    #[error("{}", .0)]
    Supplanted(Supplanted),

    /// An operation that acts on nodes within a workflow was given an invalid
    /// span to operate on.
    #[error("{}", .0)]
    InvalidSpan(InvalidSpan),

    /// There is a circular dependency between two or more collect operations.
    /// This will lead to problems with calculating reachability within the
    /// workflow and is likely to make the collect operations fail to behave as
    /// intended.
    ///
    /// If you need to have collect operations happen in a cycle, you can avoid
    /// this automatic cancellation by putting one or more of the offending
    /// collect operations into a scope that excludes the other collect
    /// operations while including the branches that it needs to collect from.
    #[error("{}", .0)]
    CircularCollect(CircularCollect),

    /// A request became undeliverable because the sender was dropped. This may
    /// indicate that a critical entity within a workflow was manually despawned.
    /// Check to make sure that you are not manually despawning anything that
    /// you shouldn't.
    #[error("request become undeliverable")]
    Undeliverable,

    /// A promise can never be delivered because the mutex inside of a [`Promise`][1]
    /// was poisoned.
    ///
    /// [1]: crate::Promise
    #[error("mutex poisoned inside of a promise")]
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
    #[error("{}", .0)]
    Broken(Broken),
}

/// A variant of [`CancellationCause`]
#[derive(ThisError, Debug)]
pub struct TriggeredCancellation {
    /// The cancellation node that was triggered.
    pub cancelled_at_node: Entity,
    /// The value that triggered the cancellation, if one was provided.
    pub value: Option<String>,
}

impl Display for TriggeredCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cancellation triggered at node [{:?}]", self.cancelled_at_node)?;
        if let Some(value) = &self.value {
            write!(f, " with value [{}]", value)?;
        } else {
            write!(f, " [no value mentioned]")?;
        }
        Ok(())
    }
}

impl From<TriggeredCancellation> for CancellationCause {
    fn from(value: TriggeredCancellation) -> Self {
        CancellationCause::Triggered(value)
    }
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

#[derive(ThisError, Debug, Clone)]
pub struct Broken {
    pub node: Entity,
    pub backtrace: Option<Backtrace>,
}

impl From<Broken> for CancellationCause {
    fn from(value: Broken) -> Self {
        CancellationCause::Broken(value)
    }
}

impl Display for Broken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "operation [{:?}] is broken", self.node)?;
        if let Some(backtrace) = &self.backtrace {
            write!(f, " at\n{backtrace:#?}")?;
        } else {
            write!(f, " [backtrace not given]")?;
        }

        Ok(())
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

    pub(crate) fn trigger(self, world: &mut World, roster: &mut OperationRoster) {
        if let Err(failure) = self.try_trigger(world, roster) {
            // We were unable to deliver the cancellation to the intended target.
            // We should move this into the unhandled errors resource so that it
            // does not get lost.
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .cancellations
                .push(failure);
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
            (cancel)(OperationCancel {
                cancel: self.clone(),
                world,
                roster,
            })
            .map_err(|error| CancelFailure::new(error, self))
        } else {
            Err(CancelFailure::new(
                OperationError::Broken(Some(Backtrace::new())),
                self,
            ))
        }
    }
}

/// A variant of [`CancellationCause`]
#[derive(ThisError, Debug)]
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
        Self {
            scope,
            session,
            disposals,
        }
    }
}

impl From<Unreachability> for CancellationCause {
    fn from(value: Unreachability) -> Self {
        CancellationCause::Unreachable(value)
    }
}

impl Display for Unreachability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.disposals.len() == 1 {
            write!(f, "termination node cannot be reached after 1 disposal:")?;
        } else {
            write!(f, "termination node cannot be reached after {} disposals:", self.disposals.len())?;
        }
        for disposal in &self.disposals {
            write!(f, "\n - {}", disposal.cause)?;
        }
        Ok(())
    }
}

/// A variant of [`CancellationCause`]
#[derive(ThisError, Debug)]
#[error("unable to calculate span from [{:?}] to [{:?}]", .from_point, .to_point)]
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

/// A variant of [`CancellationCause`]
#[derive(ThisError, Debug)]
#[error("a circular collect exists for:{}", DisplayDebugSlice(.conflicts))]
pub struct CircularCollect {
    pub conflicts: Vec<[Entity; 2]>,
}

impl From<CircularCollect> for CancellationCause {
    fn from(value: CircularCollect) -> Self {
        CancellationCause::CircularCollect(value)
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

    fn emit_broken(&mut self, backtrace: Option<Backtrace>, roster: &mut OperationRoster);
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
                    .get_resource_or_insert_with(UnhandledErrors::default)
                    .cancellations
                    .push(failure);
            });
        }
    }

    fn emit_broken(&mut self, backtrace: Option<Backtrace>, roster: &mut OperationRoster) {
        let cause = Broken {
            node: self.id(),
            backtrace,
        };
        if let Err(failure) = try_emit_cancel(self, None, cause.into(), roster) {
            // We were unable to emit the cancel according to the normal
            // procedure. We should move this into the unhandled errors resource
            // so that it does not get lost.
            self.world_scope(move |world| {
                world
                    .get_resource_or_insert_with(UnhandledErrors::default)
                    .cancellations
                    .push(failure);
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
        roster.cancel(Cancel {
            origin: source,
            target: scope,
            session,
            cancellation,
        });
    } else if let Some(session) = session {
        // The cancellation is not happening inside a scope, so we should tell
        // the session itself to cancel.
        roster.cancel(Cancel {
            origin: source,
            target: session,
            session: Some(session),
            cancellation,
        });
    } else {
        return Err(CancelFailure::new(
            OperationError::Broken(Some(Backtrace::new())),
            Cancel {
                origin: source,
                target: source,
                session,
                cancellation,
            },
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
        Cancellable {
            cancel: OperationCancelStorage(cancel),
        }
    }
}
