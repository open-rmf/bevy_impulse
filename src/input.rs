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
    prelude::{Bundle, Component, Entity},
    system::Command,
    world::{EntityRef, EntityWorldMut, World},
};

use smallvec::SmallVec;

use backtrace::Backtrace;

use crate::{
    Broken, BufferStorage, Cancel, Cancellation, CancellationCause, DeferredRoster, Detached,
    MiscellaneousFailure, OperationError, OperationRoster, OrBroken, SessionStatus,
    UnhandledErrors, UnusedTarget,
};

/// This contains data that has been provided as input into an operation, along
/// with an indication of what session the data belongs to.
pub struct Input<T> {
    pub session: Entity,
    pub data: T,
}

/// General purpose input storage used by most [operations](crate::Operation).
/// This component is inserted on the source entity of the operation and will
/// queue up inputs that have arrived for the source.
#[derive(Component)]
pub(crate) struct InputStorage<T> {
    // Items will be inserted into this queue from the front, so we pop off the
    // back to get the oldest items out.
    // TODO(@mxgrey): Consider if it's worth implementing a Deque on top of
    // the SmallVec data structure.
    reverse_queue: SmallVec<[Input<T>; 16]>,
}

impl<T> InputStorage<T> {
    pub fn new() -> Self {
        Self {
            reverse_queue: Default::default(),
        }
    }

    pub fn contains_session(&self, session: Entity) -> bool {
        self.reverse_queue
            .iter()
            .any(|input| input.session == session)
    }
}

impl<T> Default for InputStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Used to keep track of the expected input type for an operation
#[derive(Component)]
pub(crate) struct InputTypeIndicator {
    pub(crate) name: &'static str,
}

impl InputTypeIndicator {
    fn new<T>() -> Self {
        Self {
            name: std::any::type_name::<T>(),
        }
    }
}

#[derive(Bundle)]
pub struct InputBundle<T: 'static + Send + Sync> {
    storage: InputStorage<T>,
    indicator: InputTypeIndicator,
}

impl<T: 'static + Send + Sync> InputBundle<T> {
    pub fn new() -> Self {
        Self {
            storage: Default::default(),
            indicator: InputTypeIndicator::new::<T>(),
        }
    }
}

impl<T: 'static + Send + Sync> Default for InputBundle<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ManageInput {
    /// Give an input to this node. The node will be queued up to immediately
    /// process the input.
    fn give_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError>;

    /// Same as [`Self::give_input`], but the wakeup for this node will be
    /// deferred until after the async updates are flushed. This is used for
    /// async task output to ensure that all async operations, such as streams,
    /// are finished being processed before the final output gets processed.
    fn defer_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError>;

    /// Give an input to this node without flagging it in the roster. This
    /// should not generally be used. It's only for special cases where we know
    /// the node will be manually run after giving this input. It's marked
    /// unsafe to bring attention to this requirement.
    ///
    /// # Safety
    ///
    /// After calling this function you must make sure to either add the target
    /// operation to the queue or run the operation explicitly. Failing to do
    /// one of these could mean that this input (or one that follows it) will
    /// never be processed, which could cause a workflow to hang forever.
    unsafe fn sneak_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        only_if_active: bool,
        roster: &mut OperationRoster,
    ) -> Result<bool, OperationError>;

    /// Get an input that is ready to be taken, or else produce an error.
    fn take_input<T: 'static + Send + Sync>(&mut self) -> Result<Input<T>, OperationError>;

    /// Try to take an input if one is ready. If no input is ready this will
    /// return Ok(None). It only returns an error if the node is broken.
    fn try_take_input<T: 'static + Send + Sync>(
        &mut self,
    ) -> Result<Option<Input<T>>, OperationError>;

    fn cleanup_inputs<T: 'static + Send + Sync>(&mut self, session: Entity);
}

pub trait InspectInput {
    fn has_input<T: 'static + Send + Sync>(&self, session: Entity) -> Result<bool, OperationError>;
}

impl<'w> ManageInput for EntityWorldMut<'w> {
    fn give_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError> {
        if unsafe { self.sneak_input(session, data, true, roster)? } {
            roster.queue(self.id());
        }
        Ok(())
    }

    fn defer_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        roster: &mut OperationRoster,
    ) -> Result<(), OperationError> {
        if unsafe { self.sneak_input(session, data, true, roster)? } {
            roster.defer(self.id());
        }
        Ok(())
    }

    unsafe fn sneak_input<T: 'static + Send + Sync>(
        &mut self,
        session: Entity,
        data: T,
        only_if_active: bool,
        roster: &mut OperationRoster,
    ) -> Result<bool, OperationError> {
        if only_if_active {
            let active_session =
                if let Some(session_status) = self.world().get::<SessionStatus>(session) {
                    matches!(session_status, SessionStatus::Active)
                } else {
                    false
                };

            if !active_session {
                // The session being sent is not active, either it is being cleaned
                // or already despawned. Therefore we should not propogate any inputs
                // related to it.
                return Ok(false);
            }
        }

        if let Some(mut storage) = self.get_mut::<InputStorage<T>>() {
            storage.reverse_queue.insert(0, Input { session, data });
        } else if !self.contains::<UnusedTarget>() {
            let id = self.id();
            if let Some(detached) = self.get::<Detached>() {
                if detached.is_detached() {
                    // The input is going to a detached impulse that will not
                    // react any further. We need to tell that detached impulse
                    // to despawn since it is no longer needed.
                    roster.defer_despawn(id);

                    // No error occurred, but the caller should not queue the
                    // operation into the roster because it is being despawned.
                    return Ok(false);
                }
            }

            let expected = self.get::<InputTypeIndicator>().map(|i| i.name);
            // If the input is being fed to an unused target then we can
            // generally ignore it, although it may indicate a bug in the user's
            // workflow because workflow branches that end in an unused target
            // will be spuriously dropped when the scope terminates.

            // However in this case, the target is not unused but also does not
            // have the correct input storage type. This indicates a bug in
            // bevy_impulse itself, since the API should ensure that connection
            // mismatches are impossible.
            self.world_mut()
                .get_resource_or_insert_with(|| UnhandledErrors::default())
                .miscellaneous
                .push(MiscellaneousFailure {
                    error: std::sync::Arc::new(anyhow::anyhow!(
                        "Incorrect input type for operation [{:?}]: received [{}], expected [{}]",
                        id,
                        std::any::type_name::<T>(),
                        expected.unwrap_or("<null>"),
                    )),
                    backtrace: Some(Backtrace::new()),
                });
            None.or_broken()?;
        }
        Ok(true)
    }

    fn take_input<T: 'static + Send + Sync>(&mut self) -> Result<Input<T>, OperationError> {
        self.try_take_input()?.or_not_ready()
    }

    fn try_take_input<T: 'static + Send + Sync>(
        &mut self,
    ) -> Result<Option<Input<T>>, OperationError> {
        let mut storage = self.get_mut::<InputStorage<T>>().or_broken()?;
        Ok(storage.reverse_queue.pop())
    }

    fn cleanup_inputs<T: 'static + Send + Sync>(&mut self, session: Entity) {
        if self.contains::<BufferStorage<T>>() {
            // Buffers are handled in a special way because the data of some
            // buffers will be used during cancellation. Therefore we do not
            // want to just delete their contents, but instead store them in the
            // buffer storage until the scope gives the signal to clear all
            // buffer data after all the cancellation workflows are finished.
            if let Some(mut inputs) = self.get_mut::<InputStorage<T>>() {
                // Pull out only the data that
                let remaining_indices: SmallVec<[usize; 16]> = inputs
                    .reverse_queue
                    .iter()
                    .enumerate()
                    .filter_map(|(i, input)| {
                        if input.session == session {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect();

                let mut reverse_remaining: SmallVec<[T; 16]> = SmallVec::new();
                for i in remaining_indices.into_iter().rev() {
                    reverse_remaining.push(inputs.reverse_queue.remove(i).data);
                }

                // INVARIANT: Earlier in this function we checked that the
                // entity contains this component, and we have not removed it
                // since then.
                let mut buffer = self.get_mut::<BufferStorage<T>>().unwrap();
                for data in reverse_remaining.into_iter().rev() {
                    buffer.force_push(session, data);
                }
            }

            return;
        }

        if let Some(mut inputs) = self.get_mut::<InputStorage<T>>() {
            inputs
                .reverse_queue
                .retain(|Input { session: r, .. }| *r != session);
        }
    }
}

impl<'a> InspectInput for EntityWorldMut<'a> {
    fn has_input<T: 'static + Send + Sync>(&self, session: Entity) -> Result<bool, OperationError> {
        let inputs = self.get::<InputStorage<T>>().or_broken()?;
        Ok(inputs.contains_session(session))
    }
}

impl<'a> InspectInput for EntityRef<'a> {
    fn has_input<T: 'static + Send + Sync>(&self, session: Entity) -> Result<bool, OperationError> {
        let inputs = self.get::<InputStorage<T>>().or_broken()?;
        Ok(inputs.contains_session(session))
    }
}

pub(crate) struct InputCommand<T> {
    pub(crate) target: Entity,
    pub(crate) session: Entity,
    pub(crate) data: T,
}

impl<T: 'static + Send + Sync> Command for InputCommand<T> {
    fn apply(self, world: &mut World) {
        match world.get_mut::<InputStorage<T>>(self.target) {
            Some(mut storage) => {
                storage.reverse_queue.insert(
                    0,
                    Input {
                        session: self.session,
                        data: self.data,
                    },
                );

                world
                    .get_resource_or_insert_with(DeferredRoster::default)
                    .queue(self.target);
            }
            None => {
                let cause = CancellationCause::Broken(Broken {
                    node: self.target,
                    backtrace: Some(Backtrace::new()),
                });
                let cancel = Cancel {
                    origin: self.target,
                    target: self.session,
                    session: Some(self.session),
                    cancellation: Cancellation::from_cause(cause),
                };

                world
                    .get_resource_or_insert_with(DeferredRoster::default)
                    .cancel(cancel);
            }
        }
    }
}
