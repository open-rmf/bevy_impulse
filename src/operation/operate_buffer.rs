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
    system::Command,
};

use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;

use backtrace::Backtrace;

use smallvec::SmallVec;

use crate::{
    Broken, BufferAccessors, BufferSettings, BufferStorage, DeferredRoster, ForkTargetStorage,
    Gate, GateActionStorage, Input, InputBundle, InspectBuffer, ManageBuffer, ManageInput,
    MiscellaneousFailure, Operation, OperationCleanup, OperationError, OperationReachability,
    OperationRequest, OperationResult, OperationRoster, OperationSetup, OrBroken,
    ReachabilityResult, SingleInputStorage, UnhandledErrors,
};

#[derive(Bundle)]
pub(crate) struct OperateBuffer<T: 'static + Send + Sync> {
    storage: BufferStorage<T>,
}

impl<T: 'static + Send + Sync> OperateBuffer<T> {
    pub(crate) fn new(settings: BufferSettings) -> Self {
        Self {
            storage: BufferStorage::new(settings),
        }
    }
}

// TODO(@mxgrey): Implement an operation for removing / clearing items from buffers,
// and a way to subscribe to that operation.
impl<T> Operation for OperateBuffer<T>
where
    T: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            self,
            ForkTargetStorage::new(),
            SingleInputStorage::empty(),
            InputBundle::<T>::new(),
            BufferBundle::new::<T>(),
            BufferAccessors::default(),
            RelatedGateNodes::default(),
            GateState::default(),
        ));

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
        let Input { session, data } = source_mut.take_input::<T>()?;
        let mut buffer = source_mut.get_mut::<BufferStorage<T>>().or_broken()?;
        buffer.force_push(session, data);

        if source_mut
            .get::<GateState>()
            .or_broken()?
            .is_closed(session)
        {
            return Ok(());
        }

        let targets = source_mut.get::<ForkTargetStorage>().or_broken()?.0.clone();
        for target in targets {
            world
                .get_entity_mut(target)
                .or_broken()?
                .give_input(session, (), roster)?;
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if !RelatedGateNodes::is_opening_reachable(&mut reachability)? {
            if BufferAccessors::is_reachable(&mut reachability)? {
                // A buffer accessor can open the buffer gate and also push new
                // items which would then wake up listeners, so we consider this
                // buffer to be reachable.
                return Ok(true);
            }

            // If this gate is closed and will never be able to open again, then
            // this buffer is considered unreachable for its listeners.
            return Ok(false);
        }

        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        if BufferAccessors::is_reachable(&mut reachability)? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

#[derive(Component, Debug, Default)]
pub(crate) struct GateState {
    pub(crate) map: HashMap<Entity, Gate>,
}

impl GateState {
    pub fn apply(
        buffer: Entity,
        session: Entity,
        action: Gate,
        world: &mut World,
        roster: &mut OperationRoster,
    ) -> OperationResult {
        let mut states = world.get_mut::<GateState>(buffer).or_broken()?;
        let state = states.map.entry(session).or_insert(Gate::Open);
        if *state == action {
            // No change needed
            return Ok(());
        }

        *state = action;
        if state.is_open() {
            // The gate has opened up, so we should immediately wake up all
            // listeners.
            let targets = world
                .get::<ForkTargetStorage>(buffer)
                .or_broken()?
                .0
                .clone();

            for target in targets {
                world
                    .get_entity_mut(target)
                    .or_broken()?
                    .give_input(session, (), roster)?;
            }
        }

        Ok(())
    }
}

impl GateState {
    fn is_closed(&self, session: Entity) -> bool {
        self.map.get(&session).unwrap_or(&Gate::Open).is_closed()
    }
}

#[derive(Component, Default)]
pub(crate) struct RelatedGateNodes(pub(crate) SmallVec<[Entity; 8]>);

impl RelatedGateNodes {
    fn is_opening_reachable(r: &mut OperationReachability) -> ReachabilityResult {
        let source_ref = r.world.get_entity(r.source).or_broken()?;
        let gate_state = source_ref.get::<GateState>().or_broken()?;
        if !gate_state.is_closed(r.session) {
            // The gate on the buffer is already open so nothing to worry about
            // here.
            return Ok(true);
        }

        let Some(gate_nodes) = source_ref.get::<Self>() else {
            return Ok(false);
        };

        for gate in &gate_nodes.0 {
            let action = r.world.get::<GateActionStorage>(*gate).or_broken()?.0;
            if action.is_open() && r.check_upstream(*gate)? {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

#[derive(Debug)]
pub(crate) struct OnNewBufferValue {
    buffer: Entity,
    target: Entity,
}

impl OnNewBufferValue {
    pub(crate) fn new(buffer: Entity, target: Entity) -> Self {
        OnNewBufferValue { buffer, target }
    }
}

impl Command for OnNewBufferValue {
    fn apply(self, world: &mut World) {
        let Some(mut buffer_targets) = world.get_mut::<ForkTargetStorage>(self.buffer) else {
            self.on_failure(world);
            return;
        };

        buffer_targets.0.push(self.buffer);

        let Some(mut target_mut) = world.get_entity_mut(self.target) else {
            self.on_failure(world);
            return;
        };

        target_mut.insert(SingleInputStorage::new(self.buffer));
    }
}

impl OnNewBufferValue {
    fn on_failure(self, world: &mut World) {
        world
            .get_resource_or_insert_with(UnhandledErrors::default)
            .miscellaneous
            .push(MiscellaneousFailure {
                error: Arc::new(anyhow!(
                    "Unable to add target with OnNewBufferValue: {self:?}"
                )),
                backtrace: Some(Backtrace::new()),
            });
    }
}

#[derive(Bundle)]
struct BufferBundle {
    clear: ClearBufferFn,
    size: CheckBufferSizeFn,
    sessions: GetBufferedSessionsFn,
}

impl BufferBundle {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self {
            clear: ClearBufferFn::new::<T>(),
            size: CheckBufferSizeFn::new::<T>(),
            sessions: GetBufferedSessionsFn::new::<T>(),
        }
    }
}

#[derive(Component)]
pub struct ClearBufferFn(pub fn(Entity, Entity, &mut World) -> OperationResult);

impl ClearBufferFn {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self(clear_buffer::<T>)
    }
}

fn clear_buffer<T: 'static + Send + Sync>(
    source: Entity,
    session: Entity,
    world: &mut World,
) -> OperationResult {
    world
        .get_entity_mut(source)
        .or_broken()?
        .clear_buffer::<T>(session)
}

#[derive(Component)]
pub struct CheckBufferSizeFn(pub fn(Entity, Entity, &World) -> Result<usize, OperationError>);

impl CheckBufferSizeFn {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self(check_buffer_size::<T>)
    }
}

fn check_buffer_size<T: 'static + Send + Sync>(
    source: Entity,
    session: Entity,
    world: &World,
) -> Result<usize, OperationError> {
    world
        .get_entity(source)
        .or_broken()?
        .buffered_count::<T>(session)
}

#[derive(Component)]
pub struct GetBufferedSessionsFn(
    #[allow(clippy::type_complexity)]
    pub fn(Entity, &World) -> Result<SmallVec<[Entity; 16]>, OperationError>,
);

impl GetBufferedSessionsFn {
    fn new<T: 'static + Send + Sync>() -> Self {
        Self(get_buffered_sessions::<T>)
    }
}

fn get_buffered_sessions<T: 'static + Send + Sync>(
    source: Entity,
    world: &World,
) -> Result<SmallVec<[Entity; 16]>, OperationError> {
    world
        .get_entity(source)
        .or_broken()?
        .buffered_sessions::<T>()
}

pub(crate) struct NotifyBufferUpdate {
    buffer: Entity,
    session: Entity,
    /// This field is used to prevent notifications from going to the accessor
    /// that produced the key which was used for modification. That way users
    /// don't end up with unintentional infinite loops in their workflow. If
    /// this is set to None then that means the user wants to allow closed loops
    /// and is taking responsibility for managing it.
    accessor: Option<Entity>,
}

impl NotifyBufferUpdate {
    pub(crate) fn new(buffer: Entity, session: Entity, accessor: Option<Entity>) -> Self {
        Self {
            buffer,
            session,
            accessor,
        }
    }
}

impl Command for NotifyBufferUpdate {
    fn apply(self, world: &mut World) {
        let r = match world.get::<GateState>(self.buffer) {
            Some(gate_state) => {
                if gate_state.is_closed(self.session) {
                    return;
                }

                world.get_resource_or_insert_with(DeferredRoster::default);
                world.resource_scope::<DeferredRoster, _>(|world: &mut World, mut deferred| {
                    // We filter out the target that produced the key that was used to
                    // make the modification. This prevents unintentional infinite loops
                    // from forming in the workflow.
                    let targets: SmallVec<[_; 16]> = world
                        .get::<ForkTargetStorage>(self.buffer)
                        .or_broken()?
                        .0
                        .iter()
                        .filter(|t| !self.accessor.is_some_and(|a| a == **t))
                        .cloned()
                        .collect();

                    for target in targets {
                        world.get_entity_mut(target).or_broken()?.give_input(
                            self.session,
                            (),
                            &mut deferred.0,
                        )?;
                    }

                    Ok(())
                })
            }
            None => None.or_broken(),
        };

        if let Err(OperationError::Broken(backtrace)) = r {
            world
                .get_resource_or_insert_with(UnhandledErrors::default)
                .broken
                .push(Broken {
                    node: self.buffer,
                    backtrace,
                });
        }
    }
}
