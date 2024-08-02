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

use bevy_ecs::prelude::{Component, Entity};

use crate::{
    Operation, Input, ManageInput, InputBundle, OperationRequest, OperationResult,
    OperationReachability, ReachabilityResult, OperationSetup, SingleInputStorage,
    SingleTargetStorage, OrBroken, OperationCleanup, Disposal, GateAction,
    GateRequest, Buffered, emit_disposal,
};

#[derive(Component)]
pub(crate) struct BufferRelationStorage<B>(B);

#[derive(Component, Clone, Copy)]
pub(crate) struct GateActionStorage(pub(crate) GateAction);

pub(crate) struct OperateDynamicGate<T, B> {
    buffers: B,
    target: Entity,
    _ignore: std::marker::PhantomData<T>,
}

impl<B, T> OperateDynamicGate<T, B> {
    pub(crate) fn new(buffers: B, target: Entity) -> Self {
        Self { buffers, target, _ignore: Default::default() }
    }
}

impl<T, B> Operation for OperateDynamicGate<T, B>
where
    T: 'static + Send + Sync,
    B: Buffered + 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<GateRequest<T>>::new(),
            SingleTargetStorage::new(self.target),
            BufferRelationStorage(self.buffers),
            // We store GateAction::Open for this here because this component is
            // checked by buffers when examining their reachability, and dynamic
            // gates can't know if they will open or close a buffer until the
            // input arrives, so we need to treat it as opening to avoid any
            // false negatives on reachability.
            GateActionStorage(GateAction::Open),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: GateRequest { action, data } } = source_mut
            .take_input::<GateRequest<T>>()?;

        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let buffers = source_mut.get::<BufferRelationStorage<B>>().or_broken()?
            .0.clone();

        buffers.gate_action(session, action, world, roster)?;

        world.get_entity_mut(target).or_broken()?
            .give_input(session, data, roster)?;

        if action.is_close() {
            // When doing a closing, we should emit a disposal because we are
            // cutting off part of the workflow, which may alter the
            // reachability of the terminal node.
            let disposal = Disposal::closed_gate(source, buffers.as_input());
            emit_disposal(source, session, disposal, world, roster);
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<GateRequest<T>>()?;
        clean.cleanup_disposals()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}

pub(crate) struct OperateStaticGate<T, B> {
    buffers: B,
    target: Entity,
    action: GateAction,
    _ignore: std::marker::PhantomData<T>,
}

impl<T, B> OperateStaticGate<T, B> {
    pub(crate) fn new(
        buffers: B,
        target: Entity,
        action: GateAction,
    ) -> Self {
        Self { buffers, target, action, _ignore: Default::default() }
    }
}

impl<T, B> Operation for OperateStaticGate<T, B>
where
    B: Buffered + 'static + Send + Sync,
    T: 'static + Send + Sync,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.get_entity_mut(self.target).or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            SingleTargetStorage::new(self.target),
            BufferRelationStorage(self.buffers),
            GateActionStorage(self.action),
        ));

        Ok(())
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data } = source_mut.take_input::<T>()?;

        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.get();
        let action = source_mut.get::<GateActionStorage>().or_broken()?.0;
        let buffers = source_mut.get::<BufferRelationStorage<B>>().or_broken()?
            .0.clone();

        buffers.gate_action(session, action, world, roster)?;

        world.get_entity_mut(target).or_broken()?
            .give_input(session, data, roster)?;

        if action.is_close() {
            // When doing a closing, we should emit a disposal because we are
            // cutting off part of the workflow, which may alter the
            // reachability of the terminal node.
            let disposal = Disposal::closed_gate(source, buffers.as_input());
            emit_disposal(source, session, disposal, world, roster);
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<T>()?;
        clean.cleanup_disposals()?;
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<T>()? {
            return Ok(true);
        }

        SingleInputStorage::is_reachable(&mut reachability)
    }
}
