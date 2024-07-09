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

use bevy::prelude::{Component, DespawnRecursiveExt};

use crossbeam::channel::Sender;

use crate::{
    Impulsive, OperationSetup, OperationRequest,
    InputBundle, OperationCancel, OperationResult, OrBroken, Input, ManageInput,
    OnTerminalCancelled, ImpulseLifecycleChannel,
    promise::private::Sender as PromiseSender,
};

#[derive(Component)]
pub(crate) struct TakenResponse<T> {
    sender: PromiseSender<T>,
}

impl<T> TakenResponse<T> {
    pub(crate) fn new(sender: PromiseSender<T>) -> Self {
        Self { sender }
    }
}

impl<T: 'static + Send + Sync> Impulsive for TakenResponse<T> {
    fn setup(mut self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        let lifecycle_sender = world
            .get_resource_or_insert_with(|| ImpulseLifecycleChannel::default())
            .sender.clone();
        self.sender.on_promise_drop(move || { lifecycle_sender.send(source).ok(); });

        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            OnTerminalCancelled(cancel_taken_target::<T>),
            self,
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { data, .. } = source_mut.take_input::<T>()?;
        let sender = source_mut.take::<TakenResponse<T>>().or_broken()?.sender;
        sender.send(data).ok();
        source_mut.despawn_recursive();

        Ok(())
    }
}

#[derive(Component)]
pub(crate) struct TakenStream<T> {
    sender: Sender<T>,
}

impl<T> TakenStream<T> {
    pub fn new(sender: Sender<T>) -> Self {
        Self { sender }
    }
}

impl<T: 'static + Send + Sync> Impulsive for TakenStream<T> {
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world.entity_mut(source).insert((
            InputBundle::<T>::new(),
            self,
        ));
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { data, .. } = source_mut.take_input::<T>()?;
        let stream = source_mut.get::<TakenStream<T>>().or_broken()?;
        stream.sender.send(data).ok();
        source_mut.despawn_recursive();
        Ok(())
    }
}

fn cancel_taken_target<T>(
    OperationCancel { cancel, world, .. }: OperationCancel,
) -> OperationResult
where
    T: 'static + Send + Sync,
{
    let mut target_mut = world.get_entity_mut(cancel.target).or_broken()?;
    let taken = target_mut.take::<TakenResponse<T>>().or_broken()?;
    taken.sender.cancel(cancel.cancellation).ok();
    target_mut.despawn_recursive();

    Ok(())
}
