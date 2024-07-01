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
    prelude::{World, Entity},
    ecs::system::Command,
};

use crate::{
    OperationSetup, OperationRequest, OperationResult, OperationCancel,
    OperationExecuteStorage, OperationError, Cancellable,
};

pub trait Impulsive {
    fn setup(self, info: OperationSetup) -> OperationResult;

    fn execute(request: OperationRequest) -> OperationResult;
}

pub struct AddImpulse<I: Impulsive> {
    source: Entity,
    impulse: I,
}

impl<I: Impulsive + 'static + Sync + Send> Command for AddImpulse<I> {
    fn apply(self, world: &mut World) {
        self.impulse.setup(OperationSetup { source: self.source, world });
        world.entity_mut(self.source).insert((
            OperationExecuteStorage(perform_impulse::<I>),
            Cancellable::new(cancel_impulse),
        ));
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

        }
    }
}

fn cancel_impulse(
    OperationCancel { cancel, world, roster }: OperationCancel,
) -> OperationResult {
    find the last impulse in the chain and then trigger the cancellation and
    recursively delete everything
}
