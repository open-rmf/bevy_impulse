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

use bevy::prelude::DespawnRecursiveExt;

use crate::{
    Impulsive, OperationSetup, OperationRequest, OperationResult, OrBroken,
};

/// During an impulse flush, this impulse gets automatically added to the end of
/// any chain which has an unused target but was also marked as detached. This
/// simply takes care of automatically cleaning up the chain when it's finished.
pub(crate) struct Finished;

impl Impulsive for Finished {
    fn setup(self, _: OperationSetup) -> OperationResult {
        // Do nothing
        Ok(())
    }

    fn execute(
        OperationRequest { source, world, .. }: OperationRequest,
    ) -> OperationResult {
        // If this gets triggered that means the impulse chain is finished
        world.get_entity_mut(source).or_broken()?.despawn_recursive();
        Ok(())
    }
}
