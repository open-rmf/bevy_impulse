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

use bevy_ecs::{system::EntityCommands, world::EntityWorldMut};

use crate::{Delivery, DeliveryChoice, DeliverySettings};

impl DeliveryChoice for DeliverySettings {
    fn apply_entity_commands<Request: 'static + Send + Sync>(
        self,
        entity_commands: &mut EntityCommands,
    ) {
        match self {
            Self::Serial => {
                entity_commands.insert(Delivery::<Request>::serial());
            }
            Self::Parallel => {
                entity_commands.insert(Delivery::<Request>::parallel());
            }
        }
    }

    fn apply_entity_mut<Request: 'static + Send + Sync>(
        self,
        entity_mut: &mut EntityWorldMut,
    ) {
        match self {
            Self::Serial => {
                entity_mut.insert(Delivery::<Request>::serial());
            }
            Self::Parallel => {
                entity_mut.insert(Delivery::<Request>::parallel());
            }
        }
    }
}
