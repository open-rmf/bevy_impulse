/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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

use crate::RequestLabelId;

use bevy::prelude::{Component, Entity};

use std::collections::{HashMap, VecDeque};

///
pub(crate) struct DeliveryInstructions {
    label: RequestLabelId,
    interrupting: bool,
    interruptible: bool,
}

pub(crate) struct DeliveryOrder {
    target: Entity,
    instructions: Option<DeliveryInstructions>,
}

/// The delivery mode determines whether service requests are carried out one at
/// a time (serial) or in parallel.
#[derive(Component)]
pub(crate) enum Delivery {
    Serial(SerialDelivery),
    Parallel(ParallelDelivery),
}

impl Delivery {
    pub(crate) fn serial() -> Self {
        Delivery::Serial(SerialDelivery::default())
    }

    pub(crate) fn parallel() -> Self {
        Delivery::Parallel(ParallelDelivery::default())
    }
}

#[derive(Default)]
pub(crate) struct SerialDelivery {
    delivering: Option<DeliveryOrder>,
    queue: VecDeque<DeliveryOrder>,
}

#[derive(Default)]
pub(crate) struct ParallelDelivery {
    labeled: HashMap<RequestLabelId, SerialDelivery>,
}
