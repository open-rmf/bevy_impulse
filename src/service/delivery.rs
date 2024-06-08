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

use crate::{Blocker, OperationRoster, RequestLabelId};

use bevy::prelude::{Entity, Component, World};

use smallvec::SmallVec;

use std::collections::{VecDeque, HashMap};

pub(crate) fn pop_next_delivery<Request>(
    provider: Entity,
    label: Option<RequestLabelId>,
    serve_next: fn(Blocker, &mut World, &mut OperationRoster),
    world: &mut World,
) -> Option<Deliver<Request>>
where
    Request: 'static + Send + Sync,
{
    let mut delivery = world.get_mut::<Delivery<Request>>(provider)?;
    match &mut *delivery {
        Delivery::Serial(serial) => {
            pop_next_delivery_impl::<Request>(provider, serial, serve_next)
        }
        Delivery::Parallel(parallel) => {
            let label = label.expect(
                "A request in a parallel async service was blocking without a label. \
                Please report this to the bevy_impulse maintainers; this should not be possible."
            );
            let serial = parallel.labeled.get_mut(&label).expect(
                "A labeled request in a parallel async service finished but the queue \
                for its label has been erased. Please report this to the bevy_impulse \
                maintainers; this should not be possible."
            );
            pop_next_delivery_impl::<Request>(provider, serial, serve_next)
        }
    }
}

fn pop_next_delivery_impl<Request>(
    provider: Entity,
    serial: &mut SerialDelivery<Request>,
    serve_next: fn(Blocker, &mut World, &mut OperationRoster),
) -> Option<Deliver<Request>>
where
    Request: 'static + Send + Sync,
{
    let Some(DeliveryOrder { source, session, task_id, request, instructions }) = serial.queue.pop_front() else {
        return None;
    };
    let blocker = Blocker {
        provider,
        source,
        session,
        label: instructions.as_ref().map(|x| x.label.clone()),
        serve_next,
    };

    serial.delivering = Some(ActiveDelivery { source, session, task_id, instructions });
    return Some(Deliver { request, task_id, blocker });
}

pub struct Deliver<Request> {
    pub request: Request,
    /// For async services this is the Entity that manages the async task.
    /// For workflows this is the scoped session Entity.
    pub task_id: Entity,
    pub blocker: Blocker,
}

#[derive(Component, Clone, Copy)]
pub struct DeliveryInstructions {
    pub(crate) label: RequestLabelId,
    pub(crate) queue: bool,
    pub(crate) ensure: bool,
}

pub(crate) struct DeliveryOrder<Request> {
    pub(crate) source: Entity,
    pub(crate) session: Entity,
    /// For async services this is the Entity that manages the async task.
    /// For workflows this is the scoped session Entity.
    pub(crate) task_id: Entity,
    pub(crate) request: Request,
    pub(crate) instructions: Option<DeliveryInstructions>,
}

struct ActiveDelivery {
    source: Entity,
    session: Entity,
    /// For async services this is the Entity that manages the async task.
    /// For workflows this is the scoped session Entity.
    task_id: Entity,
    instructions: Option<DeliveryInstructions>,
}

/// The delivery mode determines whether service requests are carried out one at
/// a time (serial) or in parallel.
#[derive(Component)]
pub(crate) enum Delivery<Request> {
    Serial(SerialDelivery<Request>),
    Parallel(ParallelDelivery<Request>),
}

impl<Request> Delivery<Request> {
    pub(crate) fn serial() -> Self {
        Delivery::Serial(SerialDelivery::<Request>::default())
    }

    pub(crate) fn parallel() -> Self {
        Delivery::Parallel(ParallelDelivery::<Request>::default())
    }
}

pub(crate) struct SerialDelivery<Request> {
    delivering: Option<ActiveDelivery>,
    queue: VecDeque<DeliveryOrder<Request>>,
}

impl<Request> Default for SerialDelivery<Request> {
    fn default() -> Self {
        Self {
            delivering: Default::default(),
            queue: Default::default(),
        }
    }
}

pub struct ParallelDelivery<Request> {
    pub labeled: HashMap<RequestLabelId, SerialDelivery<Request>>,
}

impl<Request> Default for ParallelDelivery<Request> {
    fn default() -> Self {
        Self { labeled: Default::default() }
    }
}

pub enum DeliveryUpdate<Request> {
    /// The new request should be delivered immediately
    Immediate {
        blocking: Option<Option<RequestLabelId>>,
        request: Request,
    },
    /// The new request has been placed in the queue
    Queued {
        /// Queued requests that have been cancelled
        cancelled: SmallVec<[DeliveryStoppage; 8]>,
        /// An actively running task that has been cancelled
        stop: Option<DeliveryStoppage>,
        /// The label that the blocking is based on
        label: Option<RequestLabelId>,
    }
}

pub struct DeliveryStoppage {
    pub source: Entity,
    pub session: Entity,
    /// For async services this is the Entity that manages the async task.
    /// For workflows this is the scoped session Entity.
    pub task_id: Entity,
}

pub fn insert_new_order<Request>(
    delivery: &mut Delivery<Request>,
    order: DeliveryOrder<Request>,
) -> DeliveryUpdate<Request> {
    match delivery {
        Delivery::Serial(serial) => {
            insert_serial_order(serial, order)
        }
        Delivery::Parallel(parallel) => {
            match &order.instructions {
                Some(instructions) => {
                    let label = instructions.label.clone();
                    insert_serial_order(
                        parallel.labeled.entry(label).or_default(),
                        order,
                    )
                }
                None => {
                    DeliveryUpdate::Immediate { request: order.request, blocking: None }
                }
            }
        }
    }
}

fn insert_serial_order<Request>(
    serial: &mut SerialDelivery<Request>,
    order: DeliveryOrder<Request>,
) -> DeliveryUpdate<Request> {
    let Some(delivering) = &serial.delivering else {
        // INVARIANT: If there is anything in the queue then it should have been
        // moved into delivering when the last delivery was finished. If
        // delivering is empty then the queue should be as well.
        assert!(serial.queue.is_empty());
        serial.delivering = Some(ActiveDelivery {
            source: order.source,
            session: order.session,
            task_id: order.task_id,
            instructions: order.instructions
        });
        let label = order.instructions.map(|i| i.label);
        return DeliveryUpdate::Immediate { blocking: Some(label), request: order.request };
    };

    let Some(incoming_instructions) = order.instructions else {
        serial.queue.push_back(order);
        return DeliveryUpdate::Queued {
            cancelled: SmallVec::new(),
            stop: None,
            label: None,
        };
    };

    let mut cancelled = SmallVec::new();
    let mut stop = None;

    let should_discard = |prior_instructions: &DeliveryInstructions| {
        prior_instructions.label == incoming_instructions.label
        && !prior_instructions.ensure
    };

    if !incoming_instructions.queue {
        serial.queue.retain(|e| {
            let discard = e.instructions.as_ref().is_some_and(should_discard);
            if discard {
                cancelled.push(DeliveryStoppage {
                    source: e.source,
                    session: e.session,
                    task_id: e.task_id,
                });
            }

            !discard
        });
    }

    if delivering.instructions.as_ref().is_some_and(should_discard) {
        stop = Some(DeliveryStoppage {
            source: delivering.source,
            session: delivering.session,
            task_id: delivering.task_id,
        });
    }

    serial.queue.push_back(order);
    let label = Some(incoming_instructions.label);

    DeliveryUpdate::Queued { cancelled, stop, label }
}
