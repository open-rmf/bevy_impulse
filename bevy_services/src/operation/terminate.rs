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

use crate::{Operation, InputStorage, OperationStatus, OperationRoster, promise::private::Sender};

use bevy::prelude::{Entity, Component, World, Resource};

use crossbeam::channel::{unbounded, Sender as CbSender, Receiver as CbReceiver};

#[derive(Component)]
pub(crate) struct SenderStorage<T>(pub(crate) Sender<T>);

pub(crate) struct Terminate<Response: 'static + Send + Sync> {
    sender: Option<Sender<Response>>,
    detached: bool,
}

impl<Response: 'static + Send + Sync> Terminate<Response> {
    pub(crate) fn new(sender: Option<Sender<Response>>, detached: bool) -> Self {
        Self { sender, detached }
    }
}

#[derive(Resource)]
pub(crate) struct DroppedPromiseQueue {
    sender: CbSender<Entity>,
    pub(crate) receiver: CbReceiver<Entity>,
}

impl DroppedPromiseQueue {
    pub(crate) fn new() -> DroppedPromiseQueue {
        let (sender, receiver) = unbounded();
        DroppedPromiseQueue { sender, receiver }
    }
}

impl<T: 'static + Send + Sync> Operation for Terminate<T> {
    fn set_parameters(self, entity: Entity, world: &mut World) {
        if let Some(mut sender) = self.sender {
            if !self.detached {
                let dropped_promise_queue = world.get_resource_or_insert_with(
                    || DroppedPromiseQueue::new()
                );

                let dropped_promise_sender = dropped_promise_queue.sender.clone();
                sender.on_cancel(move || {
                    dropped_promise_sender.send(entity).expect(
                        "DroppedPromiseQueue resource has been removed unexpectedly"
                    );
                });
            }

            world.entity_mut(entity).insert(SenderStorage(sender));
        }
    }

    fn execute(
        source: Entity,
        world: &mut World,
        _: &mut OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        if let Some(sender) = source_mut.take::<SenderStorage<T>>() {
            let input = source_mut.take::<InputStorage<T>>().ok_or(())?.take();
            sender.0.send(input).ok();
        }

        Ok(OperationStatus::Finished)
    }
}
