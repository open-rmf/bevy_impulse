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

use bevy::{
    prelude::{Entity, Resource, World},
    ecs::system::{Command, CommandQueue}
};

use crossbeam::channel::{unbounded, Sender as CbSender, Receiver as CbReceiver};

use crate::Stream;

use std::cell::RefCell;

#[derive(Clone)]
pub struct Channel<Streams = ()> {
    inner: InnerChannel,
    _ignore: std::marker::PhantomData<Streams>,
}

impl<Streams> Channel<Streams> {
    pub fn batch(&self) -> BatchChannel<Streams> {
        BatchChannel {
            inner: self.inner.clone(),
            batch: RefCell::new(BatchQueue::new()),
            _ignore: Default::default(),
        }
    }
}

pub struct BatchChannel<Streams> {
    inner: InnerChannel,
    batch: RefCell<BatchQueue>,
    _ignore: std::marker::PhantomData<Streams>,
}

impl<Streams> BatchChannel<Streams> {
    pub fn flush(&self) {
        let mut batch = self.batch.borrow_mut();
        if !batch.empty {
            self.inner.sender.send(
                std::mem::replace(&mut batch.queue, CommandQueue::default())
            ).expect("ChannelQueue resource was removed or replaced. This should never happen.");
        }
        batch.empty = true;
    }
}

impl<Streams> Drop for BatchChannel<Streams> {
    fn drop(&mut self) {
        self.flush();
    }
}

struct BatchQueue {
    queue: CommandQueue,
    empty: bool,
}

impl BatchQueue {
    fn new() -> Self {
        Self { queue: CommandQueue::default(), empty: true }
    }
}

#[derive(Clone)]
pub(crate) struct InnerChannel {
    source: Entity,
    sender: CbSender<CommandQueue>,
}

impl InnerChannel {
    pub(crate) fn new(source: Entity, sender: CbSender<CommandQueue>) -> Self {
        InnerChannel { source, sender }
    }

    pub(crate) fn into_specific<Streams>(self) -> Channel<Streams> {
        Channel { inner: self, _ignore: Default::default() }
    }
}

#[derive(Resource)]
pub(crate) struct ChannelQueue {
    pub(crate) sender: CbSender<CommandQueue>,
    pub(crate) receiver: CbReceiver<CommandQueue>,
}

impl ChannelQueue {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = unbounded();
        Self { sender, receiver }
    }
}

impl Default for ChannelQueue {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ChannelTrait {
    fn push<C: Command>(&self, command: C);
    fn source(&self) -> Entity;
}

impl<Streams> ChannelTrait for Channel<Streams> {
    fn push<C: Command>(&self, command: C) {
        let mut queue = CommandQueue::default();
        queue.push(command);
        self.inner.sender.send(queue)
            .expect("ChannelQueue resource was removed or replaced. This should never happen.");
    }

    fn source(&self) -> Entity {
        self.inner.source
    }
}

impl<Streams> ChannelTrait for BatchChannel<Streams> {
    fn push<C: Command>(&self, command: C) {
        let mut batch = self.batch.borrow_mut();
        batch.empty = false;
        batch.queue.push(command);
    }

    fn source(&self) -> Entity {
        self.inner.source
    }
}

pub struct StreamChannel<'a, C: ChannelTrait, T> {
    channel: &'a C,
    _ignore: std::marker::PhantomData<T>,
}

impl<'a, C: ChannelTrait, T: Stream> StreamChannel<'a, C, T> {
    pub fn send(&self, data: T) {
        self.channel.push(StreamCommand {
            source: self.channel.source(),
            data,
        });
    }
}

struct StreamCommand<T> {
    source: Entity,
    data: T,
}

impl<T: Stream> Command for StreamCommand<T> {
    fn apply(self, world: &mut World) {
        let Some(mut source_mut) = world.get_entity_mut(self.source) else {
            return;
        };

    }
}
