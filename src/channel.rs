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

use crate::{Stream, Provider, Promise};

#[derive(Clone)]
pub struct Channel<Streams = ()> {
    inner: InnerChannel,
    _ignore: std::marker::PhantomData<Streams>,
}

impl<Streams> Channel<Streams> {
    pub fn push<C: Command>(&self, command: C) {
        let mut queue = CommandQueue::default();
        queue.push(command);
        self.push_batch(queue);
    }

    pub fn push_batch(&self, mut queue: CommandQueue) {
        self.inner.sender.send(Box::new(
            move |world: &mut World| {
                queue.apply(world);
            }
        )).ok();
    }

    // pub fn query<P: Provider>(&self, request: P::Request, provider: P) -> Promise<P::Response> {

    // }
}

#[derive(Clone)]
pub(crate) struct InnerChannel {
    source: Entity,
    sender: CbSender<ChannelItem>,
}

impl InnerChannel {
    pub(crate) fn new(source: Entity, sender: CbSender<ChannelItem>) -> Self {
        InnerChannel { source, sender }
    }

    pub(crate) fn into_specific<Streams>(self) -> Channel<Streams> {
        Channel { inner: self, _ignore: Default::default() }
    }
}

type ChannelItem = Box<dyn FnOnce(&mut World) + Send>;

#[derive(Resource)]
pub(crate) struct ChannelQueue {
    pub(crate) sender: CbSender<ChannelItem>,
    pub(crate) receiver: CbReceiver<ChannelItem>,
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


// pub struct StreamChannel<'a, T> {
//     channel: &'a C,
//     _ignore: std::marker::PhantomData<T>,
// }

// impl<'a, C: ChannelTrait, T: Stream> StreamChannel<'a, C, T> {
//     pub fn send(&self, data: T) {
//         self.channel.push(StreamCommand {
//             source: self.channel.source(),
//             data,
//         });
//     }
// }

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
