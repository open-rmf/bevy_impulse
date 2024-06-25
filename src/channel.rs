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
    ecs::system::{Command, CommandQueue, Commands},
};

use crossbeam::channel::{unbounded, Sender as CbSender, Receiver as CbReceiver};

use crate::{StreamPack, Provider, Promise, RequestExt, OperationRoster};

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
            move |world: &mut World, _: &mut OperationRoster| {
                queue.apply(world);
            }
        )).ok();
    }

    pub fn query<P: Provider>(&self, request: P::Request, provider: P) -> Promise<P::Response>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: 'static + StreamPack,
        P: 'static + Send,
    {
        self.build(move |commands| {
            commands.request(request, provider).take()
        }).flatten()
    }

    pub fn build<F, U>(&self, f: F) -> Promise<U>
    where
        F: FnOnce(&mut Commands) -> U + 'static + Send,
        U: 'static + Send,
    {
        let (promise, sender) = Promise::new();
        self.inner.sender.send(Box::new(
            move |world: &mut World, _: &mut OperationRoster| {
                let mut command_queue = CommandQueue::default();
                let mut commands = Commands::new(&mut command_queue, world);
                let u = f(&mut commands);
                command_queue.apply(world);
                let _ = sender.send(u);
            }
        )).ok();

        promise
    }
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

type ChannelItem = Box<dyn FnOnce(&mut World, &mut OperationRoster) + Send>;

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

struct StreamCommand<T> {
    source: Entity,
    data: T,
}

impl<T: StreamPack> Command for StreamCommand<T> {
    fn apply(self, world: &mut World) {
        let Some(mut source_mut) = world.get_entity_mut(self.source) else {
            return;
        };

    }
}

#[cfg(test)]
mod tests {
    use crate::{*, testing::*};
    use bevy::ecs::system::EntityCommands;

    #[test]
    fn test_channel_request() {
        let mut context = TestingContext::minimal_plugins();

        let (hello, repeat) = context.build(|commands| {
            let hello = commands.spawn_service(
                say_hello
                .with(|entity_cmds: &mut EntityCommands| {
                    entity_cmds.insert((
                        Salutation("Guten tag, ".into()),
                        Name("tester".into()),
                        RunCount(0),
                    ));
                })
            );
            let repeat = commands.spawn_service(
                repeat_service
                .with(|entity_cmds: &mut EntityCommands| {
                    entity_cmds.insert(RunCount(0));
                })
            );
            (hello, repeat)
        });

        for _ in 0..5 {
            let mut promise = context.build(|commands| {
                commands.request(
                    RepeatRequest { service: hello, count: 5 },
                    repeat,
                ).take()
            });
            context.run_while_pending(&mut promise);
            assert!(promise.peek().is_available());
        }

        let count = context.app.world.get::<RunCount>(hello.provider()).unwrap().0;
        assert_eq!(count, 25);

        let count = context.app.world.get::<RunCount>(repeat.provider()).unwrap().0;
        assert_eq!(count, 5);
    }
}
