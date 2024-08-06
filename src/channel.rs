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

use bevy_ecs::{
    prelude::{Entity, Resource, World},
    system::{CommandQueue, Commands},
};

use tokio::sync::mpsc::{
    unbounded_channel,
    UnboundedSender as TokioSender,
    UnboundedReceiver as TokioReceiver,
};

use std::sync::Arc;

use crate::{
    Stream, StreamPack, StreamRequest, Provider, Promise, RequestExt,
    OperationRoster, OperationError,
};

#[derive(Clone)]
pub struct Channel {
    inner: Arc<InnerChannel>,
}

impl Channel {
    pub fn query<P: Provider>(&self, request: P::Request, provider: P) -> Promise<P::Response>
    where
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: 'static + StreamPack,
        P: 'static + Send + Sync,
    {
        self.command(move |commands| {
            commands.request(request, provider).take().response
        }).flatten()
    }

    pub fn command<F, U>(&self, f: F) -> Promise<U>
    where
        F: FnOnce(&mut Commands) -> U + 'static + Send,
        U: 'static + Send,
    {
        let (sender, promise) = Promise::new();
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

    pub(crate) fn for_streams<Streams: StreamPack>(
        &self,
        world: &World,
    ) -> Result<Streams::Channel, OperationError> {
        Ok(Streams::make_channel(&self.inner, world))
    }

    pub(crate) fn new(
        source: Entity,
        session: Entity,
        sender: TokioSender<ChannelItem>,
    ) -> Self {
        Self {
            inner: Arc::new(InnerChannel { source, session, sender }),
        }
    }
}

#[derive(Clone)]
pub struct InnerChannel {
    source: Entity,
    session: Entity,
    sender: TokioSender<ChannelItem>,
}

impl InnerChannel {
    pub fn source(&self) -> Entity {
        self.source
    }

    pub fn sender(&self) -> &TokioSender<ChannelItem> {
        &self.sender
    }
}

pub(crate) type ChannelItem = Box<dyn FnOnce(&mut World, &mut OperationRoster) + Send>;
pub(crate) type ChannelSender = TokioSender<ChannelItem>;
pub(crate) type ChannelReceiver = TokioReceiver<ChannelItem>;

#[derive(Resource)]
pub(crate) struct ChannelQueue {
    pub(crate) sender: ChannelSender,
    pub(crate) receiver: ChannelReceiver,
}

impl ChannelQueue {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = unbounded_channel();
        Self { sender, receiver }
    }
}

impl Default for ChannelQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Use this channel to stream data using the [`StreamChannel::send`] method.
pub struct StreamChannel<T> {
    target: Option<Entity>,
    inner: Arc<InnerChannel>,
    _ignore: std::marker::PhantomData<T>,
}

impl<T: Stream> StreamChannel<T> {
    /// Send an instance of data out over a stream.
    pub fn send(&self, data: T) {
        let source = self.inner.source;
        let session = self.inner.session;
        let target = self.target;
        self.inner.sender.send(Box::new(
            move |world: &mut World, roster: &mut OperationRoster| {
                data.send(StreamRequest { source, session, target, world, roster }).ok();
            }
        )).ok();
    }

    pub(crate) fn new(target: Option<Entity>, inner: Arc<InnerChannel>) -> Self {
        Self { target, inner, _ignore: Default::default() }
    }
}

#[cfg(test)]
mod tests {
    use crate::{*, testing::*};
    use bevy_ecs::system::EntityCommands;
    use std::time::Duration;

    #[test]
    fn test_channel_request() {
        let mut context = TestingContext::minimal_plugins();

        let (hello, repeat) = context.command(|commands| {
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
            let mut promise = context.command(|commands| {
                commands.request(
                    RepeatRequest { service: hello, count: 5 },
                    repeat,
                ).take().response
            });

            context.run_with_conditions(
                &mut promise,
                FlushConditions::new().with_timeout(Duration::from_secs(5)),
            );

            assert!(promise.peek().is_available());
            assert!(context.no_unhandled_errors());
        }

        let count = context.app.world.get::<RunCount>(hello.provider()).unwrap().0;
        assert_eq!(count, 25);

        let count = context.app.world.get::<RunCount>(repeat.provider()).unwrap().0;
        assert_eq!(count, 5);
    }
}
