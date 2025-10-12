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
    system::Commands,
    world::CommandQueue,
};

use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as TokioReceiver, UnboundedSender as TokioSender,
};

use std::sync::Arc;

use crate::{OperationError, OperationRoster, Promise, Provider, RequestExt, StreamPack};

/// Provides asynchronous access to the [`World`], allowing you to issue queries
/// or commands and then await the result.
#[derive(Clone)]
pub struct Channel {
    inner: Arc<InnerChannel>,
}

impl Channel {
    /// Run a query in the world and receive the promise of the query's output.
    pub fn query<P>(&self, request: P::Request, provider: P) -> Promise<P::Response>
    where
        P: Provider,
        P::Request: 'static + Send + Sync,
        P::Response: 'static + Send + Sync,
        P::Streams: 'static + StreamPack,
        P: 'static + Send + Sync,
    {
        self.command(move |commands| commands.request(request, provider).take().response)
            .flatten()
    }

    /// Get access to a [`Commands`] for the [`World`]
    pub fn command<F, U>(&self, f: F) -> Promise<U>
    where
        F: FnOnce(&mut Commands) -> U + 'static + Send,
        U: 'static + Send,
    {
        let (sender, promise) = Promise::new();
        self.inner
            .sender
            .send(Box::new(
                move |world: &mut World, _: &mut OperationRoster| {
                    let mut command_queue = CommandQueue::default();
                    let mut commands = Commands::new(&mut command_queue, world);
                    let u = f(&mut commands);
                    command_queue.apply(world);
                    let _ = sender.send(u);
                },
            ))
            .ok();

        promise
    }

    pub(crate) fn for_streams<Streams: StreamPack>(
        &self,
        world: &World,
    ) -> Result<Streams::StreamChannels, OperationError> {
        Ok(Streams::make_stream_channels(&self.inner, world))
    }

    pub(crate) fn new(source: Entity, session: Entity, sender: TokioSender<ChannelItem>) -> Self {
        Self {
            inner: Arc::new(InnerChannel {
                source,
                session,
                sender,
            }),
        }
    }
}

#[derive(Clone)]
pub struct InnerChannel {
    pub(crate) source: Entity,
    pub(crate) session: Entity,
    pub(crate) sender: TokioSender<ChannelItem>,
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

#[cfg(test)]
mod tests {
    use crate::{prelude::*, testing::*};
    use bevy_ecs::system::EntityCommands;
    use std::time::Duration;

    #[test]
    fn test_channel_request() {
        let mut context = TestingContext::minimal_plugins();

        let (hello, repeat) = context.command(|commands| {
            let hello =
                commands.spawn_service(say_hello.with(|entity_cmds: &mut EntityCommands| {
                    entity_cmds.insert((
                        Salutation("Guten tag, ".into()),
                        Name("tester".into()),
                        RunCount(0),
                    ));
                }));
            let repeat =
                commands.spawn_service(repeat_service.with(|entity_cmds: &mut EntityCommands| {
                    entity_cmds.insert(RunCount(0));
                }));
            (hello, repeat)
        });

        for _ in 0..5 {
            let mut promise = context.command(|commands| {
                commands
                    .request(
                        RepeatRequest {
                            service: hello,
                            count: 5,
                        },
                        repeat,
                    )
                    .take()
                    .response
            });

            context.run_with_conditions(
                &mut promise,
                FlushConditions::new().with_timeout(Duration::from_secs(5)),
            );

            assert!(promise.peek().is_available());
            assert!(context.no_unhandled_errors());
        }

        let count = context
            .app
            .world()
            .get::<RunCount>(hello.provider())
            .unwrap()
            .0;
        assert_eq!(count, 25);

        let count = context
            .app
            .world()
            .get::<RunCount>(repeat.provider())
            .unwrap()
            .0;
        assert_eq!(count, 5);
    }
}
