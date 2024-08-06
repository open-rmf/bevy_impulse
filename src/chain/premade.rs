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

use bevy_ecs::prelude::In;

use smallvec::SmallVec;

use std::future::Future;

use crate::{
    BufferKey, BufferAccessMut, AsyncMap, StreamPack, Service, Cancellation,
    RequestExt, PromiseState,
};

pub(super) fn consume_buffer<const N: usize, T>(
    In(key): In<BufferKey<T>>,
    mut access: BufferAccessMut<T>,
) -> SmallVec<[T; N]>
where
    T: 'static + Send + Sync,
{
    let Ok(mut buffer) = access.get_mut(&key) else {
        return SmallVec::new();
    };

    buffer.drain(..).collect()
}

pub(crate) fn request_service<Request, Response, Streams>(
    input: AsyncMap<(Request, Service<Request, Response, Streams>), Streams>,
) -> impl Future<Output = Result<Response, Cancellation>>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync + Unpin,
    Streams: StreamPack,
    <Streams as StreamPack>::Receiver: Unpin,
{
    async move {
        let (request, service) = input.request;
        let recipient = match input.channel.command(move |commands|
            commands.request(request, service).take()
        ).await {
            PromiseState::Available(recipient) => {
                recipient
            },
            PromiseState::Cancelled(cancellation) => {
                return Err(cancellation);
            }
            PromiseState::Disposed => {
                return Err(Cancellation::undeliverable());
            }
            PromiseState::Pending | PromiseState::Taken => unreachable!(),
        };

        Streams::forward_channels(recipient.streams, input.streams).await;

        match recipient.response.await {
            PromiseState::Available(response) => {
                Ok(response)
            },
            PromiseState::Cancelled(cancellation) => {
                return Err(cancellation);
            }
            PromiseState::Disposed => {
                return Err(Cancellation::undeliverable());
            }
            PromiseState::Pending | PromiseState::Taken => unreachable!(),
        }
    }
}
