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

use bevy::{
    prelude::{Component, In, Entity, Local, Query, Commands, World},
    ecs::{
        world::EntityMut,
        system::{IntoSystem, BoxedSystem, EntityCommands, SystemParam, Command},
    }
};

use smallvec::SmallVec;

use std::collections::HashMap;

use crate::{
    BlockingService, BlockingServiceInput, IntoService, ServiceTrait, ServiceRequest,
    Input, ManageInput, ServiceBundle, OperationRequest, OperationError, StreamPack,
    UnusedStreams, ManageDisposal, OrBroken, dispose_for_despawned_service,
    OperationResult, DeferredRoster, UnhandledErrors, OperationRoster, Broken,
};

pub struct ContinuousServiceKey<Request, Response, Streams> {
    provider: Entity,
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<Request, Response, Streams> ContinuousServiceKey<Request, Response, Streams> {
    pub fn provider(&self) -> Entity {
        self.provider
    }
}

impl<Request, Response, Streams> Clone for ContinuousServiceKey<Request, Response, Streams> {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider,
            _ignore: Default::default(),
        }
    }
}

impl<Request, Response, Streams> Copy for ContinuousServiceKey<Request, Response, Streams> {}

#[derive(SystemParam)]
pub struct ContinuousQuery<'w, 's, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    queues: Query<'w, 's, &'static ContinuousQueueStorage<Request>>,
    delivered: Local<'s, HashMap<Entity, HashMap<usize, (Entity, Response)>>>,
    commands: Commands<'w, 's>,
    _ignore: std::marker::PhantomData<Streams>,
}

impl<'w, 's, Request, Response, Streams> ContinuousQuery<'w, 's, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    pub fn view<'a>(
        &'a self,
        key: &ContinuousServiceKey<Request, Response, Streams>,
    ) -> Option<ContinuousQueueView<'a, Request, Response>> {
        self.queues.get(key.provider())
            .ok()
            .map(|queue| ContinuousQueueView {
                queue,
                delivered: self.delivered.get(&key.provider()),
            })
    }

    pub fn get_mut<'a>(
        &'a mut self,
        key: &ContinuousServiceKey<Request, Response, Streams>,
    ) -> Option<ContinuousQueueMut<'w, 's, 'a, Request, Response, Streams>> {
        self.queues.get(key.provider())
            .ok()
            .map(|queue| ContinuousQueueMut {
                queue,
                delivered: self.delivered.entry(key.provider()).or_default(),
                commands: &mut self.commands,
                _ignore: Default::default(),
            })
    }
}

pub struct ContinuousQueueView<'a, Request, Response>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    queue: &'a ContinuousQueueStorage<Request>,
    delivered: Option<&'a HashMap<usize, (Entity, Response)>>,
}

impl<'a, Request, Response> ContinuousQueueView<'a, Request, Response>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    pub fn iter(&self) -> impl Iterator<Item=OrderView<'_, Request>> {
        self.queue.inner.iter()
            .enumerate()
            .filter(|(i, _)| self.delivered.is_some_and(|d| d.contains_key(i)))
            .map(|(index, item)| OrderView {
                index,
                session: item.session,
                data: &item.data,
            })
    }

    pub fn get(&self, index: usize) -> Option<OrderView<'_, Request>> {
        if self.delivered.is_some_and(|d| d.contains_key(&index)) {
            return None;
        }

        self.queue.inner
            .get(index)
            .map(|item| OrderView {
                index,
                session: item.session,
                data: &item.data,
            })
    }
}

pub struct OrderView<'a, Request> {
    pub session: Entity,
    pub data: &'a Request,
    pub index: usize,
}

pub struct ContinuousQueueMut<'w, 's, 'a, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    queue: &'a ContinuousQueueStorage<Request>,
    delivered: &'a mut HashMap<usize, (Entity, Response)>,
    commands: &'a mut Commands<'w, 's>,
    _ignore: std::marker::PhantomData<Streams>,
}

impl<'w, 's, 'a, Request, Response, Streams> ContinuousQueueMut<'w, 's, 'a, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    pub fn iter(&self) -> impl Iterator<Item=OrderView<'_, Request>> {
        self.queue.inner.iter()
            .enumerate()
            .filter(|(i, _)| self.delivered.contains_key(i))
            .map(|(index, item)| OrderView {
                session: item.session,
                data: &item.data,
                index,
            })
    }

    pub fn get_mut<'b>(
        &'b mut self,
        index: usize,
    ) -> Option<OrderMut<'w, 's, 'b, Request, Response, Streams>> {
        if index >= self.queue.inner.len() {
            return None;
        }

        if self.delivered.contains_key(&index) {
            return None;
        }

        // INVARIANT: We have already confirmed that the index is smaller than
        // the length of the SmallVec, so it should be a valid index.
        let item = self.queue.inner.get(index).unwrap();
        Some(OrderMut {
            index,
            session: item.session,
            data: &item.data,
            delivered: &mut self.delivered,
            commands: &mut self.commands,
            _ignore: Default::default(),
        })
    }

    /// Rust iterators cannot support mutable borrows inside the items that
    /// they return, so you can use this as an alternative to doing a for-loop.
    ///
    /// If you need your operation to produce an output, use [`Self::for_each_out`].
    pub fn for_each(
        &mut self,
        mut f: impl FnMut(OrderMut<Request, Response, Streams>),
    ) {
        for (index, item) in self.queue.inner.iter().enumerate() {
            if self.delivered.contains_key(&index) {
                continue;
            }

            f(OrderMut {
                index,
                session: item.session,
                data: &item.data,
                delivered: &mut self.delivered,
                commands: &mut self.commands,
                _ignore: Default::default()
            });
        }
    }

    /// Alternative to [`Self::for_each`] that allows the operations to produce
    /// outputs which will be collected into a [`SmallVec`]. The only downside
    /// here is that a heap allocation will happen if the number of items
    /// iterated over exceed the limit of the [`SmallVec`].
    ///
    /// The collection that comes out will contain both an index and the function
    /// output value where the index indicates the index of the item in the queue
    /// that was being looked at for the output. Some indices may be skipped
    /// while running this function if one of the orders was already delivered
    /// earlier in the current run of the system.
    pub fn for_each_out<U>(
        &mut self,
        mut f: impl FnMut(OrderMut<Request, Response, Streams>) -> U,
    ) -> SmallVec<[(usize, U); 16]> {
        let mut output = SmallVec::new();
        for (index, item) in self.queue.inner.iter().enumerate() {
            if self.delivered.contains_key(&index) {
                continue;
            }

            let u = f(OrderMut {
                index,
                session: item.session,
                data: &item.data,
                delivered: &mut self.delivered,
                commands: &mut self.commands,
                _ignore: Default::default(),
            });

            output.push((index, u));
        }

        output
    }
}

pub struct OrderMut<'w, 's, 'a, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    session: Entity,
    data: &'a Request,
    index: usize,
    delivered: &'a mut HashMap<usize, (Entity, Response)>,
    commands: &'a mut Commands<'w, 's>,
    _ignore: std::marker::PhantomData<Streams>,
}

impl<'w, 's, 'a, Request, Response, Streams> OrderMut<'w, 's, 'a, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    /// Look at the request of this order.
    pub fn request(&self) -> &Request {
        self.data
    }

    /// Check the session that this order is associated with.
    pub fn session(&self) -> Entity {
        self.session
    }

    /// Provide a response for this order. After calling this you will not be
    /// able to stream or give any more responses for this particular order.
    pub fn respond(self, response: Response) {
        self.delivered.insert(self.index, (self.session, response));
    }
}

impl<'w, 's, Request, Response, Streams> Drop for ContinuousQuery<'w, 's, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn drop(&mut self) {
        let mut responses = SmallVec::new();
        for (provider, delivered) in self.delivered.drain() {
            for (_, (session, data)) in delivered {
                responses.push(DeliverResponse { provider, session, data })
            }
        }

        if !responses.is_empty() {
            self.commands.add(DeliverResponses { responses });
        }
    }
}

#[derive(Component)]
struct ContinuousQueueStorage<Request> {
    inner: SmallVec<[ContinuousOrder<Request>; 16]>,
}

struct ContinuousOrder<Request> {
    data: Request,
    session: Entity,
}

struct DeliverResponses<Response> {
    responses: SmallVec<[DeliverResponse<Response>; 16]>,
}

struct DeliverResponse<Response> {
    provider: Entity,
    session: Entity,
    data: Response,
}

impl<Response: 'static + Send + Sync> Command for DeliverResponses<Response> {
    fn apply(self, world: &mut World) {
        world.get_resource_or_insert_with(|| DeferredRoster::default());
        world.resource_scope::<DeferredRoster, _>(|world: &mut World, mut deferred| {
            for DeliverResponse { provider, session, data } in self.responses {
                let r = try_give_response(provider, session, data, world, &mut *deferred);
                if let Err(OperationError::Broken(backtrace)) = r {
                    world.get_resource_or_insert_with(|| UnhandledErrors::default())
                        .broken
                        .push(Broken { node: provider, backtrace });
                }
            }
        });
    }
}

fn try_give_response<Response: 'static + Send + Sync>(
    provider: Entity,
    session: Entity,
    data: Response,
    world: &mut World,
    roster: &mut OperationRoster,
) -> OperationResult {
    world.get_entity_mut(provider).or_broken()?
        .give_input(session, data, roster)
}
