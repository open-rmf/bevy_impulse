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

use bevy_ecs::{
    prelude::{Component, Entity, Local, Query, Commands, World},
    system::{SystemParam, Command, IntoSystem},
    world::EntityWorldMut,
    schedule::{SystemConfigs, IntoSystemConfigs},
};
use bevy_hierarchy::prelude::{BuildWorldChildren, DespawnRecursiveExt};

use smallvec::SmallVec;

use std::collections::HashMap;

use crate::{
    ManageInput, OperationError, StreamPack, OrBroken, OperationResult,
    DeferredRoster, UnhandledErrors, OperationRoster, Broken, Input, ServiceRequest,
    StreamTargetMap, ScopeStorage, Blocker, ServiceBundle, ServiceTrait,
    OperationRequest, DeliveryInstructions, Delivery, DeliveryUpdate, Deliver,
    SingleTargetStorage, Disposal, DeliveryOrder, IntoContinuousService,
    ContinuousService, IntoServiceBuilder, ServiceBuilder, OperationReachability,
    ReachabilityResult, ProviderStorage,
    dispose_for_despawned_service, insert_new_order, pop_next_delivery,
    emit_disposal,
};

pub struct ContinuousServiceKey<Request, Response, Streams> {
    provider: Entity,
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>,
}

impl<Request, Response, Streams> ContinuousServiceKey<Request, Response, Streams> {
    fn new(provider: Entity) -> Self {
        Self { provider, _ignore: Default::default() }
    }
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
pub struct ContinuousQuery<'w, 's, Request, Response, Streams = ()>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    queues: Query<'w, 's, &'static ContinuousQueueStorage<Request>>,
    streams: Query<'w, 's, StreamTargetQuery<Streams>>,
    delivered: Local<'s, HashMap<Entity, HashMap<usize, DeliverResponse<Response>>>>,
    commands: Commands<'w, 's>,
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
                provider: key.provider(),
                streams: &self.streams,
                delivered: self.delivered.entry(key.provider()).or_default(),
                commands: &mut self.commands,
            })
    }
}

pub struct ContinuousQueueView<'a, Request, Response>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    queue: &'a ContinuousQueueStorage<Request>,
    delivered: Option<&'a HashMap<usize, DeliverResponse<Response>>>,
}

impl<'a, Request, Response> ContinuousQueueView<'a, Request, Response>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    pub fn iter(&self) -> impl Iterator<Item=OrderView<'_, Request>> {
        self.queue.inner.iter()
            .enumerate()
            .filter(|(i, _)| !self.delivered.is_some_and(|d| d.contains_key(i)))
            .map(|(index, item)| OrderView {
                index,
                order: item,
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
                order: item,
            })
    }

    pub fn len(&self) -> usize {
        self.queue.inner.len() - self.delivered.map(|d| d.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct OrderView<'a, Request> {
    order: &'a ContinuousOrder<Request>,
    index: usize,
}

impl<'a, Request> OrderView<'a, Request> {
    pub fn request(&self) -> &Request {
        &self.order.data
    }

    pub fn session(&self) -> Entity {
        self.order.session
    }

    pub fn source(&self) -> Entity {
        self.order.source
    }

    pub fn index(&self) -> usize {
        self.index
    }
}

pub struct ContinuousQueueMut<'w, 's, 'a, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    provider: Entity,
    queue: &'a ContinuousQueueStorage<Request>,
    streams: &'a Query<'w, 's, StreamTargetQuery<Streams>>,
    delivered: &'a mut HashMap<usize, DeliverResponse<Response>>,
    commands: &'a mut Commands<'w, 's>,
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
            .filter(|(i, _)| !self.delivered.contains_key(i))
            .map(|(index, item)| OrderView {
                order: item,
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
        let streams = self.make_stream_buffer(item.source);
        Some(OrderMut {
            index,
            streams: Some(streams),
            provider: self.provider,
            request: item,
            delivered: &mut self.delivered,
            commands: &mut self.commands,
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

            let streams = self.make_stream_buffer(item.source);
            f(OrderMut {
                index,
                streams: Some(streams),
                provider: self.provider,
                request: item,
                delivered: &mut self.delivered,
                commands: &mut self.commands,
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

            let streams = self.make_stream_buffer(item.source);
            let u = f(OrderMut {
                index,
                streams: Some(streams),
                provider: self.provider,
                request: item,
                delivered: &mut self.delivered,
                commands: &mut self.commands,
            });

            output.push((index, u));
        }

        output
    }

    fn make_stream_buffer(&self, source: Entity) -> Streams::Buffer {
        // INVARIANT: The query can't fail because all of its components are optional
        let (target_indices, target_map) = self.streams.get(source).unwrap();
        Streams::make_buffer(target_indices, target_map)
    }
}

pub struct OrderMut<'w, 's, 'a, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    request: &'a ContinuousOrder<Request>,
    provider: Entity,
    index: usize,
    // We use Option here so that the buffer can be taken inside the destructor.
    // We need to take the buffer so that the data can be sent into commands and
    // flushed later.
    streams: Option<Streams::Buffer>,
    delivered: &'a mut HashMap<usize, DeliverResponse<Response>>,
    commands: &'a mut Commands<'w, 's>,
}

impl<'w, 's, 'a, Request, Response, Streams> OrderMut<'w, 's, 'a, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    /// Look at the request of this order.
    // TODO(@mxgrey): Consider offering a take_request that allows the user to
    // fully take ownership of the request data. That would force us to change
    // this function to return an Option<&Request>.
    pub fn request(&self) -> &Request {
        &self.request.data
    }

    /// Check the session that this order is associated with.
    pub fn session(&self) -> Entity {
        self.request.session
    }

    /// Check the ID of the node that this order is associated with
    pub fn source(&self) -> Entity {
        self.request.source
    }

    /// Provide a response for this order. After calling this you will not be
    /// able to stream or give any more responses for this particular order.
    pub fn respond(self, response: Response) {
        self.delivered.insert(self.index, DeliverResponse {
            provider: self.provider,
            source: self.request.source,
            session: self.request.session,
            task_id: self.request.task_id,
            data: response,
            index: self.index,
        });
    }

    /// Access the stream buffer so you can send streams from your service.
    pub fn streams(&self) -> &Streams::Buffer {
        // INVARIANT: This is always initialized with Some(_) and never gets
        // taken until OrderMut is dropped, so this will always contain a valid
        // buffer for as long as the OrderMut is not being dropped.
        self.streams.as_ref().unwrap()
    }
}

impl<'w, 's, 'a, Request, Response, Streams> Drop for OrderMut<'w, 's, 'a, Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn drop(&mut self) {
        Streams::defer_buffer(
            // INVARIANT: This is the only place where streams are taken
            self.streams.take().unwrap(),
            self.request.source,
            self.request.session,
            self.commands,
        );
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
        for (_, delivered) in self.delivered.drain() {
            for (_, deliver) in delivered {
                responses.push(deliver)
            }
        }

        if !responses.is_empty() {
            self.commands.add(DeliverResponses::<Request, Response, Streams> {
                responses,
                _ignore: Default::default(),
            });
        }
    }
}

#[derive(Component)]
struct ContinuousQueueStorage<Request> {
    inner: SmallVec<[ContinuousOrder<Request>; 16]>,
}

impl<Request> ContinuousQueueStorage<Request> {
    fn new() -> Self {
        Self { inner: Default::default() }
    }

    fn contains_session(
        provider: Entity,
        session: Entity,
        world: &World,
    ) -> ReachabilityResult
    where
        Request: 'static + Send + Sync,
    {
        let Some(queue) = world.get_entity(provider).or_broken()?.get::<Self>() else {
            return Ok(false);
        };

        Ok(queue.inner.iter().find(|order| order.session == session).is_some())
    }
}

#[derive(Component)]
pub(crate) struct ActiveContinuousSessions(
    fn(Entity, Entity, &World) -> ReachabilityResult
);

impl ActiveContinuousSessions {
    pub(crate) fn contains_session(r: &OperationReachability) -> ReachabilityResult {
        let provider = r.world().get::<ProviderStorage>(r.source()).or_broken()?.get();
        let Some(active) = r.world().get::<ActiveContinuousSessions>(provider) else {
            return Ok(false);
        };
        let f = active.0;
        f(provider, r.session(), r.world())
    }
}

struct ContinuousOrder<Request> {
    data: Request,
    session: Entity,
    source: Entity,
    task_id: Entity,
    unblock: Option<Blocker>,
}

struct DeliverResponses<Request, Response, Streams> {
    responses: SmallVec<[DeliverResponse<Response>; 16]>,
    _ignore: std::marker::PhantomData<(Request, Streams)>,
}

struct DeliverResponse<Response> {
    provider: Entity,
    source: Entity,
    session: Entity,
    task_id: Entity,
    data: Response,
    index: usize,
}

impl<Request, Response, Streams> Command for DeliverResponses<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn apply(self, world: &mut World) {
        world.get_resource_or_insert_with(|| DeferredRoster::default());
        world.resource_scope::<DeferredRoster, _>(|world: &mut World, mut deferred| {
            let mut remove: SmallVec<[(usize, Entity); 16]> = SmallVec::new();
            for DeliverResponse { provider, source, session, data, index, task_id } in self.responses {
                remove.push((index, provider));
                let r = try_give_response(source, session, data, world, &mut *deferred);
                if let Err(OperationError::Broken(backtrace)) = r {
                    world.get_resource_or_insert_with(|| UnhandledErrors::default())
                        .broken
                        .push(Broken { node: provider, backtrace });
                }

                if Streams::has_streams() {
                    // When a continuous task with any number of streams >= 1 is
                    // finished, we should always do a disposal notification
                    // to force a reachability check. Normally there are specific
                    // events that prompt us to check reachability, but if a
                    // reachability test occurred while the continuous service
                    // node was running and the reachability depends on a stream
                    // which may or may not have been emitted, then the
                    // reachability test may have concluded with a false
                    // positive, and it needs to be rechecked now that this
                    // node has finished.
                    if let Some(scope) = world.get::<ScopeStorage>(source) {
                        deferred.disposed(scope.get(), source, session);
                    }
                }

                if let Some(task_mut) = world.get_entity_mut(task_id) {
                    task_mut.despawn_recursive();
                }
            }

            // Reverse sort by index so that as we iterate forward through this,
            // we remove the last elements first, so the earliest elements remain
            // valid.
            remove.sort_by(|(index_a, _), (index_b, _)|
                index_b.cmp(index_a)
            );
            for (index, provider) in remove {
                let r = try_retire_request::<Request>(provider, index, world, &mut *deferred);
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
    source: Entity,
    session: Entity,
    data: Response,
    world: &mut World,
    roster: &mut OperationRoster,
) -> OperationResult {
    let target = world.get::<SingleTargetStorage>(source).or_broken()?.get();
    world.get_entity_mut(target).or_broken()?
        .give_input(session, data, roster)
}

type StreamTargetQuery<Streams> = (
    <Streams as StreamPack>::TargetIndexQuery,
    Option<&'static StreamTargetMap>,
);

fn try_retire_request<Request: 'static + Send + Sync>(
    provider: Entity,
    index: usize,
    world: &mut World,
    roster: &mut OperationRoster,
) -> OperationResult {
    let mut storage = world.get_mut::<ContinuousQueueStorage<Request>>(provider).or_broken()?;
    let finished_order = storage.inner.remove(index);
    if let Some(unblock) = finished_order.unblock {
        let f = unblock.serve_next;
        f(unblock, world, roster);
    }

    Ok(())
}

struct ContinuousServiceImpl<Request, Response, Streams> {
    _ignore: std::marker::PhantomData<(Request, Response, Streams)>
}

impl<Request, Response, Streams> ServiceTrait for ContinuousServiceImpl<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    fn serve(
        ServiceRequest { provider, target, operation: OperationRequest { source, world, roster } }: ServiceRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let Input { session, data: request } = source_mut.take_input::<Request>()?;
        let instructions = source_mut.get::<DeliveryInstructions>().cloned();
        let task_id = world.spawn(()).set_parent(source).id();

        let Some(mut delivery) = world.get_mut::<Delivery<Request>>(provider) else {
            dispose_for_despawned_service(provider, world, roster);
            return Err(OperationError::NotReady);
        };

        let update = insert_new_order::<Request>(
            delivery.as_mut(),
            DeliveryOrder { source, session, task_id, request, instructions },
        );

        let (request, blocker) = match update {
            DeliveryUpdate::Immediate { blocking, request } => {
                let serve_next = serve_next_continuous_request::<Request, Response, Streams>;
                let blocker = blocking.map(|label| Blocker { provider, source, session, label, serve_next });
                (request, blocker)
            }
            DeliveryUpdate::Queued { cancelled, stop, label } => {
                for cancelled in cancelled {
                    let disposal = Disposal::supplanted(cancelled.source, source, session);
                    emit_disposal(cancelled.source, cancelled.session, disposal, world, roster);
                    if let Some(task_mut) = world.get_entity_mut(cancelled.task_id) {
                        task_mut.despawn_recursive();
                    }
                }
                if let Some(stop) = stop {
                    // This task is already running so we need to remove it from
                    // the queue
                    let mut queue = world.get_mut::<ContinuousQueueStorage<Request>>(provider)
                        .or_broken()?;
                    let stopped_index = queue.inner.iter().enumerate()
                        .find(|(_, r)| r.task_id == stop.task_id)
                        .map(|(index, _)| index);

                    // Immediately queue up an unblocking because continuous services
                    // cancel immediately.
                    if let Some(unblock) = stopped_index.map(|i| queue.inner.remove(i).unblock).flatten() {
                        let f = unblock.serve_next;
                        f(unblock, world, roster);
                    } else {
                        let serve_next = serve_next_continuous_request::<Request, Response, Streams>;
                        roster.unblock(Blocker {
                            provider, source: stop.source, session: stop.session, label, serve_next
                        });
                    }

                    let disposal = Disposal::supplanted(stop.source, source, session);
                    emit_disposal(stop.source, stop.session, disposal, world, roster);
                    if let Some(task_mut) = world.get_entity_mut(stop.task_id) {
                        task_mut.despawn_recursive();
                    }
                }

                return Ok(());
            }
        };

        serve_continuous_request::<Request, Response, Streams>(
            request,
            blocker,
            session,
            task_id,
            ServiceRequest {
                provider,
                target,
                operation: OperationRequest { source, world, roster },
            }
        )
    }
}

fn serve_continuous_request<Request, Response, Streams>(
    request: Request,
    blocker: Option<Blocker>,
    session: Entity,
    task_id: Entity,
    ServiceRequest { provider, operation: OperationRequest { source, world, .. }, .. }: ServiceRequest,
) -> OperationResult
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    // All we have to do is move the request into the service's active queue
    let mut queue = world.get_mut::<ContinuousQueueStorage<Request>>(provider).or_broken()?;
    queue.inner.push(ContinuousOrder {
        data: request,
        session,
        source,
        task_id,
        unblock: blocker,
    });

    Ok(())
}

fn serve_next_continuous_request<Request, Response, Streams>(
    unblock: Blocker,
    world: &mut World,
    roster: &mut OperationRoster,
)
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    let Blocker { provider, label, .. } = unblock;
    loop {
        let Some(Deliver { request, task_id, blocker }) = pop_next_delivery::<Request>(
            provider, label, serve_next_continuous_request::<Request, Response, Streams>, world
        ) else {
            // No more deliveries to pop, so we should return
            return;
        };

        let session = blocker.session;
        let source = blocker.source;

        let Some(target) = world.get::<SingleTargetStorage>(source) else {
            // This will not be able to run, so we should move onto the next
            // item in the queue.
            continue;
        };
        let target = target.get();

        if serve_continuous_request::<Request, Response, Streams>(
            request,
            Some(blocker),
            session,
            task_id,
            ServiceRequest {
                provider,
                target,
                operation: OperationRequest { source, world, roster }
            },
        ).is_err() {
            // The service did not launch so we should move onto the next item
            // in the queue.
            continue;
        }

        // The next delivery has begun so we can return
        return;
    }
}

impl<Request, Response, Streams, M, Sys> IntoContinuousService<(Request, Response, Streams, M)> for Sys
where
    Sys: IntoSystem<ContinuousService<Request, Response, Streams>, (), M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;

    fn into_system_config<'w>(self, entity_mut: &mut EntityWorldMut<'w>) -> SystemConfigs {
        let provider = entity_mut.insert((
            ContinuousQueueStorage::<Request>::new(),
            ActiveContinuousSessions(ContinuousQueueStorage::<Request>::contains_session),
            ServiceBundle::<ContinuousServiceImpl<Request, Response, Streams>>::new(),
        )).id();
        let continuous_key = move || ContinuousService { key: ContinuousServiceKey::new(provider) };
        continuous_key.pipe(self).into_configs()
    }
}

pub struct IntoContinuousServiceBuilderMarker<M>(std::marker::PhantomData<M>);

impl<M, Srv> IntoServiceBuilder<IntoContinuousServiceBuilderMarker<M>> for Srv
where
    Srv: IntoContinuousService<M>,
{
    type Service = Srv;
    type Deliver = ();
    type With = ();
    type Also = ();
    type Configure = ();

    fn into_service_builder(self) -> ServiceBuilder<Self::Service, (), (), (), ()> {
        ServiceBuilder::new(self)
    }
}
