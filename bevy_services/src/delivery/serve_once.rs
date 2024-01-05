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

use crate::{
    Req, Resp, Job, Assistant, IntoStreamBundle, BoxedJob,
    GenericAssistant, InputStorage, InputBundle, TaskBundle,
    TargetStorage, Operation,
    OperationStatus,
    cancel, private,
};

use bevy::{
    prelude::{World, Entity, Component, IntoSystem, In},
    ecs::system::BoxedSystem,
    tasks::AsyncComputeTaskPool,
};

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex}
};

pub mod traits {
    use super::*;

    pub trait Servable<Marker>: private::Sealed<Marker> {
        type Request;
        type Response;
        type Streams;
        fn serve(self, source: Entity, world: &mut World, queue: &mut VecDeque<Entity>);
    }

    pub trait Holdable<Marker>: private::Sealed<Marker> {
        type Request;
        type Response;
        type Streams;
        fn hold(self) -> HeldService<Self::Request, Self::Response, Self::Streams>;
    }

    pub trait Shareable<Marker>: private::Sealed<Marker> {
        type Request;
        type Response;
        type Streams;
        fn share(self) -> SharedService<Self::Request, Self::Response, Self::Streams>;
    }
}

use traits::*;

#[derive(Component)]
struct ServableStorage<ThenServe>(ThenServe);

pub(crate) struct ServeOnce<S, M> {
    service: S,
    target: Entity,
    _ignore: std::marker::PhantomData<M>,
}

impl<M, S: Servable<M>> ServeOnce<S, M> {
    pub(crate) fn new(
        service: S,
        target: Entity,
    ) -> Self {
        Self {
            service,
            target,
            _ignore: Default::default(),
        }
    }
}

impl<M, S> Operation for ServeOnce<S, M>
where
    S: 'static + Send + Sync + Servable<M>,
    S::Request: 'static + Send + Sync,
    S::Response: 'static + Send + Sync,
{
    fn set_parameters(self, entity: Entity, world: &mut World) {
        world.entity_mut(entity).insert((
            ServableStorage(self.service),
            TargetStorage(self.target),
        ));
    }

    fn execute(
        source: Entity,
        world: &mut World,
        queue: &mut VecDeque<Entity>,
    ) -> Result<super::OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let ServableStorage(service) = source_mut.take::<ServableStorage<S>>().ok_or(())?;
        service.serve(source, world, queue);
        Ok(OperationStatus::Finished)
    }
}

fn dispatch_held_service<Request: 'static + Send + Sync, Response: 'static + Send + Sync, Streams>(
    source: Entity,
    world: &mut World,
    queue: &mut VecDeque<Entity>,
    service: &mut HoldableService<Request, Response>,
) {
    let Some(mut source_mut) = world.get_entity_mut(source) else {
        cancel(world, source);
        return;
    };

    let Some(InputStorage(request)) = source_mut.take::<InputStorage<Request>>() else {
        cancel(world, source);
        return;
    };

    let Some(TargetStorage(target)) = source_mut.get::<TargetStorage>() else {
        cancel(world, source);
        return;
    };
    let target = *target;

    source_mut.despawn();

    match service {
        HoldableService::Blocking(service) => {
            let Resp(response) = service.run(Req(request), world);
            service.apply_deferred(world);

            if let Some(mut target_mut) = world.get_entity_mut(target) {
                target_mut.insert(InputBundle::new(response));
                queue.push_back(target);
            } else {
                cancel(world, target);
            }
        }
        HoldableService::Async(service) => {
            let Job(job) = service.run(Req(request), world);
            service.apply_deferred(world);

            let task = AsyncComputeTaskPool::get().spawn(async move {
                job(GenericAssistant {  })
            });

            if let Some(mut target_mut) = world.get_entity_mut(target) {
                target_mut.insert(TaskBundle::new(task));
            } else {
                cancel(world, target);
            }
        }
    }
}

enum HoldableService<Request, Response> {
    Blocking(BoxedSystem<Req<Request>, Resp<Response>>),
    Async(BoxedSystem<Req<Request>, BoxedJob<Response>>),
}

pub struct HeldService<Request, Response, Streams> {
    service: HoldableService<Request, Response>,
    initialized: bool,
    dispatch: fn(Entity, &mut World, &mut VecDeque<Entity>, &mut HoldableService<Request, Response>),
    _ignore: std::marker::PhantomData<Streams>,
}

struct SharedSentinel {
    running: bool,
    queued: VecDeque<Entity>,
}

impl SharedSentinel {
    fn new() -> Self {
        Self { running: false, queued: Default::default() }
    }
}

#[derive(Clone)]
pub struct SharedService<Request, Response, Streams> {
    sentinel: Arc<Mutex<SharedSentinel>>,
    servable: Arc<Mutex<HeldService<Request, Response, Streams>>>,
}

struct HeldServiceMarker;
struct SharedServiceMarker;

impl<Request: 'static, Response: 'static, Streams> Servable<HeldServiceMarker> for HeldService<Request, Response, Streams> {
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn serve(mut self, source: Entity, world: &mut World, queue: &mut VecDeque<Entity>) {
        if !self.initialized {
            match &mut self.service {
                HoldableService::Blocking(service) => {
                    service.initialize(world);
                }
                HoldableService::Async(service) => {
                    service.initialize(world);
                }
            }
        }
        (self.dispatch)(source, world, queue, &mut self.service);
    }
}
impl<Request, Response, Streams> private::Sealed<HeldServiceMarker> for HeldService<Request, Response, Streams> { }


impl<Request: 'static, Response: 'static, Streams> Servable<SharedServiceMarker> for SharedService<Request, Response, Streams> {
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn serve(self, source: Entity, world: &mut World, queue: &mut VecDeque<Entity>) {
        let mut servable: std::sync::MutexGuard<'_, HeldService<Request, Response, Streams>> = {
            let mut sentinel = match self.sentinel.lock() {
                Ok(sentinel) => sentinel,
                Err(poisoned) => {
                    let mut sentinel = poisoned.into_inner();
                    // If the sentinel was poisoned, let's just reset it to
                    // recover
                    sentinel.running = false;
                    sentinel.queued.clear();
                    sentinel
                }
            };

            sentinel.queued.push_back(source);
            if sentinel.running {
                // If the system is already running then we cannot run it now.
                // Instead we'll just leave its queued count incremented and
                // let it be run later.
                return;
            }

            match self.servable.lock() {
                Ok(servable) => servable,
                Err(poisoned) => {
                    let mut servable = poisoned.into_inner();
                    // If the mutex was poisoned we will treat the system as
                    // uninitialized. Hopefully that can reset any bad state
                    // that it might have accumulated.
                    servable.initialized = false;
                    servable
                }
            }
        };

        if !servable.initialized {
            match &mut servable.service {
                HoldableService::Blocking(service) => {
                    service.initialize(world);
                }
                HoldableService::Async(service) => {
                    service.initialize(world);
                }
            }
            servable.initialized = true;
        }

        while let Some(source) = {
            let mut sentinel = match self.sentinel.lock() {
                Ok(sentinel) => sentinel,
                Err(poisoned) => {
                    let mut sentinel = poisoned.into_inner();
                    sentinel.running = true;
                    // TODO(@mxgrey): What should we really assume the queue to
                    // be here? Clearing it out and immediately exiting seems
                    // like the safest choice, but that could cause invisible
                    // failures that are hard for users to diagnose.
                    sentinel.queued.clear();
                    sentinel
                }
            };

            if let Some(target) = sentinel.queued.pop_front() {
                Some(target)
            } else {
                sentinel.running = false;
                None
            }
        } {
            (servable.dispatch)(source, world, queue, &mut servable.service);
        }
    }
}
impl<Request, Response, Streams> private::Sealed<SharedServiceMarker> for SharedService<Request, Response, Streams> { }

impl<Request, Response, M, Sys>
Holdable<(Request, Response, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Resp<Response>, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();
    fn hold(self) -> HeldService<Self::Request, Self::Response, Self::Streams> {
        let service = Box::new(IntoSystem::into_system(self));
        HeldService {
            service: HoldableService::Blocking(service),
            initialized: false,
            dispatch: dispatch_held_service::<Request, Response, ()>,
            _ignore: Default::default()
        }
    }
}

impl<Request, Response, Streams, Task, M, Sys>
Holdable<(Request, Response, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Job<Task>, M>,
    Task: FnOnce(Assistant<Streams>) -> Option<Response> + 'static + Send,
    Streams: IntoStreamBundle + 'static,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn hold(self) -> HeldService<Self::Request, Self::Response, Self::Streams> {
        let service = Box::new(IntoSystem::into_system(
            self
            .pipe(
                |In(Job(task)): In<Job<Task>>| {
                    let task: BoxedJob<Response> = Job(Box::new(
                        move |assistant: GenericAssistant| {
                            task(assistant.into_specific())
                        }
                    ));
                    task
                }
            )
        ));
        HeldService {
            service: HoldableService::Async(service),
            initialized: false,
            dispatch: dispatch_held_service::<Request, Response, Streams>,
            _ignore: Default::default(),
        }
    }
}

impl<Request, Response, M, Sys>
Servable<(Request, Response, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Resp<Response>, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();
    fn serve(self, source: Entity, world: &mut World, queue: &mut VecDeque<Entity>) {
        self.hold().serve(source, world, queue);
    }
}

impl<Request, Response, Streams, Task, M, Sys>
Servable<(Request, Response, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Job<Task>, M>,
    Task: FnOnce(Assistant<Streams>) -> Option<Response> + 'static + Send,
    Streams: IntoStreamBundle + 'static,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn serve(self, source: Entity, world: &mut World, queue: &mut VecDeque<Entity>) {
        self.hold().serve(source, world, queue);
    }
}

impl<Request, Response, Streams> Shareable<HeldServiceMarker> for HeldService<Request, Response, Streams> {
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn share(self) -> SharedService<Self::Request, Self::Response, Self::Streams> {
        SharedService {
            sentinel: Arc::new(Mutex::new(SharedSentinel::new())),
            servable: Arc::new(Mutex::new(self))
        }
    }
}

impl<Request, Response, M, Sys>
Shareable<(Request, Response, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Resp<Response>, M>,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = ();
    fn share(self) -> SharedService<Self::Request, Self::Response, Self::Streams> {
        self.hold().share()
    }
}

impl<Request, Response, Streams, Task, M, Sys>
Shareable<(Request, Response, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Job<Task>, M>,
    Task: FnOnce(Assistant<Streams>) -> Option<Response> + 'static + Send,
    Streams: IntoStreamBundle + 'static,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn share(self) -> SharedService<Self::Request, Self::Response, Self::Streams> {
        self.hold().share()
    }
}
