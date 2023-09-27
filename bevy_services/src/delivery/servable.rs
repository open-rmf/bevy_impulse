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
    Req, Resp, Job, Assistant, IntoStreamOutComponents, BoxedJob,
    GenericAssistant, RequestStorage, ResponseStorage, TaskStorage, PollTask,
    Dispatch, DispatchCommand, UnusedTarget, Pending, Target,
    cancel, handle_response, poll_task, private,
};

use bevy::{
    prelude::{World, Entity, Component, IntoSystem, In},
    ecs::system::{Command, BoxedSystem},
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
        fn serve(self, world: &mut World, target: Entity);
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

pub(crate) struct MakeThenServe<ThenServe, M> {
    provider: Entity,
    servable: ThenServe,
    _ignore: std::marker::PhantomData<M>,
}

impl<M, ThenServe> MakeThenServe<ThenServe, M> {
    pub(crate) fn new(provider: Entity, servable: ThenServe) -> Self {
        Self { provider, servable, _ignore: Default::default() }
    }
}

impl<M, ThenServe> Command for MakeThenServe<ThenServe, M>
where
    M: 'static + Send,
    ThenServe: 'static + Send + Sync + Servable<M>,
    ThenServe::Request: 'static + Send + Sync,
{
    fn apply(self, world: &mut World) {
        world
            .entity_mut(self.provider)
            .insert(ServableStorage(self.servable))
            .insert(Dispatch::new_then_serve::<M, ThenServe>());
    }
}

impl Dispatch {
    fn new_then_serve<M, ThenServe>() -> Self
    where
        ThenServe: 'static + Send + Sync + Servable<M>,
        ThenServe::Request: 'static + Send +Sync,
    {
        Dispatch(dispatch_then_serve::<M, ThenServe>)
    }
}

fn dispatch_then_serve<M, ThenServe: 'static + Send + Sync + Servable<M>>(
    world: &mut World,
    cmd: DispatchCommand,
)
where
    ThenServe::Request: 'static + Send + Sync,
{
    let Some(mut provider_mut) = world.get_entity_mut(cmd.provider) else {
        cancel(world, cmd.target);
        return;
    };

    provider_mut.remove::<UnusedTarget>();
    let Some(ResponseStorage(Some(request))) = provider_mut.take::<ResponseStorage<ThenServe::Request>>() else {
        provider_mut
            .insert(Target(cmd.target))
            .insert(Pending(pending_then_serve::<M, ThenServe>));
        return;
    };

    let ServableStorage(servable) = provider_mut.take::<ServableStorage<ThenServe>>().unwrap();
    provider_mut.despawn();
    let Some(mut target_mut) = world.get_entity_mut(cmd.target) else {
        cancel(world, cmd.target);
        return;
    };
    target_mut.insert(RequestStorage(Some(request)));
    servable.serve(world, cmd.target);
}

fn pending_then_serve<M, ThenServe>(
    world: &mut World,
    queue: &mut VecDeque<Entity>,
    provider: Entity,
)
where
    ThenServe: 'static + Send + Sync + Servable<M>,
    ThenServe::Request: 'static + Send + Sync,
{
    let mut provider_mut = world.entity_mut(provider);
    let ServableStorage(servable) = provider_mut.take::<ServableStorage<ThenServe>>().unwrap();
    let target = provider_mut.get::<Target>().unwrap().0;
    let Some(ResponseStorage(Some(request))) = provider_mut.take::<ResponseStorage<ThenServe::Request>>() else {
        cancel(world, target);
        return;
    };
    provider_mut.despawn();
    let Some(mut target_mut) = world.get_entity_mut(target) else {
        cancel(world, target);
        return;
    };
    target_mut.insert(RequestStorage(Some(request)));
    servable.serve(world, target);
    queue.push_back(target);
}

fn dispatch_held_service<Request: 'static + Send + Sync, Response: 'static + Send + Sync, Streams>(
    world: &mut World,
    service: &mut HoldableService<Request, Response>,
    target: Entity,
) {
    let request = if let Some(mut target) = world.get_entity_mut(target) {
        target.get_mut::<RequestStorage<Request>>().unwrap().0.take().unwrap()
    } else {
        cancel(world, target);
        return;
    };

    match service {
        HoldableService::Blocking(service) => {
            let Resp(response) = service.run(Req(request), world);
            service.apply_deferred(world);
            if let Some(mut target_mut) = world.get_entity_mut(target) {
                target_mut.insert(ResponseStorage(Some(response)));
                handle_response(world, target);
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
                target_mut.insert((
                    TaskStorage(task),
                    PollTask {
                        provider: None,
                        poll: poll_task::<Response>,
                    }
                ));
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
    dispatch: fn(&mut World, &mut HoldableService<Request, Response>, Entity),
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
    fn serve(mut self, world: &mut World, target: Entity) {
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
        (self.dispatch)(world, &mut self.service, target);
    }
}
impl<Request, Response, Streams> private::Sealed<HeldServiceMarker> for HeldService<Request, Response, Streams> { }


impl<Request: 'static, Response: 'static, Streams> Servable<SharedServiceMarker> for SharedService<Request, Response, Streams> {
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn serve(self, world: &mut World, target: Entity) {
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

            sentinel.queued.push_back(target);
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

        while let Some(target) = {
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
            (servable.dispatch)(world, &mut servable.service, target);
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
    Streams: IntoStreamOutComponents + 'static,
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
    fn serve(self, world: &mut World, target: Entity) {
        self.hold().serve(world, target);
    }
}

impl<Request, Response, Streams, Task, M, Sys>
Servable<(Request, Response, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Job<Task>, M>,
    Task: FnOnce(Assistant<Streams>) -> Option<Response> + 'static + Send,
    Streams: IntoStreamOutComponents + 'static,
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn serve(self, world: &mut World, target: Entity) {
        self.hold().serve(world, target);
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
    Streams: IntoStreamOutComponents + 'static,
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
