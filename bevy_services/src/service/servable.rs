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
    Service, Req, Resp, Job, Assistant, IntoStreamOutComponents, BoxedJob,
    GenericAssistant,
    private,
};

use bevy::{
    prelude::{World, Entity, IntoSystem, In},
    ecs::system::BoxedSystem,
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
}

use traits::*;

fn dispatch_held_service<Request, Response, Streams>(
    world: &mut World,
    service: &mut HoldableService<Request, Response>,
    target: Entity,
) {

}

enum HoldableService<Request, Response> {
    Blocking(Option<BoxedSystem<Req<Request>, Resp<Response>>>),
    Async(Option<BoxedSystem<Req<Request>, BoxedJob<Response>>>),
}

pub struct HeldService<Request, Response, Streams> {
    service: HoldableService<Request, Response>,
    initialized: bool,
    dispatch: fn(&mut World, &mut HoldableService<Request, Response>, Entity),
    _ignore: std::marker::PhantomData<Streams>,
}

struct SharedSentinal {
    running: bool,
    queued: VecDeque<Entity>,
}

#[derive(Clone)]
pub struct SharedService<Request, Response, Streams> {
    sentinal: Arc<Mutex<SharedSentinal>>,
    servable: Arc<Mutex<HeldService<Request, Response, Streams>>>,
}

struct HeldServiceMarker;
struct SharedServiceMarker;

impl<Request, Response, Streams> Servable<HeldServiceMarker> for &mut HeldService<Request, Response, Streams> {
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn serve(self, world: &mut World, target: Entity) {
        (self.dispatch)(world, &mut self.service, target);
    }
}
impl<Request, Response, Streams> private::Sealed<HeldServiceMarker> for &mut HeldService<Request, Response, Streams> { }

impl<Request: 'static, Response: 'static, Streams> Servable<SharedServiceMarker> for SharedService<Request, Response, Streams> {
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn serve(self, world: &mut World, target: Entity) {
        let mut servable = {
            let mut sentinal = match self.sentinal.lock() {
                Ok(sentinal) => sentinal,
                Err(poisoned) => {
                    let mut sentinal = poisoned.into_inner();
                    // If the sentinal was poisoned, let's just reset it to
                    // recover
                    sentinal.running = false;
                    sentinal.queued.clear();
                    sentinal
                }
            };

            sentinal.queued.push_back(target);
            if sentinal.running {
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
                HoldableService::Blocking(Some(service)) => {
                    service.initialize(world);
                }
                HoldableService::Async(Some(service)) => {
                    service.initialize(world);
                }
                _ => {
                    panic!(
                        "When a shared service is locked it MUST contain its \
                        inner service. This is a bug in bevy_services, please \
                        report it to the maintainers."
                    );
                }
            }
            servable.initialized = true;
        }

        while let Some(target) = {
            let mut sentinal = match self.sentinal.lock() {
                Ok(sentinal) => sentinal,
                Err(poisoned) => {
                    let mut sentinal = poisoned.into_inner();
                    sentinal.running = true;
                    // TODO(@mxgrey): What should we really assume the queue to
                    // be here? Clearing it out and immediately exiting seems
                    // like the safest choice, but that could cause invisible
                    // failures that are hard for users to diagnose.
                    sentinal.queued.clear();
                    sentinal
                }
            };

            if let Some(target) = sentinal.queued.pop_front() {
                Some(target)
            } else {
                sentinal.running = false;
                None
            }
        } {
            servable.serve(world, target);
        }
    }
}
impl<Request, Response, Streams> private::Sealed<SharedServiceMarker> for SharedService<Request, Response, Streams> { }


impl<Request, Response, Streams, Task, M, Sys>
Holdable<(Request, Response, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Job<Task>, M>,
    Task: FnOnce(Assistant<Streams>) -> Option<Response> + 'static + Send,
    Streams: IntoStreamOutComponents + 'static,
    Request: 'static,
    Response: 'static,
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
            service: HoldableService::Async(Some(service)),
            initialized: false,
            dispatch: dispatch_held_service::<Request, Response, Streams>,
            _ignore: Default::default(),
        }
    }
}

impl<Request, Response, Streams, Task, M, Sys>
Servable<(Request, Response, Streams, Task, M)> for Sys
where
    Sys: IntoSystem<Req<Request>, Job<Task>, M>,
    Task: FnOnce(Assistant<Streams>) -> Option<Response> + 'static + Send,
    Streams: IntoStreamOutComponents + 'static,
    Request: 'static,
    Response: 'static,
{
    type Request = Request;
    type Response = Response;
    type Streams = Streams;
    fn serve(self, world: &mut World, target: Entity) {
        self.hold().serve(world, target);
    }
}
