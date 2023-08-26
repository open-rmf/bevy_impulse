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
    Assistant, GenericAssistant,
    stream::*,
};

use bevy::{
    prelude::{Component, In, App},
    ecs::system::{
        BoxedSystem, IntoSystem, EntityCommands, Commands
    },
};

/// A service is a type of system that takes in a request and produces a
/// response, optionally emitting events from its streams using the provided
/// assistant.
#[derive(Component)]
pub(crate) enum Service<Request, Response> {
    /// The service takes in the request and blocks execution until the response
    /// is produced.
    Blocking(BoxedSystem<Request, Response>),
    /// The service runs asynchronously in the bevy thread pool
    Async(BoxedSystem<Request, Box<dyn FnOnce(GenericAssistant) -> Response>>),
}

pub trait SpawnServicesExt<'w, 's> {
    fn spawn_async_service<'a, Request, Response, Streams, M, Sys, Task>(
        &'a mut self,
        service: Sys
    ) -> EntityCommands<'w, 's, 'a>
    where
        Sys: IntoSystem<Request, Task, M>,
        Task: FnOnce(Assistant<Streams>) -> Response,
        Streams: IntoStreamOutComponents,
        Request: 'static,
        Response: 'static,
        Streams: 'static,
        Task: 'static;

    fn spawn_blocking_service<'a, Request, Response, M, Sys>(
        &'a mut self,
        service: Sys
    ) -> EntityCommands<'w, 's, 'a>
    where
        Sys: IntoSystem<Request, Response, M>,
        Request: 'static,
        Response: 'static;
}

impl<'w, 's> SpawnServicesExt<'w, 's> for Commands<'w, 's> {
    fn spawn_async_service<'a, Request, Response, Streams, M, Sys, Task>(
        &'a mut self,
        service: Sys
    ) -> EntityCommands<'w, 's, 'a>
    where
        Sys: IntoSystem<Request, Task, M>,
        Task: FnOnce(Assistant<Streams>) -> Response + Sized,
        Streams: IntoStreamOutComponents,
        Request: 'static,
        Response: 'static,
        Streams: 'static,
        Task: 'static,
    {
        let mut cmds = self.spawn(Service::Async(
            Box::new(IntoSystem::into_system(
                service.pipe(
                    |In(task): In<Task>| {
                        let task: Box<dyn FnOnce(GenericAssistant) -> Response> = Box::new(
                            move |assistant: GenericAssistant| {
                                task(assistant.into_specific())
                            }
                        );
                        task
                    }
                )
            ))
        ));

        Streams::into_stream_out_components(&mut cmds);

        cmds
    }

    fn spawn_blocking_service<'a, Request, Response, M, Sys>(
        &'a mut self,
        service: Sys
    ) -> EntityCommands<'w, 's, 'a>
    where
        Sys: IntoSystem<Request, Response, M>,
        Request: 'static,
        Response: 'static,
    {
        self.spawn(Service::Blocking(Box::new(IntoSystem::into_system(service))))
    }
}

pub trait AddServicesExt {
    /// Call this on an App to create an async service that is available
    /// immediately.
    fn add_async_service<Request, Response, Streams, M, Sys, Task>(
        &mut self,
        service: Sys,
    ) -> &mut Self
    where
        Sys: IntoSystem<Request, Task, M>,
        Task: FnOnce(Assistant<Streams>) -> Response,
        Streams: IntoStreamOutComponents,
        Request: 'static,
        Response: 'static,
        Streams: 'static,
        Task: 'static;


    fn add_blocking_service<'a, Request, Response, M, Sys>(
        &mut self,
        service: Sys,
    ) -> &mut Self
    where
        Sys: IntoSystem<Request, Response, M>,
        Request: 'static,
        Response: 'static;
}

impl AddServicesExt for App {
    fn add_async_service<Request, Response, Streams, M, Sys, Task>(
        &mut self,
        service: Sys,
    ) -> &mut Self
    where
        Sys: IntoSystem<Request, Task, M>,
        Task: FnOnce(Assistant<Streams>) -> Response,
        Streams: IntoStreamOutComponents,
        Request: 'static,
        Response: 'static,
        Streams: 'static,
        Task: 'static,
    {
        self.world.spawn(Service::Async(
            Box::new(IntoSystem::into_system(
                service.pipe(
                    |In(task): In<Task>| {
                        let task: Box<dyn FnOnce(GenericAssistant) -> Response> = Box::new(
                            move |assistant: GenericAssistant| {
                                task(assistant.into_specific())
                            }
                        );
                        task
                    }
                )
            ))
        ));
        self
    }

    fn add_blocking_service<'a, Request, Response, M, Sys>(
        &mut self,
        service: Sys,
    ) -> &mut Self
    where
        Sys: IntoSystem<Request, Response, M>,
        Request: 'static,
        Response: 'static,
    {
        self.world.spawn(Service::Blocking(Box::new(IntoSystem::into_system(service))));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy::prelude::*;

    #[derive(Component)]
    struct TestPeople {
        name: String,
        age: u64,
    }

    #[derive(Resource)]
    struct TestSystemRan(bool);

    #[test]
    fn test_spawn_async_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_systems(Startup, sys_spawn_async_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_spawn_blocking_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_systems(Startup, sys_spawn_blocking_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_async_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_async_service(sys_async_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    #[test]
    fn test_add_blocking_service() {
        let mut app = App::new();
        app
            .insert_resource(TestSystemRan(false))
            .add_blocking_service(sys_blocking_service)
            .add_systems(Update, sys_find_service);

        app.update();
        assert!(app.world.resource::<TestSystemRan>().0);
    }

    fn sys_async_service(
        In(name): In<String>,
        people: Query<&TestPeople>,
    ) -> impl FnOnce(Assistant<()>) -> u64 {
        let mut matching_people = Vec::new();
        for person in &people {
            if person.name == name {
                matching_people.push(person.age);
            }
        }

        move |_: Assistant<()>| -> u64 {
            matching_people.into_iter().fold(0, |sum, age| sum + age)
        }
    }

    fn sys_spawn_async_service(
        mut commands: Commands,
    ) {
        commands.spawn_async_service(sys_async_service);
    }

    fn sys_blocking_service(
        In(name): In<String>,
        people: Query<&TestPeople>,
    ) -> u64 {
        let mut sum = 0;
        for person in &people {
            if person.name == name {
                sum += person.age;
            }
        }
        sum
    }

    fn sys_spawn_blocking_service(
        mut commands: Commands,
    ) {
        commands.spawn_blocking_service(sys_blocking_service);
    }

    fn sys_find_service(
        query: Query<&Service<String, u64>>,
        mut ran: ResMut<TestSystemRan>,
    ) {
        assert!(!query.is_empty());
        ran.0 = true;
    }
}
