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

pub use bevy_ecs::{
    prelude::{Commands, In, Entity, ResMut, Component, Query, Local},
    system::{CommandQueue, IntoSystem},
};
pub use bevy_app::{App, Update};
use bevy_app::ScheduleRunnerPlugin;
use bevy_core::{TaskPoolPlugin, TypeRegistrationPlugin, FrameCountPlugin};
use bevy_time::TimePlugin;

use thiserror::Error as ThisError;

pub use std::time::{Duration, Instant};

use smallvec::SmallVec;

use crate::{
    Promise, Service, AsyncServiceInput, BlockingServiceInput, UnhandledErrors,
    Scope, Builder, StreamPack, SpawnWorkflow, WorkflowSettings, BlockingMap,
    GetBufferedSessionsFn, ContinuousService, ContinuousQuery,
    AddContinuousServicesExt, ContinuousQueueView, RunCommandsOnWorldExt,
    flush_impulses,
};

pub struct TestingContext {
    pub app: App,
}

impl TestingContext {
    /// Make a testing context with the minimum plugins needed for bevy_impulse
    /// to work properly.
    pub fn minimal_plugins() -> Self {
        let mut app = App::new();
        app
            .add_plugins((
                TaskPoolPlugin::default(),
                TypeRegistrationPlugin::default(),
                FrameCountPlugin::default(),
                TimePlugin::default(),
                ScheduleRunnerPlugin::default(),
            ))
            .add_systems(Update, flush_impulses());

        TestingContext { app }
    }

    pub fn command<U>(&mut self, f: impl FnOnce(&mut Commands) -> U) -> U {
        self.app.world.command(f)
    }

    /// Build a simple workflow with a single input and output, and no streams
    /// or settings.
    pub fn spawn_io_workflow<Request, Response, Settings>(
        &mut self,
        f: impl FnOnce(Scope<Request, Response, ()>, &mut Builder) -> Settings,
    ) -> Service<Request, Response, ()>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Settings: Into<WorkflowSettings>,
    {
        self.command(move |commands| {
            commands.spawn_workflow(f)
        })
    }

    /// Build any kind of workflow with any settings.
    pub fn spawn_workflow<Request, Response, Streams, Settings>(
        &mut self,
        f: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder) -> Settings,
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
        Settings: Into<WorkflowSettings>,
    {
        self.command(move |commands| {
            commands.spawn_workflow(f)
        })
    }

    pub fn run(&mut self, conditions: impl Into<FlushConditions>) {
        self.run_impl::<()>(None, conditions);
    }

    pub fn run_while_pending<T>(
        &mut self,
        promise: &mut Promise<T>,
    ) {
        self.run_with_conditions(promise, FlushConditions::new());
    }

    pub fn run_with_conditions<T>(
        &mut self,
        promise: &mut Promise<T>,
        conditions: impl Into<FlushConditions>,
    ) -> bool {
        self.run_impl(Some(promise), conditions)
    }

    fn run_impl<T>(
        &mut self,
        mut promise: Option<&mut Promise<T>>,
        conditions: impl Into<FlushConditions>,
    ) -> bool {
        let conditions = conditions.into();
        let t_initial = std::time::Instant::now();
        let mut count = 0;
        while !promise.as_mut().is_some_and(|p| !p.peek().is_pending()) {
            if let Some(timeout) = conditions.timeout {
                let elapsed = std::time::Instant::now() - t_initial;
                if timeout < elapsed {
                    println!("Exceeded timeout of {timeout:?}: {elapsed:?}");
                    return false;
                }
            }

            if let Some(count_limit) = conditions.update_count {
                count += 1;
                if count_limit < count {
                    println!("Exceeded count limit of {count_limit}: {count}");
                    return false;
                }
            }

            self.app.update();
        }

        return true;
    }

    pub fn no_unhandled_errors(&self) -> bool {
        let Some(errors) = self.app.world.get_resource::<UnhandledErrors>() else {
            return true;
        };

        errors.is_empty()
    }

    pub fn get_unhandled_errors(&self) -> Option<&UnhandledErrors> {
        self.app.world.get_resource::<UnhandledErrors>()
    }

    // Check that all buffers in the world are empty
    pub fn confirm_buffers_empty(&mut self) -> Result<(), Vec<Entity>> {
        let mut query = self.app.world.query::<(Entity, &GetBufferedSessionsFn)>();
        let buffers: Vec<_> = query.iter(&self.app.world)
            .map(|(e, get_sessions)| (e, get_sessions.0))
            .collect();

        let mut non_empty_buffers = Vec::new();
        for (e, get_sessions) in buffers {
            if !get_sessions(e, &self.app.world).is_ok_and(|s| s.is_empty()) {
                non_empty_buffers.push(e);
            }
        }

        if non_empty_buffers.is_empty() {
            Ok(())
        } else {
            Err(non_empty_buffers)
        }
    }

    /// Create a service that passes along its inputs after a delay.
    pub fn spawn_delay<T>(&mut self, duration: Duration) -> Service<T, T>
    where
        T: Clone + 'static + Send + Sync,
    {
        self.spawn_delayed_map(duration, |t: &T| t.clone())
    }

    /// Create a service that applies a map to an input after a delay.
    pub fn spawn_delayed_map<T, U, F>(
        &mut self,
        duration: Duration,
        f: F,
    ) -> Service<T, U>
    where
        T: 'static + Send + Sync,
        U: 'static + Send + Sync,
        F: FnMut(&T) -> U + 'static + Send + Sync,
    {
        self.spawn_delayed_map_with_viewer(duration, f, |_| {})
    }

    /// Create a service that applies a map to an input after a delay and allows
    /// you to view the current set of requests.
    pub fn spawn_delayed_map_with_viewer<T, U, F, V>(
        &mut self,
        duration: Duration,
        mut f: F,
        mut viewer: V,
    ) -> Service<T, U>
    where
        T: 'static + Send + Sync,
        U: 'static + Send + Sync,
        F: FnMut(&T) -> U + 'static + Send + Sync,
        V: FnMut(&ContinuousQueueView<T, U>) + 'static + Send + Sync,
    {
        self.app.spawn_continuous_service(
            Update,
            move |In(input): In<ContinuousService<T, U>>, mut query: ContinuousQuery<T, U>, mut t: Local<Option<Instant>>| {
                if let Some(view) = query.view(&input.key) {
                    viewer(&view);
                }

                if let Some(order) = query.get_mut(&input.key).unwrap().get_mut(0) {
                    if let Some(t0) = *t {
                        if t0.elapsed() > duration {
                            let u = f(order.request());
                            order.respond(u);
                            *t = None;
                        }
                    } else {
                        *t = Some(Instant::now());
                    }
                }
            }
        )
    }

    #[cfg(test)]
    pub fn spawn_async_delayed_map<T, U, F>(
        &mut self,
        duration: Duration,
        f: F,
    ) -> Service<T, U>
    where
        T: 'static + Send + Sync,
        U: 'static + Send + Sync,
        F: FnOnce(T) -> U + 'static + Send + Sync + Clone,
    {
        use crate::AddServicesExt;
        self.app.spawn_service(
            move |In(input): AsyncServiceInput<T>| {
                let f = f.clone();
                async move {
                    let start = Instant::now();
                    let mut elapsed = start.elapsed();
                    while elapsed < duration {
                        let never = async_std::future::pending::<()>();
                        let timeout = duration - elapsed;
                        let _ = async_std::future::timeout(timeout, never).await;
                        elapsed = start.elapsed();
                    }
                    f(input.request)
                }
            }
        )
    }
}

#[derive(Default, Clone)]
pub struct FlushConditions {
    pub timeout: Option<std::time::Duration>,
    pub update_count: Option<usize>,
}

impl From<Duration> for FlushConditions {
    fn from(value: Duration) -> Self {
        Self::new().with_timeout(value)
    }
}

impl From<usize> for FlushConditions {
    fn from(value: usize) -> Self {
        Self::new().with_update_count(value)
    }
}

impl FlushConditions {
    pub fn new() -> Self {
        FlushConditions::default()
    }

    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_update_count(mut self, count: usize) -> Self {
        self.update_count = Some(count);
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub struct InvalidValue(pub f32);

pub fn spawn_test_entities(
    In(input): BlockingServiceInput<usize>,
    mut commands: Commands,
) -> SmallVec<[Entity; 8]> {
    let mut entities = SmallVec::new();
    for _ in 0..input.request {
        entities.push(commands.spawn(TestComponent).id());
    }

    entities
}

pub fn duplicate<T: Clone>(value: T) -> (T, T) {
    (value.clone(), value)
}

pub fn double(value: f64) -> f64 {
    2.0*value
}

pub fn opposite(value: f64) -> f64 {
    -value
}

pub fn add((a, b): (f64, f64)) -> f64 {
    a + b
}

pub fn sum<Values: IntoIterator<Item = f64>>(values: Values) -> f64 {
    values.into_iter().fold(0.0, |a, b| a + b)
}

pub fn repeat_string((times, value): (usize, String)) -> String {
    value.repeat(times)
}

pub fn concat<Values: IntoIterator<Item = String>>(values: Values) -> String {
    values.into_iter().fold(String::new(), |b, s| b + &s)
}

pub fn string_from_utf8<Values: IntoIterator<Item = u8>>(
    values: Values
) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(values.into_iter().collect())
}

pub fn to_uppercase(value: String) -> String {
    value.to_uppercase()
}

pub fn to_lowercase(value: String) -> String {
    value.to_lowercase()
}

#[derive(Clone, Copy, Debug)]
pub struct WaitRequest<Value> {
    pub duration: std::time::Duration,
    pub value: Value
}

/// This function is used to force certain branches to lose races in tests or
/// validate async execution with delays.
#[cfg(test)]
pub async fn wait<Value>(request: WaitRequest<Value>) -> Value {
    use async_std::future;
    let start = Instant::now();
    let mut elapsed = start.elapsed();
    while elapsed < request.duration {
        let never = future::pending::<()>();
        let timeout = request.duration - elapsed;
        let _ = future::timeout(timeout, never).await;
        elapsed = start.elapsed();
    }
    request.value
}

/// Use this to add a blocking map to the chain that simply prints a debug
/// message and then passes the data along.
pub fn print_debug<T: std::fmt::Debug>(
    header: impl Into<String>,
) -> impl Fn(BlockingMap<T>) -> T {
    let header = header.into();
    move |input| {
        println!(
            "[source: {:?}, session: {:?}] {}: {:?}",
            input.source,
            input.session,
            header,
            input.request,
        );
        input.request
    }
}

#[derive(ThisError, Debug)]
#[error("This error is for testing purposes only")]
pub struct TestError;

/// Use this to create a blocking map that simply produces an error.
/// Used for testing special operations for the [`Result`] type.
pub fn produce_err<T>(_: T) -> Result<T, TestError> {
    Err(TestError)
}

/// Use this to create a blocking map that simply produces [`None`].
/// Used for testing special operations for the [`Option`] type.
pub fn produce_none<T>(_: T) -> Option<T> {
    None
}

pub struct RepeatRequest {
    pub service: Service<(), (), ()>,
    pub count: usize,
}

#[derive(Component)]
pub struct Salutation(pub Box<str>);

#[derive(Component)]
pub struct Name(pub Box<str>);

#[derive(Component)]
pub struct RunCount(pub usize);

pub fn say_hello(
    In(input): BlockingServiceInput<()>,
    salutation_query: Query<Option<&Salutation>>,
    name_query: Query<Option<&Name>>,
    mut run_count: Query<Option<&mut RunCount>>,
) {
    let salutation = salutation_query.get(input.provider)
        .ok()
        .flatten()
        .map(|x| &*x.0)
        .unwrap_or("Hello, ");

    let name = name_query.get(input.provider)
        .ok()
        .flatten()
        .map(|x| &*x.0)
        .unwrap_or("world");

    println!("{salutation}{name}");

    if let Ok(Some(mut count)) = run_count.get_mut(input.provider) {
        count.0 += 1;
    }
}

pub fn repeat_service(
    In(input): AsyncServiceInput<RepeatRequest>,
    mut run_count: Query<Option<&mut RunCount>>,
) -> impl std::future::Future<Output=()> + 'static + Send + Sync {
    if let Ok(Some(mut count)) = run_count.get_mut(input.provider) {
        count.0 += 1;
    }

    async move {
        for _ in 0..input.request.count {
            input.channel.query((), input.request.service).await;
        }
    }
}

#[derive(Component)]
pub struct TestComponent;
