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

pub use bevy::{
    prelude::{
        Commands, App, Update, MinimalPlugins, DefaultPlugins, PbrBundle, Vec3,
        In, Entity, Assets, Mesh, ResMut, Transform, Component, Query,
    },
    render::mesh::shape::Cube,
    ecs::system::CommandQueue,
};

pub use std::time::Duration;

use crate::{
    Promise, Service, InAsyncService, InBlockingService, UnhandledErrors,
    Scope, Builder, StreamPack, SpawnWorkflow, WorkflowSettings,
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
            .add_plugins(MinimalPlugins)
            .add_systems(Update, flush_impulses);

        TestingContext { app }
    }

    /// Make a testing context that is headless but has some additional plugins
    /// for use cases that have hierarchy (parents/children), assets, and geometry.
    pub fn headless_plugins() -> Self {
        use bevy::{
            asset::AssetPlugin,
            transform::TransformPlugin,
            hierarchy::HierarchyPlugin,
            render::mesh::MeshPlugin,
        };
        let mut app = App::new();
        app
            .add_plugins(MinimalPlugins)
            .add_plugins((
                AssetPlugin::default(),
                TransformPlugin::default(),
                HierarchyPlugin::default(),
                MeshPlugin,
            ))
            .add_systems(Update, flush_impulses);

        TestingContext { app }
    }

    pub fn build<U>(&mut self, f: impl FnOnce(&mut Commands) -> U) -> U {
        let mut command_queue = CommandQueue::default();
        let mut commands = Commands::new(&mut command_queue, &self.app.world);
        let u = f(&mut commands);
        command_queue.apply(&mut self.app.world);
        u
    }

    /// Build a simple workflow with a single input and output, and no streams
    /// or settings.
    pub fn build_io_workflow<Request, Response>(
        &mut self,
        f: impl FnOnce(Scope<Request, Response, ()>, &mut Builder),
    ) -> Service<Request, Response, ()>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
    {
        self.build(move |commands| {
            commands.spawn_workflow(WorkflowSettings::default(), f)
        })
    }

    /// Build any kind of workflow with any settings.
    pub fn build_workflow<Request, Response, Streams>(
        &mut self,
        settings: WorkflowSettings,
        f: impl FnOnce(Scope<Request, Response, Streams>, &mut Builder),
    ) -> Service<Request, Response, Streams>
    where
        Request: 'static + Send + Sync,
        Response: 'static + Send + Sync,
        Streams: StreamPack,
    {
        self.build(move |commands| {
            commands.spawn_workflow(settings, f)
        })
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
        let conditions = conditions.into();
        let t_initial = std::time::Instant::now();
        let mut count = 0;
        while promise.peek().is_pending() {
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

pub struct SpawnCube {
    pub position: Vec3,
    pub size: f32,
}

/// Spawns a cube according to a [`SpawnCube`] request. Returns the entity of
/// the new cube.
pub fn spawn_cube(
    In(request): In<SpawnCube>,
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
) -> Result<Entity, InvalidValue> {
    if request.size <= 0.0 {
        return Err(InvalidValue(request.size));
    }

    let entity = commands.spawn(PbrBundle {
        mesh: meshes.add(Cube::new(request.size).into()),
        transform: Transform::from_translation(request.position),
        ..Default::default()
    }).id();

    Ok(entity)
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
    let never = future::pending::<()>();
    let _ = future::timeout(request.duration, never);
    request.value
}

/// Use this to add a blocking map to the chain that simply prints a debug
/// message and then passes the data along.
pub fn print_debug<T: std::fmt::Debug>(header: String) -> impl Fn(T) -> T {
    move |value| {
        println!("{header}: {value:?}");
        value
    }
}

/// Use this to add a blocking map to the chain that simply produces an error.
/// Used for testing special operations for the [`Result`] type.
pub fn produce_err<T>(_: T) -> Result<T, ()> {
    Err(())
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
    In(input): InBlockingService<()>,
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
    In(input): InAsyncService<RepeatRequest>,
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
