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
    prelude::{World, Commands},
    ecs::system::{CommandQueue, BoxedSystem, IntoSystem, System},
    tasks::{AsyncComputeTaskPool, TaskPool},
};

use crate::{Promise, flush_impulses};

pub struct TestContext {
    pub world: World,
    flusher: BoxedSystem,
}

impl TestContext {
    pub fn new() -> Self {
        AsyncComputeTaskPool::init(|| TaskPool::new());
        let mut world = World::new();
        let mut flusher = Box::new(IntoSystem::into_system(flush_impulses));
        flusher.initialize(&mut world);

        TestContext { world, flusher }
    }

    pub fn build<U>(&mut self, f: impl FnOnce(&mut Commands) -> U) -> U {
        let mut command_queue = CommandQueue::default();
        let mut commands = Commands::new(&mut command_queue, &self.world);
        let u = f(&mut commands);
        command_queue.apply(&mut self.world);
        u
    }

    pub fn flush_while_pending<T>(
        &mut self,
        promise: &mut Promise<T>,
    ) {
        self.flush_with_conditions(promise, FlushConditions::new());
    }

    pub fn flush_with_conditions<T>(
        &mut self,
        promise: &mut Promise<T>,
        conditions: FlushConditions,
    ) -> bool {
        let t_initial = std::time::Instant::now();
        let mut count = 0;
        while promise.peek().is_pending() {
            if let Some(timeout) = conditions.timeout {
                if timeout < std::time::Instant::now() - t_initial {
                    return false;
                }
            }

            if let Some(count_limit) = conditions.flush_count {
                count += 1;
                if count_limit < count {
                    return false;
                }
            }

            self.flusher.run((), &mut self.world);
            self.flusher.apply_deferred(&mut self.world);
        }

        return true;
    }
}

#[derive(Default, Clone)]
pub struct FlushConditions {
    pub timeout: Option<std::time::Duration>,
    pub flush_count: Option<usize>,
}

impl FlushConditions {
    pub fn new() -> Self {
        FlushConditions::default()
    }

    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_flush_count(mut self, count: usize) -> Self {
        self.flush_count = Some(count);
        self
    }
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

pub struct WaitRequest<Value> {
    pub duration: std::time::Duration,
    pub value: Value
}

pub async fn wait<Value>(request: WaitRequest<Value>) -> Value {
    std::thread::sleep(request.duration);
    request.value
}
