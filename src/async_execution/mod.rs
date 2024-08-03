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

use bevy_ecs::prelude::World;

use std::future::Future;

#[cfg(feature = "single_threaded_async")]
mod single_threaded_execution;
#[cfg(feature = "single_threaded_async")]
pub(crate) use single_threaded_execution::SingleThreadedExecution;
#[cfg(feature = "single_threaded_async")]
use single_threaded_execution::SingleThreadedExecutionSender;

pub(crate) use bevy_tasks::Task as TaskHandle;

#[cfg(not(feature = "single_threaded_async"))]
pub(crate) type CancelSender = AsyncComputeTaskPoolSender;

#[cfg(feature = "single_threaded_async")]
pub(crate) type CancelSender = SingleThreadedExecutionSender;

pub(crate) fn spawn_task<T>(
    future: impl Future<Output = T> + Sendish + 'static,
    _world: &mut World,
) -> TaskHandle<T>
where
    T: Send + 'static,
{
    #[cfg(not(feature = "single_threaded_async"))]
    {
        use bevy_tasks::AsyncComputeTaskPool;
        AsyncComputeTaskPool::get().spawn(future)
    }

    #[cfg(feature = "single_threaded_async")]
    {
        SingleThreadedExecution::get(_world).spawn(future)
    }
}

pub(crate) fn task_cancel_sender(_world: &mut World) -> CancelSender {

    #[cfg(not(feature = "single_threaded_async"))]
    {
        AsyncComputeTaskPoolSender
    }

    #[cfg(feature = "single_threaded_async")]
    {
        SingleThreadedExecution::get(_world).cancel_sender()
    }
}

#[cfg(not(feature = "single_threaded_async"))]
pub(crate) struct AsyncComputeTaskPoolSender;

#[cfg(not(feature = "single_threaded_async"))]
impl AsyncComputeTaskPoolSender {
    /// This is only used to create a task to cancel an existing task, so we
    /// always detach
    pub(crate) fn send<F>(
        &self,
        f: impl FnOnce() -> F,
    )
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        use bevy_tasks::AsyncComputeTaskPool;
        AsyncComputeTaskPool::get().spawn(f()).detach();
    }
}

/// This trait is used as a trait bound that is equivalent to [`Send`] except
/// when the `single_threaded_async` feature is active, in which case it is no
/// longer a bound at all because the single threaded async executor does not
/// require tasks to be [`Send`].
#[cfg(not(feature = "single_threaded_async"))]
pub trait Sendish: Send {}

#[cfg(not(feature = "single_threaded_async"))]
impl<T: Send> Sendish for T {}

/// This trait is used as a trait bound that is equivalent to [`Send`] except
/// when the `single_threaded_async` feature is active, in which case it is no
/// longer a bound at all because the single threaded async executor does not
/// require tasks to be [`Send`].
#[cfg(feature = "single_threaded_async")]
pub trait Sendish {}

#[cfg(feature = "single_threaded_async")]
impl<T> Sendish for T {}
