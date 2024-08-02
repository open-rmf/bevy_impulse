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

use std::future::Future;

#[cfg(feature = "single_threaded_async")]
mod single_threaded_execution;
#[cfg(feature = "single_threaded_async")]
pub(crate) use single_threaded_execution::SingleThreadedExecution;

pub(crate) use bevy_tasks::Task as TaskHandle;

pub(crate) fn spawn_task<T>(
    future: impl Future<Output = T> + Send + 'static,
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
        SingleThreadedExecution::spawn(future)
    }
}
