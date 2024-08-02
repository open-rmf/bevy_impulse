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

pub(crate) use bevy_tasks::Task as TaskHandle;
use async_task::Runnable;
use crossbeam::channel::{unbounded, Sender, Receiver};

use std::{future::Future, sync::OnceLock};

pub(crate) struct SingleThreadedExecution {
    pub(crate) sender: Sender<Runnable>,
    pub(crate) receiver: Receiver<Runnable>,
}

impl SingleThreadedExecution {
    fn new() -> Self {
        let (sender, receiver) = unbounded();
        Self { sender, receiver }
    }

    pub(crate) fn get() -> &'static SingleThreadedExecution {
        static EXECUTION: OnceLock<SingleThreadedExecution> = OnceLock::new();
        EXECUTION.get_or_init(|| Self::new())
    }

    pub(crate) fn poll(limit: Option<usize>) {
        let execution = Self::get();
        let mut count = 0;
        while let Ok(runnable) = execution.receiver.try_recv() {
            runnable.run();

            count += 1;
            if let Some(limit) = limit {
                if count >= limit {
                    break;
                }
            }
        }
    }

    pub(crate) fn spawn<T>(
        future: impl Future<Output = T> + Send + 'static,
    ) -> TaskHandle<T>
    where
        T: Send + 'static,
    {
        let execution = SingleThreadedExecution::get();
        let sender = execution.sender.clone();
        let (runnable, task) = async_task::spawn(
            future, move |runnable| { sender.send(runnable).ok(); },
        );
        let _ = execution.sender.send(runnable);
        TaskHandle::new(task)
    }
}
