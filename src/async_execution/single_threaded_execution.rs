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

pub(crate) use bevy_tasks::Task as TaskHandle;
use async_task::Runnable;
use crossbeam::channel::{unbounded, Sender, Receiver};

use std::{future::Future, pin::Pin};

type CancellingTask = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

pub(crate) struct SingleThreadedExecution {
    cancel_sender: Sender<CancellingTask>,
    cancel_receiver: Receiver<CancellingTask>,
    runnable_sender: Sender<Runnable>,
    runnable_receiver: Receiver<Runnable>,
}

impl SingleThreadedExecution {
    fn new() -> Self {
        let (cancel_sender, cancel_receiver) = unbounded();
        let (runnable_sender, runnable_receiver) = unbounded();
        Self { cancel_sender, cancel_receiver, runnable_sender, runnable_receiver }
    }

    pub(crate) fn get(world: &mut World) -> &SingleThreadedExecution {
        if !world.contains_non_send::<SingleThreadedExecution>() {
            world.insert_non_send_resource(SingleThreadedExecution::new());
        }
        world.non_send_resource::<SingleThreadedExecution>()
    }

    pub(crate) fn poll(&self, limit: Option<usize>) {
        let mut count = 0;
        while let Ok(f) = self.cancel_receiver.try_recv() {
            let sender = self.runnable_sender.clone();
            let future = f();
            let (runnable, task) = async_task::spawn_local(
                future, move |runnable| { sender.send(runnable).ok(); });
            runnable.run();
            task.detach();

            count += 1;
            if let Some(limit) = limit {
                if count >= limit {
                    break;
                }
            }
        }

        count = 0;
        while let Ok(runnable) = self.runnable_receiver.try_recv() {
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
        &self,
        future: impl Future<Output = T> + 'static,
    ) -> TaskHandle<T>
    where
        T: Send + 'static,
    {
        let sender = self.runnable_sender.clone();
        let (runnable, task) = async_task::spawn_local(
            future, move |runnable| { sender.send(runnable).ok(); },
        );
        let _ = self.runnable_sender.send(runnable);
        TaskHandle::new(task)
    }

    pub(crate) fn cancel_sender(&self) -> SingleThreadedExecutionSender {
        let sender = self.cancel_sender.clone();
        SingleThreadedExecutionSender { sender }
    }
}

#[derive(Clone)]
pub(crate) struct SingleThreadedExecutionSender {
    sender: Sender<CancellingTask>,
}

impl SingleThreadedExecutionSender {
    pub(crate) fn send<F>(
        &self,
        f: impl FnOnce() -> F + Send + 'static,
    )
    where
        F: Future<Output = ()> + Send + 'static
    {
        let u: CancellingTask = Box::new(move || Box::pin(f()));
        self.sender.send(u).ok();
    }
}
