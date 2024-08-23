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

use crate::{
    try_emit_broken, ActiveTasksStorage, Callback, CallbackRequest, InputBundle, Operation,
    OperationCleanup, OperationError, OperationReachability, OperationRequest, OperationResult,
    OperationSetup, OrBroken, PendingCallbackRequest, ReachabilityResult, SingleInputStorage,
    SingleTargetStorage, StreamPack,
};

use bevy_ecs::prelude::{Component, Entity};

pub(crate) struct OperateCallback<Request, Response, Streams> {
    callback: Callback<Request, Response, Streams>,
    target: Entity,
}

impl<Request, Response, Streams> OperateCallback<Request, Response, Streams> {
    pub(crate) fn new(callback: Callback<Request, Response, Streams>, target: Entity) -> Self {
        Self { callback, target }
    }
}

impl<Request, Response, Streams> Operation for OperateCallback<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: StreamPack,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) -> OperationResult {
        world
            .get_entity_mut(self.target)
            .or_broken()?
            .insert(SingleInputStorage::new(source));

        world.entity_mut(source).insert((
            InputBundle::<Request>::new(),
            CallbackStorage {
                callback: self.callback,
            },
            SingleTargetStorage::new(self.target),
            ActiveTasksStorage::default(),
        ));
        Ok(())
    }

    fn execute(
        OperationRequest {
            source,
            world,
            roster,
        }: OperationRequest,
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let callback = source_mut
            .get_mut::<CallbackStorage<Request, Response, Streams>>()
            .or_broken()?
            .callback
            .clone();

        let mut callback_impl = {
            let mut inner = callback.inner.lock().or_broken()?;
            match inner.callback.take() {
                Some(callback_impl) => callback_impl,
                None => {
                    // The callback implementation is not available, so queue up
                    // this request.
                    inner
                        .queue
                        .push_back(PendingCallbackRequest { source, target });
                    return Ok(());
                }
            }
        };

        // The callback implementation is available, so run it immediately
        // for this operation.
        let r = callback_impl.call(CallbackRequest {
            source,
            target,
            world,
            roster,
        });

        loop {
            // Empty out the queue in case that was filled up at all from doing the handling
            let mut inner = callback.inner.lock().or_broken()?;
            if let Some(pending) = inner.queue.pop_front() {
                let source = pending.source;
                if let Err(OperationError::Broken(backtrace)) =
                    callback_impl.call(pending.activate(world, roster))
                {
                    try_emit_broken(source, backtrace, world, roster);
                }
            } else {
                inner.callback = Some(callback_impl);
                break;
            }
        }

        r
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<Request>()?;
        clean.cleanup_disposals()?;
        if !ActiveTasksStorage::cleanup(&mut clean)? {
            // We need to wait for some async tasks to be cleared out
            return Ok(());
        }
        clean.notify_cleaned()
    }

    fn is_reachable(mut reachability: OperationReachability) -> ReachabilityResult {
        if reachability.has_input::<Request>()? {
            return Ok(true);
        }
        if ActiveTasksStorage::contains_session(&reachability)? {
            return Ok(true);
        }
        SingleInputStorage::is_reachable(&mut reachability)
    }
}

#[derive(Component)]
struct CallbackStorage<Request, Response, Streams> {
    callback: Callback<Request, Response, Streams>,
}
