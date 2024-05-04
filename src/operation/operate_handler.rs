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
    Operation, SingleTargetStorage, Handler, HandleRequest, PendingHandleRequest,
    Stream, SingleSourceStorage, OperationResult, OrBroken,
    OperationSetup, OperationRequest, ActiveTasksStorage, OperationCleanup,
};

use bevy::prelude::{Entity, Component};

pub(crate) struct OperateHandler<Request, Response, Streams> {
    handler: Handler<Request, Response, Streams>,
    target: Entity,
}

impl<Request, Response, Streams> OperateHandler<Request, Response, Streams> {
    pub(crate) fn new(
        handler: Handler<Request, Response, Streams>,
        target: Entity,
    ) -> Self {
        Self { handler, target }
    }
}

impl<Request, Response, Streams> Operation for OperateHandler<Request, Response, Streams>
where
    Request: 'static + Send + Sync,
    Response: 'static + Send + Sync,
    Streams: Stream,
{
    fn setup(self, OperationSetup { source, world }: OperationSetup) {
        if let Some(mut target_mut) = world.get_entity_mut(self.target) {
            target_mut.insert(SingleSourceStorage(source));
        }
        world.entity_mut(source).insert((
            HandlerStorage { handler: self.handler },
            SingleTargetStorage(self.target),
            ActiveTasksStorage::default(),
        ));
    }

    fn execute(
        OperationRequest { source, world, roster }: OperationRequest
    ) -> OperationResult {
        let mut source_mut = world.get_entity_mut(source).or_broken()?;
        let target = source_mut.get::<SingleTargetStorage>().or_broken()?.0;
        let handler = source_mut.get_mut::<HandlerStorage<Request, Response, Streams>>()
            .or_broken()?.handler.clone();

        let mut handler_impl = {
            let mut inner = handler.inner.lock().or_broken()?;
            match inner.handler.take() {
                Some(handler_impl) => handler_impl,
                None => {
                    // The handler implementation is not available, so queue up
                    // this request.
                    inner.queue.push_back(PendingHandleRequest { source, target });
                    return Ok(());
                }
            }
        };

        // The handler implementation is available, so run it immediately
        // for this operation.
        handler_impl.handle(HandleRequest { source, target, world, roster });

        loop {
            // Empty out the queue in case that was filled up at all from doing the handling
            let mut inner = match handler.inner.lock() {
                Ok(inner) => inner,
                Err(_) => {
                    // TODO(@mxgrey): Is there a better way to handle this?
                    return Ok(());
                }
            };

            if let Some(pending) = inner.queue.pop_front() {
                handler_impl.handle(pending.activate(world, roster));
            } else {
                inner.handler = Some(handler_impl);
                break;
            }
        }

        Ok(())
    }

    fn cleanup(mut clean: OperationCleanup) -> OperationResult {
        clean.cleanup_inputs::<Request>()?;
        ActiveTasksStorage::cleanup(clean)
    }
}

#[derive(Component)]
struct HandlerStorage<Request, Response, Streams> {
    handler: Handler<Request, Response, Streams>,
}
