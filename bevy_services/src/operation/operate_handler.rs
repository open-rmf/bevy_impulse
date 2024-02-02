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
    Operation, TargetStorage, Handler, HandleRequest, PendingHandleRequest,
    OperationStatus, Stream,
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
    fn set_parameters(
        self,
        entity: Entity,
        world: &mut bevy::prelude::World,
    ) {
        world.entity_mut(entity).insert((
            HandlerStorage { handler: self.handler },
            TargetStorage(self.target),
        ));
    }

    fn execute(
        source: Entity,
        world: &mut bevy::prelude::World,
        roster: &mut crate::OperationRoster,
    ) -> Result<OperationStatus, ()> {
        let mut source_mut = world.get_entity_mut(source).ok_or(())?;
        let target = source_mut.get::<TargetStorage>().ok_or(())?.0;
        let handler = source_mut.get_mut::<HandlerStorage<Request, Response, Streams>>().ok_or(())?.handler.clone();

        let mut handler_impl = {
            let mut inner = handler.inner.lock().map_err(|_| ())?;
            match inner.handler.take() {
                Some(handler_impl) => handler_impl,
                None => {
                    // The handler implementation is not available, so queue up
                    // this request.
                    inner.queue.push_back(PendingHandleRequest { source, target });
                    return Ok(OperationStatus::Queued{ provider: source })
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
                    return Ok(OperationStatus::Finished);
                }
            };

            if let Some(pending) = inner.queue.pop_front() {
                handler_impl.handle(pending.activate(world, roster));
            } else {
                inner.handler = Some(handler_impl);
                break;
            }
        }

        Ok(OperationStatus::Finished)
    }
}

#[derive(Component)]
struct HandlerStorage<Request, Response, Streams> {
    handler: Handler<Request, Response, Streams>,
}
