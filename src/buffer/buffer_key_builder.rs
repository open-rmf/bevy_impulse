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

use bevy_ecs::prelude::Entity;

use std::sync::Arc;

use crate::{BufferAccessLifecycle, BufferKey, ChannelSender};

pub struct BufferKeyBuilder {
    scope: Entity,
    session: Entity,
    accessor: Entity,
    lifecycle: Option<(ChannelSender, Arc<()>)>,
}

impl BufferKeyBuilder {
    pub(crate) fn build<T>(&self, buffer: Entity) -> BufferKey<T> {
        BufferKey {
            buffer: buffer,
            session: self.session,
            accessor: self.accessor,
            lifecycle: self.lifecycle.as_ref().map(|(sender, tracker)| {
                Arc::new(BufferAccessLifecycle::new(
                    self.scope,
                    buffer,
                    self.session,
                    self.accessor,
                    sender.clone(),
                    tracker.clone(),
                ))
            }),
            _ignore: Default::default(),
        }
    }

    pub(crate) fn with_tracking(
        scope: Entity,
        session: Entity,
        accessor: Entity,
        sender: ChannelSender,
        tracker: Arc<()>,
    ) -> Self {
        Self {
            scope,
            session,
            accessor,
            lifecycle: Some((sender, tracker)),
        }
    }

    pub(crate) fn without_tracking(scope: Entity, session: Entity, accessor: Entity) -> Self {
        Self {
            scope,
            session,
            accessor,
            lifecycle: None,
        }
    }
}
