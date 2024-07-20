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

use bevy::prelude::{Entity, World};

use crate::{OperationRoster, ChannelItem};

use crossbeam::channel::Sender as CbSender;

pub(crate) struct BufferAccessLifecycle {
    scope: Entity,
    session: Entity,
    sender: CbSender<ChannelItem>,
}

impl BufferAccessLifecycle {
    pub(crate) fn new(
        scope: Entity,
        session: Entity,
        sender: CbSender<ChannelItem>,
    ) -> Self {
        Self { scope, session, sender }
    }
}

impl Drop for BufferAccessLifecycle {
    fn drop(&mut self) {
        let scope = self.scope;
        let session = self.session;
        if let Err(err) = self.sender.send(
            Box::new(move |_: &mut World, roster: &mut OperationRoster| {
                roster.disposed(scope, session);
            })
        ) {
            eprintln!(
                "Failed to send disposal notice for dropped buffer key in \
                scope [{:?}] for session [{:?}]: {}",
                scope,
                session,
                err,
            );
        }
    }
}
