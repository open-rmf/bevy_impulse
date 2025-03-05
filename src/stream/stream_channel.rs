/*
 * Copyright (C) 2025 Open Source Robotics Foundation
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

use bevy_ecs::prelude::{Entity, World};

use std::sync::Arc;

use crate::{InnerChannel, OperationRoster, ReportUnhandled, StreamEffect, StreamRequest};

/// Use this channel to stream data using the [`StreamChannel::send`] method.
pub struct StreamChannel<S> {
    target: Option<Entity>,
    inner: Arc<InnerChannel>,
    _ignore: std::marker::PhantomData<fn(S)>,
}

impl<S: StreamEffect> StreamChannel<S> {
    /// Send an instance of data out over a stream.
    pub fn send(&self, data: S::Input) {
        let source = self.inner.source;
        let session = self.inner.session;
        let target = self.target;
        self.inner
            .sender
            .send(Box::new(
                move |world: &mut World, roster: &mut OperationRoster| {
                    let mut request = StreamRequest {
                        source,
                        session,
                        target,
                        world,
                        roster,
                    };

                    S::side_effect(data, &mut request)
                        .and_then(|output| request.send_output(output))
                        .report_unhandled(source, world);
                },
            ))
            .ok();
    }

    pub(crate) fn new(target: Option<Entity>, inner: Arc<InnerChannel>) -> Self {
        Self {
            target,
            inner,
            _ignore: Default::default(),
        }
    }
}
