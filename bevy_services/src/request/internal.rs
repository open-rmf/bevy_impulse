/*
 * Copyright (C) 2023 Open Source Robotics Foundation
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
    Req, RequestBuilder, DeliveryInstructions, RequestStorage,
    private,
};

use bevy::prelude::{Commands, Entity};

pub trait SubmitRequest<Request>: private::Sealed<()> {
    fn apply<'w, 's>(self, commands: &mut Commands<'w, 's>) -> Entity;
}

impl<Request: Send + Sync + 'static> SubmitRequest<Request> for Req<Request> {
    fn apply<'w, 's>(self, commands: &mut Commands<'w, 's>) -> Entity {
        RequestBuilder::new(self.0).apply(commands)
    }
}
impl<Request> private::Sealed<()> for Req<Request> { }

impl<Request: 'static + Send + Sync, L, Q, E> SubmitRequest<Request> for RequestBuilder<Request, L, Q, E> {
    fn apply<'w, 's>(self, commands: &mut Commands<'w, 's>) -> Entity {
        let target = commands.spawn(RequestStorage(Some(self.request))).id();
        if let Some(label) = self.label {
            commands.entity(target).insert(DeliveryInstructions {
                label,
                queue: self.queue,
                ensure: self.ensure,
            });
        }

        target
    }
}
impl<Request, L, Q, E> private::Sealed<()> for RequestBuilder<Request, L, Q, E> { }
