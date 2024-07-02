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

use bevy::{
    prelude::{Entity, Component, World},
    ecs::system::Command,
};

use anyhow::anyhow;

use backtrace::Backtrace;

use crate::{MiscellaneousFailure, UnusedTarget, UnhandledErrors};

#[derive(Component)]
pub(crate) struct Detached(bool);

impl Default for Detached {
    fn default() -> Self {
        Detached(false)
    }
}

pub(crate) struct Detach {
    pub(crate) session: Entity,
}

impl Command for Detach {
    fn apply(self, world: &mut World) {
        let backtrace;
        if let Some(mut session_mut) = world.get_entity_mut(self.session) {
            if let Some(mut detached) = session_mut.get_mut::<Detached>() {
                detached.0 = true;
                session_mut.remove::<UnusedTarget>();
                return;
            } else {
                // The session is missing the target properties that it's
                // supposed to have
                backtrace = Backtrace::new();
            }
        } else {
            // The session has despawned before we could manage to use it, or it
            // never existed in the first place.
            backtrace = Backtrace::new();
        }

        let failure = MiscellaneousFailure {
            error: anyhow!("Unable to detach target {:?}", self.session),
            backtrace: Some(backtrace),
        };
        world.get_resource_or_insert_with(|| UnhandledErrors::default())
            .miscellaneous
            .push(failure);
    }
}
