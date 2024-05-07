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

use bevy::prelude::{Entity, Component};

use smallvec::SmallVec;

use std::collections::HashMap;

/// The scope that the workflow node exists inside of.
#[derive(Component)]
pub struct ScopeStorage(Entity);

impl ScopeStorage {
    pub fn get(&self) -> Entity {
        self.0
    }
}

/// The contents inside a scope entity.
#[derive(Default, Component)]
pub struct ScopeContents {
    nodes: SmallVec<[Entity; 16]>,
    cleanup: HashMap<Entity, SmallVec<[Entity; 16]>>,
}

impl ScopeContents {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: Entity) {
        if let Err(index) = self.nodes.binary_search(&node) {
            self.nodes.insert(index, node);
        }
    }

    pub fn register_cleanup_of_node(&mut self, session: Entity, node: Entity) -> bool {
        let mut cleanup = self.cleanup.entry(session).or_default();
        if let Err(index) = cleanup.binary_search(&node) {
            cleanup.insert(index, node);
        }

        self.nodes == *cleanup
    }

    pub fn nodes(&self) -> &SmallVec<[Entity; 16]> {
        &self.nodes
    }
}
