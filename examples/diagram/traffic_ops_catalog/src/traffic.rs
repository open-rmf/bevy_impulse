/*
 * Copyright (C) 2026 Open Source Robotics Foundation
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

use bevy::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[repr(i32)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrafficSignal {
    Red = 0,
    Yellow = 1,
    Green = 2,
    #[default]
    Empty = 3,
}

#[derive(Clone, Debug, Default, Component)]
pub struct TrafficLight {
    pub id: i32,
    pub last_update: f32,
    pub signal: TrafficSignal,
    pub meshes: Vec<Entity>,
}

impl TrafficLight {
    pub fn new(id: i32, last_update: f32, signal: TrafficSignal, meshes: Vec<Entity>) -> Self {
        Self {
            id,
            last_update,
            signal,
            meshes,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct ApproachingIntersection {
    pub distance: f32,
}

#[derive(Clone, Debug, Component, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpeedLimit(pub f32);

impl Default for SpeedLimit {
    fn default() -> Self {
        Self(50.0)
    }
}

#[derive(Clone, Debug, Default, Component)]
#[require(Transform)]
pub struct Obstacle;

#[derive(Clone, Copy, Debug, Default, Component)]
pub struct Velocity {
    pub translation: Vec2,
}

impl Velocity {
    pub fn zero() -> Self {
        Velocity {
            translation: Vec2::ZERO,
        }
    }

    pub fn default_pedestrian() -> Self {
        // Walking pace in m/s
        Velocity {
            translation: Vec2 { x: 1.5, y: 0.0 },
        }
    }
}

#[derive(Clone, Debug, Component)]
#[require(Velocity)]
pub struct Pedestrian {
    pub alive: bool,
}
impl Default for Pedestrian {
    fn default() -> Self {
        Self { alive: true }
    }
}

impl Pedestrian {
    pub fn is_alive(&self) -> bool {
        self.alive
    }

    pub fn died(&mut self) {
        self.alive = false;
    }

    pub fn revived(&mut self) {
        self.alive = true;
    }
}
