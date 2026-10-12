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
use core::f32;
use glam::Vec2;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::spawn_world::METERS_PER_SECOND_TO_KMH;

pub const VEHICLE_LAYER_Z: f32 = 10.0;

/// Widest angle the front wheels can be turned to, in degrees.
pub const MAX_WHEEL_ANGLE_DEG: f32 = 45.0;
/// How quickly the front wheels turn toward their target angle by default,
/// in degrees per second.
pub const DEFAULT_MAX_STEER_SPEED_DEG_PER_S: f32 = 30.0;
/// Default acceleration limit, equivalent to 2.0 m/s^2 expressed in km/h
/// per second to match the unit of [`VehicleDynamics::speed`].
pub const DEFAULT_MAX_ACCELERATION_KMH_PER_S: f32 = 2.0 * METERS_PER_SECOND_TO_KMH;

#[derive(
    Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq, PartialOrd, Component,
)]
pub struct ThrottleCommand {
    /// Target speed in km/h.
    pub target_speed: f32,
    /// Acceleration limit in km/h per second.
    #[serde(default)]
    pub max_acceleration: Option<f32>,
}

#[derive(
    Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq, PartialOrd, Component,
)]
pub struct SteeringCommand {
    /// Target angle for the front wheels in degrees. Positive angles steer
    /// to the left.
    pub target_turn_angle: f32,
    /// How quickly the front wheels turn toward the target angle, in degrees
    /// per second.
    #[serde(default)]
    pub max_steer_speed: Option<f32>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub enum Lane {
    #[default]
    Left,
    Right,
}

impl Lane {
    pub fn inverse(&self) -> Lane {
        match self {
            Lane::Left => Lane::Right,
            Lane::Right => Lane::Left,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Component)]
pub struct VehicleDynamics {
    /// Current speed in km/h. This is the user-facing unit; convert to m/s
    /// before integrating positions.
    pub speed: f32,
    /// Current angle of the front wheels in degrees.
    pub wheel_rotation: f32,
}

impl Default for VehicleDynamics {
    fn default() -> Self {
        Self {
            speed: 0.0,
            wheel_rotation: 0.0,
        }
    }
}

impl VehicleDynamics {
    pub fn command(&mut self, throttle: &ThrottleCommand, steering: &SteeringCommand, dt: f32) {
        if dt <= 0.0 {
            return;
        }

        let max_accel = throttle
            .max_acceleration
            .unwrap_or(DEFAULT_MAX_ACCELERATION_KMH_PER_S);
        let dv = throttle.target_speed - self.speed;
        let a = cap(dv / dt, max_accel);
        self.speed += a * dt;

        let max_rot_speed = steering
            .max_steer_speed
            .unwrap_or(DEFAULT_MAX_STEER_SPEED_DEG_PER_S);
        let dr = steering.target_turn_angle - self.wheel_rotation;
        let v_rot = cap(dr / dt, max_rot_speed);
        self.wheel_rotation = cap(self.wheel_rotation + v_rot * dt, MAX_WHEEL_ANGLE_DEG);
    }
}

pub fn cap(value: f32, limit: f32) -> f32 {
    if f32::abs(value) > limit {
        return f32::signum(value) * limit;
    }

    value
}

#[derive(Clone, Debug, Default, Component)]
pub struct Vehicle;

#[derive(Clone, Debug, Component)]
#[require(Vehicle)]
pub struct MainVehicle;

#[derive(Clone, Debug, Component, Default)]
pub struct Position {
    pub translation: Vec2,
    pub yaw: f32,
}

#[derive(Clone, Debug, Default, Bundle)]
pub struct VehicleBundle {
    pub position: Position,
    pub dynamics: VehicleDynamics,
    pub engine: ThrottleCommand,
    pub steering: SteeringCommand,
    pub vehicle: Vehicle,
    pub transform: Transform,
}

impl VehicleBundle {
    pub fn new(x: f32, y: f32) -> Self {
        Self {
            position: Position {
                translation: Vec2::new(x, y),
                yaw: 0.0,
            },
            transform: Transform::from_xyz(x, y, VEHICLE_LAYER_Z),
            ..Default::default()
        }
    }
}
