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

use crate::{
    spawn_world::{LaneDash, METERS_PER_SECOND_TO_KMH, VEHICLE_LENGTH_M, WorldLimits},
    vehicle::{MainVehicle, Position, SteeringCommand, ThrottleCommand, VehicleDynamics},
};
use bevy::prelude::*;

#[derive(Clone, Debug, Component)]
pub struct ScrollingWorld;

#[derive(Default)]
pub struct MovementPlugin {}

impl Plugin for MovementPlugin {
    fn build(&self, app: &mut App) {
        app.add_systems(PostUpdate, move_vehicles);
    }
}

// Our vehicle doesn't actually have to move vertically, as the world is set to
// move around the vehicle (treadmill concept). This system is mainly set up to
// facilitate lane changing, so that the vehicle can move horizontally.
fn move_vehicles(
    mut transforms: Query<(
        &mut Transform,
        &mut Position,
        &mut VehicleDynamics,
        &ThrottleCommand,
        &SteeringCommand,
        Option<&MainVehicle>,
    )>,
    mut scrolling_world: Query<
        (&mut Transform, Option<&LaneDash>),
        (With<ScrollingWorld>, Without<ThrottleCommand>),
    >,
    mut camera: Query<
        &mut Transform,
        (
            With<Camera>,
            Without<ThrottleCommand>,
            Without<ScrollingWorld>,
        ),
    >,
    world_limits: Res<WorldLimits>,
    time: Res<Time>,
) {
    let scale = world_limits.convert_m_to_px;
    let dt = time.delta_secs();
    for (mut transform, mut position, mut dynamics, engine, steering, main) in transforms.iter_mut()
    {
        dynamics.command(&*engine, &*steering, dt);
        // dynamics.speed is a user-facing value in km/h; positions are in meters
        let speed = dynamics.speed / METERS_PER_SECOND_TO_KMH;

        // Kinematic bicycle model. The heading is offset by 90 degrees because
        // yaw = 0 points the vehicle up the road (+y axis).
        let w = dynamics.wheel_rotation.to_radians();
        let heading = position.yaw + f32::to_radians(90.0);
        let v = speed * Vec2::new(f32::cos(heading), f32::sin(heading));
        position.translation += v * dt;
        position.yaw += speed / VEHICLE_LENGTH_M * f32::tan(w) * dt;

        // Keep the vehicle within the road
        let half_width = 0.5 * world_limits.vehicle_size.0 / scale;
        let min_x = world_limits.lane_limits.0 / scale + half_width;
        let max_x = world_limits.lane_limits.1 / scale - half_width;
        position.translation.x = position.translation.x.clamp(min_x, max_x);

        // Update only x/y so the entity keeps the z layer it was spawned with
        let p = scale * position.translation;
        transform.translation.x = p.x;
        transform.translation.y = p.y;
        transform.rotation = Quat::from_axis_angle(Vec3::Z, position.yaw);

        if main.is_some() {
            let reference_y = transform.translation.y;
            let window_height = world_limits.window.1;

            for (mut transform, lane) in scrolling_world.iter_mut() {
                // If the world element has gone out of frame, teleport it back up
                // Let full runway be 4x window height
                let dy = reference_y - transform.translation.y;
                if dy.abs() > window_height {
                    if lane.is_some() {
                        transform.translation.y += dy.signum() * 2.0 * window_height;
                    } else {
                        transform.translation.y += dy.signum() * world_limits.full_runway;
                    }
                }
            }

            if let Ok(mut camera_tf) = camera.single_mut() {
                camera_tf.translation.y = reference_y;
            }
        }
    }
}
