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

use core::f32;

use crate::{
    movement::ScrollingWorld,
    spawn_world::{TRAFFIC_LIGHT_LAYER_Z, WorldLimits},
    traffic::SpeedLimit,
    traffic_signal::TrafficLightColors,
    vehicle::MainVehicle,
};
use bevy::prelude::*;
use bevy_color::palettes::css as Colors;
use rand::Rng;

#[derive(Clone, Debug, Default, Resource)]
pub struct CurrentSpeedLimit(pub SpeedLimit);

#[derive(Default)]
pub struct SpeedLimitPlugin {}

impl Plugin for SpeedLimitPlugin {
    fn build(&self, app: &mut App) {
        app.insert_resource(CurrentSpeedLimit(SpeedLimit::default()))
            .add_systems(Startup, spawn_speed_signs)
            .add_systems(Update, update_current_speed_limit);
    }
}

// This system spawns speed limit signs along the road
fn spawn_speed_signs(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    asset_server: Res<AssetServer>,
    traffic_light_colors: Res<TrafficLightColors>,
    world_limits: Res<WorldLimits>,
) {
    let mut rng = rand::rng();
    let n_signs = rng.random_range(1..5);
    let y_start = rng.random_range(0.0..world_limits.window.1 / 2.0);
    let y_interval = world_limits.full_runway / n_signs as f32;
    let x_sign = world_limits.lane_limits.0 - 50.0;

    // For each speed sign, spawn an entity
    for id in 0..n_signs {
        let speed_limit = 30.0 + (10.0 * id as f32);
        commands
            .spawn((
                SpeedLimit(speed_limit.clone()),
                Transform::from_xyz(
                    x_sign,
                    y_start + (id as f32 * y_interval),
                    TRAFFIC_LIGHT_LAYER_Z,
                ),
                Visibility::default(),
                ScrollingWorld,
            ))
            .with_children(|parent| {
                // Red outline
                parent.spawn((
                    Mesh2d(meshes.add(Circle::new(30.0))),
                    MeshMaterial2d(traffic_light_colors.red.clone()),
                    Transform::from_xyz(0.0, 0.0, 0.0),
                ));
                // White inner face
                parent.spawn((
                    Mesh2d(meshes.add(Circle::new(25.0))),
                    MeshMaterial2d(materials.add(Color::Srgba(Colors::WHITE))),
                    Transform::from_xyz(0.0, 0.0, 0.1),
                ));
                // Display text
                parent.spawn((
                    Text2d::new(speed_limit.to_string()),
                    TextFont {
                        font: asset_server.load("fonts/FiraSans-SemiBold.ttf"),
                        font_size: 32.0,
                        ..default()
                    },
                    TextColor(Color::Srgba(Colors::BLACK)),
                    Transform::from_xyz(0.0, 0.0, 0.2),
                ));
            });
    }
}

// This system updates the CurrentSpeedLimit resource when the vehicle passes a
// speed limit sign. The posted limit stays in effect until the next sign.
fn update_current_speed_limit(
    mut current_speed_limit: ResMut<CurrentSpeedLimit>,
    main_vehicle: Query<&Transform, With<MainVehicle>>,
    speed_signs: Query<(&Transform, &SpeedLimit)>,
    world_limits: Res<WorldLimits>,
) {
    let Ok(vehicle_y) = main_vehicle.single().map(|tf| tf.translation.y) else {
        return;
    };

    // The world scrolls down past the vehicle, so signs that have been passed
    // are below it. Only consider signs still in the window so that signs that
    // wrapped back around to the top of the runway are not counted.
    let window_height = world_limits.window.1;
    let last_passed = speed_signs
        .iter()
        .filter(|(tf, _)| tf.translation.y >= -0.5 * window_height && tf.translation.y <= vehicle_y)
        .max_by(|(a, _), (b, _)| a.translation.y.total_cmp(&b.translation.y));

    if let Some((_, speed)) = last_passed
        && current_speed_limit.0 != *speed
    {
        current_speed_limit.0 = speed.clone();
    }
}
