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

pub const METERS_PER_SECOND_TO_KMH: f32 = 3600.0 / 1000.0;
pub const VEHICLE_LENGTH_M: f32 = 5.0;

use crate::{Lane, MainVehicle, ScrollingWorld, TrafficLightColors, VehicleBundle};
use bevy::{
    prelude::*,
    render::{
        mesh::{Indices, PrimitiveTopology},
        render_asset::RenderAssetUsages,
    },
    window::{PrimaryWindow, WindowPlugin, WindowRef},
};
use bevy_color::palettes::css as Colors;
use bevy_egui::EguiPlugin;
use rand::Rng;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Use Z-values in Transform to handle layering/depth
pub const LANE_LAYER_Z: f32 = 0.0;
pub const ENV_LAYER_Z: f32 = 1.0;
pub const PEDESTRIAN_LAYER_Z: f32 = 8.0;
pub const TRAFFIC_LIGHT_LAYER_Z: f32 = 9.0;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct ObstacleLimits {
    // The maximum x-offset an obstacle can befrom the vehicle to be considered
    // in the same lane.
    pub x_threshold: f32,
    // The y-offset of an obstacle from the vehicle's center such that the vehicle
    // should stop moving forward
    pub y_stop: f32,
    // The y-offset of an obstacle from the vehicle's center such that the vehicle
    // should slow down
    pub y_slow_down: f32,
    // The absolute distance behind the vehicle's center at which an obstacle is
    // considered behind the vehicle
    pub y_back: f32,
}

impl ObstacleLimits {
    pub fn same_lane(&self, x: f32) -> bool {
        x.abs() < self.x_threshold
    }

    pub fn immediate_obstacle(&self, x: f32, y: f32) -> bool {
        if self.obstacle_behind(y) || !self.same_lane(x) {
            return false;
        }
        y < self.y_stop
    }

    pub fn obstacle_ahead(&self, x: f32, y: f32) -> bool {
        if self.obstacle_behind(y) || !self.same_lane(x) {
            return false;
        }
        y < self.y_slow_down
    }

    pub fn obstacle_behind(&self, y: f32) -> bool {
        y < -self.y_back
    }

    pub fn obstacle_adjacent(&self, x: f32, y: f32, adjacent_space: (f32, f32)) -> Option<Lane> {
        if y.abs() > self.y_stop || self.obstacle_behind(y) {
            return None;
        }
        if x < 0.0 && x.abs() < adjacent_space.0 {
            return Some(Lane::Left);
        } else if x > 0.0 && x.abs() < adjacent_space.1 {
            return Some(Lane::Right);
        }
        None
    }
}

#[derive(Clone, Debug, Resource)]
pub struct WorldLimits {
    pub window: (f32, f32),
    pub full_runway: f32,
    pub road_center: (f32, f32),
    pub user_panel_width: f32,
    pub lane_limits: (f32, f32),
    pub lane_centers: (f32, f32), // left lane center, right lane center
    pub pavement_limits: (f32, f32),
    pub vehicle_size: (f32, f32),
    pub obstacle_limits: ObstacleLimits,
    pub convert_m_to_px: f32,
}

impl FromWorld for WorldLimits {
    fn from_world(world: &mut World) -> Self {
        let mut q_window = world.query_filtered::<&Window, With<PrimaryWindow>>();
        let window = q_window
            .single(world)
            .map(|w| (w.width(), w.height()))
            .unwrap_or((1280.0, 720.0));
        let full_runway = window.1 * 4.0;
        let user_panel_width = 320.0;
        let road_center = (-0.5 * user_panel_width, 0.0);
        let (lane_limits, pavement_limits) = {
            let mut window = world.query_filtered::<&Window, With<PrimaryWindow>>();
            if let Ok(window) = window.single(world) {
                let env_width = window.width() - user_panel_width;
                let segment_x = env_width / 6.0;
                let lane_limits = (road_center.0 - segment_x, road_center.0 + segment_x);
                let pavement_limits = (
                    road_center.0 - (2.0 * segment_x),
                    road_center.0 + (2.0 * segment_x),
                );
                (lane_limits, pavement_limits)
            } else {
                (
                    (road_center.0 - 160.0, road_center.0 + 160.0),
                    (road_center.0 - 320.0, road_center.0 + 320.0),
                )
            }
        };
        let half_lane_width = 0.5 * (road_center.0 - lane_limits.0);
        let lane_centers = (
            road_center.0 - half_lane_width,
            road_center.0 + half_lane_width,
        );
        let vehicle_size = (71.0, 131.0);
        let obstacle_limits = ObstacleLimits {
            x_threshold: 0.25 * (lane_limits.1 - lane_limits.0),
            y_stop: vehicle_size.1 * 1.5,
            y_slow_down: vehicle_size.1 * 3.0,
            y_back: vehicle_size.1 * 0.5,
        };

        let convert_m_to_px = vehicle_size.1 / VEHICLE_LENGTH_M;

        Self {
            window,
            full_runway,
            road_center,
            user_panel_width,
            lane_limits,
            lane_centers,
            pavement_limits,
            vehicle_size,
            obstacle_limits,
            convert_m_to_px,
        }
    }
}

impl WorldLimits {
    pub fn on_lane(&self, x: f32) -> Option<Lane> {
        if self.lane_limits.0 < x && x < self.road_center.0 {
            return Some(Lane::Left);
        } else if self.road_center.0 < x && x < self.lane_limits.1 {
            return Some(Lane::Right);
        }
        None
    }

    pub fn other_lane(&self, x: f32) -> Option<Lane> {
        self.on_lane(x).map(|lane| lane.inverse())
    }

    pub fn adjacent_lane_space(&self, x: f32) -> Option<(f32, f32)> {
        if x < self.lane_limits.0 || x > self.lane_limits.1 {
            return None;
        }
        Some((x - self.lane_limits.0, self.lane_limits.1 - x))
    }

    pub fn obstacle_limits_mut(&mut self) -> &mut ObstacleLimits {
        &mut self.obstacle_limits
    }

    pub fn default_obstacle_limits(&self) -> ObstacleLimits {
        ObstacleLimits {
            x_threshold: 0.25 * (self.lane_limits.1 - self.lane_limits.0),
            y_stop: self.vehicle_size.1 * 1.5,
            y_slow_down: self.vehicle_size.1 * 3.0,
            y_back: self.vehicle_size.1 * 0.5,
        }
    }

    pub fn reset_obstacle_limits(&mut self) {
        self.obstacle_limits = self.default_obstacle_limits();
    }
}

#[derive(Clone, Debug, Resource)]
pub struct WorldMeshes {
    pub lane_dash: (Handle<Mesh>, Handle<ColorMaterial>),
    pub vehicle: Handle<Image>,
    pub foliage: Vec<Handle<Image>>,
}

impl FromWorld for WorldMeshes {
    fn from_world(world: &mut World) -> Self {
        let world_limits = world.resource::<WorldLimits>();
        let window_height = world_limits.window.1.clone();

        let mut meshes = world.resource_mut::<Assets<Mesh>>();
        let lane_dash_mesh = meshes.add(create_dotted_line_mesh(window_height));

        let mut materials = world.resource_mut::<Assets<ColorMaterial>>();
        let lane_dash_mat = materials.add(Color::Srgba(Colors::ANTIQUE_WHITE));

        let asset_server = world.resource::<AssetServer>();
        let vehicle = asset_server.load("sprites/cars/car_blue_1.png");
        let mut foliage = Vec::<Handle<Image>>::new();
        for id in 1..10 {
            let texture_path = format!("sprites/foliage/foliagePack_00{}.png", id);
            foliage.push(asset_server.load(texture_path));
        }

        Self {
            lane_dash: (lane_dash_mesh, lane_dash_mat),
            vehicle,
            foliage,
        }
    }
}

#[derive(Clone, Debug, Component)]
pub struct LaneDash;

#[derive(Event)]
pub struct StopRequested;

#[derive(Default)]
pub struct SpawnWorldPlugin {}

impl Plugin for SpawnWorldPlugin {
    fn build(&self, app: &mut App) {
        app.add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title: "Crossflow Traffic App".into(),
                ..default()
            }),
            ..default()
        }))
        .add_plugins(EguiPlugin {
            enable_multipass_for_primary_context: false,
        });

        app.init_resource::<WorldLimits>()
            .init_resource::<WorldMeshes>()
            .init_resource::<TrafficLightColors>()
            .add_event::<StopRequested>()
            .add_systems(Startup, (spawn_vehicle_and_camera, spawn_environment));
    }
}

fn spawn_vehicle_and_camera(
    mut commands: Commands,
    world_limits: Res<WorldLimits>,
    world_meshes: Res<WorldMeshes>,
    q_window: Query<Entity, With<PrimaryWindow>>,
) {
    let Ok(window_entity) = q_window.single() else {
        return;
    };
    commands.spawn((
        Camera2d::default(),
        Camera {
            target: WindowRef::Entity(window_entity).into(),
            ..default()
        },
    ));

    // Spawn main vehicle
    // Vehicle assets provided by https://kenney.nl/assets/racing-pack
    let scale = world_limits.convert_m_to_px;
    commands.spawn((
        Sprite {
            image: world_meshes.vehicle.clone(),
            ..default()
        },
        VehicleBundle::new(
            world_limits.lane_centers.0 / scale,
            (world_limits.road_center.1 - 100.0) / scale,
        ),
        MainVehicle,
    ));
}

fn spawn_environment(
    mut commands: Commands,
    world_limits: Res<WorldLimits>,
    world_meshes: Res<WorldMeshes>,
) {
    // Log the initial world limits for reference
    info!("Initialized world limits: {:?}", world_limits);

    let window_height = world_limits.window.1;
    // Spawn lane lines to govern left and right lanes
    let lane_segment_x = [
        world_limits.lane_limits.0,
        world_limits.road_center.0,
        world_limits.lane_limits.1,
    ];
    let lane_segment_y = [
        world_limits.road_center.1,
        world_limits.road_center.1 + window_height,
    ];
    for y in lane_segment_y.iter() {
        for x in lane_segment_x.iter() {
            commands.spawn((
                Mesh2d(world_meshes.lane_dash.0.clone()),
                MeshMaterial2d(world_meshes.lane_dash.1.clone()),
                Transform::from_xyz(x.clone(), y.clone(), LANE_LAYER_Z),
                ScrollingWorld,
                LaneDash,
            ));
        }
    }

    // Spawn foliage along the pavement
    // Foliage assets provided by https://kenney.nl/assets/foliage-pack
    let left_range = (-0.5 * world_limits.window.0, world_limits.pavement_limits.0);
    let right_range = (
        world_limits.pavement_limits.1,
        world_limits.window.0 - world_limits.user_panel_width,
    );
    let mut rng = rand::rng();
    let n_objects = rng.random_range(20..30);
    for i in 0..n_objects {
        let x = if i % 2 == 0 {
            rng.random_range(left_range.0..left_range.1)
        } else {
            rng.random_range(right_range.0..right_range.1)
        };
        let y = rng.random_range(0.0..world_limits.full_runway);

        let item_id = rng.random_range(0..9);
        commands.spawn((
            Sprite {
                image: world_meshes.foliage[item_id].clone(),
                anchor: bevy::sprite::Anchor::BottomCenter,
                ..default()
            },
            Transform::from_xyz(x, y, ENV_LAYER_Z),
            ScrollingWorld,
        ));
    }
}

fn create_dotted_line_mesh(window_height: f32) -> Mesh {
    let mut mesh = Mesh::new(
        PrimitiveTopology::TriangleList,
        RenderAssetUsages::default(),
    );
    let interval = 30.0;
    let (dash_width, dash_height) = (2.0, 20.0);

    let mut positions = Vec::new();
    let mut indices = Vec::new();
    let mut index_count = 0;

    let mut y = -window_height;
    while y < window_height {
        let x_min = -dash_width / 2.0;
        let x_max = dash_width / 2.0;
        let y_min = y - dash_height / 2.0;
        let y_max = y + dash_height / 2.0;

        positions.extend_from_slice(&[
            [x_min, y_min, 0.0],
            [x_max, y_min, 0.0],
            [x_max, y_max, 0.0],
            [x_min, y_max, 0.0],
        ]);
        indices.extend_from_slice(&[
            index_count,
            index_count + 1,
            index_count + 2,
            index_count,
            index_count + 2,
            index_count + 3,
        ]);

        index_count += 4;
        y += interval;
    }

    mesh.insert_attribute(Mesh::ATTRIBUTE_POSITION, positions);
    mesh.insert_indices(Indices::U32(indices));

    mesh
}
