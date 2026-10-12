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
    spawn_world::{TRAFFIC_LIGHT_LAYER_Z, WorldLimits, WorldMeshes},
    traffic::{TrafficLight, TrafficSignal},
    user_panel::UserPanel,
    vehicle::MainVehicle,
};
use bevy::prelude::*;
use bevy_color::{Srgba, palettes::css as Colors};
use rand::Rng;

#[derive(Event)]
pub struct TrafficSignalChange {
    pub target: Entity,
    pub next: TrafficSignal,
}

#[derive(Clone, Debug, Default, Resource)]
pub struct NextTrafficLight(pub Option<Entity>);

#[derive(Event)]
pub struct UpcomingTrafficSignal(pub TrafficSignal);

#[derive(Clone, Debug, Resource)]
pub struct TrafficLightColors {
    pub green: Handle<ColorMaterial>,
    pub yellow: Handle<ColorMaterial>,
    pub red: Handle<ColorMaterial>,
    pub empty: Handle<ColorMaterial>,
}

impl FromWorld for TrafficLightColors {
    fn from_world(world: &mut World) -> Self {
        let mut materials = world.resource_mut::<Assets<ColorMaterial>>();
        Self {
            green: materials.add(Color::Srgba(Colors::DARK_GREEN)),
            yellow: materials.add(Color::Srgba(Colors::ORANGE)),
            red: materials.add(Color::Srgba(Colors::DARK_RED)),
            empty: materials.add(Color::Srgba(Srgba::new(0.3, 0.3, 0.3, 1.0))),
        }
    }
}

#[derive(Default)]
pub struct TrafficSignalPlugin {}

impl Plugin for TrafficSignalPlugin {
    fn build(&self, app: &mut App) {
        app.insert_resource(NextTrafficLight::default())
            .add_event::<TrafficSignalChange>()
            .add_event::<UpcomingTrafficSignal>()
            .add_systems(Startup, spawn_traffic_lights)
            .add_systems(Update, change_traffic_signal)
            .add_systems(PostUpdate, monitor_upcoming_traffic_signal)
            .add_observer(on_traffic_signal_change);
    }
}

// This system spawns traffic lights along the road at the start of the simulation
fn spawn_traffic_lights(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    time: Res<Time>,
    traffic_light_colors: Res<TrafficLightColors>,
    world_limits: Res<WorldLimits>,
    world_meshes: Res<WorldMeshes>,
) {
    let mut rng = rand::rng();
    let n_lights = rng.random_range(3..5);
    let y_start = rng.random_range(0.0..world_limits.window.1 / 2.0);
    let y_interval = world_limits.full_runway / n_lights as f32;

    // For each traffic light, spawn an entity
    let now = time.elapsed_secs();
    let light_radius = 15.0;
    let x_light = world_limits.lane_limits.1 + 100.0;
    for id in 0..n_lights {
        // Spawn an entity for each signal for each traffic light
        let signals: Vec<Entity> = (-1..2)
            .map(|x| {
                commands
                    .spawn((
                        Mesh2d(meshes.add(Circle::new(light_radius))),
                        MeshMaterial2d(traffic_light_colors.empty.clone()),
                        Transform::from_xyz(x as f32 * 40.0, 0.0, 0.1),
                    ))
                    .id()
            })
            .collect();

        let traffic_light = commands
            .spawn((
                TrafficLight::new(id, now.clone(), TrafficSignal::Empty, signals.clone()),
                Mesh2d(meshes.add(Rectangle::new(130.0, 50.0))),
                MeshMaterial2d(materials.add(Color::Srgba(Srgba::new(0.2, 0.2, 0.2, 1.0)))),
                Transform::from_xyz(
                    x_light,
                    y_start + (id as f32 * y_interval),
                    TRAFFIC_LIGHT_LAYER_Z,
                ),
                Visibility::default(),
                ScrollingWorld,
            ))
            .with_children(|parent| {
                // Spawn a stop line on lanes
                let stop_line_width =
                    world_limits.lane_limits.1 - world_limits.lane_limits.0 - 20.0;
                parent.spawn((
                    Mesh2d(meshes.add(Rectangle::new(stop_line_width, 5.0))),
                    MeshMaterial2d(world_meshes.lane_dash.1.clone()),
                    Transform::from_xyz(
                        world_limits.road_center.0 - x_light,
                        0.0,
                        -TRAFFIC_LIGHT_LAYER_Z,
                    ),
                ));
            })
            .id();

        signals.into_iter().for_each(|s| {
            commands.entity(traffic_light).add_child(s);
        });
    }
}

// This system generates the next traffic signal and triggers a TrafficSignalChange
fn change_traffic_signal(
    mut commands: Commands,
    time: Res<Time>,
    traffic_lights: Query<(Entity, &TrafficLight)>,
    user_panel: Res<UserPanel>,
) {
    if !user_panel.auto_signal_change {
        return;
    }

    let mut rng = rand::rng();
    for (e, traffic_light) in traffic_lights.iter() {
        let time_to_change: u64 = match traffic_light.signal {
            TrafficSignal::Red => rng.random_range(2..=5),
            TrafficSignal::Green => rng.random_range(4..=8),
            TrafficSignal::Yellow => rng.random_range(1..=3),
            _ => 0,
        };
        let now = time.elapsed_secs();
        if (now - traffic_light.last_update) as u64 > time_to_change {
            // Change traffic signal
            let next = match traffic_light.signal {
                TrafficSignal::Red | TrafficSignal::Empty => TrafficSignal::Green,
                TrafficSignal::Green => TrafficSignal::Yellow,
                TrafficSignal::Yellow => TrafficSignal::Red,
            };
            commands.trigger(TrafficSignalChange { target: e, next });
        }
    }
}

// This system checks for traffiic signal changes and updates accordingly.
// We separate this from change_traffic_signal to allow user input via the UI.
fn on_traffic_signal_change(
    trigger: Trigger<TrafficSignalChange>,
    mut commands: Commands,
    mut traffic_lights: Query<&mut TrafficLight>,
    time: Res<Time>,
    traffic_light_colors: Res<TrafficLightColors>,
) {
    let signal_change = trigger.event();
    if let Ok(mut signal) = traffic_lights.get_mut(signal_change.target) {
        signal.last_update = time.elapsed_secs();
        signal.signal = signal_change.next.clone();

        // Insert appropriate materials into each signal mesh
        for i in 0..signal.meshes.len() {
            if (signal.signal as i32) == i as i32 {
                commands
                    .entity(signal.meshes[i])
                    .insert(get_material_for_signal(
                        &signal.signal,
                        &traffic_light_colors,
                    ));
            } else {
                commands
                    .entity(signal.meshes[i])
                    .insert(MeshMaterial2d(traffic_light_colors.empty.clone()));
            }
        }
    };
}

fn get_material_for_signal(
    signal: &TrafficSignal,
    traffic_light_colors: &TrafficLightColors,
) -> MeshMaterial2d<ColorMaterial> {
    let handle = match signal {
        TrafficSignal::Red => traffic_light_colors.red.clone(),
        TrafficSignal::Green => traffic_light_colors.green.clone(),
        TrafficSignal::Yellow => traffic_light_colors.yellow.clone(),
        TrafficSignal::Empty => traffic_light_colors.empty.clone(),
    };
    MeshMaterial2d(handle)
}

// This system observes the next immediate traffic signal and writes an
// UpcomingTrafficSignal event whenever the signal changes. If there is no
// upcoming signal, it writes a Green signal to keep the vehicle moving.
fn monitor_upcoming_traffic_signal(
    mut upcoming_signal: EventWriter<UpcomingTrafficSignal>,
    mut next_traffic_light: ResMut<NextTrafficLight>,
    main_vehicle: Query<&Transform, With<MainVehicle>>,
    traffic_lights: Query<(Entity, &Transform, &TrafficLight), Without<MainVehicle>>,
    world_limits: Res<WorldLimits>,
) {
    let Ok(vehicle_y) = main_vehicle.single().map(|tf| tf.translation.y) else {
        return;
    };

    let mut next_signal: Option<(Entity, TrafficSignal)> = None;
    let mut distance_to_next_signal = f32::INFINITY;
    for (e, transform, traffic_light) in traffic_lights.iter() {
        let offset_y = transform.translation.y - vehicle_y;
        // Ignore if traffic light is behind the main vehicle
        if offset_y < 0.0 {
            continue;
        }
        // Ignore if traffic light is off-screen
        if offset_y > 0.5 * world_limits.window.1 {
            continue;
        }

        if offset_y < distance_to_next_signal {
            let _ = next_signal.insert((e, traffic_light.signal.clone()));
            distance_to_next_signal = offset_y - (0.5 * world_limits.vehicle_size.1);
        }
    }

    if let Some((new_next_entity, new_next_signal)) = next_signal {
        if next_traffic_light.0.is_none()
            || next_traffic_light.0.is_some_and(|e| e != new_next_entity)
        {
            next_traffic_light.0 = Some(new_next_entity);
        }
        upcoming_signal.write(UpcomingTrafficSignal(new_next_signal));
        return;
    } else if next_traffic_light.0.is_some() {
        next_traffic_light.0 = None;
    }
    // If there is no upcoming traffic signal, it means that the next
    // traffic light is non-existent or off-screen. We write a Green
    // signal here to keep the vehicle moving.
    upcoming_signal.write(UpcomingTrafficSignal(TrafficSignal::Green));
}
