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
    movement::ScrollingWorld,
    spawn_world::{PEDESTRIAN_LAYER_Z, WorldLimits},
    traffic::{Obstacle, Pedestrian, Velocity},
    user_panel::UserPanel,
    vehicle::{MainVehicle, Vehicle},
};
use bevy::prelude::*;
use bevy_color::palettes::css as Colors;
use rand::Rng;

#[derive(Clone, Debug, Resource)]
pub struct PedestrianMeshes {
    pub live_pedestrian: (Handle<Mesh>, Handle<ColorMaterial>),
    pub dead_pedestrian: (Handle<Mesh>, Handle<ColorMaterial>),
}

#[derive(Clone, Debug, Event)]
pub struct TogglePedestrians(pub bool);

#[derive(Clone, Debug, Event)]
pub enum PedestrianStateChange {
    Accident(Entity),
    Revival(Entity),
}

impl FromWorld for PedestrianMeshes {
    fn from_world(world: &mut World) -> Self {
        let mut meshes = world.resource_mut::<Assets<Mesh>>();
        let live_pedestrian_mesh = meshes.add(Rectangle::new(20.0, 20.0));
        let dead_pedestrian_mesh = meshes.add(Circle::new(15.0));

        let mut materials = world.resource_mut::<Assets<ColorMaterial>>();
        let live_pedestrian_mat = materials.add(Color::Srgba(Colors::CORAL));
        let dead_pedestrian_mat = materials.add(Color::Srgba(Colors::DARK_RED));

        Self {
            live_pedestrian: (live_pedestrian_mesh, live_pedestrian_mat),
            dead_pedestrian: (dead_pedestrian_mesh, dead_pedestrian_mat),
        }
    }
}

#[derive(Default)]
pub struct PedestrianPlugin {}

impl Plugin for PedestrianPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<PedestrianMeshes>()
            .add_event::<TogglePedestrians>()
            .add_event::<PedestrianStateChange>()
            .add_systems(Startup, spawn_pedestrian_crossing)
            .add_systems(PostUpdate, (move_pedestrians, update_pedestrian_state))
            .add_observer(on_toggle_pedestrians)
            .add_observer(on_pedestrian_state_change);
    }
}

fn spawn_pedestrian_crossing(
    mut commands: Commands,
    pedestrian_meshes: Res<PedestrianMeshes>,
    world_limits: Res<WorldLimits>,
) {
    // Spawn a bunch of sprites representing pedestrians jay-walking
    let mut rng = rand::rng();
    let n_pedestrians = rng.random_range(10..=20);
    let y_range = (0.0, world_limits.full_runway);
    for i in 0..n_pedestrians {
        let x = if i % 2 == 0 {
            rng.random_range(world_limits.pavement_limits.0..world_limits.lane_limits.0)
        } else {
            rng.random_range(world_limits.lane_limits.1..world_limits.pavement_limits.1)
        };
        let y = rng.random_range(y_range.0..y_range.1);
        commands.spawn((
            ((
                Mesh2d(pedestrian_meshes.live_pedestrian.0.clone()),
                MeshMaterial2d(pedestrian_meshes.live_pedestrian.1.clone()),
                Transform::from_xyz(x, y, PEDESTRIAN_LAYER_Z),
            )),
            ScrollingWorld,
            Pedestrian::default(),
            Obstacle,
            Velocity::default_pedestrian(),
        ));
    }
}

fn move_pedestrians(
    mut transforms: Query<(&mut Transform, &mut Velocity, &Pedestrian), Without<MainVehicle>>,
    main_vehicle: Query<&Transform, With<MainVehicle>>,
    time: Res<Time>,
    user_panel: Res<UserPanel>,
    world_limits: Res<WorldLimits>,
) {
    let (threshold_x, threshold_y) = (
        0.25 * (world_limits.lane_limits.1 - world_limits.lane_limits.0),
        world_limits.vehicle_size.1,
    );
    // TODO(@xiyuoh) support pedestrian awareness for multiple vehicles
    let Ok((vehicle_x, vehicle_y)) = main_vehicle
        .single()
        .map(|t| (t.translation.x, t.translation.y))
    else {
        return;
    };

    let dt = time.delta_secs();
    for (mut transform, mut velocity, pedestrian) in transforms.iter_mut() {
        let v = velocity.translation;
        if !pedestrian.is_alive() {
            // If pedestrian is dead, they should not move
            continue;
        }
        // Velocity is in m/s while transforms are in pixels
        let scale = world_limits.convert_m_to_px;
        transform.translation.x += v.x * scale * dt;
        transform.translation.y += v.y * scale * dt;

        // If pedestrian reached the edge of the limit, flip the direction
        if transform.translation.x <= world_limits.pavement_limits.0 {
            velocity.translation.x = v.x.abs();
        } else if transform.translation.x >= world_limits.pavement_limits.1 {
            velocity.translation.x = -v.x.abs();
        }

        // If for some reason pedestrians stopped moving, ensure that they resume
        // velocity off-screen. This could be due to users toggling pedestrian
        // awareness off while the pedestrian is waiting.
        if v.x == 0.0 && transform.translation.y > 0.5 * world_limits.window.1 {
            *velocity = Velocity::default_pedestrian();
        }

        let v = velocity.translation;

        // Pedestrian awareness - if enabled, pedestrians will not cross the
        // road when a vehicle is approaching.
        if user_panel.pedestrian_awareness {
            let offset_x = transform.translation.x - vehicle_x;
            let offset_y = transform.translation.y - vehicle_y;
            let in_danger_zone = (offset_x.abs() < threshold_x) && (offset_y.abs() < threshold_y);
            let facing_main_vehicle =
                (offset_x < 0.0 && v.x >= 0.0) || (offset_x > 0.0 && v.x <= 0.0);
            let pedestrian_in_danger = in_danger_zone && facing_main_vehicle;
            let pedestrian_moving = v.x.abs() > 0.0;

            if pedestrian_moving && pedestrian_in_danger {
                *velocity = Velocity::zero();
            } else if !pedestrian_moving && !pedestrian_in_danger {
                // TODO(@xiyuoh) pedestrians should resume their velocity
                *velocity = Velocity::default_pedestrian();
            }
        }
    }
}

fn update_pedestrian_state(
    mut commands: Commands,
    transforms: Query<(Entity, &Transform, &Pedestrian), Without<Vehicle>>,
    main_vehicle: Query<&Transform, With<MainVehicle>>,
    world_limits: Res<WorldLimits>,
) {
    // TODO(@xiyuoh) support checking if pedestrian clashes with multiple vehicles
    let Ok((vehicle_x, vehicle_y)) = main_vehicle
        .single()
        .map(|t| (t.translation.x, t.translation.y))
    else {
        return;
    };
    let (threshold_x, threshold_y) = (
        0.5 * world_limits.vehicle_size.0,
        0.5 * world_limits.vehicle_size.1,
    );
    for (e, transform, pedestrian) in transforms.iter() {
        let diff = vehicle_y - transform.translation.y;
        if !pedestrian.is_alive() && diff.abs() > 0.5 * world_limits.window.1 {
            // If pedestrian is dead, revive them through the scrolling world
            commands.trigger(PedestrianStateChange::Revival(e))
        } else if pedestrian.is_alive() {
            let offset_x = transform.translation.x - vehicle_x;
            let offset_y = transform.translation.y - vehicle_y;

            if (offset_x.abs() < threshold_x) && (offset_y.abs() < threshold_y) {
                // Oh no! Pedestrian has overlapped with a vehicle!
                commands.trigger(PedestrianStateChange::Accident(e));
            }
        }
    }
}

fn on_toggle_pedestrians(
    trigger: Trigger<TogglePedestrians>,
    mut commands: Commands,
    pedestrians: Query<Entity, With<Pedestrian>>,
) {
    let enable_pedestrians = trigger.event().0;
    for e in pedestrians.iter() {
        if enable_pedestrians {
            commands
                .entity(e)
                .insert(Obstacle)
                .insert(Visibility::Inherited);
        } else {
            commands
                .entity(e)
                .insert(Visibility::Hidden)
                .remove::<Obstacle>();
        }
    }
}

fn on_pedestrian_state_change(
    trigger: Trigger<PedestrianStateChange>,
    mut commands: Commands,
    mut pedestrians: Query<&mut Pedestrian>,
    pedestrian_meshes: Res<PedestrianMeshes>,
    user_panel: Res<UserPanel>,
) {
    let event = trigger.event();
    match event {
        PedestrianStateChange::Accident(e) => {
            let Ok(mut pedestrian) = pedestrians.get_mut(*e) else {
                return;
            };
            pedestrian.died();
            commands
                .entity(*e)
                .insert((
                    Mesh2d(pedestrian_meshes.dead_pedestrian.0.clone()),
                    MeshMaterial2d(pedestrian_meshes.dead_pedestrian.1.clone()),
                    Velocity::zero(),
                ))
                .remove::<Obstacle>();
        }
        PedestrianStateChange::Revival(e) => {
            if !user_panel.pedestrian_revival {
                return;
            }
            let Ok(mut pedestrian) = pedestrians.get_mut(*e) else {
                return;
            };
            pedestrian.revived();
            commands
                .entity(*e)
                .insert((
                    Mesh2d(pedestrian_meshes.live_pedestrian.0.clone()),
                    MeshMaterial2d(pedestrian_meshes.live_pedestrian.1.clone()),
                    Velocity::default_pedestrian(),
                ))
                .insert(Obstacle);
        }
    }
}
