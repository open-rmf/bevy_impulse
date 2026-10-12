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
    pedestrian::TogglePedestrians,
    spawn_world::{StopRequested, WorldLimits},
    speed_limit::CurrentSpeedLimit,
    traffic::{TrafficLight, TrafficSignal},
    traffic_signal::{NextTrafficLight, TrafficSignalChange},
    vehicle::{MainVehicle, SteeringCommand, ThrottleCommand, VehicleDynamics},
};
use bevy::{
    ecs::system::{SystemParam, SystemState},
    prelude::*,
};
use bevy_egui::{
    EguiContexts,
    egui::{self, Grid, RichText, Ui},
};
use egui::Color32;
use egui::Response;
use rmf_site_egui::*;

#[derive(Default)]
pub struct UserInputPlugin {}

impl Plugin for UserInputPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<UserPanel>()
            .add_systems(Startup, init_ui_style)
            .add_systems(Update, app_layout);

        let panel = PanelWidget::new(user_panel, app.world_mut());
        let widget = Widget::new::<UserInteraction>(app.world_mut());
        app.world_mut().spawn((panel, widget));
    }
}

// Use same UI style as rmf_site_editor
fn init_ui_style(mut egui_context: EguiContexts) {
    let mut visuals = egui::Visuals::dark();
    visuals.override_text_color = Some(egui::Color32::from_rgb(250, 250, 250));
    egui_context.ctx_mut().set_visuals(visuals);
}

fn app_layout(
    world: &mut World,
    panel_widgets: &mut QueryState<(Entity, &mut PanelWidget)>,
    egui_context_state: &mut SystemState<EguiContexts>,
) {
    render_panels(world, panel_widgets, egui_context_state);
}

fn user_panel(In(input): In<PanelWidgetInput>, world: &mut World) {
    let panel_width = world.resource::<WorldLimits>().user_panel_width;
    egui::SidePanel::right("traffic_controls")
        .resizable(false)
        .min_width(panel_width)
        .show(&input.context, |ui| {
            if let Err(err) = world.try_show(input.id, ui) {
                error!("Unable to display user input panel: {err:?}");
            }
        });
}

#[derive(Resource)]
pub struct UserPanel {
    pub auto_signal_change: bool,
    pub allow_change_lane: bool,
    pub pedestrian_awareness: bool,
    pub pedestrian_revival: bool,
    pub include_pedestrians: bool,
}

impl FromWorld for UserPanel {
    fn from_world(_world: &mut World) -> Self {
        Self {
            auto_signal_change: true,
            allow_change_lane: false,
            pedestrian_awareness: true,
            pedestrian_revival: true,
            include_pedestrians: true,
        }
    }
}

#[derive(SystemParam)]
pub struct UserInteraction<'w, 's> {
    request_stop: EventWriter<'w, StopRequested>,
    commands: Commands<'w, 's>,
    current_speed_limit: Res<'w, CurrentSpeedLimit>,
    next_traffic_light: Res<'w, NextTrafficLight>,
    traffic_lights: Query<'w, 's, (Entity, &'static TrafficLight)>,
    user_panel: ResMut<'w, UserPanel>,
    vehicle_state: Query<'w, 's, &'static VehicleDynamics, With<MainVehicle>>,
    vehicle_commands: Query<
        'w,
        's,
        (&'static mut ThrottleCommand, &'static mut SteeringCommand),
        With<MainVehicle>,
    >,
}

impl<'w, 's> WidgetSystem for UserInteraction<'w, 's> {
    fn show(_: (), ui: &mut Ui, state: &mut SystemState<Self>, world: &mut World) -> () {
        let mut params = state.get_mut(world);
        params.show_widget(ui);
    }
}

impl<'w, 's> UserInteraction<'w, 's> {
    pub fn show_widget(&mut self, ui: &mut Ui) {
        ui.add_space(10.0);

        ui.heading("Traffic Monitor");
        ui.separator();
        ui.add_space(20.0);

        ui.label(RichText::new("Vehicle State").size(14.0));
        ui.separator();
        ui.add_space(10.0);
        if let Ok(vehicle_state) = self.vehicle_state.single() {
            Grid::new("vehicle_state").show(ui, |ui| {
                ui.label("Speed: ");
                ui.label(format!("{:0.1} km/h", vehicle_state.speed));
                ui.end_row();

                ui.label("Turning Angle: ");
                ui.label(format!("{:.0}°", vehicle_state.wheel_rotation));
            });
            ui.add_space(20.0);
        }

        if ui.button("Send stop request").clicked() {
            self.request_stop.write(StopRequested);
        }

        if ui
            .button("Halt vehicle")
            .on_hover_text("Resets throttle and steering")
            .clicked()
        {
            for (mut throttle, mut steering) in &mut self.vehicle_commands {
                *throttle = ThrottleCommand::default();
                *steering = SteeringCommand::default();
            }
        }

        ui.label(RichText::new("Traffic Settings").size(14.0));
        ui.separator();
        ui.add_space(10.0);

        ui.label(
            RichText::new(format!(
                "Current speed limit: {}",
                self.current_speed_limit.0.0
            ))
            .size(14.0),
        );
        ui.add_space(10.0);

        ui.horizontal(|ui| {
            ui.add_space(20.0);
            if let Some((target, traffic_light)) = self
                .next_traffic_light
                .0
                .and_then(|e| self.traffic_lights.get(e).ok())
            {
                let (next, color) = match traffic_light.signal {
                    TrafficSignal::Green => (TrafficSignal::Yellow, Color32::from_rgb(0, 255, 0)),
                    TrafficSignal::Red => (TrafficSignal::Green, Color32::from_rgb(255, 0, 0)),
                    TrafficSignal::Yellow => (TrafficSignal::Red, Color32::from_rgb(255, 255, 0)),
                    TrafficSignal::Empty => {
                        error!("Upcoming traffic signal is Empty, initializing it to green!");
                        self.commands.trigger(TrafficSignalChange {
                            target,
                            next: TrafficSignal::Green,
                        });
                        return;
                    }
                };
                if draw_traffic_light_button(ui, color, true)
                    .on_hover_text("Click to toggle the traffic signal")
                    .clicked()
                {
                    self.commands.trigger(TrafficSignalChange { target, next });
                }
                ui.add_space(20.0);
                ui.vertical(|ui| {
                    ui.label(
                        RichText::new(format!("Next signal: {:?}", traffic_light.signal))
                            .size(14.0),
                    );
                    ui.label(
                        RichText::new("Click on the light to toggle traffic signal!")
                            .italics()
                            .color(egui::Color32::from_gray(200)),
                    );
                });
            } else {
                let color = Color32::from_rgb(50, 50, 50);
                draw_traffic_light_button(ui, color, true);
                ui.add_space(20.0);
                ui.label(RichText::new("Next signal: None").size(14.0));
            }
        });
        ui.add_space(20.0);

        // Customize whether they want automatic traffic signal updates
        ui.checkbox(
            &mut self.user_panel.auto_signal_change,
            "Automatic traffic signal changes",
        )
        .on_hover_text(
            "If enabled, traffic signals will be updated automatically in the
             sequence [Green --> Yellow --> Red].",
        );
        // Customize whether to allow lane changes
        ui.checkbox(&mut self.user_panel.allow_change_lane, "Allow lane changes")
            .on_hover_text(
                "If enabled, the main vehicle will consider changing lanes when
             there are obstacles in front of it.",
            );

        ui.add_space(20.0);

        ui.label(RichText::new("Pedestrian Settings").size(14.0));
        ui.separator();
        ui.add_space(10.0);
        // Customize whether to enable pedestrians
        let include_pedestrians = self.user_panel.include_pedestrians;
        ui.checkbox(
            &mut self.user_panel.include_pedestrians,
            "Include pedestrians",
        )
        .on_hover_text("If enabled, the world will include pedestrians crossing the road.");
        if include_pedestrians != self.user_panel.include_pedestrians {
            self.commands
                .trigger(TogglePedestrians(self.user_panel.include_pedestrians));
        }
        // Customize whether to enable pedestrian awareness
        ui.checkbox(
            &mut self.user_panel.pedestrian_awareness,
            "Pedestrian awareness",
        )
        .on_hover_text(
            "If enabled, pedestrians will be aware not to cross the road
             when a vehicle is approaching.",
        );
        // Customize whether to enable pedestrian revival
        ui.checkbox(
            &mut self.user_panel.pedestrian_revival,
            "Pedestrian revival",
        )
        .on_hover_text("If enabled, dead pedestrians will revive off-screen.");
    }
}

fn draw_traffic_light_button(ui: &mut Ui, color: Color32, active: bool) -> Response {
    let radius = 30.0;
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(radius * 2.0, radius * 2.0), egui::Sense::click());

    let center = rect.center();
    let painter = ui.painter();

    painter.circle_filled(rect.center(), radius, color);

    if active {
        let highlight_pos = center - egui::vec2(radius * 0.25, radius * 0.25);
        painter.circle_filled(
            highlight_pos,
            radius * 0.2,
            egui::Color32::from_white_alpha(160),
        );
        painter.circle_stroke(center, radius * 0.9, (2.0, color.gamma_multiply(0.5)));
    } else {
        let reflect_pos = center - egui::vec2(radius * 0.2, radius * 0.2);
        painter.circle_filled(
            reflect_pos,
            radius * 0.1,
            egui::Color32::from_white_alpha(40),
        );
    }

    let visuals = ui.style().interact(&response);
    if response.hovered() {
        painter.circle_stroke(center, radius, (2.0, visuals.bg_fill));
    }

    response
}
