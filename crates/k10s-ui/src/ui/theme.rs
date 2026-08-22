//! Restrained shell styling: default egui dark visuals and status colors.

use egui::{Color32, Context, Visuals};

pub(super) const HEALTHY: Color32 = Color32::from_rgb(80, 190, 110);
pub(super) const CONNECTING: Color32 = Color32::from_rgb(220, 170, 70);

pub(super) fn apply(context: &Context) {
    if context.theme() != egui::Theme::Dark {
        context.set_visuals(Visuals::dark());
    }
}
