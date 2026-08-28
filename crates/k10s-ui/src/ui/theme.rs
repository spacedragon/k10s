//! Tokenized, compact operations-console styling shared by native and web.

use egui::{
    Color32, Context, CornerRadius, FontFamily, FontId, Frame, Margin, Shadow, Stroke, TextStyle,
    Visuals, vec2,
};

pub(super) const APP_BACKGROUND: Color32 = Color32::from_rgb(15, 17, 19);
pub(super) const PANEL_BACKGROUND: Color32 = Color32::from_rgb(23, 25, 28);
pub(super) const WINDOW_BACKGROUND: Color32 = Color32::from_rgb(27, 30, 33);
pub(super) const CONTROL_BACKGROUND: Color32 = Color32::from_rgb(36, 40, 44);
pub(super) const BORDER: Color32 = Color32::from_rgb(61, 66, 72);
pub(super) const TEXT: Color32 = Color32::from_rgb(229, 232, 235);
pub(super) const MUTED_TEXT: Color32 = Color32::from_rgb(171, 178, 186);
pub(super) const ACCENT: Color32 = Color32::from_rgb(51, 169, 216);
pub(super) const ACCENT_DARK: Color32 = Color32::from_rgb(24, 104, 140);
pub(super) const HEALTHY: Color32 = Color32::from_rgb(91, 214, 156);
pub(super) const CONNECTING: Color32 = Color32::from_rgb(246, 200, 95);
pub(super) const WARNING: Color32 = Color32::from_rgb(246, 200, 95);
pub(super) const DANGER: Color32 = Color32::from_rgb(255, 107, 120);

pub(super) const TOP_BAR_HEIGHT: f32 = 29.0;
pub(super) const LAUNCHER_WIDTH: f32 = 196.0;
pub(super) const TASKBAR_HEIGHT: f32 = 29.0;

pub(super) fn apply(context: &Context) {
    context.set_theme(egui::Theme::Dark);
    let mut style = (*context.style_of(egui::Theme::Dark)).clone();
    style.spacing.item_spacing = vec2(6.0, 3.0);
    style.spacing.window_margin = Margin::symmetric(10, 8);
    style.spacing.button_padding = vec2(6.0, 2.0);
    style.spacing.menu_margin = Margin::symmetric(6, 4);
    style.spacing.indent = 14.0;
    style.spacing.interact_size = vec2(32.0, 20.0);
    style.spacing.combo_width = 160.0;
    style.spacing.text_edit_width = 160.0;
    style.spacing.icon_width = 12.0;
    style.spacing.icon_width_inner = 7.0;
    style.spacing.icon_spacing = 4.0;
    for text_style in [TextStyle::Body, TextStyle::Button, TextStyle::Monospace] {
        style
            .text_styles
            .insert(text_style, FontId::new(12.0, FontFamily::Monospace));
    }
    style
        .text_styles
        .insert(TextStyle::Heading, FontId::new(13.0, FontFamily::Monospace));
    style.visuals = console_visuals();
    context.set_style_of(egui::Theme::Dark, style);
}

fn console_visuals() -> Visuals {
    let mut visuals = Visuals::dark();
    visuals.override_text_color = Some(TEXT);
    visuals.weak_text_color = Some(MUTED_TEXT);
    visuals.panel_fill = PANEL_BACKGROUND;
    visuals.window_fill = WINDOW_BACKGROUND;
    visuals.window_stroke = Stroke::new(1.0, BORDER);
    visuals.window_corner_radius = CornerRadius::same(4);
    visuals.window_shadow = Shadow {
        offset: [0, 8],
        blur: 22,
        spread: 0,
        color: Color32::from_black_alpha(150),
    };
    visuals.popup_shadow = visuals.window_shadow;
    visuals.extreme_bg_color = APP_BACKGROUND;
    visuals.faint_bg_color = Color32::from_rgb(31, 34, 37);
    visuals.code_bg_color = APP_BACKGROUND;
    visuals.warn_fg_color = WARNING;
    visuals.error_fg_color = DANGER;
    visuals.hyperlink_color = ACCENT;
    visuals.selection.bg_fill = ACCENT_DARK;
    visuals.selection.stroke = Stroke::new(1.0, TEXT);
    visuals.widgets.noninteractive.bg_fill = WINDOW_BACKGROUND;
    visuals.widgets.noninteractive.weak_bg_fill = PANEL_BACKGROUND;
    visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0, TEXT);
    visuals.widgets.inactive.bg_fill = CONTROL_BACKGROUND;
    visuals.widgets.inactive.weak_bg_fill = CONTROL_BACKGROUND;
    visuals.widgets.inactive.bg_stroke = Stroke::new(1.0, BORDER);
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0, TEXT);
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(45, 51, 56);
    visuals.widgets.hovered.weak_bg_fill = Color32::from_rgb(45, 51, 56);
    visuals.widgets.hovered.bg_stroke = Stroke::new(1.0, ACCENT);
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.0, Color32::WHITE);
    visuals.widgets.active.bg_fill = ACCENT_DARK;
    visuals.widgets.active.weak_bg_fill = ACCENT_DARK;
    visuals.widgets.active.bg_stroke = Stroke::new(1.0, ACCENT);
    visuals.widgets.active.fg_stroke = Stroke::new(1.0, Color32::WHITE);
    visuals.widgets.open = visuals.widgets.active;
    visuals.button_frame = true;
    visuals.collapsing_header_frame = false;
    visuals.indent_has_left_vline = true;
    visuals.striped = true;
    visuals.window_highlight_topmost = true;
    visuals
}

pub(super) fn top_bar_frame() -> Frame {
    Frame::NONE
        .fill(APP_BACKGROUND)
        .inner_margin(Margin::symmetric(7, 3))
        .stroke(Stroke::new(1.0, BORDER))
}

pub(super) fn launcher_frame() -> Frame {
    Frame::NONE
        .fill(PANEL_BACKGROUND)
        .inner_margin(Margin::symmetric(8, 8))
        .stroke(Stroke::new(1.0, BORDER))
}

pub(super) fn taskbar_frame() -> Frame {
    Frame::NONE
        .fill(APP_BACKGROUND)
        .inner_margin(Margin::symmetric(7, 3))
        .stroke(Stroke::new(1.0, BORDER))
}

pub(super) fn canvas_frame() -> Frame {
    Frame::NONE.fill(APP_BACKGROUND)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn luminance(color: Color32) -> f32 {
        fn linear(channel: u8) -> f32 {
            let value = f32::from(channel) / 255.0;
            if value <= 0.04045 {
                value / 12.92
            } else {
                ((value + 0.055) / 1.055).powf(2.4)
            }
        }
        0.2126 * linear(color.r()) + 0.7152 * linear(color.g()) + 0.0722 * linear(color.b())
    }

    fn contrast(foreground: Color32, background: Color32) -> f32 {
        let foreground = luminance(foreground);
        let background = luminance(background);
        (foreground.max(background) + 0.05) / (foreground.min(background) + 0.05)
    }

    #[test]
    fn text_tokens_meet_wcag_aa_on_every_shell_surface() {
        for background in [APP_BACKGROUND, PANEL_BACKGROUND, WINDOW_BACKGROUND] {
            assert!(contrast(TEXT, background) >= 4.5);
            assert!(contrast(MUTED_TEXT, background) >= 4.5);
        }
        assert!(contrast(Color32::WHITE, ACCENT_DARK) >= 4.5);
    }
}
