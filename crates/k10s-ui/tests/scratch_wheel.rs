#[global_allocator]
static A: std::alloc::System = std::alloc::System;

fn probe(name: &str, unit: egui::MouseWheelUnit, phase: egui::TouchPhase, delta: f32) {
    let ctx = egui::Context::default();
    let mut last_range = (0usize, 0usize);
    for frame in 0..8 {
        let mut events = vec![egui::Event::PointerMoved(egui::Pos2::new(60.0, 60.0))];
        if frame >= 2 {
            events.push(egui::Event::MouseWheel {
                unit,
                delta: egui::vec2(0.0, delta),
                modifiers: Default::default(),
                phase,
            });
        }
        let input = egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(
                egui::Pos2::ZERO,
                egui::vec2(300.0, 300.0),
            )),
            events,
            ..Default::default()
        };
        let mut output = ctx.run_ui(input, |ui| {
            let row_h = ui.spacing().interact_size.y;
            let mut range_report: Vec<(usize, usize)> = Vec::new();
            egui::ScrollArea::vertical().id_salt("probe").show_rows(
                ui,
                row_h,
                1000,
                |ui, range| {
                    for i in range.clone() {
                        let _ = ui.button(format!("row-{i}"));
                    }
                    range_report.push((range.start, range.end));
                },
            );
            last_range = *range_report.last().unwrap();
        });
        output.textures_delta.clear();
    }
    println!("{name}: final rows {last_range:?}");
}

#[test]
fn wheel_matrix() {
    probe(
        "point-move-neg",
        egui::MouseWheelUnit::Point,
        egui::TouchPhase::Move,
        -240.0,
    );
    probe(
        "point-startmove-neg",
        egui::MouseWheelUnit::Point,
        egui::TouchPhase::Start,
        -240.0,
    );
    probe(
        "line-move-neg",
        egui::MouseWheelUnit::Line,
        egui::TouchPhase::Move,
        -3.0,
    );
}
