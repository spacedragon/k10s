use std::error::Error;

use k10s_desktop::DesktopApp;

fn main() -> Result<(), Box<dyn Error>> {
    let app = DesktopApp::launch()?;
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default().with_inner_size([640.0, 420.0]),
        ..eframe::NativeOptions::default()
    };
    eframe::run_native("k10s", options, Box::new(move |_| Ok(Box::new(app))))?;
    Ok(())
}
