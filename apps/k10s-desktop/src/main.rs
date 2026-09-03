use std::error::Error;

use k10s_backend::BackendMode;
use k10s_desktop::DesktopApp;

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
        .with_writer(std::io::stderr)
        .init();
    let fake = std::env::args()
        .skip(1)
        .any(|argument| argument == "--fake");
    let app = if fake {
        DesktopApp::launch_with_mode(&BackendMode::Fake)?
    } else {
        DesktopApp::launch()?
    };
    let title = format!("k10s v{}", env!("CARGO_PKG_VERSION"));
    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_title(&title)
            .with_inner_size([1280.0, 800.0]),
        ..eframe::NativeOptions::default()
    };
    eframe::run_native(&title, options, Box::new(move |_| Ok(Box::new(app))))?;
    Ok(())
}
