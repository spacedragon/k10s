use std::error::Error;
use std::thread;
use std::time::{Duration, Instant};

use k10s_desktop::launch_embedded_server;
use k10s_ui::client::ConnectTarget;
use k10s_ui::{AppView, K10sApp};

fn main() -> Result<(), Box<dyn Error>> {
    let mut server = launch_embedded_server()?;
    let target = ConnectTarget::new(server.control_url(), server.access_token());
    let mut app = K10sApp::connect(target)?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while matches!(app.view(), AppView::Connecting) && Instant::now() < deadline {
        app.poll();
        thread::sleep(Duration::from_millis(10));
    }
    println!("{}", app.render_text());
    drop(app);
    server.shutdown()?;
    Ok(())
}
