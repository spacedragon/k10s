//! Shared UI and protocol-client state for k10s (native and WASM).

mod app;
mod connection;

pub use app::{AppView, K10sApp, K10sAppEvent};
pub use connection::{ConnectionGate, PersistedSettings, derive_control_url};

/// Target-neutral protocol client.
pub mod client;

/// Default-egui application shell and free-window canvas.
pub mod ui;

/// Command-driven workspace state: windows, focus order, per-window
/// resource state, dedicated details, and navigation guards.
pub mod workspace;
