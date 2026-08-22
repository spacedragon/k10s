//! Shared UI and protocol-client state for k10s (native and WASM).

mod app;
mod connection;

pub use app::{AppView, K10sApp};
pub use connection::{ConnectionGate, GateError, PersistedSettings, derive_control_url};

/// Target-neutral protocol client.
pub mod client;
