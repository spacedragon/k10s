//! Shared UI and protocol-client state for k10s (native and WASM).

mod app;

pub use app::{AppView, K10sApp};

/// Target-neutral protocol client.
pub mod client;
