//! Axum-based control server and embeddable runtime for k10s.

mod auth;
mod config;
mod control;
mod lifecycle;
mod outbound;

pub use config::ServerConfig;
pub use lifecycle::{ServerHandle, run, spawn_loopback};
pub use outbound::Priority;
