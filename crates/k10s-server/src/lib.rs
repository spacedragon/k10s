//! Axum-based control server and embeddable runtime for k10s.

mod auth;
mod config;
mod control;
mod lifecycle;
mod origin;
mod outbound;
mod probes;

pub use config::{
    AccessTokenSourceError, ServerConfig, StandaloneConfig, StandaloneConfigError,
    resolve_access_token, resolve_backend_mode,
};
pub use lifecycle::{
    Admission, ConnectionTasks, DrainSignals, MutationGate, ServerHandle, router, run,
    run_with_assets, spawn_loopback,
};
pub use outbound::{EnqueueError, Priority, ScheduledItem, Scheduler};
pub use probes::{Readiness, ReadinessState};
