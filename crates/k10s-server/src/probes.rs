//! Credential-free liveness and readiness state.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::http::StatusCode;

/// Public server initialization and shutdown states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReadinessState {
    /// The backend or application router is still initializing.
    Starting = 0,
    /// Backend initialization completed and application requests are accepted.
    Ready = 1,
    /// Backend initialization failed.
    InitializationFailed = 2,
    /// Shutdown started and new application requests are rejected.
    Draining = 3,
}

/// Shared readiness transition handle.
#[derive(Debug)]
pub struct Readiness(AtomicU8);

impl Readiness {
    /// Construct readiness in `Starting` state.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU8::new(ReadinessState::Starting as u8)))
    }

    /// Read the current state.
    #[must_use]
    pub fn state(&self) -> ReadinessState {
        match self.0.load(Ordering::Acquire) {
            0 => ReadinessState::Starting,
            1 => ReadinessState::Ready,
            2 => ReadinessState::InitializationFailed,
            3 => ReadinessState::Draining,
            _ => unreachable!("readiness is only written from ReadinessState"),
        }
    }

    /// Publish a lifecycle transition.
    pub fn set(&self, state: ReadinessState) {
        self.0.store(state as u8, Ordering::Release);
    }
}

pub(crate) async fn health() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok\n")
}

pub(crate) async fn ready(readiness: Arc<Readiness>) -> (StatusCode, &'static str) {
    match readiness.state() {
        ReadinessState::Starting => (StatusCode::SERVICE_UNAVAILABLE, "starting\n"),
        ReadinessState::Ready => (StatusCode::OK, "ready\n"),
        ReadinessState::InitializationFailed => {
            (StatusCode::SERVICE_UNAVAILABLE, "initialization failed\n")
        }
        ReadinessState::Draining => (StatusCode::SERVICE_UNAVAILABLE, "draining\n"),
    }
}
