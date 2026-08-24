//! Startup validation for every configurable server resource budget.

use std::io::ErrorKind;
use std::time::Duration;

use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_server::{ServerConfig, spawn_loopback};

macro_rules! rejects_zero {
    ($($name:ident),+ $(,)?) => {$(
        #[test]
        fn $name() {
            let mut config = ServerConfig::default();
            config.$name = 0;
            let error = config.validate().unwrap_err();
            assert_eq!(error.field(), stringify!($name));
        }
    )+};
}

rejects_zero!(
    max_frame_size,
    max_message_size,
    max_unauthenticated_connections,
    max_authenticated_connections,
    outbound_queue_capacity,
    max_resource_subscriptions_per_session,
    snapshot_rows_per_chunk,
    max_stream_frame_size,
    max_stream_message_size,
    stream_rate_budget_bytes_per_sec,
    max_stream_connections,
    resume_max_journal_entries,
    resume_max_sessions,
);

#[test]
fn zero_durations_and_impossible_lifecycle_budgets_are_rejected() {
    for update in [
        |config: &mut ServerConfig| config.hello_timeout = Duration::ZERO,
        |config: &mut ServerConfig| config.graceful_flush_timeout = Duration::ZERO,
        |config: &mut ServerConfig| config.drain_grace_timeout = Duration::ZERO,
        |config: &mut ServerConfig| config.drain_timeout = Duration::ZERO,
        |config: &mut ServerConfig| config.stream_hello_timeout = Duration::ZERO,
        |config: &mut ServerConfig| config.resume_entry_max_age = Duration::ZERO,
    ] {
        let mut config = ServerConfig::default();
        update(&mut config);
        assert!(config.validate().is_err());
    }

    let mut flush = ServerConfig::default();
    flush.graceful_flush_timeout = flush.drain_timeout + Duration::from_millis(1);
    assert_eq!(
        flush.validate().unwrap_err().field(),
        "graceful_flush_timeout"
    );
}

#[tokio::test]
async fn invalid_budget_refuses_startup_before_serving() {
    let config = ServerConfig {
        snapshot_rows_per_chunk: 0,
        ..ServerConfig::default()
    };
    let kernel = BackendKernel::new(FakeKubernetes::standard());
    let error = spawn_loopback(config, kernel).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("snapshot_rows_per_chunk"));
}

#[test]
fn production_defaults_form_one_valid_budget_set() {
    ServerConfig::default().validate().unwrap();
}
