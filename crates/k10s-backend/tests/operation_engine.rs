use std::time::Duration;

use k10s_backend::{AcceptOutcome, BackendError, BackendEvent, OperationEngine, OperationState};

fn accepted(outcome: AcceptOutcome) -> String {
    match outcome {
        AcceptOutcome::Accepted(id) => id.as_str().to_owned(),
        AcceptOutcome::Replayed(_) => panic!("expected a fresh operation"),
    }
}

#[test]
fn lifecycle_separates_pre_submit_cancel_from_post_submit_unknown_outcome() {
    let engine = OperationEngine::new("server-a");
    let cancelled = accepted(engine.accept("key-cancel", "scale/ns/web/3").unwrap());
    engine.cancel_before_submit(&cancelled).unwrap();
    assert_eq!(
        engine.status(std::slice::from_ref(&cancelled)).operations[0].state,
        OperationState::Cancelled
    );

    let unknown = accepted(engine.accept("key-unknown", "delete/ns/web").unwrap());
    assert!(engine.outcome_unknown(&unknown).is_err());
    engine.running(&unknown, None).unwrap();
    engine.outcome_unknown(&unknown).unwrap();
    assert_eq!(
        engine.status(std::slice::from_ref(&unknown)).operations[0].state,
        OperationState::OutcomeUnknown
    );
    assert!(
        !engine.status(std::slice::from_ref(&unknown)).operations[0]
            .state
            .is_terminal(),
        "unknown outcomes stay blocked until explicit reconciliation"
    );
    engine.succeeded(&unknown).unwrap();
}

#[test]
fn idempotency_replays_exact_requests_and_blocks_ambiguous_duplicates() {
    let engine = OperationEngine::new("server-a");
    let first = accepted(engine.accept("same-key", "scale/ns/web/3").unwrap());
    let replay = engine.accept("same-key", "scale/ns/web/3").unwrap();
    assert_eq!(replay.operation_id().as_str(), first);
    assert!(matches!(
        engine.accept("same-key", "scale/ns/web/4"),
        Err(BackendError::Conflict(_))
    ));
    assert!(matches!(
        engine.accept("other-key", "scale/ns/web/3"),
        Err(BackendError::Conflict(_))
    ));
    engine.running(&first, Some((1, 1))).unwrap();
    engine.succeeded(&first).unwrap();
    assert_eq!(
        engine
            .accept("same-key", "scale/ns/web/3")
            .unwrap()
            .operation_id()
            .as_str(),
        first
    );
}

#[tokio::test]
async fn subscription_publishes_every_state_and_late_joiners_get_live_state() {
    let engine = OperationEngine::new("server-a");
    let id = accepted(engine.accept("key", "apply/hash").unwrap());
    let mut late = engine.subscribe();
    let BackendEvent::Operation(snapshot) = late.recv().await.unwrap() else {
        panic!("operation")
    };
    assert_eq!(snapshot.id, id);
    assert_eq!(snapshot.state, OperationState::Pending);
    engine.running(&id, Some((1, 2))).unwrap();
    let BackendEvent::Operation(running) = late.recv().await.unwrap() else {
        panic!("operation")
    };
    assert_eq!(running.state, OperationState::Running);
    engine
        .failed(&id, "the api server rejected the mutation")
        .unwrap();
    let BackendEvent::Operation(failed) = late.recv().await.unwrap() else {
        panic!("operation")
    };
    assert_eq!(failed.state, OperationState::Failed);
}

#[test]
fn bounded_ttl_eviction_and_restart_detection_fail_closed() {
    let engine = OperationEngine::with_limits("server-a", 1, 1, Duration::from_millis(5));
    let first = accepted(engine.accept("one", "first").unwrap());
    engine.cancel_before_submit(&first).unwrap();
    std::thread::sleep(Duration::from_millis(10));
    assert!(
        engine
            .status(std::slice::from_ref(&first))
            .operations
            .is_empty()
    );
    let second = accepted(engine.accept("two", "second").unwrap());
    assert_ne!(first, second);

    let restarted = OperationEngine::new("server-b");
    assert!(restarted.status(&[second]).operations.is_empty());
}

#[test]
fn capacity_never_evicts_an_in_flight_operation() {
    let engine = OperationEngine::with_limits("server-a", 1, 1, Duration::from_secs(60));
    let _ = engine.accept("one", "first").unwrap();
    assert!(matches!(
        engine.accept("two", "second"),
        Err(BackendError::Conflict(_))
    ));
}

#[test]
fn idempotency_bounds_and_ttl_never_forget_an_in_flight_submission() {
    let engine = OperationEngine::with_limits("server-a", 2, 1, Duration::from_millis(5));
    let first = accepted(engine.accept("one", "first").unwrap());
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(
        engine
            .accept("one", "first")
            .unwrap()
            .operation_id()
            .as_str(),
        first,
        "TTL cannot erase authority for a live operation"
    );
    assert!(matches!(
        engine.accept("two", "second"),
        Err(BackendError::Conflict(_))
    ));
    engine.cancel_before_submit(&first).unwrap();
    assert!(engine.accept("two", "second").is_ok());
}
