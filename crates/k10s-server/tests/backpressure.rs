//! Focused pressure-policy tests over the public fixed-capacity scheduler.

use axum::extract::ws::Message;
use k10s_server::{EnqueueError, Priority, Scheduler};

fn text(value: &str) -> Message {
    Message::Text(value.to_owned().into())
}

async fn next_text(scheduler: &Scheduler) -> String {
    scheduler
        .recv()
        .await
        .unwrap()
        .message
        .to_text()
        .unwrap()
        .to_owned()
}

#[tokio::test(start_paused = true)]
async fn p0_p1_p2_order_and_same_resource_coalescing_are_exact() {
    let scheduler = Scheduler::new(8, 2);
    scheduler.enqueue_p2("pod/a", text("old")).unwrap();
    scheduler.enqueue_p2("pod/a", text("new")).unwrap();
    scheduler.enqueue_p2("pod/b", text("telemetry")).unwrap();
    scheduler.enqueue(Priority::P1, text("response")).unwrap();
    scheduler.enqueue(Priority::P0, text("terminal")).unwrap();

    assert_eq!(scheduler.len(), 4, "same-resource P2 uses one slot");
    assert_eq!(next_text(&scheduler).await, "terminal");
    assert_eq!(next_text(&scheduler).await, "response");
    assert_eq!(next_text(&scheduler).await, "new");
    assert_eq!(next_text(&scheduler).await, "telemetry");
    assert!(scheduler.is_empty());
}

#[tokio::test(start_paused = true)]
async fn p2_cannot_consume_the_terminal_operation_reserve() {
    let scheduler = Scheduler::new(4, 2);
    scheduler.enqueue_p2("pod/a", text("a")).unwrap();
    scheduler.enqueue_p2("pod/b", text("b")).unwrap();
    assert_eq!(
        scheduler.enqueue_p2("pod/c", text("c")),
        Err(EnqueueError::Coalesced)
    );

    scheduler
        .enqueue_p0_sequenced(|| Ok((1, text("operation-terminal"))))
        .unwrap();
    scheduler
        .enqueue_sequenced(|| Ok((2, text("snapshot-terminal"))))
        .unwrap();
    assert_eq!(scheduler.len(), 4);

    // Sequenced frames preserve wire contiguity even though terminal
    // operation traffic is P0; neither reliable frame may be dropped.
    assert_eq!(next_text(&scheduler).await, "operation-terminal");
    assert_eq!(next_text(&scheduler).await, "snapshot-terminal");
    assert_eq!(next_text(&scheduler).await, "a");
    assert_eq!(next_text(&scheduler).await, "b");
}

#[tokio::test(start_paused = true)]
async fn overload_close_discards_backlog_and_emits_one_explicit_reason() {
    let scheduler = Scheduler::new(4, 1);
    scheduler.enqueue(Priority::P1, text("stale-a")).unwrap();
    scheduler.enqueue_p2("pod/a", text("stale-b")).unwrap();
    scheduler.overload_close(text("close: outbound overload"));
    assert_eq!(scheduler.len(), 1);
    assert_eq!(next_text(&scheduler).await, "close: outbound overload");
    assert!(scheduler.is_empty());
}
