//! Production scheduler executable load assertions for slow consumers.
//!
//! Full control-socket snapshot and dedicated log-stream paths live in the
//! `load_paths` integration gate invoked by `tests/load/run.rs`.

use std::time::{Duration, Instant};

use axum::extract::ws::Message;
use k10s_server::{EnqueueError, Scheduler};

const EVENT_BURST: usize = 10_000;
const PROTOCOL_CEILING: Duration = Duration::from_secs(30);

fn text(value: impl Into<String>) -> Message {
    Message::Text(value.into().into())
}

fn main() {
    let started = Instant::now();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let scheduler = Scheduler::new(64, 16);
    let mut next_sequence = 1_u64;

    // Exercise the production sequenced P2 API. Ten thousand replacements of
    // one hot resource retain the original connection sequence and one slot.
    for revision in 0..EVENT_BURST {
        scheduler
            .enqueue_p2_sequenced("pods", "default/hot", |queued| {
                let sequence = queued.unwrap_or_else(|| {
                    let allocated = next_sequence;
                    next_sequence += 1;
                    allocated
                });
                Ok((sequence, text(format!("{sequence}:hot:{revision}"))))
            })
            .unwrap();
    }
    assert_eq!(scheduler.len(), 1);

    // Fill the real P2 partition with distinct sequenced resources. New P2
    // work is explicitly coalesced once the reliable reserve is protected.
    let mut coalesced = 0_usize;
    for resource in 0..EVENT_BURST {
        let result = scheduler.enqueue_p2_sequenced("pods", format!("default/{resource}"), |_| {
            let sequence = next_sequence;
            next_sequence += 1;
            Ok((sequence, text(format!("{sequence}:delta:{resource}"))))
        });
        if matches!(result, Err(EnqueueError::Coalesced)) {
            coalesced += 1;
        }
    }
    assert!(scheduler.len() <= 48);
    assert!(coalesced > 0);

    // Operation forwarding uses the production sequenced P0 API. It must be
    // admitted losslessly from the reliable reserve even with a full P2
    // partition; recv must then preserve one contiguous wire sequence.
    let operation_sequence = next_sequence;
    scheduler
        .enqueue_p0_sequenced(|| {
            Ok((
                operation_sequence,
                text(format!("{operation_sequence}:operation-terminal")),
            ))
        })
        .unwrap();
    let queued = scheduler.len();
    let mut previous = 0_u64;
    let mut saw_operation = false;
    for _ in 0..queued {
        let item = runtime.block_on(scheduler.recv()).unwrap();
        let payload = item.message.to_text().unwrap();
        let sequence = payload.split(':').next().unwrap().parse::<u64>().unwrap();
        assert!(
            sequence > previous,
            "sequenced drain regressed or duplicated"
        );
        previous = sequence;
        saw_operation |= payload.ends_with("operation-terminal");
    }
    assert!(saw_operation);
    assert_eq!(previous, operation_sequence);

    let elapsed = started.elapsed();
    assert!(elapsed < PROTOCOL_CEILING, "protocol load took {elapsed:?}");
    println!(
        "protocol_load OK: burst={EVENT_BURST} queued={queued} coalesced={coalesced} operation_sequence={operation_sequence} elapsed={elapsed:?}"
    );
}
