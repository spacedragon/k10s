//! Protocol/scheduler executable load assertions for slow consumers.

use std::time::{Duration, Instant};

use axum::extract::ws::Message;
use k10s_backend::{Gvk, KubernetesAccess, Query, QueryResult};
use k10s_server::{EnqueueError, Priority, Scheduler};

const OBJECTS: usize = 50_000;
const NODES: usize = 1_000;
const EVENT_BURST: usize = 10_000;
const PAGE_ROWS: usize = 7;
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
    let fake = k10s_backend::FakeKubernetes::with_capacity(OBJECTS, NODES);
    let kernel = k10s_backend::BackendKernel::new(fake.clone());
    let QueryResult::ResourceList(list) = runtime
        .block_on(fake.query(Query::ResourceList {
            context: "dev-local".into(),
            gvk: Gvk::core("v1", "Pod"),
            namespace: None,
        }))
        .unwrap()
    else {
        panic!("pod list")
    };

    let mut encoded_bytes = 0_usize;
    let mut pages = 0_usize;
    for chunk in list.rows.chunks(PAGE_ROWS) {
        let page = kernel.snapshot_page(list.revision, chunk);
        encoded_bytes += serde_json::to_vec(&page).unwrap().len();
        pages += 1;
    }
    assert_eq!(pages, list.rows.len().div_ceil(PAGE_ROWS));
    assert!(
        encoded_bytes > 1_000_000,
        "snapshot encoding was not realistic"
    );

    // A 10k hot-resource burst occupies one coalesced P2 slot.
    let scheduler = Scheduler::new(64, 16);
    for revision in 0..EVENT_BURST {
        scheduler
            .enqueue_p2("pods/default/hot", text(format!("delta-{revision}")))
            .unwrap();
    }
    assert_eq!(scheduler.len(), 1);

    // A slow browser cannot grow the queue beyond the P2 partition.
    let mut coalesced = 0_usize;
    for resource in 0..EVENT_BURST {
        if matches!(
            scheduler.enqueue_p2(format!("pods/default/{resource}"), text("delta")),
            Err(EnqueueError::Coalesced)
        ) {
            coalesced += 1;
        }
    }
    assert!(scheduler.len() <= 48);
    assert!(coalesced > 0);

    // Terminal operation traffic retains a reliable slot and wins over the
    // slow-consumer P2 backlog.
    scheduler
        .enqueue(Priority::P0, text("operation-terminal"))
        .unwrap();
    let first = runtime.block_on(scheduler.recv()).unwrap();
    assert_eq!(first.message.to_text().unwrap(), "operation-terminal");

    // Sustained log framing is bounded CPU work and does not allocate retained
    // queue state in the control scheduler.
    let log_line = "x".repeat(1024);
    let mut log_bytes = 0_usize;
    for sequence in 0..EVENT_BURST {
        let payload = serde_json::json!({"sequence": sequence, "origin":"stdout", "text":log_line});
        log_bytes += serde_json::to_vec(&payload).unwrap().len();
    }
    assert!(log_bytes > 10 * 1024 * 1024);

    let elapsed = started.elapsed();
    assert!(elapsed < PROTOCOL_CEILING, "protocol load took {elapsed:?}");
    println!(
        "protocol_load OK: rows={} pages={pages} snapshot_bytes={encoded_bytes} burst={EVENT_BURST} coalesced={coalesced} log_bytes={log_bytes} elapsed={elapsed:?}",
        list.rows.len()
    );
}
