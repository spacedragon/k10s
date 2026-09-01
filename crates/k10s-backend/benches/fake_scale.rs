//! Deterministic fake-adapter capacity benchmark (Plan 2 gate).
//!
//! Generates the 50,000-object / 1,000-node dataset and proves:
//! 1. construction stays within a recorded wall-time ceiling,
//! 2. a full `resource.list` query of the dominant kind stays within a
//!    recorded wall-time ceiling, and
//! 3. memory is stable: repeated queries return the allocator to its
//!    baseline instead of growing without bound.
//!
//! Run with `cargo bench -p k10s-backend --bench fake_scale` for the
//! measured report, or append `-- --test` to run each scenario once with
//! only the ceiling assertions (the CI mode). The harness is hand-rolled
//! because the ceilings themselves are the assertion target and the
//! benchmark must stay deterministic; no criterion dependency exists in
//! this workspace.
//!
//! This binary carries a scoped `unsafe_code` allowance: the counting
//! global allocator below needs an `unsafe impl GlobalAlloc`. It wraps
//! [`System`] directly and adds only atomic counters; no other unsafe code
//! exists here, and library/test targets keep the workspace-wide denial.

#![allow(unsafe_code)]

use k10s_backend::KubernetesAccess as _;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Dataset size fixed by the Plan 2 gate.
const OBJECTS: usize = 50_000;
/// Cluster size fixed by the Plan 2 gate.
const NODES: usize = 1_000;
/// Dominant snapshot kind: three eighths of the objects are pods.
const EXPECTED_POD_ROWS: usize = OBJECTS * 3 / 8;

/// Generous but meaningful ceilings; CI runners are far slower than
/// workstations, so each ceiling carries an order of magnitude of headroom
/// over the recorded baseline while still catching order-of-magnitude
/// regressions.
const BUILD_CEILING: Duration = Duration::from_secs(60);
const LIST_QUERY_CEILING: Duration = Duration::from_secs(10);
/// Live-memory drift tolerated between iterations (allocator slack).
const MEMORY_DRIFT_TOLERANCE_BYTES: i64 = 8 * 1024 * 1024;

static ALLOCATED_TOTAL: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED_TOTAL.fetch_add(1, Ordering::Relaxed);
        LIVE_BYTES.fetch_add(layout.size() as i64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn live_bytes() -> i64 {
    LIVE_BYTES.load(Ordering::Relaxed)
}

fn allocations() -> u64 {
    ALLOCATED_TOTAL.load(Ordering::Relaxed)
}

fn fail(message: String) -> ! {
    eprintln!("\nfake_scale FAILED: {message}");
    std::process::exit(1);
}

fn main() {
    let test_mode = std::env::args().any(|argument| argument == "--test");
    let iterations = if test_mode { 3 } else { 10 };
    println!(
        "k10s fake_scale capacity bench ({} mode)",
        if test_mode { "test" } else { "measure" }
    );

    // -- 1. Deterministic dataset construction ----------------------------
    let start = Instant::now();
    let fake = k10s_backend::FakeKubernetes::with_capacity(OBJECTS, NODES);
    let build_elapsed = start.elapsed();
    println!(
        "dataset build: {build_elapsed:?} for {} records ({} objects + {NODES} nodes)",
        fake.total_records(),
        OBJECTS
    );
    assert_eq!(fake.total_records(), OBJECTS + NODES);
    if build_elapsed > BUILD_CEILING {
        fail(format!(
            "dataset build took {build_elapsed:?}, ceiling {BUILD_CEILING:?}"
        ));
    }

    // -- 2. Full list query of the dominant kind --------------------------
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let pod_gvk = k10s_backend::Gvk::core("v1", "Pod");

    let query_once = || {
        let future = fake.query(k10s_backend::Query::ResourceList {
            context: "dev-local".to_owned(),
            gvk: pod_gvk.clone(),
            namespace: None,
        });
        rt.block_on(future).expect("pod list query succeeds")
    };

    // Warmup outside the measurement window.
    let warm = query_once();
    let rows = match &warm {
        k10s_backend::QueryResult::ResourceList(data) => data.rows.len(),
        other => fail(format!("unexpected query result: {other:?}")),
    };
    if rows != EXPECTED_POD_ROWS {
        fail(format!(
            "expected exactly {EXPECTED_POD_ROWS} pod rows, got {rows}"
        ));
    }
    println!("pod list rows per query: {rows}");

    let mut timings = Vec::with_capacity(iterations);
    let baseline_memory = live_bytes();
    let mut first_iteration_allocs = 0_u64;
    for iteration in 0..iterations {
        let before_allocs = allocations();
        let start = Instant::now();
        let result = query_once();
        timings.push(start.elapsed());
        let _ = match result {
            k10s_backend::QueryResult::ResourceList(data) => data.rows.len(),
            other => fail(format!("unexpected query result: {other:?}")),
        };
        if iteration == 0 {
            first_iteration_allocs = allocations() - before_allocs;
        }
        // Memory stability: after the query result is dropped the live
        // total must settle back near its baseline every single time.
        let drift = live_bytes() - baseline_memory;
        if drift.abs() > MEMORY_DRIFT_TOLERANCE_BYTES {
            fail(format!(
                "live memory drifted by {drift} bytes after iteration {iteration}; \
                 capacity traffic must not accumulate state"
            ));
        }
    }
    let average = timings.iter().sum::<Duration>() / timings.len() as u32;
    let worst = timings.iter().max().copied().unwrap_or_default();
    println!(
        "pod list query: avg {average:?} worst {worst:?} over {iterations} runs \
         (~{} allocs/query)",
        first_iteration_allocs
    );
    if average > LIST_QUERY_CEILING {
        fail(format!(
            "average list query took {average:?}, ceiling {LIST_QUERY_CEILING:?}"
        ));
    }

    // -- 3. Subscription snapshot registration -----------------------------
    let subscribe_future = fake.subscribe(k10s_backend::Subscribe::ResourceWatch {
        context: "dev-local".to_owned(),
        gvk: pod_gvk,
        namespace: None,
        identity: None,
    });
    let start = Instant::now();
    let mut handle = rt.block_on(subscribe_future).expect("watch subscribes");
    let subscribe_elapsed = start.elapsed();
    let snapshot_rows = rt
        .block_on(async {
            let mut events = handle.take_events().expect("resource watches carry events");
            loop {
                match events.recv().await {
                    Ok(k10s_backend::BackendEvent::Snapshot(list)) => return Some(list.rows.len()),
                    Ok(_) => continue,
                    Err(_) => return None,
                }
            }
        })
        .unwrap_or_default();
    println!(
        "watch subscription with initial snapshot: {subscribe_elapsed:?} ({snapshot_rows} rows)"
    );
    drop(handle);
    let drift = live_bytes() - baseline_memory;
    println!("final live-memory drift from baseline: {drift} bytes");
    if drift.abs() > MEMORY_DRIFT_TOLERANCE_BYTES {
        fail(format!(
            "live memory drifted by {drift} bytes after teardown; \
             capacity traffic must not accumulate state"
        ));
    }

    println!("fake_scale OK");
}
