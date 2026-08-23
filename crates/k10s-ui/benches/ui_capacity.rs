//! Deterministic UI capacity benchmark (Plan 2 gate).
//!
//! Renders the full application shell against the 50,000-object fake
//! model (mirroring `k10s-backend`'s capacity dataset) at a fixed
//! 1440x900 viewport and default density, with two workload windows open.
//! Each measured frame applies the live filter over every row, sorts
//! nothing unrequested, and scrolls the pod list — then the harness fails
//! if either recorded ceiling is breached:
//!
//! 1. frame time: with virtualized rows the per-frame cost is bounded by
//!    the viewport, not the model; an order-of-magnitude regression (e.g.
//!    someone removing the virtualization) breaches the ceiling.
//! 2. allocations: egui frame allocations must stay bounded and
//!    independent of the 50k-object model size.
//!
//! Run with `cargo bench -p k10s-ui --bench ui_capacity` for the measured
//! report, or append `-- --test` for the CI mode (fewer frames, same
//! ceilings). The harness is hand-rolled because the ceilings are the
//! assertion target; no criterion dependency exists in this workspace.
//!
//! Like `benches/fake_scale.rs`, this binary carries a scoped
//! `unsafe_code` allowance solely for the counting global allocator; it
//! wraps [`System`] and adds only atomic counters.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use k10s_protocol::{BackendRevision, GroupVersionKind, ResourceIdentity, ResourceListRow};
use k10s_ui::{
    ui::{ConnectionState, ResourceFeed, UiShell},
    workspace::{LauncherItem, WorkloadKind as W, WorkspaceCommand},
};

/// Model size fixed by the Plan 2 gate.
const OBJECTS: usize = 50_000;
const VIEWPORT: egui::Vec2 = egui::vec2(1_440.0, 900.0);
/// Warm-up frames excluded from measurements (fonts, area state, caches).
const WARMUP_FRAMES: usize = 30;
/// Measured frames per scenario in bench mode.
const MEASURED_FRAMES: usize = 120;
/// Measured frames per scenario in `--test` mode.
const TEST_FRAMES: usize = 20;

/// Generous but meaningful ceilings: roughly an order of magnitude of
/// headroom over the recorded baseline on developer hardware, while still
/// failing on structural regressions such as losing row virtualization or
/// per-frame model-wide allocations.
const FRAME_TIME_CEILING: Duration = Duration::from_millis(100);
/// Filter frames legitimately allocate the filtered row vector over the
/// 18,750-row pod list (~58k allocs measured); the ceiling sits well above
/// that steady state but far below anything that renders whole-model rows
/// per frame.
const ALLOCS_PER_FRAME_CEILING: u64 = 150_000;

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

fn allocations() -> u64 {
    ALLOCATED_TOTAL.load(Ordering::Relaxed)
}

fn fail(message: String) -> ! {
    eprintln!("\nui_capacity FAILED: {message}");
    std::process::exit(1);
}

/// The same deterministic distribution as the backend's capacity dataset:
/// a repeating kind cycle where three eighths of the objects are pods.
fn build_feed() -> ResourceFeed {
    const KIND_CYCLE: [(&str, &str); 8] = [
        ("apps", "Deployment"),
        ("", "Pod"),
        ("", "Pod"),
        ("", "Pod"),
        ("apps", "StatefulSet"),
        ("apps", "DaemonSet"),
        ("batch", "Job"),
        ("batch", "CronJob"),
    ];
    const SUMMARIES: [&str; 4] = ["Running", "2/2 ready", "0/1 ready", "1/1 up"];

    let mut lists = std::collections::HashMap::new();
    let mut by_kind: std::collections::HashMap<W, Vec<ResourceListRow>> =
        std::collections::HashMap::new();
    for index in 0..OBJECTS {
        let (group, kind) = KIND_CYCLE[index % KIND_CYCLE.len()];
        let workload_kind = match kind {
            "Deployment" => W::Deployments,
            "Pod" => W::Pods,
            "StatefulSet" => W::StatefulSets,
            "DaemonSet" => W::DaemonSets,
            "Job" => W::Jobs,
            _ => W::CronJobs,
        };
        by_kind
            .entry(workload_kind)
            .or_default()
            .push(ResourceListRow {
                identity: ResourceIdentity {
                    context: "dev-local".to_owned(),
                    gvk: GroupVersionKind {
                        group: group.to_owned(),
                        version: "v1".to_owned(),
                        kind: kind.to_owned(),
                    },
                    namespace: Some("default".to_owned()),
                    name: format!("scale-{}-{index:06}", kind.to_lowercase()),
                    uid: format!(
                        "uid-dev-local-{}-default-scale-{index:06}",
                        kind.to_lowercase()
                    ),
                },
                revision: BackendRevision::new(1_000),
                labels: Default::default(),
                summary: SUMMARIES[index % SUMMARIES.len()].to_owned(),
                created_at: format!(
                    "2026-08-21T{:02}:{:02}:{:02}Z",
                    (index / 3_600) % 24,
                    (index / 60) % 60,
                    index % 60
                ),
            });
    }
    lists.extend(by_kind);
    ResourceFeed {
        lists,
        ..ResourceFeed::default()
    }
}

fn main() {
    let test_mode = std::env::args().any(|argument| argument == "--test");
    let measured_frames = if test_mode {
        TEST_FRAMES
    } else {
        MEASURED_FRAMES
    };
    println!(
        "k10s ui_capacity bench ({} mode): {OBJECTS} objects at {}x{:.0}",
        if test_mode { "test" } else { "measure" },
        VIEWPORT.x,
        VIEWPORT.y
    );

    let feed = build_feed();
    let mut shell = UiShell::<ResourceIdentity>::new();
    shell.apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
        LauncherItem::Workload(W::Deployments),
    ));
    shell.apply_workspace_command(WorkspaceCommand::ActivateLauncherItem(
        LauncherItem::Workload(W::Pods),
    ));
    // Give the pods window nearly the full canvas height so its list pane
    // is large and the scroll/filter workload is meaningful.
    let pods_id = shell
        .workspace()
        .windows()
        .iter()
        .find(|window| window.kind == k10s_ui::workspace::WindowKind::Workload(W::Pods))
        .expect("pods window open")
        .id;
    shell.apply_workspace_command(WorkspaceCommand::SetSplitRatio(pods_id, 0.0));

    let contexts = ["dev-local".to_owned()];
    let mut selected_context = Some("dev-local".to_owned());
    let ctx = egui::Context::default();

    let mut pointer_placed = false;
    let mut filter_on = false;
    let mut timings = Vec::with_capacity(measured_frames);
    let mut allocs_per_frame: Vec<u64> = Vec::with_capacity(measured_frames);

    let total_frames = WARMUP_FRAMES + measured_frames;
    for frame in 0..total_frames {
        // Park the pointer inside the pods list so wheel events scroll it.
        let mut events = Vec::new();
        if !pointer_placed {
            events.push(egui::Event::PointerMoved(egui::Pos2::new(500.0, 400.0)));
            pointer_placed = true;
        } else if frame >= WARMUP_FRAMES {
            events.push(egui::Event::MouseWheel {
                unit: egui::MouseWheelUnit::Point,
                delta: if frame % 2 == 0 {
                    egui::vec2(0.0, -240.0)
                } else {
                    egui::vec2(0.0, 240.0)
                },
                modifiers: egui::Modifiers::default(),
                phase: egui::TouchPhase::Move,
            });
        }

        // Exercise the live filter path over the whole 18,750-row pod list
        // periodically: on, then off again.
        if frame >= WARMUP_FRAMES && frame % 40 == 0 {
            filter_on = !filter_on;
            shell.apply_workspace_command(WorkspaceCommand::SetSearch(
                pods_id,
                if filter_on {
                    "scale-pod-00012".to_owned()
                } else {
                    String::new()
                },
            ));
        }

        let input = egui::RawInput {
            screen_rect: Some(egui::Rect::from_min_size(egui::Pos2::ZERO, VIEWPORT)),
            events,
            ..egui::RawInput::default()
        };

        let before_allocs = allocations();
        let start = Instant::now();
        let _output = ctx.run_ui(input, |ui| {
            shell.show_with_resources(
                ui,
                ConnectionState::Connected,
                &contexts,
                &mut selected_context,
                None,
                &feed,
            );
        });
        let elapsed = start.elapsed();
        if frame >= WARMUP_FRAMES {
            timings.push(elapsed);
            allocs_per_frame.push(allocations() - before_allocs);
        }
    }

    timings.sort();
    let average = timings.iter().sum::<Duration>() / timings.len() as u32;
    let worst = timings.last().copied().unwrap_or_default();
    let median = timings[timings.len() / 2];
    allocs_per_frame.sort_unstable();
    let average_allocs =
        allocs_per_frame.iter().sum::<u64>() / allocs_per_frame.len().max(1) as u64;
    let worst_allocs = allocs_per_frame.last().copied().unwrap_or_default();

    println!("frame time: avg {average:?} median {median:?} worst {worst:?}");
    println!("allocations/frame: avg {average_allocs} worst {worst_allocs}");

    if average > FRAME_TIME_CEILING {
        fail(format!(
            "average frame took {average:?}, ceiling {FRAME_TIME_CEILING:?}"
        ));
    }
    if worst > FRAME_TIME_CEILING.mul_f32(4.0) {
        fail(format!(
            "worst frame took {worst:?}, ceiling {:?}",
            FRAME_TIME_CEILING.mul_f32(4.0)
        ));
    }
    if average_allocs > ALLOCS_PER_FRAME_CEILING {
        fail(format!(
            "average frame allocated {average_allocs} times, ceiling \
             {ALLOCS_PER_FRAME_CEILING}; per-frame work must stay bounded"
        ));
    }
    let drift = LIVE_BYTES.load(Ordering::Relaxed);
    println!("final live bytes held by the UI process: {drift}");

    println!("ui_capacity OK");
}
