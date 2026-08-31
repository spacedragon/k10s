//! Deterministic UI capacity benchmark (Plan 2 gate).
//!
//! Renders the full application shell against the 50,000-object fake
//! model (mirroring `k10s-backend`'s capacity dataset) at a fixed
//! 1440x900 viewport and default density, with two workload windows open.
//! Each measured frame applies the live filter over every row and renders
//! a moving virtualized window of rows; the harness fails if either
//! recorded ceiling is breached:
//!
//! 1. frame time: with virtualized rows the per-frame cost is bounded by
//!    the viewport, not the model; an order-of-magnitude regression (e.g.
//!    someone removing the virtualization) breaches the ceiling.
//! 2. allocations: egui frame allocations must stay bounded and
//!    independent of the 50k-object model size.
//!
//! After the timed phase the benchmark proves scroll correctness of the
//! virtualized list through egui's own AccessKit scroll actions: stepped
//! scrolling must move the rendered row window consistently, and the very
//! last row of the 18,750-row pod list must become reachable and laid out
//! inside the window's bounds.
//!
//! Run with `cargo bench -p k10s-ui --bench ui_capacity` for the measured
//! report, or append `-- --test` for the CI mode (fewer frames, same
//! ceilings). The harness is hand-rolled because the ceilings are the
//! assertion target; no criterion dependency exists in this workspace.
//!
//! Like `benches/fake_scale.rs`, this binary carries a scoped
//! `#![allow(unsafe_code)]` allowance solely for the counting global
//! allocator; it wraps [`System`] and adds only atomic counters.

#![allow(unsafe_code)]

use kittest::{NodeT as _, Queryable as _};
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
/// headroom over the recorded baseline on developer hardware and CI
/// runners, while still failing on structural regressions such as losing
/// row virtualization or doing model-sized work per frame.
const FRAME_TIME_CEILING: Duration = Duration::from_millis(100);
const WORST_FRAME_CEILING: Duration = Duration::from_millis(400);
/// Filter frames legitimately allocate the filtered row vector over the
/// 18,750-row pod list (~58k allocs measured); the ceiling sits well above
/// that steady state but far below anything that renders whole-model rows
/// per frame.
const ALLOCS_PER_FRAME_CEILING: u64 = 150_000;
/// Frames budgeted for each scroll proof phase before failing.
const SCROLL_STEP_BUDGET: usize = 30;

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

/// Minimal [`kittest::NodeT`] wrapper so the benchmark can query the
/// accessibility tree of frames it drives manually.
#[derive(Clone, Debug)]
struct BenchNode<'a>(kittest::AccessKitNode<'a>);

impl<'a> kittest::NodeT<'a> for BenchNode<'a> {
    fn accesskit_node(&self) -> kittest::AccessKitNode<'a> {
        self.0
    }

    fn new_related(&self, child_node: kittest::AccessKitNode<'a>) -> Self {
        Self(child_node)
    }
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
                projection: None,
            });
    }
    lists.extend(by_kind);
    ResourceFeed {
        lists,
        ..ResourceFeed::default()
    }
}

/// Name of the very last pod row of the model.
fn last_pod_name(objects: usize) -> String {
    let mut index = objects - 1;
    while index.is_multiple_of(8) || index % 8 > 3 {
        index -= 1;
    }
    format!("scale-pod-{index:06}")
}

/// One deterministic headless frame: run egui, discard unpaintable texture
/// deltas, and fold the accessibility tree update into the queryable tree.
fn render_frame(
    ctx: &egui::Context,
    shell: &mut UiShell<ResourceIdentity>,
    feed: &ResourceFeed,
    contexts: &[String],
    selected_context: &mut Option<String>,
    accesskit_tree: &mut Option<kittest::State>,
    events: Vec<egui::Event>,
) -> (Duration, u64) {
    let input = egui::RawInput {
        screen_rect: Some(egui::Rect::from_min_size(egui::Pos2::ZERO, VIEWPORT)),
        events,
        ..egui::RawInput::default()
    };
    let before_allocs = allocations();
    let start = Instant::now();
    let mut output = ctx.run_ui(input, |ui| {
        shell.show_with_resources(
            ui,
            ConnectionState::Connected,
            contexts,
            selected_context,
            None,
            feed,
        );
    });
    let elapsed = start.elapsed();
    let allocated = allocations() - before_allocs;
    // This harness never paints, so font/texture deltas are discarded
    // explicitly instead of being dropped unapplied.
    output.textures_delta.clear();
    match output.platform_output.accesskit_update.take() {
        Some(update) => {
            *accesskit_tree = Some(match accesskit_tree.take() {
                Some(mut tree) => {
                    tree.update(update);
                    tree
                }
                None => kittest::State::new(update),
            });
        }
        None => fail("accesskit was enabled but a frame emitted no tree update".to_owned()),
    }
    (elapsed, allocated)
}

/// Locate a visible pod row so an AccessKit action request can target its
/// containing ScrollArea.
/// Locate every visible pod row; AccessKit scroll requests are coalesced
/// per target widget, so scrolling fast means targeting many widgets.
fn locate_visible_pods(
    tree: &kittest::State,
) -> Vec<(egui::accesskit::NodeId, egui::accesskit::TreeId)> {
    let mut located: Vec<_> = BenchNode(tree.root())
        .children_recursive()
        .filter(|node| {
            node.accesskit_node()
                .label()
                .is_some_and(|label| label.starts_with("scale-pod-"))
        })
        .map(|node| node.accesskit_node().locate())
        .collect();
    located.dedup();
    located
}

/// An egui event asking the containing ScrollArea of a row to scroll down.
fn scroll_event(
    (target_node, target_tree): (egui::accesskit::NodeId, egui::accesskit::TreeId),
) -> egui::Event {
    egui::Event::AccessKitActionRequest(egui::accesskit::ActionRequest {
        action: egui::accesskit::Action::ScrollDown,
        target_node,
        target_tree,
        data: None,
    })
}

fn visible_pod_labels(tree: &kittest::State) -> Vec<String> {
    BenchNode(tree.root())
        .children_recursive()
        .filter_map(|node| node.accesskit_node().label())
        .filter(|label| label.starts_with("scale-pod-"))
        .collect()
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
    // Deterministic, animation-free rendering plus an accessibility tree
    // the scroll-proof phases can query and act on.
    ctx.enable_accesskit();
    ctx.all_styles_mut(|style| {
        style.scroll_animation = egui::style::ScrollAnimation::none();
        style.animation_time = 0.0;
    });
    // Initialized lazily from the first frame's tree update, which carries
    // the root tree definition.
    let mut accesskit_tree: Option<kittest::State> = None;

    // Keep the pointer inside the pods list, like a user would.
    render_frame(
        &ctx,
        &mut shell,
        &feed,
        &contexts,
        &mut selected_context,
        &mut accesskit_tree,
        vec![egui::Event::PointerMoved(egui::Pos2::new(500.0, 400.0))],
    );
    for _ in 0..WARMUP_FRAMES {
        render_frame(
            &ctx,
            &mut shell,
            &feed,
            &contexts,
            &mut selected_context,
            &mut accesskit_tree,
            Vec::new(),
        );
    }

    // -- Measured phase ----------------------------------------------------
    // Two explicit halves so every mode gates both structures:
    // an UNFILTERED half that renders the large virtualized window over
    // all 18,750 rows (this is what catches losing row virtualization),
    // and a FILTERED half that exercises the model-wide live filter.
    let mut timings = Vec::with_capacity(measured_frames);
    let mut allocs_per_frame: Vec<u64> = Vec::with_capacity(measured_frames);

    shell.apply_workspace_command(WorkspaceCommand::SetSearch(pods_id, String::new()));
    let unfiltered = measured_frames / 2 + measured_frames % 2;
    for _ in 0..unfiltered {
        let (elapsed, allocated) = render_frame(
            &ctx,
            &mut shell,
            &feed,
            &contexts,
            &mut selected_context,
            &mut accesskit_tree,
            Vec::new(),
        );
        timings.push(elapsed);
        allocs_per_frame.push(allocated);
    }

    shell.apply_workspace_command(WorkspaceCommand::SetSearch(
        pods_id,
        "scale-pod-00012".to_owned(),
    ));
    for _ in 0..(measured_frames - unfiltered) {
        let (elapsed, allocated) = render_frame(
            &ctx,
            &mut shell,
            &feed,
            &contexts,
            &mut selected_context,
            &mut accesskit_tree,
            Vec::new(),
        );
        timings.push(elapsed);
        allocs_per_frame.push(allocated);
    }

    // -- Scroll proof: stepped scrolling moves the rendered window ---------
    // egui applies AccessKit ScrollDown action requests by scrolling the
    // containing ScrollArea; this is the platform-independent equivalent of
    // a user wheel-scrolling the list.
    shell.apply_workspace_command(WorkspaceCommand::SetSearch(pods_id, String::new()));
    for _ in 0..4 {
        render_frame(
            &ctx,
            &mut shell,
            &feed,
            &contexts,
            &mut selected_context,
            &mut accesskit_tree,
            Vec::new(),
        );
    }
    let visible_before = visible_pod_labels(accesskit_tree.as_ref().expect("tree initialized"));
    for _ in 0..SCROLL_STEP_BUDGET {
        let anchors = locate_visible_pods(accesskit_tree.as_ref().unwrap());
        render_frame(
            &ctx,
            &mut shell,
            &feed,
            &contexts,
            &mut selected_context,
            &mut accesskit_tree,
            anchors.iter().map(|&a| scroll_event(a)).collect(),
        );
    }
    let visible_after = visible_pod_labels(accesskit_tree.as_ref().expect("tree initialized"));
    assert!(
        !visible_after.is_empty() && visible_after != visible_before,
        "repeated scroll steps did not move the rendered row window \
         (before: {:?}..{:?}, after: {:?}..{:?})",
        visible_before.first(),
        visible_before.last(),
        visible_after.first(),
        visible_after.last()
    );

    // -- Scroll proof: the tail row must be reachable ----------------------
    // Keep stepping down until the very last pod row of the model renders;
    // this fails if virtual offsets ever diverge from the rendered geometry
    // or if the end of the list cannot be reached at all.
    let tail_label = last_pod_name(OBJECTS);
    let mut reached_tail = false;
    // The full list is ~350k points tall; each visible widget contributes
    // one bounded 100-point step per frame, so crossing the whole list
    // takes a four-digit number of frames.
    // The 196 px launcher leaves slightly fewer row anchors per frame than
    // the legacy shell, so retain enough deterministic steps to traverse
    // the same 18,750-row model at the supported shell geometry.
    for frame in 0..(SCROLL_STEP_BUDGET * 150) {
        let anchors = locate_visible_pods(accesskit_tree.as_ref().unwrap());
        render_frame(
            &ctx,
            &mut shell,
            &feed,
            &contexts,
            &mut selected_context,
            &mut accesskit_tree,
            anchors.iter().map(|&a| scroll_event(a)).collect(),
        );
        if frame % 100 == 99 {
            let tree = accesskit_tree.as_ref().unwrap();
            reached_tail = BenchNode(tree.root()).query_by_label(&tail_label).is_some();
            let last = visible_pod_labels(tree).last().cloned();
            eprintln!("tail frame {frame}: reached={reached_tail} last={last:?}");
            if reached_tail {
                break;
            }
        }
    }
    if !reached_tail {
        fail(format!(
            "stepped scrolling never reached the tail row {tail_label:?}; \
             the virtualized list cannot reach its end"
        ));
    }
    println!("scroll-to-tail: row {tail_label:?} became reachable");

    // The tail row must also be laid out inside the Pods window bounds,
    // not just present in the accessibility tree.
    let tree = accesskit_tree.as_ref().unwrap();
    let tail_rect = BenchNode(tree.root())
        .query_by_label(&tail_label)
        .and_then(|node| node.accesskit_node().bounding_box())
        .expect("the tail row has a bounding box once reachable");
    let window_rect = BenchNode(tree.root())
        .children_recursive()
        .find(|node| {
            node.accesskit_node().role() == egui::accesskit::Role::Window
                && node
                    .accesskit_node()
                    .label()
                    .is_some_and(|label| label == "Pods")
        })
        .and_then(|node| node.accesskit_node().bounding_box())
        .expect("the Pods window has a bounding box");
    // Manual overlap test; accesskit's Rect has no intersects helper.
    let overlaps = tail_rect.x0 < window_rect.x1
        && tail_rect.x1 > window_rect.x0
        && tail_rect.y0 < window_rect.y1
        && tail_rect.y1 > window_rect.y0;
    assert!(
        overlaps,
        "tail row {tail_rect:?} is not inside the Pods window {window_rect:?}"
    );

    // -- Report ------------------------------------------------------------
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
    if worst > WORST_FRAME_CEILING {
        fail(format!(
            "worst frame took {worst:?}, ceiling {WORST_FRAME_CEILING:?}"
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
