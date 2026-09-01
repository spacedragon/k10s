# Detail Window Redesign

## Goal

Rebuild dedicated and integrated resource Detail views to follow `09-detail-redesign.html`: identity belongs in the window title, current state belongs in a vital strip, navigation and actions share one top tab row, and the Overview body prioritizes operational state over static metadata.

The work ships as two stacked pull requests developed concurrently:

1. **PR A — shared Detail frame and Pod Detail**
2. **PR B — Deployment Detail**, stacked on PR A until PR A merges

## Shared Detail Frame

### Window identity

A dedicated window must never be titled only `Detail`. Its title and taskbar label use the pinned identity:

```text
Pod · namespace / name
Deployment · namespace / name
```

Cluster-scoped resources omit the namespace separator. Integrated panes use the same identity heading inside their host window, but do not create a second outer title.

The outer title is derived from `DetailState` and stays stable when live data is loading, stale, failed, or gone.

### Vertical structure

Every kind-specific Detail uses this order:

1. identity title;
2. vital strip;
3. one tab/action row;
4. independently scrollable active-tab body;
5. shortcut footer.

The old three-line identity header and separate `Scale / Delete / View logs / Exec shell / Edit YAML` action row are removed. Logs, Shell, YAML, Events, and Pods are tabs; mutation commands remain actions on the right side of the tab row.

### Tab/action row

Tabs remain backed by the existing per-window `DetailTab` state and keyboard shortcuts. The active tab receives the existing selected visual treatment and accessible label `Tab <name>`.

Actions are capability-driven:

- both kinds: `Copy name`, overflow actions, and `Delete…`;
- Deployment additionally: `Scale…` and `Restart…`;
- YAML editing is reached from the YAML tab;
- Logs and Shell are reached from their tabs;
- unavailable or stale mutation authority keeps actions disabled with the current explanatory copy.

`ResourceCapabilities` gains an explicit `can_restart` field, defaulting to
`false`; the backend sets it only for workload kinds accepted by the existing
`workload.restart` operation. The frame never derives restart authority from a
kind string. Rollback is not supported in this scope: rollout history is always
read-only and no `can_rollback` field or rollback control is added. The HTML's
copy dropdown is reduced to the deterministic `Copy name` command.

The `Actions` overflow contains only non-tab, read-only identity commands:
`Open owner` when an exact verified owner identity exists, `Copy namespace`
when namespaced, and `Copy UID` when the pinned identity has a UID. These
commands remain enabled during stale/failed loading because they use pinned
identity data; `Open owner` disappears if verification is unavailable. The menu
is omitted if no command is visible. Mutations never move into this menu:
Scale, Restart, and Delete keep their explicit capability- and
mutation-authority-driven buttons.

Integrated panes retain `Pop out` and `Maximize/Restore`, placed with frame-level controls rather than reintroducing a second action row.

### Scrolling and responsive layout

The frame allocates its rect from the outside in: identity/vitals and tab row at
the top, shortcut footer at the bottom, then the finite remaining rect to the
active body. The header, tabs, actions, and footer never participate in vertical
scrolling and cannot be pushed out by body content. Exactly one vertical scroll
owner exists per `(WindowId, DetailTab)`; its stable ID contains both values.
Sections, grids, tables, and long values may scroll horizontally, but never own
a nested vertical scroll area.

At `available_width() >= 760.0`, Overview uses two columns:

- left: operational state — failures, containers/pods, conditions, recent events;
- right: identity and configuration — ownership, placement/template, network/management, labels, annotations, identity.

Below 760 points it becomes a single column. Operational sections render first;
a metadata umbrella follows them, collapsed by default, and contains placement
or template, network/management, labels, annotations, and identity. The vital
strip remains one logical row: lower-priority vitals collapse behind `… more`
instead of wrapping into another row. Vital expansion is transient state keyed
by `WindowId`, defaults collapsed, and is independent between integrated and
dedicated views.

### Data projection

The renderer does not read Kubernetes state ad hoc across widgets. A focused presentation layer projects the existing authoritative `ResourceDetailResponse` into typed UI models:

- `DetailFrameProjection` — identity, freshness, tabs, actions, shortcut labels;
- `PodDetailProjection` — vitals, failure reason, containers, conditions, placement, network, labels, annotations, identity, owner links, recent events;
- `DeploymentDetailProjection` — rollout vitals, pods, rollout history, template, management information, labels, identity, recent events.

The actual shared input is `DetailPresentationInput`: the pinned identity and
primary `ResourceDetailResponse`, the exact-identity `ResourceMetricsResponse`
entry when present, current relation loading/result/error state, and UI-owned
freshness/gone/mutation-authority state. PR A owns this input type and the feed
plumbing at the shared-frame boundary. Metrics are accepted only when the
response `ResourceIdentity` exactly equals the pinned Pod identity; container
samples then match by exact container name. PR B consumes relation
state through the frozen input and does not access the feed independently.

PR A extends protocol v1.2 with the complete shared wire slots
`ResourceProjection::Pod`, `ResourceProjection::Deployment`, and
`ResourceProjection::ReplicaSet`, including their typed payloads, backend port
variants, mapping arms, serde contracts, and default/legacy behavior. The Pod
adapter is populated in PR A; PR B populates Deployment and ReplicaSet adapters.
PR A also adds backward-compatible named-container samples to
`ResourceMetricsResponse`; the backend preserves the metrics API's container
names instead of exposing only the existing Pod aggregate.
The Kubernetes adapter normalizes these from
the fetched object, and the UI consumes the typed projection only. Pod and
Deployment widgets do not parse `manifest`, `summary`, or display-oriented
`DetailSection` rows. Legacy/missing projections render the shared frame and an
explicit `Structured details unavailable` body, while YAML and other existing
tabs remain usable. Missing, forbidden, or incomplete fields render `—` or
explicit unavailable copy.

Authoritative source rules are:

- freshness/live state: the existing UI primary-detail loading/loaded/failed,
  gone, and mutation-authority state, not Kubernetes status text;
- Pod phase/readiness/restarts, container state/waiting reason, last terminated
  exit, conditions, node, Pod IP, images, labels, annotations, and created time:
  fields normalized into `PodProjection` from Pod metadata/spec/status;
- Deployment rollout/ready/up-to-date/available counts, conditions, strategy,
  template, manager labels/annotations, and created time: fields normalized
  into `DeploymentProjection` from Deployment metadata/spec/status;
- failure reason: the typed Pod waiting/terminated reason or Deployment
  progressing/available condition reason; if absent, no failure section;
- rollout history: backend-resolved exact ReplicaSet relation rows carrying
  `ResourceProjection::ReplicaSet { revision, replicas, ready_replicas,
  created_at }`; PR B normalizes the revision annotation and status into that
  wire type. Rows without a revision are omitted, never guessed;
- metrics: `ResourceMetricsResponse` matched to the exact Pod identity, then its
  new typed container samples matched by exact container name; unavailable,
  partial, unmatched, or not-yet-loaded metrics display `—` and are never
  inferred from Pod aggregates or resource requests/limits;
- events: `ResourceDetailResponse.events`; rows show only reason, message, count,
  and last seen. Event type/source from the HTML are omitted because the wire
  payload does not carry them.

Existing `owner_references`, events, related rows, capabilities, identity, and freshness state remain authoritative. Owner links are clickable only when the exact target identity can be constructed safely. If only an immediate owner is known, show only that verified link plus the current object; do not fabricate a full Deployment → ReplicaSet chain.

## PR A — Shared Frame and Pod Detail

### Pod vital strip

Show, in priority order:

- Status, with color and shape;
- Ready;
- Restarts;
- Age;
- Node;
- Pod IP;
- freshness/live marker.

At narrow widths preserve Status, Ready, Restarts, and Age; move lower-priority vitals into `… more`.
The expansion control is labeled `Show more Pod vitals` / `Hide more Pod
vitals`; expanded content is Node and Pod IP.

### Pod Overview

Wide layout:

- **Left column:** `WHY IT'S FAILING` when unhealthy, `CONTAINERS`, `CONDITIONS`, `RECENT EVENTS`.
- **Right column:** `OWNER CHAIN`, `PLACEMENT`, `NETWORK`, `LABELS`, collapsed `ANNOTATIONS`, `IDENTITY`.

The failure section appears only when authoritative state identifies a failure or waiting reason. When previous-container logs are supported, it offers the existing previous-log workflow rather than creating a new transport path.

Containers show the fields supported by authoritative data: name, image, state/reason, ready, restart count, last exit, and metrics when available. Missing metrics remain `—`.

Labels are sorted by key and use wrapped chips. The first four show initially,
followed by `Show N more labels`; expanded state is transient and keyed by
`WindowId`. Annotations are always collapsed by default. Key/value metadata uses
aligned `egui::Grid` rows rather than individually highlighted selectable
labels.

### Pod compatibility

Events, YAML, Logs, and Shell retain their existing stores, stream lifecycles, shortcuts, reconnect behavior, and authorization. Gone/loading/failed/stale states keep the common frame without resurrecting stale body actions.

PR A also migrates Service and generic kinds onto the new bounded shared frame
while preserving their current specialized bodies, tabs, actions, and data
sources. Only Pod receives a redesigned Overview in PR A. Regression tests cover
Service Ports and generic Overview compatibility, including their header/footer
remaining fixed while the body scrolls.

Service and generic kinds use a compact generic vital strip rather than a new
kind projection. It shows `Status` from the existing normalized status/Overview
row when present, `Age` from `ResourceDetailResponse.created_at`, and the shared
freshness marker; absent Status/Age render `—`. This is the only allowed generic
row lookup and preserves the specialized body unchanged. It never exposes Pod-
or Deployment-specific vitals.

## PR B — Deployment Detail

PR B consumes the shared frame and components from PR A and owns Deployment-specific presentation and tests. It must not fork or duplicate the shared chrome.

### Deployment vital strip

Show, in priority order:

- Rollout state;
- Ready;
- Up-to-date;
- Available;
- Strategy;
- Age;
- freshness/live marker.

At narrow widths Rollout, Ready, Up-to-date, and Available remain visible;
Strategy and Age move behind `Show more Deployment vitals` / `Hide more
Deployment vitals`, using the same per-window transient state contract.

### Deployment Overview

Wide layout:

- **Left column:** `PODS`, `ROLLOUT HISTORY`, and recent rollout events where available.
- **Right column:** `TEMPLATE`, `MANAGED BY`, `LABELS`, collapsed annotations, and identity.

Pods use the existing backend-resolved related-resource state and preserve its loading, failed, stale, retry, and exact-identity behavior. Rollout history reads only backend-resolved related ReplicaSet rows carrying the typed ReplicaSet projection and is read-only; there is no rollback affordance in this scope.

`Restart…` uses the existing workload restart operation and its safety/authority checks. `Scale…`, `Delete…`, Events, Pods, and YAML retain existing command and dialog flows.

## PR Boundaries and Parallel Work

### PR A owns

- shared Detail frame/chrome and title/taskbar identity;
- shared vital, section, chip, metadata-grid, and responsive helpers;
- Pod projection and Pod Overview body;
- shared and Pod-specific tests/snapshots.

### PR B owns

- Deployment projection and Deployment Overview body;
- Deployment-specific action composition using shared APIs;
- Deployment-specific tests/snapshots.

PR B is created from PR A's shared-frame integration commit and initially targets PR A's branch. The PR B agent must not edit shared-frame files unless an interface defect blocks Deployment; such a defect is reported back to PR A rather than worked around with duplicate code.

PR A freezes and pushes a named shared-frame integration commit before PR B is
forked. That commit owns `ui/detail/mod.rs`, shared frame/layout helpers,
workspace window-title/state changes, and shared tests. While PR A completes the
Pod body, PR B may work concurrently only in Deployment/ReplicaSet normalization
modules, the Deployment UI module, and Deployment-specific test fixtures. PR A
owns all protocol types/variants and shared exhaustive-match updates before the
freeze, so PR B never changes the shared enum. Deployment registration in
`ui/detail/mod.rs` happens after the shared API freeze as a small integration
commit on PR B. PR B does not modify shared helpers; API defects return to PR A
and require a new documented freeze commit before rebasing PR B.

## Accessibility and Interaction

- Dedicated window and taskbar labels expose kind, namespace, and name.
- Tabs preserve stable `Tab <name>` labels and keyboard navigation.
- Vital state never relies on color alone; shape and text are always present.
- Collapsed metadata sections expose clear expand/collapse labels.
- Clickable owners and related resources carry complete kind/namespace/name labels.
- Scroll regions have stable IDs per window and tab.
- Existing focus ownership prevents global shortcuts while an editor owns keyboard focus.
- Pod footer: `l logs · s shell · y yaml · e events · c copy name · Esc restore/close`.
  Previous logs remain controlled by the existing Logs-tab checkbox; no `p`
  shortcut is added because the current protocol cannot prove availability.
- Deployment footer: `p pods · y yaml · e events · c copy name · Esc restore/close`.
- `o owner` appears only when a verified exact owner identity exists.
- Delete has no keyboard shortcut in this scope.
- In an integrated pane, `Esc` restores a maximized split; otherwise it closes
  the dedicated Detail window using the existing focus/close routing.

## Testing

Both PRs use test-first development.

PR A regression coverage includes:

- dedicated Pod title/taskbar identity;
- integrated versus dedicated frame controls;
- top vital/tab/action ordering and removal of the duplicate action row;
- healthy and CrashLoopBackOff Pod projections;
- containers, conditions, events, placement, network, labels, annotations, identity, and verified owner links;
- wide two-column and narrow single-column behavior;
- bounded body scrolling at minimum window size;
- loading, failed, gone, stale, disconnected logs, Shell, YAML, and destructive-action authority;
- accessibility snapshots and browser/native visual baselines.

PR B regression coverage includes:

- Deployment title/taskbar identity and vital values;
- Pods and rollout-history projections;
- template, management, labels, and missing-data behavior;
- Scale, Restart, Delete, Pods, Events, and YAML flows;
- wide/narrow responsive behavior and bounded scrolling;
- loading, failed, stale, relation retry, and mutation authority;
- accessibility snapshots and browser/native visual baselines.

Each PR must pass formatting, Clippy with warnings denied, relevant focused tests, full `k10s-ui` tests, WASM checks, browser tests, and native desktop visual verification.

## Non-goals

- New Kubernetes mutation capabilities other than already supported Scale, Restart, Delete, and YAML apply.
- Fabricated owner ancestry, metrics, rollout revisions, or failure causes.
- Rewriting Logs, Shell, YAML, Events, relation transport, or mutation protocols.
- Redesigning resource list windows, Services-specific content, or Overview cluster windows beyond adapting them to the shared frame where necessary.
