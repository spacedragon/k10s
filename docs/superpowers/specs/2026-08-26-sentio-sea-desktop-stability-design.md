# Desktop Large-Cluster Connection Stability Design

**Date:** 2026-08-26

## Problem

The native desktop application authenticates its loopback control WebSocket and
reports `Connected`, but a large real context such as `sentio-sea` remains in
loading states and can later transition to `Connection failed`.

The failure is not kubeconfig authentication or API-server reachability:

- `kubectl --context sentio-sea cluster-info` succeeds.
- The embedded control session authenticates successfully.
- A direct `KubeAdapter` probe completes against the context.

The production path instead amplifies a slow cold start:

1. kube-rs legacy discovery performs sequential N+2 requests across every API
   group. One `sentio-sea` discovery took about 21.6 seconds while kubectl's
   aggregated discovery took about 3 seconds.
2. Bootstrap eagerly creates six workload subscriptions, one Service
   subscription, and one `resource.types` request. Every cold request misses
   the same cache and starts its own discovery. A deterministic recorded-server
   probe observed ten concurrent requests hitting `/apis` ten times rather
   than sharing one discovery.
3. Every workload subscription is cluster-wide even when the kubeconfig
   context has a default namespace. The context currently exposes roughly
   4,300 Pods.
4. The server defaults to 16 rows per snapshot page and the desktop transport
   inbox holds 64 frames. A Pod snapshot alone therefore produces roughly 270
   chunk frames, excluding begin/end and other simultaneous subscriptions.
5. The real Kube adapter does not implement the infrastructure query or watch,
   but the Overview surface retains a loading presentation instead of showing
   a terminal unavailable state.

## Goals

- Perform at most one cold discovery per context and share its result with all
  same-context waiters.
- Use Kubernetes aggregated discovery where supported without breaking older
  clusters.
- Start only the resource streams required by visible workspace windows.
- Default namespaced windows to the kubeconfig context namespace while keeping
  an explicit All namespaces option.
- Transfer a 4,300-row all-namespace snapshot without overflowing the desktop
  control connection.
- Project unsupported infrastructure as an explicit, retryable/unavailable UI
  state instead of permanent loading.
- Preserve bounded queues, secret redaction, context isolation, and the rule
  that production never falls back to fake Kubernetes data.

## Non-goals

- Implement Nodes, Storage, metrics, or the full infrastructure backend.
- Introduce a new protocol version or an ACK-window flow-control protocol.
- Change Kubernetes RBAC behavior.
- Preload counts for every launcher entry when its resource window is closed.
- Make cluster-wide subscriptions the default for a namespaced context.

## Chosen approach

Use a surgical stabilization layer rather than a protocol redesign or larger
timeouts alone:

1. Per-context discovery single-flight with a double-checked catalog cache.
2. Aggregated discovery first, with a classified legacy fallback.
3. Workspace-driven resource subscription reconciliation.
4. Explicit namespace scope per namespaced resource window.
5. Larger bounded snapshot pages, a larger bounded desktop inbox, and
   serialized initial snapshots per control session.
6. Explicit infrastructure-unavailable projection.

This removes the request storm and initial data flood at their sources while
retaining the existing control protocol.

## Backend design

### Per-context catalog single-flight

`KubeAdapter` will own a per-context asynchronous discovery gate. The gate
registry is bounded by the kubeconfig context set and shared by adapter users.

`catalog_for(context)` will:

1. Validate context existence and availability.
2. Return a fresh cached catalog immediately.
3. Acquire that context's discovery gate.
4. Re-check the cache after acquiring the gate.
5. Run discovery only if the cache is still absent or expired.
6. Publish one result before releasing the gate.

Waiters receive the published cache result. A failed run is not cached; one
subsequent caller may retry. Different contexts remain independent.

Forced discovery used by context switching acquires the same gate but skips
the fresh-cache return. This prevents a forced switch validation and a normal
cold lookup from racing to publish conflicting catalogs.

### Aggregated discovery and fallback

Discovery first calls kube-rs `run_aggregated()`, which uses `/api` and `/apis`
with aggregated discovery media types. The legacy `run()` path is used only
when the response demonstrates that aggregated discovery is unsupported or
structurally incompatible.

Authentication, context-unavailable, and transport failures do not silently
fall back and double the traffic. They retain the existing sanitized error
mapping. If the classified fallback runs, its final failure is normalized by
the same boundary.

## UI subscription design

### Workspace-driven streams

The app will derive a desired subscription set from open workspace windows:

- One workload subscription for each workload kind with at least one open
  list window.
- One Service subscription only while the Service window is open.
- One `resource.types` request only while the Custom Resources picker/window
  needs the catalog.

Multiple windows for the same kind share one subscription. Closing the last
window unsubscribes it. Reconnect and context-switch recovery reconstruct the
same desired set from persisted workspace state.

Launcher count badges for unopened resources display an unknown/not-loaded
state rather than causing hidden background subscriptions.

### Namespace scope

Namespaced resource windows gain an explicit scope:

- The kubeconfig context namespace is selected by default.
- If the context has no namespace, `default` is used.
- `All namespaces` is an explicit user selection.
- Cluster-scoped resources do not render this control.

The selected scope is part of workspace persistence and the resource
subscription key. Changing it replaces the old subscription through the normal
navigation guard and stale-state behavior.

## Snapshot and transport design

- Raise the default snapshot page size from 16 to 128 rows. A roughly
  4,300-row list then needs about 34 chunks rather than about 270.
- Raise the native/web control inbox bound from 64 to 256 frames. The inbox
  remains bounded and preserves explicit overflow handling.
- Serialize initial snapshot emission per control session so several restored
  windows cannot enqueue their complete initial snapshots concurrently.
- Preserve the existing 1 MiB frame and 4 MiB message limits. Snapshot pages
  still contain normalized list rows rather than manifests or secret payloads.
- Preserve P2 delta coalescing and resync behavior after the initial snapshot.

The page and inbox changes are not substitutes for discovery and subscription
fixes; all three controls are required. A future protocol-level ACK window may
replace snapshot serialization, but it is outside this change.

## Overview error projection

An infrastructure request or subscription rejected as unsupported completes
the Overview load with an explicit unavailable state. The panel displays a
safe message such as `Cluster overview is not available in this build` and a
Refresh action. It does not keep a spinner active and does not transition the
whole control connection to failed.

Transport failures remain connection failures. Request- or subscription-scoped
capability failures remain local to the affected panel.

## Error handling and observability

- Discovery waiters receive the same sanitized success or failure category.
- No raw kubeconfig, credentials, API response bodies, or access tokens are
  added to logs.
- Safe tracing records the context, discovery mode (`aggregated` or `legacy`),
  whether a caller waited for an in-flight run, duration, and normalized
  outcome.
- Snapshot tracing records subscription ID, row count, and chunk count, not
  object contents.
- Context switching remains prepare-then-commit; a failed forced discovery
  leaves the prior context active.

## Testing strategy

Implementation follows red-green TDD in this order:

1. A recorded-server concurrency test starts eight cold same-context catalog
   requests and expects one aggregated `/apis` hit and one `/api` hit.
2. Aggregated-discovery compatibility tests prove supported clusters use two
   requests, unsupported clusters perform one shared legacy fallback, and
   auth/transport failures do not trigger fallback traffic.
3. UI client tests prove bootstrap subscribes only open windows, duplicate
   windows share a stream, closing the last window unsubscribes it, and the
   context namespace is the default selector.
4. Workspace tests cover persistence and restoration of namespace versus All
   namespaces scope.
5. A loopback capacity test transfers at least 4,300 normalized rows with
   default production bounds and proves the socket stays connected and the
   complete snapshot is applied.
6. UI tests prove unsupported infrastructure ends loading with a local
   unavailable message while the connection remains ready.
7. Existing backend, server, UI, desktop, formatting, and Clippy gates run
   after focused tests pass.
8. An ignored opt-in live-context smoke test accepts `K10S_LIVE_CONTEXT` and
   verifies bootstrap, catalog discovery, a namespace-scoped Pod snapshot, and
   60 seconds of stable control connectivity without hard-coding credentials.

## Success criteria

- Eight concurrent cold catalog users produce one discovery run per context.
- `sentio-sea` reaches usable resource state without a discovery storm.
- Opening Pods in the context namespace does not fetch all cluster Pods.
- Explicit All namespaces can transfer the approximately 4,300-row Pod
  snapshot without inbox overflow or reconnect.
- The default Overview reports unavailable instead of loading forever.
- The top-level connection remains ready when a panel receives an unsupported
  capability error.
- All existing and new tests pass with no fake fallback on production paths.

