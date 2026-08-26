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

`KubeAdapter` will own a per-context asynchronous discovery flight registry.
The registry is bounded by the kubeconfig context set and shared by adapter
users. A flight is an immutable, cloneable future identified by a generation;
all callers that observe the same running generation await the same result.

`catalog_for(context)` will:

1. Validate context existence and availability.
2. Return a fresh cached catalog immediately.
3. Under the short-held flight-registry lock, join the current flight or
   install one new flight generation.
4. Await the shared flight without holding the registry or catalog lock.
5. Publish a successful result to the cache.
6. Remove the flight only when its generation still matches the registry.

Every caller that joined one flight generation receives the same success or
the same cloned, sanitized failure. A failed result is not cached, but it
remains the result of that generation for its existing waiters. Only a caller
arriving after the failed generation has been removed may install the next
attempt. Different contexts remain independent.

Forced discovery used by context switching joins an already-running flight or
installs a new generation while skipping the fresh-cache return. This prevents
a forced switch validation and a normal cold lookup from racing to publish
conflicting catalogs.

### Aggregated discovery and fallback

Discovery first calls kube-rs `run_aggregated()`, which uses `/api` and `/apis`
with aggregated discovery media types. The legacy `run()` path is used only
for this explicit compatibility set:

- HTTP 404, 406, or 415 from an aggregated discovery request.
- A JSON shape mismatch (`SerdeError`) caused by a server returning legacy
  discovery despite content negotiation.
- A kube-rs `DiscoveryError` while converting the aggregated v2 document.
- A nominally successful aggregated result that contains no usable core API
  version/resources. This detects legacy `/api` and `/apis` documents that
  deserialize into the v2 type's default-empty fields.

HTTP 401, 403, 429, and 5xx responses; auth errors; TLS/proxy/service/hyper
errors; cancellation; and timeouts do not fall back and double the traffic.
They retain the existing sanitized error mapping. If the classified fallback
runs, its final failure is normalized by the same boundary.

## UI subscription design

### Workspace-driven streams

The app will derive a desired subscription set from open workspace windows.
The canonical subscription identity is `(context, GVK, namespace scope)`:

- One subscription for each distinct canonical identity required by an open
  workload, Service, or Custom Resource list window.
- One Service subscription only while the Service window is open.
- One `resource.types` request only while the Custom Resources picker/window
  needs the catalog.

Windows with the same canonical identity share one reference-counted
subscription. Same-kind windows with different namespace scopes do not share.
Custom Resource windows with different GVKs do not share. Changing one
window's scope replaces only that window's reference; it cannot retarget or
interrupt another window. Closing the last reference unsubscribes the
canonical identity. Reconnect and context-switch recovery reconstruct the same
desired set from persisted workspace state.

Launcher count badges for unopened resources display an unknown/not-loaded
state rather than causing hidden background subscriptions.

### Namespace scope

Namespaced resource windows gain an explicit scope:

- The kubeconfig context namespace is selected by default.
- If the context has no namespace, `default` is used.
- `All namespaces` is an explicit user selection.
- Cluster-scoped resources do not render this control.

The live and persisted model uses an explicit `NamespaceScope` enum with
`ContextDefault`, `Namespace(String)`, and `AllNamespaces`; `Option<String>` is
not used for scope. The selected scope is part of workspace persistence and
the resource subscription key. Changing it replaces the old subscription
through the normal navigation guard and stale-state behavior.

The workspace snapshot format advances from version 1 to version 2. The loader
accepts version 1 only through a one-way migration:

- Legacy `namespace: Some(value)` becomes `Namespace(value)`.
- Legacy `namespace: None` becomes `ContextDefault`, never `AllNamespaces`.
- Other compatible window settings and geometry are preserved.
- The normalized snapshot is later written as version 2 by the existing
  debounced state store. Unknown versions remain rejected.

On a context switch, `ContextDefault` re-resolves against the destination's
kubeconfig namespace (or `default`), while `Namespace(value)` and
`AllNamespaces` preserve the user's explicit selection. This prevents a
restored legacy window or a context switch from silently widening access to a
cluster-wide subscription.

## Snapshot and transport design

- Raise the default snapshot page size from 16 to 128 rows. A roughly
  4,300-row list then needs about 34 chunks rather than about 270.
- Raise the native/web control inbox bound from 64 to 256 frames. The inbox
  remains bounded and preserves explicit overflow handling.
- Serialize each complete initial snapshot emission per control session. One
  permit covers its contiguous lifecycle from `snapshotBegin` through every
  `snapshotChunk` and `snapshotEnd`; cancellation releases the permit without
  emitting an end frame for an incomplete snapshot. Several restored windows
  therefore cannot enqueue complete initial snapshots concurrently.
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
   A failing-flight variant proves every caller in one generation receives the
   same failure and that only a later caller starts generation two.
2. Aggregated-discovery compatibility tests prove supported clusters use two
   requests, unsupported clusters perform one shared legacy fallback, and
   auth/transport failures do not trigger fallback traffic.
3. UI client tests prove bootstrap subscribes only open windows, duplicate
   windows share a stream, closing the last window unsubscribes it, and the
   context namespace is the default selector. Two same-kind windows with
   different namespace scopes and two Custom Resource GVKs remain independent.
4. Workspace tests cover persistence and restoration of namespace versus All
   namespaces scope. Migration tests prove a version-1 `namespace: null`
   becomes `ContextDefault`, an explicit legacy namespace is preserved, and a
   context switch re-resolves only `ContextDefault` against the destination.
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
