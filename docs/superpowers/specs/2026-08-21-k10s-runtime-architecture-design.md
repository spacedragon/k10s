# k10s Runtime Architecture and Technology Stack Design

Status: Approved on 2026-08-21

Date: 2026-08-21

## Summary

k10s is a cross-platform Kubernetes console with one shared egui UI for macOS, Linux, Windows, and the web. Both the native and web applications use the same versioned WebSocket protocol and the same backend implementation. The web deployment runs the backend as a standalone process. The desktop application starts that backend in-process, binds it to a random loopback port, and connects to it exactly as the web UI does.

The backend is a deep modular monolith. A small `BackendKernel` interface hides Kubernetes discovery, demand-driven watches, normalized caches, validation, mutations, logs, exec sessions, reconnect behavior, and error classification. It is not split into microservices or organized around a public actor model. Internally, it may use supervised Tokio tasks and bounded channels.

Delivery happens in two phases without changing the UI, protocol, server adapter, or backend kernel:

1. A deterministic fake Kubernetes adapter drives a fully connected static prototype.
2. A kube-rs adapter replaces the fake and connects to real clusters through the backend host's kubeconfig.

This design supersedes the architecture and technology-stack assumptions in `docs/superpowers/plans/2026-08-21-k10s-egui-static-prototype.md`. That plan must not be executed as written; a replacement implementation plan will follow approval of this specification.

## Context and constraints

The approved UI design is in `docs/superpowers/specs/2026-08-21-k10s-egui-console-design.md`.

The architecture must support:

- One shared egui UI and identical product behavior on macOS, Linux, Windows, and web.
- Full feature parity across native and web, including discovery, lists, detail views, mutations, YAML validation and apply, resource watches, logs, and interactive shell sessions.
- A standalone web backend and a desktop-embedded backend using the same byte-level application protocol.
- Backend access to one local kubeconfig containing one or more contexts.
- A single-user, trusted-environment web deployment behind a TLS reverse proxy or VPN.
- A primary capacity target of approximately 1,000 nodes and 50,000 normalized resource objects.
- A first phase that is useful without a live cluster but does not create a throwaway UI or mock-only architecture.

## Goals

- Concentrate Kubernetes and operational complexity behind a small backend interface.
- Keep the UI independent of kube-rs, Tokio, kubeconfig, authentication plugins, and process execution.
- Use one versioned transport contract for native and web clients.
- Make state ownership, reconnect behavior, mutation safety, and stream lifetimes explicit.
- Bound memory, queues, subscriptions, and stream counts.
- Make the fake and real Kubernetes adapters satisfy the same behavioral contract.
- Keep deployment simple: one native application or one server process plus matching static web assets.
- Provide measurable verification for protocol compatibility, failure recovery, UI behavior, and the medium-large cluster target.

## Non-goals

- Multi-user identity, tenancy, audit persistence, or per-user Kubernetes impersonation.
- Direct browser access to a Kubernetes API server.
- Microservices, distributed actors, a general event bus, or event sourcing.
- A public third-party API in the first release.
- GraphQL, gRPC, or gRPC-Web in the first protocol version.
- Durable backend caches or operation history across backend restarts.
- Supporting multiple independently deployed frontend and backend versions indefinitely.
- Replacing Kubernetes authorization. Kubernetes RBAC remains authoritative.

## Architectural approach

### Deep modular monolith

The backend is one deployment unit and one Rust process. Its external interface has three behaviors:

```text
query(Query) -> QueryResult
execute(Command) -> OperationId
subscribe(Subscription) -> EventStream
```

These names describe behavior, not three generic pass-through routes. The implementation owns validation, cache coordination, Kubernetes resource versions, reconnects, idempotency, mutation state, and error translation. Callers do not orchestrate kube-rs calls.

The backend can use task supervisors, channels, cancellation tokens, and state machines internally. These are internal seams, not part of the interface. Actor addresses, mailboxes, and event-bus semantics are not exposed to callers.

### Real external seams

The design has two external seams.

1. **Network seam**
   - Production adapter: the versioned WebSocket server and client.
   - Test adapter: an in-memory protocol harness for focused module tests.
   - Product E2E tests still use the real loopback WebSocket path.

2. **Kubernetes seam**
   - Production adapter: kube-rs using local kubeconfig and the Kubernetes API.
   - Prototype/test adapter: a deterministic in-memory Kubernetes implementation.

Internal backend modules do not each receive a public port or become separate crates merely to permit mocking.

## Deployment topology

### Desktop

```text
Native egui UI
    |
    | WebSocket protocol + ephemeral launch token
    v
Embedded k10s server on 127.0.0.1:<OS-assigned-port>
    |
    | kube-rs + local kubeconfig
    v
Kubernetes API server
```

The native UI runs on the platform event-loop thread. The embedded server runs a Tokio runtime on a dedicated thread. Startup proceeds as follows:

1. Generate a high-entropy per-launch access token.
2. Bind the server to `127.0.0.1:0` and obtain the OS-assigned port.
3. Start the backend kernel and server adapter.
4. Send a readiness result and connection information to the UI through an in-process one-shot channel.
5. Connect the normal protocol client to the loopback server.
6. On application exit, cancel the server and drain its supervised tasks within a bounded deadline.

The UI never calls backend Rust functions directly after startup.

### Web

```text
Browser + WASM egui UI
    |
    | WSS + single-user access token
    v
TLS reverse proxy / VPN
    |
    v
Standalone k10s server
    |
    | kube-rs + server-local kubeconfig
    v
Kubernetes API server
```

The standalone server publishes the matching WASM and static assets so frontend and backend versions normally move together. It defaults to a loopback or private bind address. A public bind requires explicit configuration.

The operator configures the single-user token out of band through a secret file, environment variable, or command-line secret source before starting the server. A non-loopback bind refuses to start without an explicitly configured token. The server never embeds the token in static assets or returns it from an endpoint.

On a fresh browser tab, the web application displays a minimal connection gate before constructing the main k10s workspace. The backend URL defaults to the page's same origin. The user enters the operator-provided token; the UI keeps it only in memory and sends it in the first WebSocket frame. Authentication failure returns to the gate with a safe error. Refreshing or closing the tab discards the token and requires entry again. This gate is web-only startup UI; after authentication, native and web render the same approved application shell.

Normal HTTP is retained only for static assets and health/readiness probes. The application API is WebSocket-only.

## Workspace and dependency direction

The initial Rust workspace has these crates and applications:

```text
crates/
  k10s-protocol/   # wire envelopes, payloads, IDs, errors, compatibility helpers
  k10s-ui/         # egui views, workspace state, protocol client facade
  k10s-backend/    # BackendKernel and internal runtime/operation/session modules
  k10s-server/     # Axum network adapter, auth, assets, health, server lifecycle
apps/
  k10s-desktop/    # native entry point and embedded-server launcher
  k10s-web/        # WASM entry point
  k10s-server/     # standalone server entry point
```

Dependency direction is one-way:

```text
k10s-protocol <- k10s-ui <- desktop/web apps
k10s-protocol <- k10s-backend <- k10s-server <- desktop/server apps
```

`k10s-protocol` has no egui, kube-rs, Tokio runtime, or operating-system dependency. `k10s-ui` compiles for both native and `wasm32-unknown-unknown`. `k10s-backend` and `k10s-server` are native-only.

The real and fake Kubernetes adapters are internal modules in `k10s-backend`; they do not become separate crates in the first version.

## Module responsibilities and interfaces

### UI module

**Does:** Renders the approved egui interface; owns presentation and workspace state; turns user actions into protocol requests; projects server snapshots and events into rows and details.

**Used through:** `K10sApp` and a small protocol client facade.

**Depends on:** `k10s-protocol`, egui/eframe, and a cross-platform WebSocket client. It does not depend on kube-rs or backend types.

### Protocol client module

**Does:** Authenticates, correlates requests and responses, manages subscriptions, detects event gaps, applies reconnect policy, and exposes bounded inboxes to the UI.

**Used through:** Behavior-oriented methods such as bootstrap, query, execute, subscribe, open logs, and open exec. UI code does not manually construct wire envelopes.

**Depends on:** `k10s-protocol` and the native/WASM WebSocket adapter.

### Server adapter module

**Does:** Serves assets and probes; upgrades WebSocket connections; authenticates the first frame; enforces connection, frame, and subscription limits; translates wire payloads to the backend interface; attaches correlation IDs; and manages connection lifetimes.

**Used through:** An embeddable `Server` lifecycle used by both desktop and standalone entry points.

**Depends on:** `k10s-protocol`, `k10s-backend`, Axum, Tower, and Tokio.

### Backend kernel module

**Does:** Owns all Kubernetes-facing product behavior: contexts, discovery, watches, caches, details, capability projection, validation, mutation state, logs, exec, errors, and shutdown.

**Used through:** Typed query, execute, and subscription behavior. The network adapter and tests cross the same interface.

**Depends on:** An internal Kubernetes access interface implemented by the real and fake adapters.

### Kubernetes adapter

**Does:** Loads contexts, constructs cluster clients, discovers resources, reads resources, opens recoverable watch streams, submits Kubernetes operations, performs server-side dry runs, streams logs, and opens exec sessions.

**Used through:** A behavior-level internal interface owned by the backend kernel. It returns Kubernetes identities, opaque resource versions, normalized errors, and async streams; it does not expose kube-rs types outside `k10s-backend`.

**Adapters:** kube-rs and deterministic fake.

## Backend kernel internals

### Context registry

- Loads kubeconfig only on the backend host.
- Exposes context names and safe cluster metadata; it never exposes credentials or raw kubeconfig to the UI.
- Creates and retires cluster runtimes.
- Treats a context switch as prepare-then-commit: failure to connect or discover the destination leaves the current context usable.
- Supports kubeconfig exec authentication plugins only on the backend host.

### Cluster runtime

One supervised cluster runtime exists for each context currently needed by a connected client. The approved UI has one globally selected context, so the normal case is one active runtime. A previous runtime may remain alive only for a short, bounded handover or linger period.

The runtime owns:

- API discovery and discovered GVK/scope/capability metadata.
- Demand-driven list/watch tasks keyed by context, GVK, and API scope.
- Normalized resource-summary caches.
- Selected-resource detail reads.
- RBAC capability projections.
- Watch health, last successful synchronization time, and stale state.

The first subscriber for a key starts its watch. Subscribers share the same cache and watch. The final unsubscribe starts a short configurable linger timer to prevent rapid tab changes from repeatedly creating watches. Expiration cancels the watch and releases its cache subject to the global cache budget.

Built-in workload summaries use typed k8s-openapi objects. Arbitrary resources use discovery plus `DynamicObject`. Dynamic list summaries contain standard identity and metadata plus configured generic columns; full unstructured content is fetched on detail demand.

### Resource cache and revisions

Kubernetes `resourceVersion` is opaque and remains inside the Kubernetes adapter and cache. It is never compared numerically by the UI.

Each frontend subscription instead has a monotonic `BackendRevision`. Initial synchronization is atomic:

1. Begin an initialization buffer.
2. Consume initial objects from the recoverable Kubernetes watcher.
3. Normalize objects into summary rows.
4. When initialization completes, atomically replace the live cache.
5. Publish `SnapshotBegin`, bounded `SnapshotChunk` messages, and `SnapshotEnd` with a new backend revision.
6. Publish later apply/delete deltas with increasing backend revisions.

If Kubernetes watch history is unavailable, the adapter reinitializes. The old cache remains visible as stale until the new initialization completes; a half-built list is never published.

Details and YAML are fetched on selection rather than stored for all 50,000 objects. A list cache keeps only the normalized fields needed by list rendering, filtering, sorting, identity, status, and age.

### Metrics collector

CPU and memory usage come only from the Kubernetes Resource Metrics API, `metrics.k8s.io/v1beta1`, normally served by metrics-server or a compatible adapter. The collector uses dynamically discovered `NodeMetrics` and `PodMetrics`; it does not infer live usage from requests, limits, node capacity, or allocatable values.

Metrics are polled and cached rather than watched because the Resource Metrics API exposes point-in-time samples. Polling starts only while Overview, Nodes, or another metrics-consuming view has an active subscription, uses a configurable interval, and stops after a short linger period. The latest sample retains its source timestamp and collection window.

- Overview CPU and memory usage aggregate available `NodeMetrics` samples and report coverage when some nodes are missing.
- Node rows combine live usage from `NodeMetrics` with capacity/allocatable values from core `Node` objects.
- Pod capacity is scheduled pod count versus summed node pod allocatable capacity from core APIs; it does not require metrics-server.
- Pod/container usage, when a detail view requires it, comes from `PodMetrics`.
- If the Metrics API is absent, forbidden, stale, or partially populated, affected values are `Unavailable` or `Partial` with timestamp/coverage details. They are never reported as zero.

The metrics collector is an internal cluster-runtime module and uses the existing Kubernetes seam. Prometheus, custom metrics, external metrics, and direct kubelet Summary API access are non-goals for the first release.

### Operation engine

The engine owns scale, restart, delete, YAML apply, suspend/resume, run-now, and create-from operations. It enforces:

- Exact context, GVK, namespace/scope, name, UID, and resourceVersion checks where applicable.
- Capability and RBAC preflight for explanatory UI state. The Kubernetes API remains the final authorization authority.
- A client-supplied idempotency key for each submission.
- A server-generated `OperationId` and one in-flight operation per prohibited duplicate target/action combination.
- Structured states: accepted, running, succeeded, failed, cancelled-before-submit, or outcome-unknown.

Operation submission returns promptly with `OperationId`. Progress and terminal state are delivered over the control socket. The UI can query an operation by ID after reconnect.

The idempotency table and operation history are bounded and in-memory. A backend restart changes `server_instance_id`; clients treat previously in-flight operations as outcome-unknown and refresh their targets before permitting another mutation.

### YAML validation and apply

The UI owns the editable text and local diff presentation. The backend owns authoritative validation.

Validation proceeds as follows:

1. Parse YAML using a maintained YAML 1.2 parser/Serde adapter.
2. Convert to the Kubernetes JSON representation.
3. Reject changes to target kind, namespace, or name and verify the original UID.
4. Perform schema checks when discovery data makes them available.
5. Submit server-side dry run against the exact target and current resourceVersion.
6. Return results and an opaque, expiring validation ticket stored by the backend.

The ticket is bound to server instance, context, GVK, scope, name, UID, resourceVersion, and a cryptographic hash of the exact buffer. Any mismatch, edit, resource refresh, expiration, or server restart invalidates the ticket. Apply requires the ticket and the same buffer hash.

Conflict and unknown-outcome responses preserve the client buffer. The engine never automatically retries an apply whose submission outcome is unknown.

### Stream and session hub

The hub owns connection tickets and lifetimes for resource events, logs, and exec.

- A logs or exec request on the control socket returns a short-lived, single-use `StreamTicket` bound to the exact context, resource UID, container, and requested parameters.
- The dedicated stream socket must authenticate normally and redeem that ticket.
- Logs use a bounded ring buffer and report truncation counts. Disconnect retains text already present in the UI and permits explicit reconnect with new log parameters.
- Exec supports stdin, stdout, stderr, terminal resize, and exit status. It never starts implicitly. Disconnect or parent cancellation terminates the Kubernetes exec session; exec is not resumed or replayed.

## WebSocket protocol

### Physical connections

The application API uses one transport technology but multiple physical connections.

1. **Control socket** at `/api/v1/control`
   - One per UI session.
   - Bootstrap, query, refresh, validation, execute, cancel, resource subscription, connection status, and operation status.

2. **Logs socket** at `/api/v1/logs`
   - One per active log stream.
   - Requires a short-lived stream ticket issued on the control socket.

3. **Exec socket** at `/api/v1/exec`
   - One per active exec session.
   - Requires a short-lived stream ticket issued on the control socket.

Separate bulk/interactive sockets prevent TCP and application-queue contention from delaying control events.

### Authentication handshake

Every application socket begins with a JSON `Hello` frame before other messages are accepted:

```text
Hello {
  protocol_major,
  protocol_minor,
  capabilities,
  access_token,
  optional server_instance_id,
  optional session_id,
  optional last_acked_sequence,
  optional stream_ticket
}
```

The access token is placed in the first frame rather than the URL because URLs are commonly logged and browser WebSocket clients cannot reliably set arbitrary authorization headers. The unauthenticated connection has a short handshake deadline, small message limits, and a global connection limit.

The server responds with `Welcome`, negotiated capabilities, new session identifiers, and resume status, or closes with a structured reason.

### Control envelope

Control messages are UTF-8 JSON text frames. The outer envelope is stable and parses the message kind before the payload:

```text
Envelope {
  kind: string,
  optional request_id,
  optional subscription_id,
  optional sequence,
  payload: JSON value
}
```

The protocol helper then decodes the payload into a known Rust type. This allows an old peer to report or ignore an unknown kind instead of failing to deserialize an entire tagged enum.

Message families are:

- `Request` / `Response`
- `CancelRequest`
- `Subscribe` / `Subscribed` / `Unsubscribe` / `Complete`
- `Event`
- `SnapshotBegin` / `SnapshotChunk` / `SnapshotEnd`
- `OperationUpdate`
- `ResyncRequired`
- `Error`
- `Ping` / `Pong`
- `ShutdownNotice`

Each request has a unique request ID and optional deadline. Each mutation also has an idempotency key. Each event has a connection sequence and, where applicable, a subscription ID and backend revision.

### Stream frames

Logs and exec sockets use text frames for handshake, terminal state, structured errors, and close reasons. Bulk content uses binary frames with a small versioned header containing frame kind and sequence followed by raw payload bytes. Because each physical connection owns one stream session, the data header does not repeat a stream UUID.

Control protocol v1 remains JSON for inspection and dynamic-resource support. List snapshots contain normalized view models and are chunked; they do not contain full YAML. A binary control encoding is not introduced until profiling proves JSON is a bottleneck. A future encoding must be negotiated as a capability and retain the same semantic messages.

### Compatibility

- `Hello` carries major, minor, and capabilities.
- A major-version mismatch is rejected with the server's supported range.
- A minor mismatch negotiates the common capability set.
- Fields added within a major version are optional or have defaults.
- Existing field meanings do not change within a major version.
- Unknown envelope fields are ignored.
- Unknown request kinds receive `UnsupportedMessage`; unknown optional notifications may be ignored and recorded.
- Contract tests keep golden transcripts for the current and previous minor version.

Desktop UI and embedded server come from one build. Web static assets are published with the matching server. Compatibility negotiation still protects long-lived browser tabs and rolling deployment mistakes.

## State ownership

### UI-owned state

- Window instances, geometry, focus/MRU order, collapse state, and split ratios.
- Namespace, search, filters, sort, selected identity, active detail tab, and scroll positions.
- Unsubmitted YAML text, edit mode, and diff presentation.
- Displayed log text and terminal transcript already received.
- A projection of backend connection and operation states.

### Backend-owned state

- Context clients, discovery, Kubernetes resource versions, watch health, and RBAC capability projection.
- Normalized resource caches and backend revisions.
- Validation results and validation tickets.
- Idempotency records, operations, and unknown outcomes.
- Resume journals and subscription lifetimes.
- Kubernetes logs and exec session lifetimes.

A disconnect does not clear UI-owned state. Backend-derived data is marked stale with the last successful update time.

## Reconnect, resume, and resynchronization

The protocol client state machine is:

```text
Connecting -> Ready -> Disconnected -> Reconnecting -> Resumed | Resyncing
```

- Network failures use exponential backoff with full jitter and an upper bound.
- Authentication failure, incompatible major version, and explicit close do not automatically retry.
- The server keeps a bounded, short-lived resume journal per session.
- The client acknowledges the highest contiguous control sequence it has applied.
- On reconnect, the server replays only when the requested sequence remains in the journal.
- Otherwise the server emits `ResyncRequired`; the client reissues subscriptions and receives fresh atomic snapshots.
- Safe reads may be reissued. Mutations are recovered by idempotency key or `OperationId`, never by blind resubmission.
- Dirty YAML, workspace layout, filters, and selection identity survive reconnect.
- Logs retain received text and offer explicit retry.
- Exec disconnect is terminal.

## Backpressure and resource budgets

All application queues are bounded.

The control-socket outbound scheduler has three priorities:

1. **P0:** authentication, shutdown, terminal operation results, and resync-required signals.
2. **P1:** request responses, subscription lifecycle, connection status, and permission status.
3. **P2:** resource deltas.

P0 and P1 are never silently discarded. When pressure persists, the server closes the slow client with a structured overload reason. P2 events for the same resource identity may coalesce to the latest state, but any loss of a contiguous revision must be detectable and lead to resync.

The implementation defines configurable budgets for:

- Control frame and message size.
- Snapshot chunk size and total snapshot size.
- Per-connection outbound queue.
- Client-side inbound queue.
- Resume-journal entries and age.
- Active subscriptions and cached resource keys.
- Concurrent logs and exec sessions.
- Logs ring-buffer bytes and lines.
- Unauthenticated connections and handshake time.

The exact defaults are set from the first load-test baseline rather than guessed in the architecture document. Tests must prove bounded memory and correct overload behavior.

## Error contract

All application errors use:

```text
ErrorFrame {
  code,
  safe_message,
  retryability,
  scope,
  correlation_id,
  details
}
```

Error categories include:

- Protocol: incompatible version, invalid frame, unsupported message, or size limit.
- Access: unauthenticated, forbidden, or capability unavailable.
- Resource: not found, gone, conflict, or stale resource version.
- Validation: invalid YAML, schema failure, dry-run rejection, or expired ticket.
- Transport: disconnected, timeout, or overloaded.
- Stream: truncated logs, slow consumer, or ended session.
- Operation: failed, cancelled before submission, or outcome unknown.
- Internal: unexpected failure identified by correlation ID.

`retryability` is explicit: never, after reconnect, after refresh, or after user action. Raw credentials, kubeconfig contents, access tokens, stack traces, and sensitive Kubernetes response content are never sent to the UI or written to normal logs.

## Security model

### Desktop

- Bind only to `127.0.0.1:0`.
- Generate a high-entropy per-launch token and retain it only in process memory.
- Require authentication in the first frame within a short deadline.
- Rate-limit unauthenticated connections to reduce localhost abuse.
- Never put the token in a URL, file, normal log, or crash report.
- Reject new commands during shutdown and terminate child streams.

### Web

- Deploy behind a TLS reverse proxy or VPN and use WSS.
- Serve UI assets and application sockets from the same origin.
- Require the operator to configure one high-entropy single-user token from a secret source before non-loopback startup.
- Collect that token through the web-only connection gate and send it in the first frame.
- Keep the token in browser session memory by default; do not write it to localStorage.
- Bind to loopback/private addresses by default; require explicit configuration for public interfaces.
- Trust forwarded headers only from configured proxies.
- Apply connection, frame, subscription, and stream limits before expensive work.

UI capability checks are explanatory only. Every mutation is validated in the backend and submitted to the Kubernetes API, which performs final authorization.

## Graceful shutdown

Shutdown proceeds in order:

1. Stop accepting new connections.
2. Send `ShutdownNotice` and reject new mutations.
3. Cancel watches and logs.
4. Terminate exec sessions.
5. Close control and stream sockets with explicit reasons.
6. Close the task tracker and cancel the root token.
7. Drain supervised tasks within a bounded deadline.
8. Exit.

Normal UI navigation still applies the approved dirty-YAML and active-shell guards. Forced process termination cannot show a guard, but backend cancellation must still close remote exec sessions promptly when the process is able to run cleanup.

## Technology stack

The Rust toolchain and release dependency graph are reproducible through an exact `rust-toolchain.toml` and checked-in `Cargo.lock`. Versions explicitly shown below are the approved 2026-08-21 core baselines. The implementation plan will record exact compatible versions for supporting direct dependencies such as `serde_json`, UUID, Tower, `tower-http`, tracing, `egui_kittest`, `proptest`, and `k8s-openapi` after resolving the complete graph. Dependency updates are explicit changes reviewed with their tests.

| Concern | Choice |
| --- | --- |
| Language | Rust 1.97.1, edition 2024 |
| UI | `eframe`, `egui`, `egui_extras` 0.36.1 |
| Renderer | wgpu with WebGPU preference and WebGL fallback on web |
| Cross-platform WebSocket client | `ewebsock` 0.8.0 |
| Protocol serialization | `serde` 1.0.229, matching `serde_json`, UUID IDs |
| Async runtime | Tokio 1.53.1 |
| Task lifecycle | `tokio-util` 0.7.19 `CancellationToken` and `TaskTracker` |
| Server | Axum 0.8.9, matching Tower and `tower-http` |
| Kubernetes | kube 4.2.0 and the exact compatible `k8s-openapi` selected during dependency resolution |
| Dynamic Kubernetes resources | kube discovery, `Api<DynamicObject>` |
| YAML | `serde_yaml_ng` 0.10.0 behind one internal parser module |
| Observability | `tracing`, `tracing-subscriber`, `tower-http` trace |
| UI testing | `egui_kittest` matching egui 0.36.1 |
| Property testing | `proptest` for protocol and state-machine invariants |

The YAML parser is hidden behind one internal module so it can be replaced without changing the backend interface if its maintenance status changes.

No database, actor framework, GraphQL implementation, gRPC toolchain, or general event bus is added in the first release.

## Persistence

- Kubernetes remains the resource source of truth.
- Backend resource caches, resume journals, validation tickets, idempotency records, and operations are in memory.
- Native UI preferences use eframe persistence.
- Web UI preferences use browser local storage only for non-secret presentation settings.
- The web connection token is supplied at the connection gate and retained only in memory for the current tab.
- Access tokens, kubeconfig content, YAML edit buffers, logs, and shell transcripts are not persisted by default.
- A backend restart invalidates tickets and sessions and changes `server_instance_id`.

## Observability

Every request, subscription, operation, and stream receives a correlation ID. Structured tracing includes:

- Server instance and session IDs.
- Context name and safe resource identity fields.
- Request kind, subscription key, operation kind, and lifecycle transitions.
- Queue pressure, dropped/coalesced P2 events, resyncs, and slow-client disconnects.
- Watch restart and initialization duration.
- Logs/exec open, close, and byte counts without content.

Tokens, kubeconfig, YAML bodies, terminal input/output, and log contents are excluded from normal tracing.

## Testing strategy

### Pure state and protocol tests

- Workspace, split-pane, selection, context-guard, YAML-ticket, and operation state machines.
- Golden JSON transcripts for the current and previous protocol minor.
- Major/minor negotiation and unknown field/message behavior.
- Property tests for duplicated, reordered, cancelled, gapped, expired, and retried messages.
- Binary stream frame round trips and malformed-frame rejection.

### Module-interface tests

- Backend kernel behavior through its public interface using the deterministic fake adapter.
- One behavioral contract suite shared by fake and real Kubernetes adapters where behavior is common.
- Axum server and real loopback WebSocket client integration.
- Authentication deadline, limits, correlation, cancellation, reconnect, resume, and resync.
- Slow-client and full-queue cases proving bounded memory and explicit closure.
- Desktop server start/readiness/shutdown lifecycle.

### UI tests

- Pure workspace tests independent of egui.
- `egui_kittest` AccessKit interaction tests for the approved UI behaviors.
- Fixed-size visual snapshots with per-platform tolerances.
- UI protocol projection tests for stale, forbidden, conflict, gone, truncated, disconnected, and outcome-unknown states.

### Browser and cluster E2E

- WASM/browser smoke tests for the connection gate, authentication, control requests, reconnect, tab refresh token loss, and one logs/exec lifecycle.
- Ephemeral kind-cluster tests for discovery, list/watch, context behavior, server-side dry run, apply, logs, exec, RBAC denial, and resource deletion.
- Metrics tests with Resource Metrics API available, absent, forbidden, partial, and stale; usage must never fall back to requests, capacity, or zero.
- macOS, Linux, and Windows native build and launch smoke tests.

## Capacity verification

The medium-large target is accepted only when automated load scenarios prove:

| Scenario | Required behavior |
| --- | --- |
| 50,000 normalized objects | Chunked snapshot; no oversized frame; virtualized list; usable first view does not wait for all details |
| 10,000-event burst | Bounded memory; same-resource P2 coalescing; no starvation of P0 operation events |
| Slow browser consumer | Explicit overload close and resync path; no unlimited buffering |
| Sustained high-throughput logs | Control responsiveness and UI interaction remain usable; truncation count is visible |
| Kubernetes watch disconnect/history loss | Old snapshot remains stale; new initialization atomically replaces it |
| Backend restart during mutation | New server instance is detected; in-flight client operation becomes outcome-unknown; target refresh is required |

The first working baseline records concrete memory, frame-time, startup-time, and event-latency budgets on named reference hardware and CI runners. Those numbers then become regression thresholds. Architecture acceptance requires bounded, isolated, recoverable behavior before numeric optimization.

## Delivery phases

### Phase 1: Fully connected static prototype

- Create the complete workspace and protocol.
- Run the same server used by production.
- Implement the backend kernel and deterministic fake Kubernetes adapter.
- Build native and web UI entry points.
- Exercise every approved UI state, mutation, watch, logs, exec, reconnect, and failure path through real WebSocket connections.
- Establish protocol, UI, and capacity baselines.

### Phase 2: Real Kubernetes integration

- Add the kube-rs adapter behind the existing Kubernetes seam.
- Load kubeconfig contexts and discovery.
- Add on-demand watch/cache normalization for built-ins and dynamic resources.
- Add demand-driven Resource Metrics API polling with partial-availability projection.
- Implement real dry-run, mutations, logs, and exec.
- Run the shared adapter contract and kind E2E suites.
- Package desktop applications and the standalone web server.

Phase 2 must not require a UI data-source rewrite or protocol redesign. Any such requirement is an architecture regression that must be reviewed explicitly.

## Deliverables

- macOS, Linux, and Windows desktop applications containing the embedded server.
- A standalone server binary/container serving matching WASM and static assets.
- A deterministic fake profile for development, demos, tests, and the static phase.
- Protocol transcript fixtures and compatibility tests.
- Ephemeral-cluster E2E tooling.
- Load and fault-injection scenarios for the stated capacity target.
- Deployment documentation for kubeconfig, bind address, token secret source and web connection gate, TLS reverse proxy, and health probes.

## Verification criteria

- Desktop and web use the same control, logs, and exec wire contracts.
- No UI module depends on kube-rs or reads fixtures directly.
- The static prototype reaches the UI only through the fake Kubernetes adapter, backend kernel, server adapter, and real WebSocket client.
- Replacing the fake with the kube-rs adapter does not change UI-facing data flow.
- Only the two documented external seams are public replacement points.
- All queues and journals are bounded and have tested overload behavior.
- Kubernetes `resourceVersion` never becomes a numerically compared frontend revision.
- Watch reinitialization atomically replaces cache state.
- Control traffic remains isolated from logs and exec traffic.
- Dirty YAML and UI workspace state survive control reconnect.
- Exec never starts implicitly and never resumes after disconnect.
- Mutations require exact target identity, use idempotency keys, and expose outcome-unknown without blind retry.
- Desktop binds only to a random loopback port and uses a per-launch token.
- Web tokens are not placed in URLs or persisted by default.
- A fresh web tab obtains its operator-provided token through the connection gate; the server never injects it into assets or returns it from an endpoint.
- CPU and memory usage comes from the Resource Metrics API, while missing or partial metrics remain explicitly unavailable/partial rather than becoming zero or inferred usage.
- Protocol compatibility tests cover the current and previous minor.
- Capacity tests cover 50,000 objects, a 10,000-event burst, slow clients, high-throughput logs, watch reinitialization, and backend restart during mutation.

## References

- [eframe 0.36.1](https://docs.rs/crate/eframe/0.36.1)
- [Axum WebSocket support](https://docs.rs/axum/0.8.9/axum/extract/ws/)
- [ewebsock cross-platform client](https://docs.rs/ewebsock/0.8.0/ewebsock/)
- [Tokio](https://docs.rs/tokio/1.53.1/tokio/)
- [tokio-util task lifecycle](https://docs.rs/tokio-util/0.7.19/tokio_util/task/struct.TaskTracker.html)
- [kube](https://docs.rs/kube/4.2.0/kube/)
- [Kubernetes API watch and resource-version semantics](https://kubernetes.io/docs/reference/using-api/api-concepts/)
- [Kubernetes Resource Metrics API](https://kubernetes.io/docs/reference/external-api/metrics.v1beta1/)
- [Browser WebSocket backpressure limitations](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API/index.html)
