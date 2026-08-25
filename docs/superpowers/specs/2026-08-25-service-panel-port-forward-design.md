# Service Panel and Desktop Port Forward Design

**Status:** Proposed

**Scope:** Add a Services panel to the shared k10s workspace and allow the native desktop application to open loopback-only Kubernetes Service port forwards. This document defines behavior and architecture only; it does not authorize implementation.

## Goals

- Make core `v1/Service` objects discoverable in a dedicated, namespace-aware panel.
- Show enough normalized Service information to choose a declared Service port without parsing YAML in the UI.
- Let desktop users start, observe, copy, and stop local port-forward sessions.
- Keep every local listener on loopback and make the feature unavailable from the standalone web deployment by default.
- Preserve the existing boundary: UI code consumes normalized protocol data and never imports kube-rs.

## Non-goals for the first version

- UDP forwarding; Kubernetes port-forward is TCP-only.
- Forwarding ExternalName Services or endpoints that do not identify a Pod.
- Acting as a load balancer. One ready backing Pod is selected when a session starts and remains pinned for that session.
- Automatically reconnecting a session to a replacement Pod.
- Persisting or restoring sessions after application restart.
- Exposing a listener on LAN, wildcard, IPv6 wildcard, or a user-selected interface.
- Enabling port-forward in the standalone server or browser client.

## User experience

### Launcher and window

Add a singleton **Services** item to a new, initially expanded **Network** launcher group. Selecting it opens or focuses one Services window. The window follows existing workspace behavior for focus, geometry, context switching, namespace filtering, search, sorting, integrated details, and deliberate detail pop-outs.

The list columns are:

| Column | Content |
| --- | --- |
| Name | Service name |
| Namespace | Namespace |
| Type | ClusterIP, NodePort, LoadBalancer, or ExternalName |
| Cluster IP | Primary cluster IP, `Headless`, or `None` |
| Ports | Compact declared ports such as `http 80→8080/TCP` |
| Age | Existing normalized creation time rendering |

The selected Service detail has these tabs:

- **Overview:** type, cluster IPs, selector summary, session affinity, external name/traffic policy when present.
- **Ports:** structured Service ports and the desktop port-forward controls.
- **YAML:** the existing guarded YAML workflow.
- **Events:** existing normalized events.

### Starting a forward

Each TCP entry in the Ports tab shows:

- Service port name, Service port, resolved target port, protocol, and optional app protocol.
- A local port input. Blank or `0` means “choose an available port.” An explicit value must be in `1..=65535`.
- A **Start** button.

Starting is explicit. Opening the panel never creates a listener. On success, the row shows:

- `127.0.0.1:<local-port>` with a copy button.
- The selected Pod name and remote Pod port.
- Session state and a **Stop** button.

Multiple forwards are allowed when their local addresses do not conflict. Repeating Start for the same Service UID and Service-port identity focuses the existing session rather than silently creating a duplicate.

### Session states

The UI uses the following state machine:

```text
Starting -> Active -> Stopping -> Stopped
    |          |
    +-------> Failed
```

Connection lifetime is separate from session lifetime. Each accepted local TCP connection owns its own Kubernetes port-forward stream, and that stream normally ends when either side closes: an upstream application finishing a response, or the local client disconnecting. Clean EOF on a forwarded connection is a successful connection completion, never a session failure. The listener keeps accepting, and a second connection started immediately after an upstream close must succeed.

`Failed` retains a short safe reason and permits Retry. It is reserved for faults that make the pinned target unusable, not ordinary connection turnover. A session moves to `Failed` only when:

- Its listening socket fails.
- The pinned Pod disappears.
- Three consecutive new port-forward streams fail with a transport or protocol error before transferring any byte, indicating the pinned target is gone or unreachable rather than a peer hanging up.
- A data pump terminates with an error other than clean EOF.

A single abnormal stream error fails only that one connection. The session does not automatically select a different Pod because that could move a connection to a different workload instance without operator intent.

### Desktop-only presentation

The server advertises `service.portForward` only when its runtime configuration enables the feature. The desktop embedded server enables it; the standalone server does not. The UI renders the Ports data everywhere, but renders Start/Stop controls only when the negotiated capability is present. Web users see “Port forwarding is available in the desktop application.”

Capability absence is not the security boundary. The server also rejects every port-forward request when disabled.

## Protocol model

Add a `port_forward` protocol module. All payloads use existing request/response envelopes.

### Normalized Service projection

Service data must not be extracted from manifest text in the UI. Add an optional kind-specific projection shared by resource lists and detail responses:

```rust
pub enum ResourceProjection {
    Service(ServiceProjection),
}

pub struct ServiceProjection {
    pub service_type: String,
    pub cluster_ips: Vec<String>,
    pub selector: BTreeMap<String, String>,
    pub external_name: Option<String>,
    pub session_affinity: Option<String>,
    pub ports: Vec<ServicePort>,
}

pub struct ServicePort {
    pub name: Option<String>,
    pub service_port: u16,
    pub target_port: TargetPort,
    pub node_port: Option<u16>,
    pub protocol: TransportProtocol,
    pub app_protocol: Option<String>,
}
```

The same normalized projection feeds both surfaces:

- `ResourceListRow.projection: Option<ResourceProjection>` — added to the existing normalized list-row struct with `#[serde(default)]`. Snapshot pages and upsert deltas embed `ResourceListRow` (`ResourceSnapshotPage.rows`, `ResourceChanged.row`), so the Services table renders Type, Cluster IP, Ports, and Age for every row straight from snapshots and live deltas, before anything is selected. The UI never parses `summary`; `summary` stays untouched for existing generic windows.
- `ResourceDetailResponse.projection: Option<ResourceProjection>` — the same enum on detail responses.

Both fields are optional and default to `None`: older payloads without the field decode unchanged on new clients, and older clients ignore the unknown field on newer payloads. Servers populate the projection for core/v1 Service rows and details wherever the projection ships. The protocol minor version increases; the major version remains unchanged.

### Lifecycle requests

Use three request kinds:

- `portForward.start`
- `portForward.stop`
- `portForward.list`

The start request carries the exact Service `ResourceIdentity`, a stable port selector (port name when present, otherwise Service port number), and requested local port. It never accepts an arbitrary remote host or Pod supplied by the UI.

The start response returns a generated session ID, bound loopback address, Service identity, selected Pod identity, resolved numeric Pod port, and initial state. Stop is idempotent by session ID. List returns every non-expired session owned by this embedded-server instance so a control WebSocket reconnect can reconstruct the panel.

Session status changes use a bounded `portForward.sessions` subscription. Events contain complete session snapshots and a monotonic revision, allowing coalescing and safe replay. Terminal sessions are retained for a short bounded interval for diagnostics, then removed.

Port-forward lifecycle is neither a Kubernetes mutation nor a durable Operation Engine operation. It gets a dedicated `PortForwardManager` owned by the server lifecycle and cancelled during server shutdown.

## Runtime architecture

```text
Services panel
    -> control WebSocket request
        -> server capability gate
            -> Backend Kernel validates normalized request
                -> KubeAdapter resolves Service + EndpointSlice + Pod
                    -> PortForwardManager binds 127.0.0.1
                        -> kube-rs Pod port-forward stream
```

### Ownership

- `k10s-ui` owns panel state, input validation feedback, and rendering.
- `k10s-protocol` owns normalized Service and session payloads.
- `k10s-server` owns capability enforcement, request dispatch, session subscription, listener tasks, shutdown, and resource bounds.
- `k10s-backend` owns Kubernetes resolution, identity/RBAC validation, and creation of a Pod port-forward stream through kube-rs.
- `k10s-desktop` enables the server feature in its embedded `ServerConfig`.
- `k10s-server` binary leaves it disabled by default; `k10s-web` has no host-side escape hatch.

The manager belongs in `k10s-server`, rather than `k10s-ui` or the operation engine, because it owns local TCP resources and must share the embedded server's Tokio runtime and cancellation token. Kubernetes object resolution still crosses the Backend Kernel seam.

The byte stream is an internal, non-serializable seam. Add a cloneable `PortForwardConnector` exposed by the Backend Kernel with two behavior-level methods:

```rust
async fn resolve_service_port(request: PortForwardTarget) -> Result<ResolvedPortForward, BackendError>;
async fn connect(resolved: &ResolvedPortForward) -> Result<AsyncReadWrite, BackendError>;
```

`ResolvedPortForward` contains only backend-owned values: context, Service UID, Pod name/UID, and numeric Pod port. `AsyncReadWrite` is an opaque boxed async byte stream; kube-rs types never cross the backend crate boundary. The manager resolves once when the session starts and calls `connect` once per accepted local TCP connection. Neither this connector nor its stream is placed in `QueryResult`, serialized onto the control WebSocket, or exposed to the UI.

### Service-to-Pod resolution

For every start request, the backend:

1. Fetches the named core/v1 Service in the supplied context and namespace.
2. Verifies the live Service UID equals the request identity.
3. Rejects non-TCP ports, ExternalName Services, absent ports, and ambiguous unnamed port selection.
4. Lists `discovery.k8s.io/v1` EndpointSlices with label `kubernetes.io/service-name=<service-name>`.
5. Discards every slice whose `metadata.ownerReferences` contains no entry whose UID equals the fetched Service UID. The name label alone is not authoritative: after a Service is deleted and recreated, stale slices can coexist with current ones until asynchronous cleanup, and their Pods would pass the later Pod UID check. Controller-created slices carry an owner reference to their Service, so comparing that UID binds each slice to the exact fetched object. Ownerless or hand-crafted slices without a matching Service owner reference are skipped in this first version; accepting them requires a separately reviewed resolution policy.
6. Keeps endpoints whose `conditions.ready` is not false and whose `targetRef` is a Pod in the same namespace.
7. Matches the requested Service port to the EndpointSlice port and obtains a numeric target port.
8. Sorts candidates by Pod name then UID and chooses the first, making selection deterministic and testable.
9. Fetches that Pod and verifies its UID before opening `pods/portforward`.

The session pins the selected Pod UID and numeric remote port. Endpoint changes after startup do not retarget it. If no eligible endpoint exists, Start fails without binding a local socket.

The first version deliberately does not fall back to legacy Endpoints, Service selectors, or arbitrary endpoint IPs. Supporting those later requires a separately reviewed resolution policy.

### Local listener and data flow

- Bind only `127.0.0.1:<requested-port>`; port `0` lets the OS assign an available port.
- Bind before returning success, so an advertised address is immediately usable.
- Each accepted local TCP connection obtains its own Kubernetes Pod port-forward stream for the pinned remote port.
- Pump bytes bidirectionally with bounded copy buffers and cancellation-aware tasks.
- Closing one local connection does not close the listening session.
- Stop cancels the accept loop, closes active pumps, releases the local port, emits `Stopped`, and succeeds if repeated.
- Embedded server shutdown cancels and joins every session before its runtime exits.

### Context-switch atomicity

Stopping all sessions and committing the context switch must be atomic with respect to `portForward.start`. A UI confirmation alone cannot close this race: a Start already resolving through the backend, or another authenticated Start arriving concurrently, can bind a listener after the stop-all snapshot and leave exactly the invisible old-context forward this design forbids.

The `PortForwardManager` therefore owns a transition gate:

- The manager keeps a monotonic epoch counter. Session publication happens under the manager lock and stamps the epoch observed at Start time.
- A context switch takes the manager write side under that same lock, increments the epoch, stops and joins every active session, and only then lets the backend commit the switch. Additional `portForward.start` requests serialize behind the gate instead of interleaving with the drain.
- A Start whose resolution raced the switch detects the epoch mismatch at publication time, aborts without binding any socket, and returns a typed retryable context-transition error.
- The sessions subscription emits the resulting terminal snapshots, so every connected panel converges on zero old-context sessions.

The client-side **Stop all and switch** blocker remains as presentation: it confirms intent and lets the UI drain sessions early when possible. The server-side gate is the enforcement point, including for clients that bypass the prompt.

## Safety and resource limits

- Loopback binding is hard-coded server-side and is not a request field.
- Default maximum: 16 active sessions per embedded server, 32 simultaneous accepted connections total, and 8 connections per session.
- Session IDs are random opaque values and are scoped to the authenticated server instance.
- Requests validate Service identity and context; no kubeconfig paths, credentials, raw Kubernetes errors, or arbitrary socket targets cross the protocol.
- Error text uses stable categories: unavailable endpoint, forbidden, conflict/local port in use, vanished/recreated resource, unsupported Service, and transport closed.
- Kubernetes authorization remains authoritative. Expected access is `get` on Services and Pods, `list` on EndpointSlices, and `create` on `pods/portforward`. Advisory SelfSubjectAccessReview results may disable the button but never replace the real API check.
- Context switching drains all sessions through the manager's transition gate (see “Context-switch atomicity”); the navigation confirmation is presentation only. Keeping invisible forwards alive across contexts is unsafe and confusing.
- Closing the Services window does not implicitly stop sessions; active sessions remain visible via a count badge and are recoverable by reopening the panel.

## Workspace changes

Add `WindowKind::Services`, `LauncherItem::Services`, and `WindowContent::Services(ServiceWindowState)`. Service windows are singleton because active local sessions are global desktop resources and should not be fragmented across duplicate windows.

`ServiceWindowState` contains only UI/workspace data: namespace filter, search, sort, selection, split ratio, local-port draft per Service port, and selected session. Authoritative rows, projections, and session snapshots remain in the client state/feed.

Dirty YAML and active port forwards are different guards:

- Existing dirty-YAML rules continue unchanged.
- Context switch with active forwards adds a blocker listing the number of sessions and offers **Stop all and switch** or **Cancel**. This prompt is presentation only; the server-side transition gate enforces atomicity even if it is bypassed.
- Window close is not blocked because it does not end sessions.
- Application shutdown stops sessions automatically; it does not prompt.

## Failure handling

| Failure | Behavior |
| --- | --- |
| Feature disabled | Typed unsupported-capability error; controls hidden |
| Service UID changed | Typed vanished/recreated error; no listener bound |
| No ready Pod endpoint | Start fails; Retry remains available |
| Missing RBAC | Safe forbidden error naming the required resource/verb |
| Local port occupied | Conflict error; preserve the input for correction |
| Pod disappears after Start | Session becomes Failed and listener closes |
| Upstream closes one forwarded connection | That connection completes; session stays Active |
| Start races a context switch | Typed retryable context-transition error; no listener bound |
| Control WebSocket reconnects | `portForward.list` plus subscription rebuilds state; data forwarding continues |
| Embedded server shuts down | All listeners and pumps are cancelled and joined |

## Verification strategy

Implementation follows TDD in these slices:

1. **Protocol contracts:** JSON round trips, older payload defaults, invalid ports/protocols, list-row projection defaults on legacy payloads and populated Service projections on snapshot pages and `resource.changed` deltas, golden minor-version negotiation.
2. **Workspace/UI:** launcher singleton behavior, namespace/search/sort, structured port rendering from row projections without parsing `summary`, capability gating, duplicate Start prevention, context-switch guard, accessibility labels.
3. **Backend resolution:** recorded Kubernetes API tests for UID binding, EndpointSlice port matching, owner-reference slice binding including a mixed stale/current-slice set after Service delete/recreate, deterministic ready-Pod selection, named target ports, ExternalName/UDP/no-endpoint rejection, and sanitized failures.
4. **Server manager:** loopback-only binding, OS-assigned port, occupied port, multiple local connections, a second connection succeeding after upstream EOF, limits, idempotent stop, reconnect/list reconstruction, in-flight Start aborted by a concurrent context switch with no listener left behind, concurrent Start/switch interleavings yielding no old-context session, and cancellation on shutdown.
5. **Desktop integration:** embedded config advertises and accepts `service.portForward`; standalone config neither advertises nor accepts it.
6. **Real kind E2E:** create a Deployment and Service, start an automatic local port, issue HTTP through it, stop it, prove the port is released, then repeat with an explicit local port.

The release gate must run the real kind port-forward test on the Linux self-hosted runner. Windows desktop receives a loopback lifecycle test; a real Windows-cluster forward may remain an environment-specific manual check unless the runner has stable access to a test cluster.

## Delivery slices

1. Add normalized Service projections and backend list/detail coverage.
2. Add the Services launcher/window without forwarding controls.
3. Add backend resolution and the bounded server `PortForwardManager`.
4. Add protocol lifecycle requests/subscription and client state.
5. Enable and render desktop-only Start/Stop controls plus context guard.
6. Add kind, desktop, security, recovery, documentation, and packaging gates.

Each slice remains independently testable. The forwarding capability is not advertised until all lifecycle, shutdown, and loopback-security tests pass.
