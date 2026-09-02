# Pod Port Forward and Unified Session Management Design

**Status:** Approved

**Scope:** Add direct Pod port forwarding to the native desktop application and a singleton window that manages both Pod- and Service-originated port-forward sessions. This document extends the existing Service port-forward design and does not replace its security or lifecycle requirements.

## Goals

- Put a **Port Forward** action beside every declared TCP port in Pod details.
- Ask for the local port in a modal before starting a Pod forward.
- Manage all Pod and Service forwards in one global desktop window.
- Reuse the existing bounded, loopback-only session lifecycle, subscription, context-switch gate, and shutdown cleanup.
- Keep arbitrary hosts and undeclared Pod ports outside the protocol boundary.

## Non-goals

- UDP or SCTP forwarding.
- Forwarding a port that is not declared by a Pod container.
- Persisting or restoring sessions after application restart.
- Retargeting a session when its Pod disappears or is recreated.
- Binding any address other than IPv4 loopback.
- Enabling port forwarding in the standalone web application.
- Creating new forwards directly from the management window.

## User experience

### Pod port rows

The Pod Overview renders declared container ports in a dedicated **PORTS** section instead of collapsing them into one metadata value. Each row contains the declared port name when present, container port and protocol, declaring container, and an action area. Pod-spec order is preserved.

TCP rows display **Port Forward**. UDP and SCTP rows remain visible but read-only. The action is present only when the negotiated desktop port-forward capability is available; web users see the existing desktop-only availability treatment.

### Start modal

Selecting **Port Forward** opens a modal that identifies the context, namespace, Pod, container, and remote port. The local-port field is prefilled with the remote container port. It accepts an integer in `0..=65535`; both blank and `0` ask the operating system for an available port. Other non-numeric input is invalid. The same blank-or-zero rule applies when the modal is reused by the Service surface, preserving the existing Service behavior.

The modal has **Start** and **Cancel** actions. Start remains disabled while the input is invalid or a request is pending. A local-port conflict or another recoverable start rejection leaves the modal open, preserves the input, and shows the safe server error. On success the modal closes and the UI exposes the bound `127.0.0.1:<port>` address for copying.

If an equivalent active session already exists, Start returns that session instead of creating another listener. The UI opens or focuses the Port Forwards window and focuses the existing row.

### Launcher and management window

Add a singleton **Port Forwards** launcher item to the Network group. Its badge counts sessions in `Starting`, `Active`, or `Stopping`; failed and retained terminal sessions do not increase the badge.

The Port Forwards window consumes the authoritative global session feed and presents both Pod and Service sessions. Its primary columns are:

| Column | Content |
| --- | --- |
| Target | Target type plus Pod or Service name |
| Namespace | Kubernetes namespace |
| Remote | Pod container and remote port, or Service port and selected backing Pod |
| Local address | Bound loopback address with copy action |
| Status | Starting, Active, Stopping, or Failed, including a safe failure summary |
| Actions | Copy, Stop, or Retry when applicable |

Copy is available once a bound local address exists. Stop is available for starting or active sessions and becomes disabled while stopping. Failed sessions expose Retry. Retry uses the recorded target and requested local port. If that port is no longer available, the session remains failed and explains that a new forward must be started from the Pod or Service source surface with another port.

Retained `Stopped` snapshots render as muted rows with no actions until the server's normal terminal-retention window expires. `Failed` snapshots remain visible with their failure summary and Retry action for the same retention window. Neither state contributes to the launcher badge. The client must therefore retain terminal snapshots supplied by list/events instead of eagerly deleting them; server expiry is authoritative.

The empty state directs users to a Pod's Ports section or a Service's Ports tab. Closing the management window never stops sessions. Reopening it reconstructs state from `portForward.list` and the existing session subscription.

## Protocol model

Generalize the start target while retaining one request family and one session stream:

```rust
pub enum PortForwardTarget {
    Service {
        identity: ResourceIdentity,
        port: PortForwardPortSelector,
    },
    Pod {
        identity: ResourceIdentity,
        container_name: String,
        remote_port: u16,
    },
}
```

`PortForwardStartRequest` carries `target: PortForwardTarget` and `local_port`. Existing Service start call sites migrate to the Service variant. The protocol minor version increases while the major version remains unchanged. New fields added to session payloads use serde defaults where older payload decoding requires them; endpoint compatibility follows the repository's negotiated minor-version policy.

Every session snapshot records enough typed source data to render and retry without querying the source window:

- its `PortForwardTarget`;
- the requested local port, including `0` when automatic allocation was requested;
- the actual bound loopback address;
- the resolved Pod identity and numeric Pod port;
- the existing state, revision, and safe failure information.

Service sessions continue to record both the Service target and resolved backing Pod. Pod sessions use their target Pod as the resolved Pod. No arbitrary address, kubeconfig path, manifest fragment, or unvalidated remote port enters the request.

`portForward.start`, `portForward.stop`, `portForward.list`, and `portForward.sessions` remain the only lifecycle messages. Stop remains idempotent.

### Capability and wire compatibility

Keep `service.portForward` unchanged and add `pod.portForward`. The desktop embedded server advertises both; standalone and web servers advertise neither. Service controls require the Service capability, Pod controls require the Pod capability, and the global management window is available when either capability is present. Server dispatch validates the capability matching the requested target in addition to its runtime feature gate.

The new server accepts both the current legacy Service start payload (`service`, `port`, `localPort`) and the new target-discriminated payload (`target`, `localPort`). New clients continue sending the legacy shape for Service starts, so they remain usable with an older Service-capable server; they send the target-discriminated shape only for Pod starts after negotiating `pod.portForward`.

For list responses and session events, the server encodes the legacy Service session shape for clients on the prior negotiated minor version. On the new minor version it sends the generalized session shape for both targets. An older client can never create a Pod session and does not receive a generalized Pod snapshot. The new client decodes legacy Service snapshots into `PortForwardTarget::Service`, deriving the requested local port from the bound address when the older snapshot has no separate requested-port field. A legacy snapshot therefore cannot preserve the distinction between an automatic `0` request and its assigned port; Retry after that compatibility conversion attempts the derived explicit port. This compatibility path must be covered by protocol and control-socket tests rather than relying on serde defaults alone.

## Backend validation and resolution

The backend connector accepts the generalized target and resolves it to the existing internal `ResolvedPortForward` representation.

For a Pod target, it:

1. Requires the core `v1/Pod` GVK and the request's current context and namespace.
2. Fetches the named Pod and verifies its live UID matches the request identity.
3. Finds exactly the named regular container. Init and ephemeral containers are outside this version's UI and request model.
4. Verifies that container declares the requested numeric port and that its protocol is TCP.
5. Records the verified Pod UID and numeric port in the resolved target.
6. Opens `pods/portforward` only when a local connection is accepted, using the same per-connection stream behavior as Service sessions.

An absent container, changed declaration, recreated Pod, or non-TCP port produces a typed safe rejection and binds no listener. Kubernetes authorization remains authoritative; expected access is `get` on Pods and `create` on `pods/portforward`.

Service resolution retains every existing requirement, including Service UID validation, EndpointSlice ownership checks, deterministic ready-Pod selection, and pinning.

## Session identity and lifecycle

The existing `PortForwardManager` owns both target variants. Equivalent-session detection is target-specific:

- Pod: Pod UID, container name, and remote port.
- Service: Service UID and stable Service-port identity, as today.

The requested local port is not part of equivalence. Starting the same logical target focuses the existing non-terminal session even if a different local port was entered. Failed and stopped sessions do not block a new session.

All existing bounds apply across the combined set: maximum sessions, total accepted connections, and per-session connections. Loopback-only binding, consecutive connection-open failure handling, terminal retention, and per-connection stream isolation remain unchanged.

The manager's context transition gate drains Pod and Service sessions together before committing a context switch. Application shutdown also cancels and joins both target types. No session survives invisibly across either boundary.

## UI state and components

- `WindowKind`, `LauncherItem`, and `WindowContent` gain a singleton Port Forwards variant and minimal view state for sort, selection, and focused session.
- The Port Forwards window itself owns no authoritative sessions; it projects the client feed.
- The Pod detail projection retains structured `PodContainerPort` values so rows can emit a typed start intent. Display strings are not parsed back into authority.
- A reusable start-modal state holds the typed target, remote-port presentation, local-port draft, pending state, and safe error. It can also serve the existing Service entry point so both surfaces use consistent validation and feedback.
- Runtime actions generalize from Service-specific start/stop actions to target-based start, stop, retry, focus, and copy intents.
- Window geometry may be persisted, but active sessions and modal state are not persisted.
- The existing Service Ports tab retains its inline start and active-session controls. It opens the shared modal for new starts and reflects the same global session feed; the Port Forwards window is an additional global management surface, not a replacement.

## Error handling

| Failure | Behavior |
| --- | --- |
| Feature unavailable | Pod controls are hidden and the server rejects direct requests |
| Pod UID changed | Typed vanished/recreated error; no listener is bound |
| Container or port declaration changed | Typed invalid/unsupported target error; prompt the user to refresh |
| UDP or SCTP port | No Pod action; server rejects forged requests |
| Missing RBAC | Safe forbidden error naming the required operation |
| Local port occupied | Start modal remains open and preserves the draft |
| Pod disappears after start | Session becomes Failed and closes its listener under existing manager rules |
| Equivalent session exists | Return it, open/focus the management window, and select its row |
| Retry cannot reclaim local port | Keep failure visible and direct the user to create a new forward |
| Context switch | Stop and join every Pod and Service session before switching |

## Testing

### Protocol

- Round-trip both target variants and generalized session snapshots.
- Verify backward-compatible defaults and negotiated minor-version behavior.
- Reject malformed names, ports outside the wire type's valid domain, and invalid target shapes.

### Backend and server

- Accept an exact live Pod UID, declared container, and TCP port.
- Reject recreated Pods, missing containers, undeclared ports, UDP/SCTP, and forbidden access.
- Verify that forged input cannot select an arbitrary host or port.
- Test target-specific duplicate detection and the combined resource limits.
- Exercise start/list/subscription/stop/retry for both target variants.
- Confirm context switching and shutdown drain both Pod and Service sessions.
- Add a real kind test that forwards a declared Pod TCP port over loopback and exchanges data.

### UI and workspace

- Render one row per declared Pod port and actions only for TCP when capability is present.
- Validate modal defaults, blank/`0` automatic allocation, invalid text, pending state, conflict errors, cancel, and success on both Pod and Service surfaces.
- Verify the singleton launcher item, active badge, window focus, and focused duplicate session.
- Render mixed Pod and Service sessions with copy, stop, failure reason, and retry actions.
- Verify empty, unavailable, disconnected, and reconnect/list-reconstruction states.
- Verify workspace snapshot compatibility and that sessions themselves are never persisted.

## Acceptance criteria

- In the native desktop app, every declared Pod TCP port has a Port Forward action beside it.
- The action always opens a modal before starting and defaults the local port to the remote port.
- A successful forward listens only on `127.0.0.1` and appears immediately in the global Port Forwards window.
- The management window presents all Pod and Service sessions and supports copying, stopping, and retrying failures.
- The launcher badge reflects only currently live or transitioning sessions.
- Duplicate starts focus the existing session rather than binding a second listener.
- Forged, stale, undeclared, or non-TCP Pod targets are rejected server-side.
- Context switching and application shutdown leave no Pod or Service listener running.
