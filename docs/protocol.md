# Protocol operations

k10s uses JSON control frames on `/api/v1/control` and versioned binary payloads
on ticket-authenticated `/api/v1/logs` and `/api/v1/exec` sockets. The access
token is carried only in the first `hello` frame.

## Compatibility

The current protocol is major `1`, minor `5`. Supported negotiation is major
`1` with peer minor `0..=5`; the negotiated minor is the lower value. A major
mismatch is rejected. Unknown message kinds return a structured
`unsupportedMessage` error instead of being ignored or crashing a peer.

Control envelopes carry a discriminator plus optional request ID,
subscription ID, monotonic sequence, and kind-specific payload. IDs are opaque.
Request IDs also serve as correlation IDs; payloads are not diagnostic context
and must not be logged.

## Connection, replay, and recovery

The client sends `hello` with version/capabilities and optional prior server
instance, session, and highest contiguous acknowledged sequence. `welcome`
reports a fresh, resumed, or resync-required session. Replay is valid only when
the instance/session match and the entire gap remains inside bounded age/count
budgets; duplicates are suppressed by sequence.

The server sends `resyncRequired` whenever safe complete replay is impossible,
including journal gaps and dropped/coalesced state that requires a fresh
projection. Its payload is `{ "reason": "<safe reason>" }`. On this recovery
message the client clears server-issued projections, bootstraps, recreates its
desired subscriptions, and queries nonterminal operations. It never applies a
partial replay as authoritative state.

During ordered shutdown the server sends `shutdownNotice` with a safe reason
and optional retry delay. Mutations close before the bounded status-read grace
period ends. A new `serverInstanceId` makes old in-flight operation outcomes
unknown until refreshed; clients must not guess success or blindly retry.

Dedicated logs/exec sockets require a single-use ticket obtained on the
control channel. Their binary header version is independent and currently `1`;
unknown header versions or payload kinds are rejected explicitly.

## Resource projections and port forwarding

Added in minor `2`: list rows and detail responses carry an optional
kind-specific `projection` (currently `service`) with normalized structured
columns. The field defaults to absent; legacy payloads decode unchanged and
`summary` remains authoritative for existing generic windows.

Port-forward sessions use three request kinds — `portForward.start`,
`portForward.stop`, and `portForward.list` — plus a bounded
`portForwardSessions` subscription whose events carry complete session
snapshots with monotonic revisions. Start requires the exact core/v1 Service
identity including UID, a port selector by name or number, and a local port of
`0..=65535` where `0` lets the OS assign one. Stop is idempotent by session ID.
Servers advertise `service.portForward` only when the feature is enabled;
disabled servers reject every port-forward request regardless of advertised
capabilities.

Added in minor `4`: resource-watch selectors may carry an exact `name` and
`uid` alongside their context, GVK, and namespace. Dedicated Pod and Deployment
Detail windows use this selector as their independent lifecycle and mutation
authority; peers negotiated below minor `4` must keep that authority unavailable.

Added in minor `5`: a `traffic` subscription selects one kubeconfig context.
`traffic.updated` events report one-second application-payload upload/download
rates, cumulative byte counts, total requests, and active requests measured at
the server-side kube-rs transport. The telemetry contains no request paths,
headers, credentials, or payload content and may be coalesced under pressure.
