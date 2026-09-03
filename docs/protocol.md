# Protocol operations

k10s uses JSON control frames on `/api/v1/control` and versioned binary log
payloads on ticket-authenticated `/api/v1/logs` sockets. The access token is
carried only in the first `hello` frame.

## Compatibility

The current protocol is major `1`, minor `6`. Supported negotiation is major
`1` with peer minor `0..=6`; the negotiated minor is the lower value. A major
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

Dedicated log sockets require a single-use ticket obtained on the control
channel. Their binary header version is independent and currently `1`;
unknown header versions or payload kinds are rejected explicitly.

Minor `6` removes active embedded exec. For one current/previous-minor
compatibility window only, legacy exec ticket requests and `/api/v1/exec`
remain as fail-closed tombstones: a ticket request receives the typed
`unsupportedMessage` error; the route authenticates a normal hello, never
redeems its arbitrary legacy ticket or calls Kubernetes exec, emits the typed
unsupported stream error, and closes. Invalid access tokens still receive the
authentication error. New clients do not request exec, and the retained legacy
numeric discriminants have no new meaning. External desktop shells are local
application behavior and are never advertised or transported by this protocol.

## Resource projections and port forwarding

Added in minor `2`: list rows and detail responses carry an optional
kind-specific `projection` (currently `service`) with normalized structured
columns. The field defaults to absent; legacy payloads decode unchanged and
`summary` remains authoritative for existing generic windows.

Port-forward sessions use three request kinds — `portForward.start`,
`portForward.stop`, and `portForward.list` — plus a bounded
`portForwardSessions` subscription whose events carry complete session
snapshots with monotonic revisions. A shared session manager owns all Pod and
Service sessions, so limits, listing, stop, retention, and lifecycle cleanup
apply to the combined set. The global Port Forwards window consumes this same
authoritative list and event stream.

Service starts require the exact core/v1 Service identity including UID and a
declared port selector by name or number. Pod starts require the exact core/v1
Pod identity including UID, a named regular container, and a declared numeric
TCP port on that container. The local port field is `0..=65535`; `0` requests
automatic allocation. Stop is idempotent by session ID.

The desktop server advertises both `service.portForward` and
`pod.portForward`; disabled standalone and web servers advertise neither and
reject every port-forward request regardless of a client's capabilities. The
manager's terminal snapshots are retained for 30 seconds, subject to its hard
count bound. A context switch stops and joins all Pod and Service sessions
before committing the new context, while shutdown cancels and joins them and
closes every listener.

Added in minor `6`: start targets and current session snapshots distinguish
Service and Pod variants. A minor-`5` client retains the legacy Service wire
shape and never receives Pod sessions.

For compatibility, new and current clients always encode Service starts with
the legacy `{service, port, localPort}` shape. The server accepts both the
legacy and target-discriminated Service shapes. Pod starts use the
target-discriminated shape. When decoding a legacy Service snapshot, the
client derives `requestedLocalPort` from its bound local address. That
conversion is lossy: automatic `0` becomes the assigned explicit port on Retry.

Added in minor `4`: resource-watch selectors may carry an exact `name` and
`uid` alongside their context, GVK, and namespace. Dedicated Pod and Deployment
Detail windows use this selector as their independent lifecycle and mutation
authority; peers negotiated below minor `4` must keep that authority unavailable.

Added in minor `5`: a `traffic` subscription selects one kubeconfig context.
`traffic.updated` events report one-second application-payload upload/download
rates, cumulative byte counts, total requests, and active requests measured at
the server-side kube-rs transport. The telemetry contains no request paths,
headers, credentials, or payload content and may be coalesced under pressure.
