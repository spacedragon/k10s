# Protocol operations

k10s uses JSON control frames on `/api/v1/control` and versioned binary payloads
on ticket-authenticated `/api/v1/logs` and `/api/v1/exec` sockets. The access
token is carried only in the first `hello` frame.

## Compatibility

The current protocol is major `1`, minor `1`. Supported negotiation is major
`1` with peer minor `0..=1`; the negotiated minor is the lower value. A major
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
