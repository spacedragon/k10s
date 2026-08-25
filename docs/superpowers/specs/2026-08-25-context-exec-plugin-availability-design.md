# Context Exec Plugin Availability Design

## Goal

Allow kubeconfig exec credential plugins to run through kube-rs without letting
one failing plugin terminate k10s. A context whose plugin fails remains visible
but disabled, carries a safe diagnostic for the UI, and can recover through an
explicit refresh.

## Product behavior

Each context has one runtime availability state:

- `Unknown`: the context has not needed a client yet. It remains selectable.
- `Available`: its client and credentials were initialized successfully.
- `Unavailable(reason)`: credential-plugin initialization or refresh failed. It
  remains visible but cannot be selected.

The configured current context is validated during the first bootstrap. If its
plugin cannot initialize, k10s records and prints the failure, marks only that
context unavailable, then tries every other context in stable kubeconfig order,
including `Unknown` exec contexts, until one succeeds. That context becomes
current. If no context succeeds, the application still opens with no selected
context. Contexts with classified exec failures are disabled; a non-exec client
construction failure remains unknown/retryable and receives only a bounded
generic operator diagnostic.

Non-current contexts are lazy: their exec plugins run only when a user selects
them. A failed selection leaves the previous context and workspace unchanged,
marks the destination unavailable, and keeps the connection alive. Backend
validation rejects requests for unavailable contexts even if a client bypasses
the disabled UI.

Global Refresh retries unavailable contexts. A successful retry makes the
context selectable again. A plugin refresh failure after a context was already
in use also marks it unavailable; existing UI data remains as stale data while
new requests stop until Refresh recovers it. If the failed context was current,
the registry atomically moves current to the first already-`Available` context
in stable kubeconfig order, or clears current when none is available. Runtime
failure handling never launches an `Unknown` plugin from inside an unrelated
request; the next Bootstrap performs the ordered fallback search when needed.

## Backend architecture

Kubeconfig parsing remains synchronous and credential-free. It validates
context, cluster, and current-context structure but no longer rejects an
`AuthInfo.exec` field. kube-rs remains the sole implementation of the Kubernetes
exec credential protocol, including `KUBERNETES_EXEC_INFO`, versioned output
parsing, token refresh, and certificate credentials.

`ContextRegistry` owns availability beside the existing current marker and is
shared with a request-error observer. Client construction is still lazy and
cached per context. `ClientBuilder::try_from` is the initial exec boundary:
`kube::Error::Auth(kube::client::AuthError)` is matched structurally, never
formatted generically. A small outer tower error-observer layer inspects boxed
request errors for the same `AuthError` variants, which is the later token
refresh boundary. It atomically marks only the owning context unavailable and
logs only on the first state transition, so concurrent failures cannot repeatedly
change state or emit duplicate diagnostics.

The adapter checks registry availability before returning a cached client.
Marking a context unavailable therefore stops subsequent requests immediately;
the observer also schedules eviction of that context's cached client. In-flight
requests may finish with the same typed failure, but cannot commit a context
switch after the availability transition.

Bootstrap performs only the work necessary to establish an available current
context. It does not eagerly execute every non-current plugin while a current
context remains usable. A dedicated refresh-operation mutex serializes Bootstrap
probes, but the registry lock is never held while an external plugin executes.
Each probe snapshots the context generation, runs outside the state guard, then
briefly reacquires the guard to commit only if the generation still matches.
Every later Bootstrap is the refresh operation and runs this stable-order
algorithm:

1. Evict and reconstruct every `Unavailable` context in kubeconfig order, so
   its plugin must execute again. This includes the just-failed former current
   context at its normal position in that order.
2. After probe results have been generation-checked and committed individually,
   briefly lock the registry and keep the current marker if that context is now
   `Available`; otherwise choose the first already-`Available` context in stable
   order.
3. If there is still no current context, probe `Unknown` contexts in stable
   order until one succeeds, then commit it as current. Continue past non-exec
   construction failures without disabling those contexts.
4. Take one final locked snapshot and return the authoritative state of every
   context. Each availability retry commits independently: success clears the
   reason, while a classified exec failure retains its latest bounded reason.

## Error boundary

Only credential-plugin failures change context availability: failure to start,
non-zero exit, malformed plugin output, missing credential status, or later
exec-token refresh failure. API unreachability, RBAC denial, request timeout,
and unrelated transport errors remain ordinary retryable backend failures.

Diagnostics may contain:

- context name;
- plugin command name;
- exit code or start/parse failure category;
- at most 2 KiB of stderr.

Control characters are stripped or collapsed. Common bearer-token, JWT, PEM
private-key, and credential-assignment patterns are replaced with `[REDACTED]`.
Stdout is never included because it carries the ExecCredential response and can
contain bearer tokens or private key data. Generic kube-rs error `Display` and
`Debug`, command arguments, exec environment values, and raw process `Output`
are never logged or serialized. The same bounded diagnostic is logged with
`tracing::error!` and sent as the context's unavailable reason.

Showing plugin stderr is explicitly a best-effort local diagnostic, not an
absolute non-disclosure guarantee: a malicious or careless plugin can place an
unknown secret format in stderr. k10s truncates the presented stderr to 2 KiB
after kube-rs returns it, but kube-rs's `Command::output` buffers the child output
before k10s receives it. Enabling exec therefore treats the local kubeconfig and
its named executables as trusted local code.

kube-rs invokes the configured executable directly, never through a shell.
k10s forces the in-memory exec configuration to non-interactive mode:
`interactiveMode: Always` is rejected promptly, while `IfAvailable` is treated
as `Never`, so a GUI launch never inherits stdin. kube-rs 4.2 does not expose a
killable child handle; this iteration does not promise hard process cancellation.
Plugin execution occurs off the single-thread server runtime so a slow plugin
does not freeze connection and lifecycle processing, and duplicate probes for
one context are coalesced.

## Protocol and UI

Backend `ContextInfo` and protocol `Context` gain an exact availability enum
serialized as `"unknown"`, `"available"`, or `"unavailable"`, plus optional
`unavailableReason`. An absent availability field defaults to `available` when a
new peer reads an old payload. `unavailableReason` is retained only when state is
`unavailable`; constructors and deserialization normalize it away otherwise.
Older serde-based peers ignore the added fields when reading new payloads.

The application keeps full context options rather than reducing bootstrap data
to names. The top-bar selector renders unknown and available options normally.
Unavailable options are visible, greyed out, and not clickable; their hover and
accessibility text expose the bounded reason.

A context-switch plugin failure uses the existing stable `Conflict` error code,
`AfterRefresh` retryability, and a bounded safe message, avoiding an unknown enum
variant for older peers. Its existing optional error `details` object carries
the machine-readable shape
`{"kind":"contextUnavailable","context":"<name>","reason":"<safe reason>"}`.
Old peers ignore `details`; new peers branch only on the exact `kind`, never on
human-readable text. The UI verifies that `context` matches the pending switch
destination, disables that option immediately without entering `AppView::Failed`,
changing the workspace, or disconnecting, then requests Bootstrap to reconcile
with the authoritative context list.

If the selected context becomes unavailable later, the current data remains
visible under the existing stale/error presentation. The typed backend error
queues an availability Bootstrap; after the backend selects an already-known
available fallback (or Bootstrap finds one), the authoritative response commits
that fallback through the normal workspace switch path. If none exists, the
selector has no current value and every unavailable option stays visible.

## Testing

Tests use temporary executable credential-plugin scripts and never require a
cloud account.

- Config tests prove exec kubeconfigs construct successfully and non-current
  plugins are not executed at startup.
- Bootstrap tests cover a successful current plugin, current failure with
  ordered fallback, and all contexts unavailable without process failure.
- Switch tests prove first selection executes the plugin, failure preserves the
  previous current context, and disabled contexts are rejected before another
  execution.
- Diagnostic tests prove exit status and stderr are retained while stdout,
  tokens, control characters, and excess output are excluded.
- Refresh tests change the fixture plugin from failure to success and verify the
  cached failed client is evicted, the plugin is forced to run again, partial
  successes commit independently, and the option becomes selectable again.
- Protocol tests cover the exact enum representation, reason/state invariants,
  a new peer reading an old payload, and an old serde peer ignoring new fields.
- UI tests cover disabled rendering, hover/accessibility reason text, and the
  invariant that one context failure never moves the application into its
  terminal failed view. The disabled option uses a hover-capable non-interactive
  selectable response rather than a native disabled control that drops hover.
- Runtime tests use an expiring token fixture, then fail the next exec refresh;
  concurrent requests cause one availability transition and one diagnostic,
  later requests are blocked, the cached client is evicted, and fallback/no-
  fallback current-marker behavior remains atomic.
- Adversarial diagnostic tests put JWT, bearer, credential assignment, PEM,
  control characters, and oversized data in stderr and credential material in
  stdout. They also verify generic kube error formatting never reaches logs or
  the wire. The memory limitation of kube-rs child-output buffering is recorded
  as a dependency constraint rather than asserted as a k10s bound.
- Existing workspace, server, desktop, and packaging tests remain green.

## Non-goals

- Reimplementing the Kubernetes exec credential protocol.
- Silently falling back to fake Kubernetes data.
- Disabling a context for ordinary API, RBAC, or network failures.
- Persisting plugin stderr or credential material beyond the in-memory runtime.
- Guaranteeing termination or bounded child-output memory for a hostile exec
  plugin while kube-rs owns process execution.
