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

The configured current context is validated during the first bootstrap. If it
cannot initialize, k10s records and prints the failure, then tries contexts in
stable kubeconfig order until one succeeds. That context becomes current. If no
context succeeds, the application still opens with no selected context and all
failed contexts disabled.

Non-current contexts are lazy: their exec plugins run only when a user selects
them. A failed selection leaves the previous context and workspace unchanged,
marks the destination unavailable, and keeps the connection alive. Backend
validation rejects requests for unavailable contexts even if a client bypasses
the disabled UI.

Global Refresh retries unavailable contexts. A successful retry makes the
context selectable again. A plugin refresh failure after a context was already
in use also marks it unavailable; existing UI data remains as stale data while
new requests stop until Refresh recovers it.

## Backend architecture

Kubeconfig parsing remains synchronous and credential-free. It validates
context, cluster, and current-context structure but no longer rejects an
`AuthInfo.exec` field. kube-rs remains the sole implementation of the Kubernetes
exec credential protocol, including `KUBERNETES_EXEC_INFO`, versioned output
parsing, token refresh, and certificate credentials.

`ContextRegistry` owns availability beside the existing current marker. Client
construction is still lazy and cached per context. The adapter records
credential-plugin failures in the registry, logs the safe diagnostic, and
returns a typed context-unavailable error. Successful construction or Refresh
clears the failure and caches the usable client.

Bootstrap performs only the work necessary to establish an available current
context. It does not eagerly execute every non-current plugin. A refresh-mode
bootstrap retries only contexts already known to be unavailable.

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

Control characters are stripped or collapsed. Stdout is never included because
it carries the ExecCredential response and can contain bearer tokens or private
key data. The same bounded diagnostic is logged with `tracing::error!` and sent
as the context's unavailable reason.

## Protocol and UI

Backend `ContextInfo` and protocol `Context` gain availability and an optional
unavailable reason. Protocol fields use backward-compatible defaults so payloads
from older peers remain available when the fields are absent.

The application keeps full context options rather than reducing bootstrap data
to names. The top-bar selector renders unknown and available options normally.
Unavailable options are visible, greyed out, and not clickable; their hover and
accessibility text expose the bounded reason.

A context-switch failure carries a typed context-unavailable response. The UI
uses the pending switch destination to update that option immediately without
entering `AppView::Failed`, changing the workspace, or disconnecting. Refresh
requests an availability retry and replaces the local context list with the
authoritative response.

If the selected context becomes unavailable later, the current data remains
visible under the existing stale/error presentation. After the backend chooses
an available fallback, the next authoritative context response commits that
fallback through the normal workspace switch path.

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
  option becomes selectable again.
- Protocol tests cover new-field serialization and backward-compatible defaults.
- UI tests cover disabled rendering, hover/accessibility reason text, and the
  invariant that one context failure never moves the application into its
  terminal failed view.
- Existing workspace, server, desktop, and packaging tests remain green.

## Non-goals

- Reimplementing the Kubernetes exec credential protocol.
- Silently falling back to fake Kubernetes data.
- Disabling a context for ordinary API, RBAC, or network failures.
- Persisting plugin stderr or credential material beyond the in-memory runtime.
