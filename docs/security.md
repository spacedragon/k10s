# Security

k10s is a single-user cluster client, not a multi-tenant authorization layer.
Kubernetes RBAC is authoritative. Remote web deployment additionally requires
TLS, authentication, and access control at a reverse proxy.

## Secrets and credentials

- Prefer `--token-file`; then `K10S_ACCESS_TOKEN_FILE`; use
  `K10S_ACCESS_TOKEN` only for constrained automation. File precedence is
  intentional, empty files fail, and non-loopback listening requires a token.
- The access token appears only in the first WebSocket `Hello`. It must not be
  placed in URLs, web assets, browser persistence, errors, or probes.
- Config debug output substitutes `[REDACTED]`; normal telemetry records safe
  identifiers and state transitions, never credentials or raw kubeconfig.
- For backend-mediated operations, kubeconfig exec credential plugins run only
  on the backend host, by direct argv execution without a shell.
  `interactiveMode: Always` is refused and `IfAvailable` is forced to `Never`.
  The configured current context is checked during Bootstrap; other plugins
  run lazily when selected. External desktop shells use the user's local
  `kubectl`; any credential plugin for that invocation executes locally in the
  user's terminal process, outside the backend-mediated boundary.
- A plugin failure disables only its context. The process and other contexts
  stay live, the selector keeps the failed context visible, and Refresh retries
  it. Diagnostics include exit status and best-effort sanitized stderr (control
  characters removed, common credential forms redacted, UTF-8-safe 2 KiB
  presentation bound); stdout, command arguments, environment, and raw process
  output are never copied into normal errors or logs.
- Plugin programs are trusted local executables with the user's filesystem and
  network authority. kube-rs buffers child output and does not expose a hard
  process-kill seam here, so k10s does not claim protection from a hostile or
  indefinitely hung plugin. Protect kubeconfig and plugin binaries with
  operating-system permissions and run k10s as a dedicated user.
- The OCI image runs as `10001:10001`; mount credentials read-only and grant
  only the Kubernetes RBAC needed for intended operations.

## External desktop shell

Shell launch is available only to the desktop composition root for its own
embedded-local connection. Loopback or same-machine connectivity is not proof
of this capability; Web and remote/standalone connections expose no shell.
The process is the user's installed `kubectl` in the user's system terminal and
therefore has the same local filesystem, exec-auth-plugin, and Kubernetes
authority as that user.

At embedded-server startup, the desktop freezes a launch descriptor containing
the resolved kubectl executable, exact ordered kubeconfig sources, selected
context, connection generation, and only explicitly allowed non-secret
environment needed by kubectl/plugins. It withholds the capability if the
configuration is in-memory, unavailable locally, depends on sensitive
environment, or otherwise cannot be reproduced exactly. Kubeconfig contents,
credentials, access tokens, and user resource values are never copied into a
fallback command.
Windows additionally retains the audited non-secret `SYSTEMROOT` and `WINDIR`
runtime variables so PowerShell remains functional; arbitrary inherited
variables are still removed before kubectl runs.
Windows may add its own hidden `=X:` drive-current-directory pseudo-entry at
`CreateProcess` time. It is OS-managed, contains no credential material, and
cannot be enumerated or removed through PowerShell's `Env:` provider; it is not
treated as part of the application environment allowlist.
Finback writes the exact merged configuration prepared for the backend as an
owner-only file beside the launch script and points kubectl at that immutable
snapshot. It is never embedded in the script, and self-cleanup removes the
snapshot together with the script and manifest. Later edits to the original
source paths therefore cannot retarget an already prepared shell.

The private script checks the live Pod UID immediately before invoking exec.
This preflight prevents common stale-name mistakes but is not atomic: Kubernetes
exec accepts no UID/resource-version precondition, so deletion and same-name
recreation can still race between the check and exec. That residual race is an
explicit limitation.

Scripts use random safe names in an owner-only Finback temporary directory
(Unix directory/file mode `0700`; Windows owner-only ACL), create-new and
no-follow/reparse checks, platform-specific quoting, and direct executable/argv
launch. They self-delete after execution; synchronous launch failure deletes
immediately, and bounded startup cleanup removes validated owned entries older
than 24 hours. Cleanup refuses symlinks/reparse points and unvalidated
lookalikes and never recursively targets the general temporary directory.

## Logs and diagnostics

Request/subscription/operation IDs are safe correlation IDs for joining client
errors to server lifecycle and operation logs. Operators may share those IDs,
timestamps, error codes, and redacted configuration. They must never log payloads,
YAML bodies, external-shell script bodies, log-stream contents, access tokens, or raw
kubeconfig. See [Troubleshooting](troubleshooting.md) for the correlation flow.

Browser WebSocket origins are checked against the observed host. The server
ignores `X-Forwarded-*` and `X-Real-IP` as trust signals. Preserve Host/Origin
at the proxy and do not expose an unprotected upstream.
# Desktop Pod and Service port forwarding

The `service.portForward` and `pod.portForward` capabilities are desktop-only
and are not the security boundary: disabled standalone and web servers reject
the requests as well. Listeners are hard-coded to `127.0.0.1`.

Kubernetes remains authoritative. Service forwarding requires `get` on
`services`, `list` on `endpointslices`, `get` on `pods`, and `create` on
`pods/portforward`. Direct Pod forwarding requires `get` on `pods` and
`create` on `pods/portforward`. Each direct request must carry the exact
core/v1 Pod identity and UID, a named regular container, and a declared numeric
TCP port; the server refetches and validates all of them before binding.
ExternalName Services, UDP, SCTP, ownerless EndpointSlices, arbitrary IPs,
non-Pod endpoints, init containers, and ephemeral containers are not
supported.
