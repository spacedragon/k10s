# Deployment

Use the desktop packages for a local native client, the server archive for a
single-host web deployment, or the OCI image for a container runtime. All ship
the same fingerprinted web assets and protocol version.

## Standalone archive

1. Extract the `.tar.xz` (Linux/macOS) or `.zip` (Windows).
2. Create a token file readable only by the service account.
3. Ensure the service account can read its kubeconfig and any executable named
   by an exec credential plugin. Plugins run non-interactively on the backend
   host. A plugin failure disables only its context; the server and other
   contexts remain available, and Refresh retries the failed context. Then
   start `k10s-server --listen 127.0.0.1:8080 --token-file /run/secrets/k10s-token`.
4. Put TLS and authentication at a reverse proxy before exposing the service.
5. Configure liveness and readiness independently as described below.

The OCI image runs as numeric user/group `10001:10001`, exposes port 8080, and
contains embedded assets. Mount kubeconfig and token read-only; do not bake
either into an image layer.

## Reverse proxy example

k10s requires the browser's origin authority and the host observed by k10s to
match. The proxy must preserve both `Host` and `Origin`, support WebSocket
upgrade, enforce TLS/authentication, and must not rewrite the control route.

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Host $http_host;
    proxy_set_header Origin $http_origin;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 1h;
}
```

Forwarded identity headers are not trusted by k10s. Keep the upstream
loopback/private, and enforce authorization at the proxy and Kubernetes RBAC.

## Probes and shutdown

| Endpoint | Exact response and meaning |
| --- | --- |
| `/healthz` | `200 ok\n` while the event loop/listener is alive, including drain. Restart only when this fails. |
| `/readyz` | `503 starting\n` during initialization; `200 ready\n` after initialization and request acceptance; `503 initialization failed\n` after failed initialization; `503 draining\n` after shutdown begins. Remove the instance from traffic on every 503. |

On SIGINT/SIGTERM, readiness changes first, upgrades stop, clients receive a
shutdown notice, streams/watches are cancelled, tracked tasks drain, and the
listener closes last. Orchestrators must allow at least the configured
`drain_timeout` plus proxy/network margin before force-killing the process.

## Clean-artifact walkthrough checklist

This is the release acceptance procedure on a machine without a source tree.
Any required step not described here is a release-blocking documentation bug.

- [ ] Verify the downloaded archive/package and extract/install it.
- [ ] Provide a readable kubeconfig; if it uses an exec credential plugin,
      verify that the service account can execute the trusted plugin binary.
- [ ] Create a restricted token file; never put the token in a URL.
- [ ] Start the binary and observe `/readyz`: starting 503, then ready 200.
- [ ] Load `/`, authenticate, select a context, list/detail a resource, and run
      an RBAC-permitted operation.
- [ ] Verify logs only when cluster RBAC permits them. Shell is available only
      from the embedded-local desktop through the user's external `kubectl`.
- [ ] Send SIGINT/SIGTERM; observe draining 503 before `/healthz` disappears.
- [ ] For OCI, additionally verify UID/GID 10001 and read-only secret mounts.
