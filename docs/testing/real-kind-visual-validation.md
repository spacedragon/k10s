# Real-kind visual validation

Issue #170 establishes a reproducible visual baseline through the production
Kubernetes backend, control protocol, shared client, and equivalent WebGL
renderer. It does not add a fixture-only UI path.

## Fixed capture contract

| Property | Required value |
| --- | --- |
| Viewport | 1280 × 800 CSS pixels; full page |
| Theme | egui dark theme |
| Density | compact shell defaults; browser scale 100% |
| Renderer | Chromium WebGL through the Trunk web build |
| Context | `kind-bunyip` |
| Namespace scope | all namespaces |
| Cluster state | healthy Pods, one `ImagePullBackOff` Pod, one completed Job, one ready StatefulSet, and a dense all-namespace Pod list (at least 9 Pods) |
| Access | read-only navigation (`get`, `list`, and `watch`); no mutation controls |

The committed evidence is:

- [before: deterministic fake UI](../screenshots/issue-170/before-fake-1280x800.png)
- [after: real-kind Pods](../screenshots/issue-170/after-real-kind-pods-1280x800.png)
- [after: real-kind completed Job](../screenshots/issue-170/after-real-kind-job-1280x800.png)
- [after: real-kind StatefulSet](../screenshots/issue-170/after-real-kind-statefulset-1280x800.png)

The Pod capture supplies both healthy and `ImagePullBackOff` states and the
dense-list case. The other two captures make completion and controller replica
state independently reviewable.

## Reproduce

Prerequisites are Rust, the `wasm32-unknown-unknown` target, Trunk 0.21.14,
Node dependencies from `npm ci`, Chromium from `npx playwright install
chromium`, and an existing kubeconfig context named `kind-bunyip`. Confirm the
state without modifying it:

```powershell
kubectl --context kind-bunyip get pods,jobs,statefulsets -A
trunk build --release
$env:K10S_DIST_DIR = (Resolve-Path dist)
$server = Start-Process -PassThru -WindowStyle Hidden target/release/k10s-server -ArgumentList '--token-file','tests/browser/token.txt','--listen','127.0.0.1:18081'
$env:K10S_E2E_PORT = '18081'
$env:K10S_REAL_KIND = '1'
npx playwright test tests/browser/real-kind-visual.spec.ts --project=chromium
Stop-Process -Id $server.Id
```

Build `target/release/k10s-server` first with `cargo build --locked --release
-p k10s-server-app --bin k10s-server`. The capture test verifies that the
connected context is `kind-bunyip` and that the expected live rows are present
before writing screenshots.

## Privacy and metadata

Screenshots must not show the connection form, access token, kubeconfig
contents, Secrets, terminal output, machine username, or filesystem paths.
Before commit, inspect the images at native resolution and run:

```powershell
rg -a -n "kubeconfig|Access token|Users\\|/home/|BEGIN .*PRIVATE KEY" docs/screenshots/issue-170
```

PNG files contain only renderer output and standard image chunks; do not add
sidecar metadata containing the local kubeconfig path or workstation identity.

## Windows native-surface fallback

Glow/OpenGL desktop windows can render as a black or transparent surface in
Windows capture APIs because the compositor owns the final pixels. First try a
normal desktop capture after the connected workspace has stopped repainting.
If pixels are missing, use the equivalent Chromium WebGL renderer above at the
same 1280 × 800 viewport. It runs the same shared UI, client, protocol, and real
Kubernetes backend, while Playwright captures the composited surface reliably.
Record the renderer in the PR; never replace the real backend with `--fake` to
work around a native capture problem.
