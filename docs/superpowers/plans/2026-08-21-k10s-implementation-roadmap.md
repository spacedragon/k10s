# k10s Implementation Roadmap

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this roadmap plan-by-plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved native/web k10s architecture through five independently testable plans.

**Architecture:** One Rust workspace contains the shared egui UI, a versioned WebSocket protocol, an Axum server adapter, and a deep Backend Kernel. Desktop embeds the server; web deploys it standalone. A deterministic fake Kubernetes adapter is replaced by kube-rs without changing UI or protocol.

**Tech Stack:** Rust 1.97.1, eframe/egui 0.36.1, Serde JSON, ewebsock 0.8.0, Axum 0.8.9, Tokio 1.53.1, kube 4.2.0, egui_kittest 0.36.1.

---

## Plan order

1. [Runtime Foundation](2026-08-21-k10s-runtime-foundation.md) — workspace, protocol, fake Backend Kernel, real WebSocket path, desktop embed, web connection gate.
2. [Connected UI Prototype](2026-08-21-k10s-connected-ui-prototype.md) — complete approved UI through the fake adapter and real protocol.
3. [Kubernetes Read Path](2026-08-21-k10s-kubernetes-read-path.md) — kubeconfig, discovery, list/watch/cache, details, events, owner traversal, metrics.
4. [Kubernetes Operations](2026-08-21-k10s-kubernetes-operations.md) — YAML validation/apply, mutations, logs, exec, and operation recovery.
5. [Release Hardening](2026-08-21-k10s-release-hardening.md) — resume, backpressure, security limits, capacity tests, packaging, and cross-platform delivery.

Plans are sequential. Each plan must pass its verification gate before the next begins. The end of every plan produces runnable software:

| Plan | Runnable outcome |
| --- | --- |
| 1 | Native and browser clients authenticate and render fake bootstrap data through the real server |
| 2 | Complete UI and failure-state prototype on all targets |
| 3 | Read-only real-cluster console with live watches and metrics |
| 4 | Full real-cluster operational workflows |
| 5 | Bounded, tested, packaged release candidates |

## Cross-plan rules

- Follow `docs/superpowers/specs/2026-08-21-k10s-runtime-architecture-design.md` and the approved UI spec.
- Use TDD. Every behavior begins with a failing test and ends with a focused commit.
- UI code never imports kube-rs or reads fixture data directly.
- Fake and real Kubernetes implementations cross the same internal seam.
- All protocol queues and stream buffers are bounded from their first implementation.
- Commit `Cargo.lock` and `package-lock.json`; every Cargo CI/release command uses `--locked`, and Trunk builds with its locked Cargo mode.
- Baseline reconnect/full-resync ships in Plan 1; Plan 5 journal replay is only an optimization and must always fall back safely.
- Do not start a later plan by bypassing an unfinished verification gate.
- The superseded `2026-08-21-k10s-egui-static-prototype.md` is reference material only and must not be executed.

## Final system verification

After Plan 5, run:

```bash
cargo fmt --all -- --check
trunk build --release
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets
cargo build --locked --release -p k10s-desktop
cargo build --locked --release -p k10s-server
npm ci
npx playwright test
```

Expected: all commands pass, followed by browser, kind-cluster, load, and packaging jobs defined in Plan 5.
