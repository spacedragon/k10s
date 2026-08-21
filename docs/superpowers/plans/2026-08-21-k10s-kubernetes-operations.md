# k10s Kubernetes Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect the approved YAML, mutation, Logs, and Shell workflows to real Kubernetes APIs while preserving exact identity, idempotency, bounded streams, and outcome-unknown safety.

**Architecture:** `OperationEngine` and `StreamHub` retain their existing protocol and UI contracts. Real kube-rs behavior is added only behind the Kubernetes seam. Server-side validation tickets bind an exact buffer to target UID/resourceVersion; operations publish state over the control socket; Logs and Exec redeem single-use tickets on dedicated sockets.

**Tech Stack:** Existing workspace plus kube 4.2.0 dynamic/typed APIs, serde_yaml_ng 0.10.0, cryptographic buffer hashing, Kubernetes log and exec streaming.

---

Every task extends the existing `KubernetesAccess` port and crosses `BackendKernel`; no server or UI code calls kube-rs. Operation/stream limits, correlation tracing, and recovery are added when each feature is introduced, not deferred. All Cargo commands use `--locked`.

### Task 1: Implement authoritative YAML parsing and validation tickets

**Files:** create `k10s-backend/src/validation/{mod,yaml,ticket}.rs`, modify `kube/read.rs`, modify backend `{port,kernel}.rs`, modify server control/client state, modify `crates/k10s-backend/tests/kube_contract.rs`, test `tests/yaml_validation.rs` and `crates/k10s-server/tests/yaml_validation_loopback.rs`.

- [ ] Write failing tests for YAML parse errors, JSON conversion, immutable identity rejection, UID/RV mismatch, schema unavailable, dry-run rejection, ticket hash/expiry/server-instance binding, and restart invalidation.
- [ ] Run `cargo test --locked -p k10s-backend --test yaml_validation`; expect missing validator.
- [ ] Implement the internal YAML parser wrapper and opaque in-memory ticket store. Dry-run the exact object through the adapter and return structured field errors without raw secrets.
- [ ] Run validation plus existing fake workflow tests; expect both adapters to satisfy the same ticket contract.
- [ ] Commit `feat: validate kubernetes yaml`.

### Task 2: Harden the Operation Engine for real submissions

**Files:** modify `operation.rs`, create `operation/{idempotency,state}.rs`, modify backend `{port,kernel}.rs`, modify protocol operation payloads, modify server control/client state, test `tests/operation_engine.rs` and `crates/k10s-server/tests/operation_recovery.rs`.

- [ ] Write failing tests for accepted/running/succeeded/failed/cancel-before-submit/outcome-unknown, one in-flight prohibited duplicate, idempotency replay, bounded TTL eviction, backend restart detection, and `OperationId` status query after a control reconnect/full resync.
- [ ] Run focused tests; expect failures.
- [ ] Implement the bounded operation state/idempotency stores and P0 event publication through the existing reserved scheduler. Separate pre-submit cancellation from unknown post-submit outcomes; never infer success from socket closure. Trace operation/correlation IDs and lifecycle transitions. On reconnect, the client queries every nonterminal `OperationId`; this is the baseline recovery contract independent of Plan 5 journal replay.
- [ ] Run property and real-socket tests with duplicate, reordered, timeout, and reconnect sequences; expect PASS.
- [ ] Commit `feat: harden operation state machine`.

### Task 3: Connect scale, restart, delete, and YAML apply

**Files:** create `kube/mutate.rs`, modify backend `{operation,port,kernel}.rs`, modify `crates/k10s-backend/tests/kube_contract.rs`, test `tests/core_mutations.rs`.

- [ ] Write failing tests for exact target display data, scale range/capability, rollout restart annotation, typed delete gating, propagation policy support, YAML ticket consumption, conflict, forbidden, success, and unknown transport outcome.
- [ ] Run focused tests; expect missing real mutation methods.
- [ ] Implement patches/deletes through `BackendKernel::execute` using UID/resourceVersion preconditions where Kubernetes supports them. Refresh the target after an unknown outcome before another submission is accepted.
- [ ] Run recorded-service tests and existing UI dialog tests; expect unchanged protocol behavior.
- [ ] Commit `feat: execute core kubernetes mutations`.

### Task 4: Connect Job/CronJob and custom-resource operations

**Files:** modify `kube/mutate.rs`, create `kube/create.rs`, test `tests/specialized_mutations.rs`.

- [ ] Write failing tests for create Job from Job, CronJob run-now, suspend/resume, generated name/source identity, custom-resource scale subresource, and capability-disabled custom delete/scale.
- [ ] Run focused tests; expect failures.
- [ ] Implement destination identity validation, Kubernetes-native generated-name handling, and dynamic scale requests only when discovery advertises the subresource. Do not add k10s-specific owner/source annotations unless Kubernetes itself requires a field for correctness.
- [ ] Run specialized operation and UI action-matrix tests; expect PASS.
- [ ] Commit `feat: add specialized workload operations`.

### Task 5: Stream real Pod logs through bounded dedicated sockets

**Files:** create `kube/logs.rs`, modify backend `{stream,port,kernel}.rs`, modify server `{logs,config}.rs`, test `tests/log_stream.rs` and `crates/k10s-server/tests/log_stream_loopback.rs`.

- [ ] Write failing tests for container validation, tail/since/timestamps/follow, pause semantics, UTF-8 handling, ring-buffer truncation, cancellation, RBAC, pod replacement UID mismatch, and explicit reconnect.
- [ ] Run focused tests; expect missing kube log adapter.
- [ ] Issue the bound ticket through `BackendKernel::query`, then redeem it in the kernel-owned Stream Hub behind `BackendKernel::subscribe`; acquire configured connection/rate budgets, open kube-rs log stream, frame bounded batches into the existing ring/queue limits, track truncation/pressure in tracing, and cancel upstream when socket/parent closes. Do not let log payloads enter the control queue.
- [ ] Run recorded-stream tests, UI Logs tests, and a kind log smoke; expect PASS.
- [ ] Commit `feat: stream kubernetes pod logs`.

### Task 6: Connect interactive Pod Exec

**Files:** create `kube/exec.rs`, modify backend `{stream,port,kernel}.rs`, modify server `{exec,config}.rs`, test `tests/exec_session.rs` and `crates/k10s-server/tests/exec_loopback.rs`.

- [ ] Write failing tests for exact pod UID/container/command, explicit connect, TTY stdin plus one merged output stream, resize, exit status, missing binary, forbidden, parent close, socket loss, and no resume. Add separate stdout/stderr assertions only to an explicitly non-TTY exec test.
- [ ] Run focused tests; expect missing real exec session.
- [ ] Issue the ticket through `BackendKernel::query`, then open kube-rs attach/exec in the kernel-owned Stream Hub behind `BackendKernel::subscribe` with a child cancellation token. For the interactive shell configure `AttachParams` with `tty(true)`, stdin/stdout enabled, and `stderr(false)`; treat TTY output as one merged stream and wire terminal resize. Only a distinct non-TTY mode may enable separate stderr. Acquire configured session/rate budgets, bridge bounded binary frames, close stdin and abort upstream on disconnect, trace pressure/lifecycle without payloads, and report exit exactly once. Never execute a local shell.
- [ ] Run unit/recorded tests and kind exec smoke; expect PASS.
- [ ] Commit `feat: add kubernetes pod exec`.

### Task 7: Integrate mutation and stream recovery with UI guards

**Files:** modify backend kernel, server control, UI client state, UI guards/dialogs/tools, test `k10s-ui/tests/real_operation_projection.rs` and `crates/k10s-server/tests/recovery_loopback.rs`.

- [ ] Write failing projection tests for conflict buffer preservation, ticket invalidation after watch RV change, unknown outcome refresh gate, context switch with active shell, logs disconnected state, and operation query after control reconnect.
- [ ] Run focused tests; expect projection gaps.
- [ ] Implement only missing state transitions; do not add kube-specific branches to UI. Reuse protocol error codes and operation updates. Force a socket drop and prove full resync, nonterminal operation query, ticket invalidation, dirty-buffer preservation, Logs explicit reconnect, and Exec terminal disconnect.
- [ ] Run UI, backend, and server integration suites; expect PASS in fake and real recorded modes.
- [ ] Commit `feat: integrate kubernetes operation recovery`.

### Task 8: Verify operations against kind with least privilege

**Files:** create `tests/kind/operations.yaml`, create `k10s-backend/tests/kind_operations.rs`, modify CI/README.

- [ ] Write ignored E2E tests for dry-run/apply, conflict, scale, restart, delete propagation, Job/CronJob actions, logs, exec, forbidden actions, and outcome reconciliation after induced proxy failure.
- [ ] Run the test and confirm it fails until fixtures/harness operations are wired.
- [ ] Implement disposable namespaces, least-privilege roles, test workloads, deterministic cleanup, and a controllable API proxy for timeout/unknown-outcome cases.
- [ ] Run `cargo test --locked -p k10s-backend --test kind_operations -- --ignored --nocapture` and full workspace tests; expect PASS.
- [ ] Commit `test: verify kubernetes operations`.

## Plan 4 verification gate

- Every approved mutation executes through OperationEngine and returns OperationId.
- YAML apply requires a current exact validation ticket.
- Unknown outcomes require refresh and are never blindly retried.
- Logs and Exec use dedicated bounded sockets and exact UID/container tickets.
- Exec disconnect terminates the upstream session and never resumes.
- kind E2E proves successful and forbidden paths with least privilege.
