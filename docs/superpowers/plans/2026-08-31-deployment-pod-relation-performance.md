# Deployment Pod Relation Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `apps/Deployment` Pod relations independent of unrelated namespaced APIs by listing only the matching ReplicaSet version and core/v1 Pods.

**Architecture:** Add a pure descriptor-selection boundary in `kube/owners.rs`. The real adapter selects descriptors from the cached discovery catalog before invoking the existing serial candidate sweep; strict controller UID traversal remains unchanged, while non-Deployment targets keep the full catalog behavior.

**Tech Stack:** Rust, Tokio, kube-rs dynamic API, recorded tower API server, Cargo tests

---

### Task 1: Lock down Deployment request fan-out

**Files:**
- Modify: `crates/k10s-backend/tests/resource_details.rs`

- [ ] **Step 1: Write the failing adapter-level test**

Extend `relations_traverse_controller_uids_not_labels_or_reused_names` or add a focused test. Override the recorded `/apis` group list to include an unrelated `k10s.example.com/v1alpha1` API group, serve its discovery document with a namespaced `Gadget` resource, and then:

```rust
server.set_hanging_path(
    "/apis/k10s.example.com/v1alpha1/namespaces/default/gadgets",
);

let relations = tokio::time::timeout(
    Duration::from_millis(500),
    kernel.query(Query::ResourceRelations { reference: requested }),
)
.await
.expect("unrelated APIs cannot delay Deployment Pods")
.expect("relations resolve");

assert_eq!(server.hit_count("/apis/apps/v1/namespaces/default/replicasets"), 1);
assert_eq!(server.hit_count("/api/v1/namespaces/default/pods"), 1);
assert_eq!(server.hit_count(
    "/apis/k10s.example.com/v1alpha1/namespaces/default/gadgets",
), 0);
```

Keep the existing response assertions so this test exercises the real adapter, discovery, normalization, and owner traversal.

- [ ] **Step 2: Run the test to verify RED**

Run: `cargo test -p k10s-backend --test resource_details relations_traverse_controller_uids_not_labels_or_reused_names -- --exact`

Expected: FAIL after the 500 ms timeout because the current generic sweep reaches the hanging Gadget CRD LIST.

- [ ] **Step 3: Commit the red test**

```bash
git add crates/k10s-backend/tests/resource_details.rs
git commit -m "test: expose deployment relation catalog sweep"
```

### Task 2: Select only Deployment Pod-chain descriptors

**Files:**
- Modify: `crates/k10s-backend/src/kube/owners.rs`
- Modify: `crates/k10s-backend/src/kube/mod.rs:820-826`

- [ ] **Step 1: Add failing pure selection tests**

In `owners.rs`, add `#[cfg(test)]` coverage using descriptors for `apps/v1/ReplicaSet`, `apps/v1beta1/ReplicaSet`, `core/v1/Pod`, Service, Event, and same-kind CRDs. Assert:

```rust
let selected = candidate_descriptors(&deployment_reference(), &catalog);
assert_eq!(selected.iter().map(|d| &d.gvk).collect::<Vec<_>>(), vec![
    &Gvk::new("apps", "v1", "ReplicaSet"),
    &Gvk::core("v1", "Pod"),
]);
```

Also assert a non-`apps` `Deployment` and a StatefulSet retain every catalog descriptor; the sweep itself continues to exclude cluster-scoped resources and Events. Add an `apps/v1` Deployment catalog with only `apps/v1beta1/ReplicaSet` plus core/v1 Pod and assert selection returns only Pod, proving there is no version fallback.

- [ ] **Step 2: Run the unit tests to verify RED**

Run: `cargo test -p k10s-backend --lib kube::owners::tests`

Expected: FAIL because `candidate_descriptors` does not exist.

- [ ] **Step 3: Implement minimal descriptor selection**

Add a pure helper with this behavior:

```rust
pub(super) fn candidate_descriptors<'a>(
    reference: &ResourceRef,
    catalog: &'a [ApiResourceDescriptor],
) -> Vec<&'a ApiResourceDescriptor> {
    if reference.gvk.group == "apps" && reference.gvk.kind == "Deployment" {
        catalog.iter().filter(|descriptor| {
            descriptor.gvk == Gvk::new("apps", reference.gvk.version.clone(), "ReplicaSet")
                || descriptor.gvk == Gvk::core("v1", "Pod")
        }).collect()
    } else {
        catalog.iter().collect()
    }
}
```

Change `sweep_candidates` to accept the selected descriptor references. In `resource_relations`, resolve the catalog once, call `candidate_descriptors`, and pass the result to `sweep_candidates`. Do not change `related_data`.

- [ ] **Step 4: Run focused tests to verify GREEN**

Run: `cargo test -p k10s-backend --lib kube::owners::tests && cargo test -p k10s-backend --test resource_details relations_traverse_controller_uids_not_labels_or_reused_names -- --exact`

Expected: PASS; Gadget CRD hit count remains zero and ReplicaSet/Pod are each listed once.

- [ ] **Step 5: Commit the implementation**

```bash
git add crates/k10s-backend/src/kube/owners.rs crates/k10s-backend/src/kube/mod.rs
git commit -m "fix(backend): bound deployment relation queries"
```

### Task 3: Regression verification

**Files:**
- No additional files expected

- [ ] **Step 1: Format and check the diff**

Run: `cargo fmt --all -- --check && git diff --check`

Expected: PASS with no formatting or whitespace errors.

- [ ] **Step 2: Run backend relation coverage**

Run: `cargo test -p k10s-backend --lib kube::owners::tests && cargo test -p k10s-backend --test resource_details`

Expected: PASS with zero failures.

- [ ] **Step 3: Run server wire-level coverage**

Run: `cargo test -p k10s-server --test detail_loopback deployment_detail_traverses_replicasets_and_pods_by_controller_uid -- --exact`

Expected: PASS with the same ReplicaSet and 20 Pod response.

- [ ] **Step 4: Review final scope**

Run: `git status --short && git diff HEAD~2 --stat`

Expected: only the approved spec/plan, backend relation test, and two backend implementation files changed; no debug instrumentation remains.
