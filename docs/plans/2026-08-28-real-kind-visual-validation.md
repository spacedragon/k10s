# Real-kind Visual Validation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restore an authoritative Design 08 target and add reproducible, sanitized real-kind visual evidence for issue #170.

**Architecture:** Preserve the surviving Design 08 export as the authoritative artifact and expose it through a local HTML comparison page. Drive the existing web renderer through the production server, Kubernetes backend, protocol, and client seams against `kind-bunyip`; the capture test performs only reads and UI navigation.

**Tech Stack:** Rust workspace, eframe/egui WebAssembly, Playwright, Kubernetes/kind, Markdown/HTML documentation.

---

### Task 1: Restore the Design 08 authority

**Files:**
- Create: `docs/design/08-improvements.html`
- Create: `docs/design/issue-159/design-08-reference.png`
- Modify: `docs/design/README.md`

1. Restore the surviving PNG from `issue-159-comparison-assets`.
2. Add a self-contained HTML viewer that identifies the PNG as the authoritative replacement for the unavailable source.
3. Link and explain the replacement in the design archive.
4. Open the HTML locally and verify that the target renders without network access.

### Task 2: Add reproducible real-kind capture coverage

**Files:**
- Create: `tests/browser/real-kind-visual.spec.ts`
- Create: `docs/testing/real-kind-visual-validation.md`
- Create: `docs/screenshots/issue-170/before-fake-1280x800.png`
- Create: `docs/screenshots/issue-170/after-real-kind-1280x800.png`

1. Add a Playwright test that connects through the existing server/client protocol path.
2. Navigate read-only resource lists that expose healthy Pods, `ImagePullBackOff`, completed Jobs, StatefulSets, and dense rows.
3. Capture at exactly 1280x800 in dark theme and compact density.
4. Document cluster prerequisites, launch/capture commands, sanitization rules, and Windows native-surface fallback.
5. Verify screenshots contain no secrets, kubeconfig data, usernames, or machine-specific paths.

### Task 3: Validate documentation and runtime behavior

**Files:**
- Modify: `tests/documentation_acceptance.rs`

1. Add documentation acceptance assertions for the restored authority and capture contract.
2. Run `cargo fmt --all -- --check` and Clippy with workspace/all-targets/all-features.
3. Run relevant Rust tests and UI snapshot tests.
4. Run the real-kind capture, full browser suite, and desktop smoke.
5. Inspect the produced screenshots at full resolution and correct any visual or privacy issue.

### Task 4: Deliver and land

1. Commit and push `spacedragon/issue-170`.
2. Update issue #159's definition of done with links to the new evidence.
3. Open a non-draft PR containing `Closes #170`, implementation/test evidence, and inline before/after images.
4. Monitor required checks, CI code review, review threads, requested changes, and mergeability; fix and push as needed.
5. Rebase on current `origin/main` if needed, then squash merge once all gates pass.
