# k10s egui Console UI Design

Status: Approved on 2026-08-21

## Summary

k10s is a desktop Kubernetes console built from default egui components. It uses a fixed application shell and a free canvas containing draggable, resizable, collapsible, and closable inner windows. The UI follows the visual language of the egui demo: dark panels, compact controls, thin borders, modest shadows, native tables, selectable labels, combo boxes, progress bars, tabs, text editors, and standard window chrome.

The default canvas opens only the Overview window. Users open other windows from a fixed left launcher. The selected design uses an integrated list-and-detail pattern by default to control window count, while preserving an explicit way to pop a resource into a dedicated detail window.

## Goals

- Make cluster health and resource state understandable at a glance.
- Preserve egui's flexible multi-window workspace model.
- Support simultaneous views of the same workload kind with different namespaces or filters.
- Keep routine list-to-detail navigation inside one window to prevent window explosion.
- Support operational workflows: related pods, logs, shell, YAML editing, scale, restart, and delete.
- Make context, mutation scope, stale data, and destructive actions unambiguous.

## Non-goals

- Reproducing a web dashboard visual system inside egui.
- A permanently docked IDE layout.
- Showing every Kubernetes field in the initial list or Overview window.
- Expanding every installed CRD into the launcher.
- Defining Kubernetes API or authentication implementation details in this UI-only design.

## Application shell

### Top bar

The compact top bar contains `File`, `View`, and `Help` on the left. Connection state, refresh, and the global Kubernetes context/cluster combo box appear on the right. The global switch changes context/cluster only; namespace remains a local filter inside resource windows.

The context switch always displays the complete selected context in its tooltip. Switching context preserves window kinds, geometry, sizes, filters where valid, and split ratios, but clears selected resource identities and reloads all data. A context switch with an active shell or dirty YAML editor requires confirmation. Confirming disconnects shells and either applies or discards edits according to the user's explicit choice.

### Fixed left launcher

The launcher is a fixed left panel. It uses selectable-label highlighting, not checkboxes.

- `Overview`, `Nodes`, and `Storage` are top-level items.
- `Workloads` is a collapsible group.
- Built-in workload children are `Deployments`, `Pods`, `StatefulSets`, `DaemonSets`, `Jobs`, `CronJobs`, and `Custom Resources…`.
- A highlighted workload item means at least one list-window instance is open.
- A numeric badge shows the number of open list-window instances.
- Clicking a highlighted item focuses and raises its most recently used instance.
- A compact `+` button opens another independent list-window instance.
- Closing the final instance removes the highlight and badge.

Every workload list window owns its namespace, search, filters, selection, position, size, active detail tab, and list/detail split ratio. This lets two Pods windows show `payments` and `observability` simultaneously.

`Custom Resources…` opens a generic custom-resource window rather than adding arbitrary CRDs to the launcher. Its searchable kind picker searches discovered resources by kind, plural name, API group, and version. After selecting a kind, the window lists its instances with namespace and text filters.

## Canvas and window behavior

The area to the right of the launcher is a free canvas. Inner windows use standard egui chrome: collapse triangle, icon, centered title, close button, border, shadow, and resize grip. New windows open in sensible staggered positions and retain their last position and size for the session.

Overview is the only window open on first launch. Opening additional windows does not close Overview. The active window receives normal egui focus ordering and moves above other windows.

## Default integrated list-and-detail model

Deployments, Pods, StatefulSets, DaemonSets, Jobs, CronJobs, and custom-resource windows use a vertically split layout:

1. The upper pane contains the resource toolbar and table.
2. Selecting a table row displays its detail view in the lower pane.
3. A draggable horizontal separator changes the list/detail ratio.
4. The detail pane can be hidden to maximize the list.
5. The list pane can be collapsed to give Logs, Shell, or YAML most of the window.
6. Selecting another row updates the integrated detail pane.

This integrated layout is the default because it keeps the canvas window count stable and makes list-to-detail navigation fast.

### Preserved dedicated-detail behavior

The independent detail-window behavior remains available:

- Single-click selects a row and updates the integrated lower detail pane.
- Double-click opens that resource in a dedicated, movable Detail window.
- The row context menu also offers `Open in dedicated window`.
- Dedicated Detail windows contain the same identity header, tabs, actions, and state as the integrated pane.
- Multiple dedicated Detail windows may be opened deliberately for side-by-side comparison.

This hybrid is intentional: integrated details handle routine navigation; dedicated details support focused work and comparison without making every selection create a window.

## Resource windows

### Overview

Overview contains:

- Totals for nodes, pods, workloads, and persistent storage.
- CPU, memory, and pod-capacity progress bars.
- Workload-health counts with status dots and text.
- A short `Needs attention` table containing unhealthy or pending resources.
- Refresh and `last updated` state.

### Deployments

The list toolbar contains namespace, search, status filters, and refresh. The table shows namespace, name, desired/ready/available replicas, rollout status, image, and age.

Deployment detail tabs are:

- `Overview`: replica counts, rollout status, strategy, images, selectors, labels, conditions, and recent rollout history.
- `Pods`: pods resolved through the Deployment's ReplicaSets and owner references. This is an owner-filtered table inside the Deployment context, not a loose label-only search. Double-clicking a pod uses the same dedicated-detail behavior as the main Pods list.
- `YAML`: shared guarded YAML view/edit workflow.
- `Events`: newest-first Kubernetes events with reason, message, source, count, and time.

The detail action bar contains `Scale…`, `Restart…`, and `Delete…`.

### Pods

The list toolbar contains namespace, status, search, and refresh. The table shows name, ready containers, phase/reason, restarts, node, pod IP, owner, and age.

Pod detail tabs are:

- `Overview`: status, owner, node, pod IP, QoS class, conditions, containers, images, probes, and restart/exit summaries.
- `YAML`: shared guarded YAML view/edit workflow.
- `Logs`: streaming or bounded logs for a selected container.
- `Shell`: an explicit exec session for a selected container.
- `Events`: pod-related events.

The Pod detail action bar contains `Delete…`. Logs and Shell are contained in the Pod detail pane/window and do not create additional canvas windows.

### StatefulSets, DaemonSets, Jobs, and CronJobs

These use the same list-and-detail shell, tailored columns, Overview content, YAML, Events, and applicable actions.

- StatefulSet: pods, replicas, update strategy, volume claim templates; `Scale…`, `Restart…`, `Delete…`.
- DaemonSet: pods, desired/current/ready/available counts, node selector, update strategy; `Restart…`, `Delete…`.
- Job: pods, completions, parallelism, succeeded/failed counts, duration; `Delete…` and applicable rerun/create-from controls.
- CronJob: schedules, suspend state, active jobs, last schedule, next schedule; `Suspend/Resume`, `Run now…`, `Delete…`.

### Nodes

Nodes contains Ready/Not Ready totals and a searchable, sortable table for name, status, roles, Kubernetes version, CPU, memory, pods, and age. Resource usage uses standard progress bars and always includes numeric text.

### Storage

Storage uses selectable tabs for PersistentVolumeClaims, PersistentVolumes, and StorageClasses. Tables show status, capacity, access modes, class, bindings, reclaim policy, namespace where applicable, and age.

## Detail tools

### Logs

Logs includes:

- Container combo box for multi-container pods.
- Tail amount and optional `Since` control.
- Follow/pause toggle.
- Timestamps toggle.
- Find-in-logs field and clear button.
- Monospace scrolling log view with restrained ANSI color support.

Changing containers restarts the stream only after the selection is committed. Stream state is visible as `Following`, `Paused`, `Disconnected`, or `Error`. A disconnected stream keeps already loaded text and offers Retry.

### Shell

Shell includes:

- Container combo box.
- Command choice such as `/bin/sh` or `/bin/bash`.
- Explicit `Connect` and `Disconnect` controls.
- Connection state and elapsed session time.
- A monospace terminal area.

No exec session starts merely by visiting the tab. Closing the tab's parent window, switching context, selecting another pod, or pressing Disconnect ends the session after confirmation when appropriate. RBAC denial and missing shell binaries are presented inline without closing the detail pane.

### YAML view and edit

All workload detail views share one guarded YAML workflow:

1. YAML opens read-only.
2. `Edit` enables the text editor and marks the detail state dirty.
3. `Review changes` shows a side-by-side or unified diff.
4. Local syntax/schema checks and a server-side dry run report validation results.
5. `Apply changes` is enabled only after validation succeeds.
6. `Back to edit` preserves the buffer; `Discard` requires confirmation when changes exist.

The review clearly warns when a change triggers rollout, recreation, or another disruptive operation. Conflict responses caused by an updated resource preserve the user's buffer and offer `Reload`, `Review against latest`, or `Cancel`.

## Workload actions

Actions use standard egui buttons and modal confirmation windows.

- `Scale…`: shows current replicas and an integer input/drag value, validates range, summarizes the change, then applies.
- `Restart…`: explains that a rollout restart changes the pod-template annotation, shows context/namespace/name, and requires confirmation.
- `Delete…`: uses danger styling, shows exact context/namespace/kind/name, offers valid propagation choices, and requires explicit confirmation. High-impact deletes require typing the resource name.

Buttons are disabled with an explanatory tooltip when RBAC forbids an action or the resource kind does not support it. Pending mutations show progress and cannot be submitted twice. Success produces a short inline confirmation; failures remain attached to the originating action and include Retry when safe.

## State and data flow

The UI state is conceptually divided into:

- `AppState`: selected context, connection state, refresh state, and discovered API resources.
- `WorkspaceState`: open window instances, focus order, geometry, collapse state, and the most recently used instance per launcher item.
- `ResourceWindowState`: kind, namespace, filters, sort, selected identity, split ratio, and detail-pane visibility.
- `ResourceDetailState`: resource identity/version, active tab, loaded detail data, YAML edit buffer and validation state, log stream state, and shell session state.
- `OperationState`: confirmation, in-flight mutation, success, or error for scale/restart/delete/apply.

Reads populate resource-window-local snapshots. Selection resolves detail data by stable resource identity. A selected resource that disappears becomes a `Resource no longer exists` state rather than silently selecting another row. Watches or refreshes update lists without discarding unsaved YAML or active tool state.

## Loading, empty, stale, and error states

- Loading keeps window chrome and controls stable, with a spinner and descriptive label in the content area.
- Empty filters show `No resources match these filters` and a `Clear filters` action.
- An empty namespace shows `No <kind> in this namespace`.
- Connection loss marks displayed data as stale, shows the last successful update time, pauses live streams, and exposes Retry.
- RBAC denial names the forbidden verb and resource without exposing credentials.
- Partial metrics availability leaves affected values as `—` with a tooltip instead of reporting zero.
- Deleted or replaced resources preserve the selection area long enough to explain what changed.
- Status never relies on color alone; icon/dot and text always accompany color.

## Visual constraints

- Use default egui dark styling and components wherever possible.
- Keep padding and density close to the egui demo rather than a spacious web dashboard.
- Use blue for selection and links, green for healthy, amber for warning/pending, and red for failure/destructive actions.
- Use monospace text for YAML, logs, shell, resource identifiers, and numeric operational data.
- Avoid ornamental gradients, custom card systems, oversized typography, and persistent animation.

## Static screen set

The approved design set covers:

1. Overview initial canvas.
2. Fixed left launcher with expanded Workloads group.
3. Multiple Pods list windows with independent namespaces.
4. Generic Custom Resources kind search.
5. Deployment detail Overview and owner-filtered Pods.
6. Pod Logs and Shell.
7. Guarded YAML edit/diff/apply.
8. Separate-detail versus integrated-detail comparison.
9. Final hybrid behavior: integrated detail by default, dedicated detail on double-click or context-menu request.

Brainstorm mockups are stored under `.superpowers/brainstorm/` for local reference and are intentionally excluded from version control.

## Verification criteria

- Only Overview is open on first launch.
- The left launcher contains no checkboxes.
- Every Workloads child can open multiple list-window instances with independent namespaces and filters.
- Launcher highlight and count always match open list-window instances.
- Single-click selection updates the integrated lower detail pane.
- Double-click and `Open in dedicated window` open equivalent standalone details.
- Deployment Pods lists only pods owned through the selected Deployment's controller chain.
- Pod Logs and Shell require a container choice when more than one is available.
- Shell never connects implicitly.
- Dirty YAML cannot be lost by closing, changing selection, or switching context without confirmation.
- Scale, restart, delete, and apply actions display exact target scope and prevent duplicate submission.
- Loading, empty, stale, forbidden, conflict, and resource-gone states fit without changing the window shell.
- Status remains understandable without color.
- Layout remains usable when inner windows are resized to their defined minimum sizes.
