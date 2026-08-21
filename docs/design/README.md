# k10s UI Design Archive

This directory preserves the standalone HTML mockups created while designing the k10s egui console. The canonical behavioral specification is [the k10s egui console design](../superpowers/specs/2026-08-21-k10s-egui-console-design.md).

Open any HTML file directly in a browser. The files include local shared assets and do not depend on the original brainstorming server.

## Design decisions

1. **Use default egui visual language.** The app uses egui's dark theme, native windows, tables, selectable labels, combo boxes, progress bars, tabs, text editors, and compact spacing. It avoids a custom web-dashboard design system.
2. **Use an inner-window workspace.** Resource views are draggable, resizable, collapsible, and closable egui windows on a free canvas.
3. **Open only Overview on launch.** Other windows are opened explicitly and retain their session position and size.
4. **Keep the launcher fixed on the left.** This was chosen over a right rail, top command bar, and floating launcher palette.
5. **Use highlight instead of checkboxes.** An active launcher item is highlighted. Workload items display an instance-count badge.
6. **Make Workloads a collapsible group.** It contains Deployments, Pods, StatefulSets, DaemonSets, Jobs, CronJobs, and Custom Resources.
7. **Allow multiple workload list windows.** Clicking an active item focuses its most recently used window; clicking `+` creates another instance. Every instance owns its namespace, filters, selection, position, and size.
8. **Keep Custom Resources bounded.** One generic Custom Resources window provides searchable CRD-kind selection instead of expanding every discovered CRD into the launcher.
9. **Use integrated details by default.** Selecting a resource shows its Detail in a resizable lower pane inside the list window, limiting window proliferation.
10. **Preserve deliberate pop-out details.** Double-clicking a Deployment or Pod, or choosing the row context-menu action, opens a dedicated Detail window for comparison or focused work.
11. **Resolve Deployment Pods by controller ownership.** The related Pods tab follows Deployment UID → controller ReplicaSet UID → controller Pod owner reference, not label matching alone.
12. **Keep operational tools in resource Detail tabs.** Deployment uses Overview, Pods, YAML, and Events. Pod uses Overview, YAML, Logs, Shell, and Events.
13. **Guard YAML changes.** YAML is read-only by default and follows Edit → Review diff → Validate/dry-run → Apply. Dirty buffers block destructive navigation until reviewed, discarded, or cancelled.
14. **Make Shell explicit and scoped.** Shell requires a container, command, and Connect action. It never starts merely by opening the tab.
15. **Scope every mutation visibly.** Scale, restart, delete, run-now, suspend/resume, and YAML apply show exact cluster/context, namespace or cluster scope, GVK, name, and UID where applicable.
16. **Switch context globally, namespace locally.** The top bar switches Kubernetes context/cluster. Namespace remains an independent filter inside each resource window.

## Mockup index

| File | Purpose | Status |
| --- | --- | --- |
| [01-workspace-approaches.html](01-workspace-approaches.html) | Top command bar, fixed rail, and floating palette comparison | Exploration |
| [02-side-panel-placement.html](02-side-panel-placement.html) | Left-versus-right fixed launcher comparison | Decision source: left selected |
| [03-k10s-screen-set.html](03-k10s-screen-set.html) | Overview, Workloads, Nodes, and Storage screen set | Core visual direction |
| [04-workloads-resource-windows.html](04-workloads-resource-windows.html) | Workload grouping and searchable Custom Resources | Historical; checkbox treatment was superseded |
| [05-multi-window-workloads.html](05-multi-window-workloads.html) | Highlight/count/plus launcher and two Pods namespaces | Approved launcher behavior |
| [06-resource-detail-workflows.html](06-resource-detail-workflows.html) | Deployment Pods, Pod Logs/Shell, YAML, and actions | Approved Detail content |
| [07-detail-placement-comparison.html](07-detail-placement-comparison.html) | Independent versus integrated Detail comparison | Final decision: integrated default plus pop-out |

## Relationship to the implementation plan

The implementation plan is [the k10s egui static prototype plan](../superpowers/plans/2026-08-21-k10s-egui-static-prototype.md). Where an exploratory HTML conflicts with the specification, the specification and the decision log above take precedence.
