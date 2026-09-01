# List + Detail Redesign

## Goal

Adopt `10-list-detail-redesign.html` as the visual reference for every resource
list and integrated Detail view. Remove the full-width Clear selection control,
make dense information readable without losing important suffixes, and share
one responsive frame across Deployment, Pod, Service, and generic resources.

This work changes presentation and selection interaction. Existing resource
subscriptions, authoritative data sources, mutations, YAML editing, logs,
shells, port forwarding, navigation guards, and dedicated Detail windows remain
intact.

## Design Reference

Copy the supplied `/Users/draco/Downloads/10-list-detail-redesign.html` to
`docs/designs/10-list-detail-redesign.html`. It is the visual acceptance
reference. The implementation follows the project's egui design system rather
than reproducing browser CSS literally.

## Architecture

Use a shared-frame-first implementation:

1. Shared List owns column sizing, priority, overflow, selection presentation,
   and selection-clearing gestures.
2. Shared Detail frame owns the splitter, identity row, vital strip, tabs and
   actions, finite body, and shortcut footer.
3. Deployment, Pod, Service, and generic adapters supply typed presentation
   data, resource-specific Overview sections, and capability-driven actions.
4. Existing commands and state stores remain the source of truth. Presentation
   adapters do not parse display strings when typed data is available.

Do not duplicate the shared frame per resource and do not perform unrelated
workspace or transport refactors.

## Shared List

Columns use three roles:

- fixed critical columns for compact numeric or state values;
- elastic identity columns such as Name;
- lower-priority columns that hide automatically by responsive priority at
  narrow widths.

User-configurable column visibility and persistence are not part of this
scope. Each resource adapter declares a deterministic priority order. The
shared table measures the available content width, allocates fixed columns,
gives the Name column the remainder, and then removes lower-priority columns in
adapter order until Name retains its declared minimum. Columns reappear in the
reverse order when the window grows, without changing user state.

Initial adapter fixtures, measured in egui points before standard cell padding,
are:

| Adapter | Columns and minimum widths | Automatic hide order |
| --- | --- | --- |
| Deployment | Namespace 112, Name 180 elastic, Ready 56, Status 112, Image 180 elastic, Age 56 | Image, then Status |
| Pod | Namespace 112, Name 180 elastic, Ready 56, Status 112, Restarts 64, Node 120, Age 56 | Node, then Restarts |
| Service | Namespace 112, Name 180 elastic, Type 88, Cluster IP 120, Ports 180 elastic, Age 56 | Cluster IP, then Type |
| Generic namespaced | Namespace 112, Name 180 elastic, Status 120, Age 56 | Status, then Age |
| Generic cluster-scoped | Name 220 elastic, Status 120, Age 56 | Status, then Age |

At the 1000-point fixture all listed columns render. At 640 points columns hide
only as needed in the stated order. Namespace and Name are never automatically
hidden. After all eligible columns are removed, any remaining width deficit
uses one horizontal table scroller; fixed critical columns and identity widths
do not compress below their declared minimum. Cluster-scoped rows omit
Namespace and reclaim its allocation.

Namespace and Name remain discoverable. Numeric values align right. Status uses
text plus shape/color rather than color alone. Long image and identifier values
use middle elision so meaningful suffixes, especially image tags, remain
visible. The complete value is available through a tooltip and copy affordance
where the row already supports copying.

The full-width Clear selection row is removed. A selection clears through any
of these equivalent gestures:

- the close control in the integrated Detail identity row;
- `Esc` while the resource window is active;
- clicking the selected row again.

All three paths call the same guarded workspace command. Dirty YAML and live
shell blockers continue through the existing navigation guard; selection is
not discarded silently.

The shared row interaction receives both the row identity and current
selection. It emits `SelectRow(identity)` for a different row and
`ClearSelection(window_id)` for the already-selected row; it never emits a
second `SelectRow` for a toggle-off gesture. If the guard blocks or defers the
clear command, the row, Detail state, and selection highlight remain intact.

When no selection exists, integrated Detail content collapses and the list
receives the released height. A splitter grip remains only when it represents a
real, restorable split state.

All resource windows adopt selection-derived Detail visibility. The Service
window's legacy Hide/Show details control is removed. A restored snapshot with
`detail_visible = false` is accepted for compatibility but ignored: no Detail
appears without a live selection, and selecting a row shows Detail. New
snapshots serialize the compatibility field as `true` until a later snapshot
schema revision removes it.

## Shared Detail Frame

The vertical structure is:

1. compact splitter grip for integrated Detail;
2. identity row with freshness, Pop out, Maximize/Restore, and close controls;
3. one-line vital strip;
4. tab and action row;
5. one finite, vertically scrolling body;
6. fixed shortcut footer.

Dedicated Detail windows use the same content frame without the integrated
selection-close affordance. Their pinned identity and lifecycle stay unchanged.

Vitals are independent chips containing a small muted label and a readable
value. They never wrap into another row and are never partially clipped.
Resource adapters mark priorities; lower-priority vitals move into an
accessible `Show more` popover when width is insufficient. The popover lists
the hidden chips vertically and never expands the vital strip. Failure and
warning states retain text and shape indicators. Ties use declaration order so
repeated resize operations are stable.

Overview and Events each give their shared body the only vertical scroll area
for that tab. Header, tabs, actions, and footer stay fixed. Tables and
exceptional long values may scroll horizontally only when elision would destroy
required information. Tool tabs (Logs, Shell, YAML editor, and Service port
forward) replace the shared body scroller with their existing dedicated
viewport; the frame does not wrap those viewports in another vertical
ScrollArea. Thus every `(window, tab)` has exactly one vertical scroll owner and
tool interaction remains unchanged.

## Responsive Overview

At content widths of 760 points or more, Overview uses two columns with the
operational/table side and configuration/KV side assigned a `1.35 : 1` share
after the gutter. Below 760 points it becomes one column ordered as operational
state, configuration, then identity. Layout tests use 1000-point and 640-point
content widths; resizing across 760 points must not change selection, scroll
state, tab, or vital declaration order.

At 1000 points every adapter shows all resource-specific required vitals inline.
Freshness remains the separate identity-row marker and is not duplicated as a
vital chip. At 640 points,
Deployment preserves Rollout, Ready, Up-to-date, and Available; Pod preserves
Status, Ready, Restarts, and Age; Service and generic kinds preserve Status,
and Age. Remaining values appear in the `Show more` popover in declaration
order. If even the preserved chips cannot fit, the strip scrolls horizontally
as a last-resort accessibility fallback rather than clipping or wrapping.

Long KV fields such as image, selector, and annotations use a two-line form:
the label occupies its own line, while the value receives the full column
width. Image paths use middle elision and preserve the tag. Full values remain
available via tooltip and copy. Empty sections do not reserve large blocks;
short empty-state copy attaches to the nearest meaningful section.

Resource adapters provide:

- Deployment: rollout vitals, Pods, rollout history, template, management,
  labels, annotations, identity, and existing scale/restart/delete actions.
- Pod: status/readiness/restarts vitals, failure context, containers,
  conditions, events, placement, network, metadata, and existing log/shell
  flows.
- Service: status/age vitals, ports and port-forward workflows, selectors,
  metadata, and existing actions.
- Generic resources: compact status/age/freshness vitals and the current
  specialized or generic Overview body without inventing unsupported fields.

## Loading, Failure, and Authority

Loading, reconnecting, stale, forbidden, failed, gone, filtered-empty, and
empty states retain the shared frame where an identity is available. Mutations
remain capability- and mutation-authority-driven. Stale or failed live data
disables operations that require current authority, while pinned-identity
commands such as copying a name or namespace remain available when safe.

Missing values render explicit unavailable copy or `—`; the UI does not infer
Kubernetes state from unrelated fields. A missing typed projection preserves
YAML and other existing tabs and shows a structured-details-unavailable body.

Shared actions follow this matrix. `Yes` means visible and enabled when the
resource capability allows it; `View` means read-only access; `No` means hidden
or disabled with the existing explanatory tooltip.

| State | Pop out | Copy identity | Logs/Shell | YAML view | YAML edit/apply | Scale/Restart/Delete | Port forward |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Live | Yes | Yes | Yes | View | Yes | Yes | Yes |
| Loading/reconnecting with pinned identity | Yes | Yes | No | No | No | No | No |
| Stale or failed with last good data | Yes | Yes | No | View | No | No | No |
| Forbidden | Yes | Yes | No | No | No | No | No |
| Gone | Yes | Yes | No | View when cached | No | No | No |

Maximize/Restore is frame-local and remains enabled whenever integrated Detail
exists. Tabs with no authoritative or cached content render their current
unavailable state rather than starting a new request through a disabled action.
Action availability is always the intersection of the current freshness row,
the resource capability, and mutation authority. Typed-projection availability
is orthogonal: a legacy or missing projection changes only the structured
Overview body and never grants an action disallowed by the current freshness or
authority state.

## Accessibility and Keyboard Behavior

Every icon-only control has an accessible label and tooltip. State is expressed
with text and shape in addition to color. The close control is labeled `Clear
selection`; the vital overflow control announces whether it will show or hide
more values.

Existing tab shortcuts remain valid. `Esc` clears selection only when no
higher-priority modal or editor escape behavior consumes it. Guarded selection
clearing uses the same confirmation semantics regardless of mouse or keyboard
origin.

## Testing and Acceptance

Tests cover:

- all three selection-clearing gestures and dirty YAML/live shell guards;
- selected-row presentation and collapse/reclaimed list height;
- column roles, numeric alignment, middle elision, and image-tag preservation;
- one-line vital priority and overflow behavior;
- wide and narrow Overview layouts and a single vertical scroll owner;
- fixed Detail chrome and footer at constrained heights;
- capability-driven actions for Deployment, Pod, Service, and generic kinds;
- loading, stale, forbidden, failed, gone, empty, and legacy projection states;
- accessible labels and keyboard precedence;
- regression coverage for YAML, logs, shell, events, port forwarding, Pop out,
  Maximize/Restore, and dedicated Detail windows.

Verification runs focused Rust tests during implementation, then formatting,
Clippy, relevant workspace tests, WASM compilation, and native visual checks at
wide and narrow sizes against `docs/designs/10-list-detail-redesign.html`.

## Delivery Strategy

Implement shared primitives first, then migrate resource adapters in this
order: Deployment, Pod, Service, generic resources. Each migration keeps the
workspace compiling and its focused tests passing. The feature is considered
complete only when every resource kind uses the shared frame and the old
full-width Clear selection control has no remaining render path.
