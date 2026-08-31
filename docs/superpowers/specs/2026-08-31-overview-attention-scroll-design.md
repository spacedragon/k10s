# Overview Attention Scroll Design

## Problem

The Overview window renders every `InfrastructureResponse::attention` row in a horizontally scrolling grid. With a real cluster containing many unhealthy or pending resources, the grid grows vertically without a bound. The Overview window then extends below the workspace canvas, making its lower content and window management controls difficult or impossible to reach.

## Scope

This change applies only to the Overview window's **Needs attention** panel. It does not establish a global scrolling policy, change other resource windows, or alter backend data, ordering, filtering, or limits.

## Design

The refresh row, summary cards, capacity panel, and workload-health panel remain fixed at the top of Overview. The metrics footer remains below the attention panel.

The Needs attention panel receives a finite viewport derived from the current persisted/configured Overview content height, not from the table's desired height. After rendering the fixed top sections, Overview subtracts their measured height, spacing, the measured metrics-footer height, and scrollbar occupancy from that finite content-height budget. The remaining value is clamped to a normal minimum attention viewport of 96 logical pixels. Its table uses a two-axis `egui::ScrollArea`: vertical scrolling exposes all attention rows without increasing the window's content height, and horizontal scrolling preserves full access to long resource names, statuses, and reasons.

At compact sizes where the fixed sections, footer, spacing, and 96-pixel attention viewport cannot all fit, the normal minimum no longer wins over the window boundary. The attention viewport shrinks to the remaining non-negative space, with a 48-pixel interactive floor when that floor itself fits. If even the fixed content plus that floor cannot fit at the existing 480×320 window minimum, Overview uses a bounded outer vertical scroll as a compact fallback. This fallback keeps the outer window within the canvas; it is not used at ordinary sizes, where the summary and health content stay fixed while only the attention table scrolls.

The attention table uses one `egui::ScrollArea::both()` with the existing stable ID `k10s.overview.attention.scroll`. Both scrollbar gutters count inside the finite viewport rather than adding to it. Empty attention state remains a simple message and does not display unnecessary scrollbars.

The existing egui window remains movable, resizable, collapsible, and constrained by the current window-management code. Resizing Overview changes the attention viewport rather than truncating or limiting the underlying rows.

## Data and Behavior

`overview::show` continues to receive the same immutable `InfrastructureResponse`. No protocol or backend changes are required. All rows remain rendered in their existing order inside the scroll region, and the existing table headings and accessible labels remain unchanged.

Refresh and connection behavior are unchanged. The scroll position is local egui widget state keyed by the existing stable attention scroll identifier.

## Testing

Add a focused UI regression test using a fixed harness viewport and explicit Overview geometry, plus enough uniquely named attention rows to exceed the available window height. The test must demonstrate that:

1. the rendered Overview window stays within its configured/canvas bounds;
2. a uniquely labeled late row is initially clipped;
3. sending a vertical scroll event to the attention region makes that late row visible without changing the Overview window rectangle;
4. fixed summary/health content retains stable positions before and after the attention scroll; and
5. existing refresh and metrics content remains available.

Add a compact-viewport case at the existing 480×320 minimum to prove the fallback keeps the Overview window inside the canvas and provides a scroll path to content that cannot fit simultaneously. The test should interact with the scroll regions rather than treating presence in the accessibility tree as proof of viewport scrolling.

Run the focused UI test first through a red-green cycle, then run the relevant `k10s-ui` test target and formatting checks. Finally, launch the desktop app with representative large attention data and visually confirm that the list scrolls while the Overview window stays manageable.

## Non-goals

- Pagination, row truncation, or a separate “Show all” view.
- Changing attention-row selection or navigation behavior.
- Changing global window geometry persistence.
- Retrofitting other long lists in this change.
