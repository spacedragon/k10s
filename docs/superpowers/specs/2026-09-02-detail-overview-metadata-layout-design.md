# Detail Overview Metadata Layout Design

## Goal

Bring the native resource Detail Overview back in line with the approved dense desktop reference: visually separate its information sections, make the Pods table legible and aligned, and present Kubernetes labels and annotations without sparse or misleading spacing.

## Scope

This change is limited to the shared Detail Overview metadata renderer and the deployment Overview Pods-table renderer. It does not change backend projections, Kubernetes data, tab behavior, mutations, or the responsive List/Detail split.

## Visual contract

### Section hierarchy

Template, Managed by, Labels/Annotations, and Identity remain plain regions within the right-hand Overview column. A full-width hairline appears exactly once between consecutive rendered regions and spans the locally visible column width within one standard item-spacing unit. Empty optional regions are omitted: Managed by is absent when it has no rows, Labels/Annotations is absent when both collections are empty, and the annotation disclosure is absent when annotations are empty. There are no leading, trailing, or doubled dividers. Dividers encode section boundaries; they do not introduce cards, shadows, or additional decoration.

### Pods table

Pod names and values use `egui::TextStyle::Body` and the same `max(interact_size.y, Body text height)` row-height calculation as the main resource list. Headers and values remain aligned to resolved semantic columns, numeric values stay right-aligned where applicable, and long names elide without pushing neighboring columns into one another. When required semantic minima exceed the locally visible width, the existing narrow horizontal scrolling remains available rather than compressing columns into overlap.

### Labels

All label chips remain visible. Chips flow left-to-right and automatically wrap onto subsequent lines according to the available detail-column width. There is no fixed visible-label count and no `Show N more labels` truncation. Each chip retains its full accessible value and hover text when visually elided.

### Annotations

Annotations are collapsed by default into one compact `Annotations N ▾` disclosure beside/below the labels flow as space permits. When expanded, the stable label changes only its direction glyph to `Annotations N ▴`; its button semantics expose an explicit expanded state. Activating it expands a compact key/value list directly beneath the disclosure; activating it again collapses the list. Long or unbroken keys and values elide with their complete text retained for accessibility and hover. Each expanded row is constrained to the locally visible column width, uses normal Body line height plus standard item spacing, and has no fixed or minimum height greater than its measured content.

## Responsive behavior

The same rules apply in wide two-column and narrow one-column Overview layouts, including the existing narrow metadata disclosure path. Label chips wrap rather than overflow horizontally. Expanded annotations consume only their measured content height. Section dividers span the locally visible detail width.

## Accessibility

The annotation disclosure exposes a button role, a stable label containing the annotation count, and an explicit expanded/collapsed property. Label and annotation values preserve their complete semantic text even when visual elision is necessary. Existing keyboard navigation order remains unchanged.

## Verification

Native UI regression tests will assert:

- each pair of consecutive rendered Overview sections has exactly one separator spanning the visible local width within one standard item-spacing unit, with no leading/trailing/doubled separator;
- label chips wrap at constrained widths and remain discoverable without a hidden-count control;
- annotations start collapsed, toggle open and closed with the correct accessibility state, and expanded rows use measured Body spacing;
- Pods table uses the shared Body style and resource-list row-height calculation, does not overlap columns, and retains narrow horizontal scrolling;
- narrow-layout coverage includes expanded annotations, long/unbroken label and annotation content, empty metadata, and the existing metadata disclosure path;
- existing wide and narrow Detail Overview snapshots are deliberately refreshed only after the behavior tests pass.

The focused Detail tests, full `k10s-ui` tests, formatting, and strict Clippy must pass. The desktop app will then be launched against the local kubeconfig for manual visual confirmation.
