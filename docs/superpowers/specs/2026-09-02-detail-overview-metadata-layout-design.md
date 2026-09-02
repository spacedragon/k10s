# Detail Overview Metadata Layout Design

## Goal

Bring the native resource Detail Overview back in line with the approved dense desktop reference: visually separate its information sections, make the Pods table legible and aligned, and present Kubernetes labels and annotations without sparse or misleading spacing.

## Scope

This change is limited to the shared Detail Overview renderer. It does not change backend projections, Kubernetes data, tab behavior, mutations, or the responsive List/Detail split.

## Visual contract

### Section hierarchy

Template, Managed by, Labels/Annotations, and Identity remain plain regions within the right-hand Overview column. A full-width hairline separates adjacent regions. Dividers encode section boundaries; they do not introduce cards, shadows, or additional decoration.

### Pods table

Pod names and values use the same body text scale and row rhythm as the main resource lists. Headers and values remain aligned to stable columns, numeric values stay right-aligned where applicable, and long names elide without pushing neighboring columns into one another.

### Labels

All label chips remain visible. Chips flow left-to-right and automatically wrap onto subsequent lines according to the available detail-column width. There is no fixed visible-label count and no `Show N more labels` truncation. Each chip retains its full accessible value and hover text when visually elided.

### Annotations

Annotations are collapsed by default into one compact `Annotations N ▾` disclosure beside/below the labels flow as space permits. Activating it expands a compact key/value list directly beneath the disclosure; activating it again collapses the list. Expanded rows use normal body line height with bounded wrapping and no artificially tall allocation.

## Responsive behavior

The same rules apply in wide two-column and narrow one-column Overview layouts. Label chips wrap rather than overflow horizontally. Expanded annotations consume only their measured content height. Section dividers span the locally visible detail width.

## Accessibility

The annotation disclosure exposes a button role, a stable label containing the annotation count, and expanded/collapsed state. Label and annotation values preserve their complete semantic text even when visual elision is necessary. Existing keyboard navigation order remains unchanged.

## Verification

Native UI regression tests will assert:

- adjacent Overview sections expose non-overlapping full-width separators;
- label chips wrap at constrained widths and remain discoverable without a hidden-count control;
- annotations start collapsed, toggle open and closed, and expanded rows use compact measured spacing;
- Pods table body text uses the normal body scale and its columns do not overlap;
- existing wide and narrow Detail Overview snapshots are deliberately refreshed only after the behavior tests pass.

The focused Detail tests, full `k10s-ui` tests, formatting, and strict Clippy must pass. The desktop app will then be launched against the local kubeconfig for manual visual confirmation.
