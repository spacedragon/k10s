# Issue #193 integrated layout repair

## Goal

Make the real browser-integrated Deployment List + Detail view meet the approved
reference: its 1000x700 viewport retains a usable 1.35:1 Overview layout, and
its 640x700 viewport deliberately collapses to one column without hiding any
single-line controls behind a clipped edge.

## Design

The workload window will choose its normal geometry from the available canvas
so that the 1000x700 browser viewport reserves enough body width for the
reference Overview. The Detail body keeps its existing 1.35:1 operational to
configuration ratio when that local width is available. At 640x700 it uses the
existing one-column contract.

Every single-line control region will budget against its own local available
width, never the global canvas. The toolbar preserves search, namespace, and
status as its primary controls; lower-priority column and refresh controls move
to an accessible overflow menu when necessary. Detail tabs and actions retain
their active/high-frequency items and expose displaced actions through a visible
overflow affordance rather than an invisible horizontal scroll area.

## Validation

Add regression coverage for the browser-integrated 1000x700 and 640x700
geometries. Assert the wide view exposes both Overview columns at the target
ratio, the narrow view uses the metadata disclosure, and controls stay within
their owning rectangles. Visual verification uses committed before/after assets
plus Playwright image assertions, so a later overlap or clipping regression
fails CI.
