# Issue #193 integrated layout repair

## Goal

Make the real browser-integrated Deployment List + Detail view meet the approved
reference: its 1000x700 viewport retains a usable 1.35:1 Overview layout, and
its 640x700 viewport deliberately collapses to one column without hiding any
single-line controls behind a clipped edge.

## Design

The workload window's persisted and manually resized geometry is never changed.
Only the first creation of a Deployment window (before any persisted or manual
geometry exists) uses a default geometry that fits the 1000x700 browser canvas
while reserving a 760-point Detail body. When that geometry cannot fit, it uses
the normal default and the Detail uses its documented one-column contract. The
Detail body keeps its existing 1.35:1 operational to
configuration ratio at or above 760 points. At 640x700 it uses the existing
one-column contract.

Every single-line control region budgets against its own local available width,
never the global canvas. The toolbar preserves search, namespace, status, and
Live; Columns and refresh move to an accessible overflow menu when necessary.
The match line preserves result/selection state and moves sort/age switching to
its overflow. Detail identity keeps its name and close control; vitals preserve
Rollout and Ready while less-important vitals use their existing visible `Show
more` popover; tabs preserve the active tab and use a visible tab overflow;
actions preserve Scale and delete while Restart and Actions move to overflow.
No region uses an always-hidden horizontal scrollbar as its overflow mechanism.
Every overflow trigger is keyboard reachable; its menu exposes the displaced
commands, closes with Escape/outside click, and its bounds remain inside the
window/canvas.

At the 640-point detail width, PODS and rollout history retain their existing
semantic columns in a horizontally scrollable region with a visible scrollbar,
so every required value remains reachable rather than clipped.

## Validation

Add regression coverage with the deterministic fake-data fixture, DPR 1, and
the browser's 1000x700 and 640x700 viewport geometries. Assert the wide view
exposes both Overview columns at the target ratio, the narrow view uses the
metadata disclosure, and every visible control/menu rectangle is contained by
its owning window (within one point). Visual verification retains committed
before/after assets and adds Playwright `toHaveScreenshot` baselines, so a later
overlap or clipping regression fails CI.
