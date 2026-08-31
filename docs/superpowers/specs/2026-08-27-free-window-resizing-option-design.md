# Free Window Resizing Option Design

## Goal

Add a persisted workspace-level `Free window resizing` toggle to the View menu. The option is off by default. When off, workspace windows retain the existing minimum sizes. When on, users may resize windows below those minima in either dimension while content remains reachable through window-level scrolling.

The working tree currently contains the preceding free-resize experiment: `ui/window.rs` unconditionally uses a zero minimum and bidirectional window scrolling. This feature turns that experiment into the enabled branch of an explicit policy and restores the pre-experiment minimum-size behavior as the default disabled branch.

## User Experience

The View menu contains a checkable `Free window resizing` item.

- Off is the default for a new workspace and for snapshots written by older versions.
- Turning the item on immediately enables unrestricted resizing for every open window and every window opened later.
- Turning it off immediately restores the normal minimum-size policy: Workload and Detail windows use 640 by 420 egui points; Overview, Nodes, Storage, and Services use 480 by 320 points.
- Existing window geometry is not rewritten when the option changes. If a window is smaller than the restored minimum when free resizing is turned off, egui expands it to the applicable minimum during rendering and the normal geometry update records that rendered size.
- Window positions remain constrained to the workspace canvas in both modes. In normal mode the kind-specific minimum takes precedence when the outer viewport makes the canvas smaller than that minimum, so an internal window may be larger than the visible canvas rather than violating its content-safe size. Free mode can shrink to the available canvas.

## State and Command Flow

`WorkspaceState` owns one boolean free-resizing preference. It exposes a read-only accessor and changes only through a new `WorkspaceCommand` toggle. This keeps the menu, renderer, tests, and persistence on the existing command-driven state path.

The top bar receives the current value, renders the checkable View-menu item, and queues the toggle command when activated. The window canvas receives the same authoritative value from `WorkspaceState` and selects one of two policies:

- Normal: kind-specific minimum size, sourced from named UI policy constants in `ui/window.rs`, with no window-level scrolling added by this feature. Workload and Detail use 640 by 420; Overview, Nodes, Storage, and Services use 480 by 320.
- Free: zero explicit minimum size and bidirectional window-level scrolling so child content cannot force the outer window back to its natural size.

The setting is global to the workspace rather than per-window. There is no partially free state and no backend interaction.

## Persistence and Compatibility

The preference is part of `WorkspaceSnapshot`, because it controls persisted workspace layout behavior. The current snapshot schema is extended through version 3. The decoder continues accepting both v1 and v2, marks either source version as migrated so desktop persistence rewrites it, and initializes the missing preference to `false`. Version 3 snapshots require an explicit boolean; a malformed v3 payload with the field missing remains rejected rather than silently defaulting. New snapshots always write the explicit boolean.

Migration preserves all existing window geometry and view state. Malformed or unsupported snapshot behavior remains unchanged. Desktop debounce and final-flush persistence continue to compare and write the full snapshot, so toggling the preference schedules persistence without a separate settings store.

## Testing

Tests cover:

1. A new workspace reports free resizing off.
2. The workspace command toggles the setting without affecting window identity or content state.
3. The View menu exposes an accurately checked item and clicking it updates workspace state.
4. Normal mode enforces the existing kind-specific minima.
5. Free mode preserves a requested compact geometry such as 240 by 160.
6. A snapshot round trip preserves the setting.
7. Both v1 and v2 snapshots migrate with the setting off and retain migration provenance for rewrite.
8. A v3 payload missing the required preference is rejected.
9. Normal mode preserves its minimum-size policy even when the workspace canvas is smaller than that minimum; free mode can shrink within the same canvas.
10. Accessibility snapshots are regenerated only where the checked menu item or free-mode scroll container intentionally changes structure.

The full `k10s-ui` and desktop persistence test suites must pass, along with formatting checks.
