# Pod shell action and sync status design

## Goal

Keep Pod detail chrome focused and unambiguous by moving external shell launch into the existing `Actions` menu and showing source-sync state only when attention is required.

## External shell action

- Remove the container selector and `Open shell` control currently rendered above the shared detail frame.
- Expose shell launch only for a live, identity-matched core `v1/Pod` when the native desktop shell capability is available and at least one non-empty container name exists.
- Add `Open shell` to the existing detail `Actions` menu.
- For a Pod with exactly one container, selecting `Open shell` immediately queues an external shell for that container using `/bin/sh`.
- For a Pod with multiple containers, `Open shell` opens a nested menu containing one item per container. Selecting a container queues an external shell for that exact container using `/bin/sh`.
- Keep the existing dispatch-time generation, identity, authority, and container validation. Web builds and unavailable, stale, failed, forbidden, gone, or otherwise non-authoritative details must not expose the action.
- The action must not depend on or change the container selected in the Logs tool.

## Sync status placement and labels

- Remove the normal `Live`/`Ready` freshness badge from the identity or tab/action chrome.
- Surface exceptional source-sync state as a vital chip in the same strip as `STATUS`, `READY`, `RESTARTS`, and other resource facts.
- Use the chip label `SYNC`, with these displayed values:
  - initial loading: `Connecting`
  - stale while retrying: `Stale`
  - reconnecting: `Reconnecting`
  - forbidden: `Access denied`
  - failed or unavailable: `Failed`
  - resource gone: `Gone`
- Do not render a `SYNC` chip for `Live` or `ReadyEmpty`.
- Use warning styling for `Connecting`, `Stale`, and `Reconnecting`; use danger styling for `Access denied`, `Failed`, and `Gone`.
- Preserve detailed freshness context in hover text, including last-sync/retry or backend error information when available.
- This chip describes synchronization authority, not the Kubernetes workload phase; the existing `STATUS` chip remains unchanged.

## Layout and accessibility

- Both integrated and dedicated detail views use the same vital-strip placement and the same action-menu behavior.
- The action menu and nested container items must have stable accessible names. A single-container action is `Open shell`; multi-container items use `Open shell: <container>`.
- An exceptional `SYNC` chip has sufficient priority to remain visible in compact layouts instead of being hidden in the vital overflow.
- Removing the old badge must return its reserved width to tabs/actions in dedicated detail layouts.
- The change must preserve compact and stacked detail layouts without overlap or clipping.

## Verification

- Add or update UI interaction tests for absent native capability, single-container direct launch, multi-container selection, exact queued target, and absence when mutations are not allowed.
- Add or update frame tests for hidden healthy sync state and each exceptional `SYNC` projection.
- Update affected deterministic snapshots.
- Run focused Pod detail/snapshot tests, formatting, and the relevant crate test suite.
