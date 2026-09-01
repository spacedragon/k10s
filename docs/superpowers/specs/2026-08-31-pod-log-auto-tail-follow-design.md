# Pod Log Auto-Tail and Scroll Follow Design

## Goal

Make Pod logs start streaming as soon as the user opens the Logs tab, and make
scroll position—not a manual checkbox—control whether the viewport follows new
output.

## Scope

This change is confined to the existing Pod Logs tool and its UI/application
integration. It does not change the log protocol, server-side tail parameters,
bounded retained-history policy, pause behavior, or shell streams.

## Connection behavior

When a Logs tab renders with a valid Pod/container target, a newly created or
otherwise eligible disconnected log view immediately transitions to Connecting
and queues one `OpenLogs` action. The existing stream request continues to use
the selected container, Previous setting, Since setting, the 200-line server
tail, timestamps, and `follow: true`.

Automatic connection happens only after the Logs tab is opened. Selecting a Pod
while viewing another detail tab must not create a log stream.

The automatic attempt is one-shot for a given disconnected state. Rendering
additional frames while the request is pending, streaming, or failed must not
queue duplicate requests. A failed view preserves its safe error and retained
history and exposes an explicit Retry action. Changes to container, Previous,
or Since disconnect the current source and make the changed source eligible for
one new automatic connection when the Logs tab next renders.

## Scroll-follow behavior

The scroll area's actual position is authoritative:

- At the bottom, within a small logical-pixel tolerance, the view is following.
- While following, appended log lines cause the viewport to remain at the newest
  visible line.
- When the user scrolls upward, following stops immediately. Incoming lines
  continue to append to the existing bounded buffer, but the viewport remains
  visually stationary.
- Scrolling back to the bottom restores following automatically.

The manual Follow checkbox is removed. A fresh or reconnected source begins in
follow mode and starts at the bottom. Pause remains independent: pausing retains
its existing meaning of dropping incoming chunks and does not become a scroll
control.

When the bounded client tail evicts old lines while the user is not following,
the renderer should preserve the viewport as closely as egui permits; it must
not deliberately jump to the bottom. The existing truncation counter continues
to explain removed history.

## State and rendering responsibilities

`LogsTool` remains the pure stream and retained-buffer state machine. It tracks
connection-attempt eligibility and the renderer's current follow projection,
but does not own pixel offsets. `LogsViews` continues to queue protocol actions
once per window.

The Logs renderer owns scroll geometry. After rendering the scroll area it
derives whether the viewport is at the bottom, updates the tool's follow state,
and requests bottom alignment only when the view was already following. This
ordering prevents a newly appended line from making a previously bottom-aligned
view appear non-following before autoscroll is applied.

The application continues to drain `OpenLogs` actions and open the dedicated
socket through the existing ticket path; no transport or backend changes are
needed.

## Failure handling

Ticket or socket failures leave the viewer disconnected with its error visible
and do not trigger an automatic request loop. Retry clears the prior error and
starts one new attempt. Control-connection loss keeps retained logs readable and
uses the same explicit retry behavior after connectivity returns.

## Testing

Implementation follows red-green TDD and covers:

1. First rendering of a valid Logs tab queues exactly one open action and moves
   the tool to Connecting; subsequent frames do not duplicate it.
2. Merely selecting a Pod outside the Logs tab does not connect logs.
3. Failure does not retry every frame, while explicit Retry queues one attempt.
4. A view at the bottom stays at the bottom when lines append.
5. A view scrolled upward preserves its viewport while new lines append.
6. Returning to the bottom restores follow behavior.
7. Container, Previous, and Since changes reconnect once with the updated
   parameters.
8. Existing tail truncation, pause, find, export, and stream ticket tests remain
   green.

