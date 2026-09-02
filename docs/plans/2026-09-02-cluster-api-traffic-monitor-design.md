# Cluster API Traffic Monitor Design

## Scope

The monitor measures logical HTTP payload traffic between each server-side
`kube::Client` and the Kubernetes API server. It includes ordinary API calls,
watch and log response streams, and exec/port-forward HTTP upgrade handshakes.
It does not packet-capture Pod, Service, Node, TLS framing, or upgraded stream
payload traffic, and it never records paths, headers, credentials, or content.

## Data flow

Every lazily constructed real context client receives a Tower traffic layer.
Atomic per-context counters track uploaded bytes, downloaded bytes, request
count, and active response bodies. The backend traffic subscription samples a
counter once per second and calculates rates from the subscriber's prior cut,
so multiple frontend sessions cannot distort one another's rate calculation.

Protocol minor 1.5 adds the context-scoped `traffic` selector and the
coalescible `traffic.updated` event. The server forwards samples through the
existing P2 scheduler: control responses remain higher priority, and a slow UI
can skip an intermediate sample because the next complete sample self-heals.
Older peers negotiate a lower minor and do not open this subscription.

The shared client retains at most sixty samples per context. The application
switches subscriptions only after a backend-confirmed context transition. The
top bar renders current download/upload rates to the right of the context
selector, a two-series one-minute sparkline at normal widths, and compact text
at narrow widths. Its tooltip provides totals, request counts, active requests,
status, and the exact privacy-preserving measurement scope.

## Failure behavior

An unknown or unavailable context rejects the subscription without weakening
other control operations. Disconnects leave the last sample visible as stale;
normal subscription recovery resumes it. Counter overflow and clock anomalies
saturate instead of panicking. Fake mode emits a deterministic zero sample.
