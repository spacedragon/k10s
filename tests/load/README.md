# Medium-large cluster capacity gate

This gate fixes the release-load model at 50,000 normalized objects and 1,000
nodes. It asserts first-snapshot chunking, a 10,000-event watch burst, repeated
relist cuts, slow-browser P2 coalescing, lossless operation priority, sustained
log encoding, latency, and non-accumulating memory in the existing allocator
gate.

Run the exact two-pass gate from the repository root:

```sh
rustc tests/load/run.rs -o /tmp/k10s-load
/tmp/k10s-load --test
```

The runner prints OS, CPU, and verbose Rust metadata before executing every
benchmark twice. Two passes prevent a cold-cache or one-time compilation effect
from being mistaken for stable runtime behavior. Thresholds are intentionally
generous enough for the self-hosted Linux runner while still detecting
order-of-magnitude regressions:

| Scenario | Budget |
| --- | ---: |
| 51,000-record fake construction | 60 s |
| 18,750-row normalized list | 10 s average |
| 10,000-event real watch burst with explicit bounded-lag recovery | 30 s |
| 20 complete watch relists | 30 s |
| real 7-row control-socket snapshot delivery | 30 s |
| dedicated log socket under a slow 10 MiB source | 30 s, explicit bounded overload |
| sequenced P2 coalescing and lossless P0 admission | 30 s |
| post-query live allocator drift | 8 MiB |

Public protocol semantics are not adjusted by these budgets. A failure should
be profiled first; optimize only the measured normalization, snapshot,
coalescing, projection, or scheduling path.

## Reviewed baseline

Recorded 2026-08-25 on the self-hosted `Gti` runner (WSL2 Linux 6.6.87.2,
Intel Core i9-12900H, x86_64, Rust 1.97.1). After release artifacts were warm,
the two required passes measured:

| Scenario | Pass 1 | Pass 2 |
| --- | ---: | ---: |
| fake construction | 129 ms | 134 ms |
| normalized list average | 19 ms | 20 ms |
| 10,000-event watch burst + lag recovery | 1.98 s | 1.95 s |
| 20 relists | 639 ms | 692 ms |
| sequenced scheduler load | 2.08 ms | 2.07 ms |
| real snapshot + slow-log socket gates | 1.10 s | 1.10 s |
| live allocator drift | 54,929 B | 54,929 B |

The published ceilings above are the reviewed failure thresholds. They retain
substantial scheduling headroom for a busy self-hosted runner while remaining
far below an order-of-magnitude regression from this named baseline.
