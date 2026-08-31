# Deployment Pod Relation Performance Design

## Problem

Opening a Deployment's Pods tab issues `resource.relations`. The real
Kubernetes adapter currently lists every namespaced resource type advertised
by discovery before resolving controller-owner UID relationships. Those LISTs
run serially, so unrelated CRDs and slow aggregated APIs delay the Pod list.

## Design

Keep the protocol, UI lifecycle, and controller-ownership semantics unchanged.
Before sweeping relation candidates, select the resource descriptors required
for the target kind. A Deployment requires only `apps/*/ReplicaSet` and
`core/v1/Pod` candidates. The existing traversal then resolves the strict
Deployment UID -> controller ReplicaSet UID -> controller Pod owner-reference
chain.

All other target kinds retain the existing generic namespaced catalog sweep.
This bounds the fix to the reported path without inventing incomplete relation
rules for other Kubernetes controllers.

Descriptor selection belongs in the owner-traversal module as a pure helper.
The Kubernetes adapter passes the selected slice into the existing candidate
sweep. Selection uses group and kind rather than a hard-coded ReplicaSet
version so discovery remains authoritative about served API versions; Pods
remain core `v1`.

## Error Handling

Candidate LIST failures keep their current best-effort behavior: an unavailable
or forbidden ReplicaSet or Pod API contributes no rows but does not fail the
entire relation request. The selected Deployment identity is still verified by
an exact GET and UID comparison before candidate collection.

## Tests

- A unit test supplies a catalog containing ReplicaSet, Pod, unrelated built-in
  resources, Events, and CRDs, and asserts that a Deployment selects only
  ReplicaSet and Pod descriptors.
- Existing owner-traversal tests continue to prove strict controller UID
  matching and the Deployment -> ReplicaSet -> Pod result.
- Focused backend and server relation suites verify the optimized selection did
  not alter the wire response.

## Success Criteria

- Opening a Deployment's Pods tab causes at most the Deployment identity GET,
  one ReplicaSet LIST, and one Pod LIST, independent of catalog/CRD size.
- No label-only, name-only, or non-controller relationship is accepted.
- Other resource relation behavior remains unchanged.
