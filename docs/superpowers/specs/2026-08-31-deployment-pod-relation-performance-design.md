# Deployment Pod Relation Performance Design

## Problem

Opening a Deployment's Pods tab issues `resource.relations`. The real
Kubernetes adapter currently lists every namespaced resource type advertised
by discovery before resolving controller-owner UID relationships. Those LISTs
run serially, so unrelated CRDs and slow aggregated APIs delay the Pod list.

## Design

Keep the protocol, UI lifecycle, and controller-UID matching unchanged. Before
sweeping relation candidates, select the resource descriptors required for an
exact `apps/Deployment` target. A Deployment uses only the `apps/ReplicaSet`
descriptor whose served version matches the Deployment version and the exact
`core/v1/Pod` descriptor. The existing traversal then resolves descendants by
strict controller owner-reference UID. As today, this traversal also accepts a
Pod directly controller-owned by the Deployment; it does not add new typed-hop
validation.

This intentionally narrows a Deployment's generic related-resource result to
the ReplicaSets and Pods needed by its Pods tab. Any unusual custom resource
directly controller-owned by a Deployment no longer appears in that result.
That trade-off is accepted to make the interactive Pods path independent of
unrelated APIs.

All other target kinds retain the existing generic namespaced catalog sweep.
This bounds the fix to the reported path without inventing incomplete relation
rules for other Kubernetes controllers.

Descriptor selection belongs in the owner-traversal module as a pure helper.
The Kubernetes adapter passes the selected descriptors into the existing
candidate sweep. Exact group, version, and kind matching guarantees at most one
ReplicaSet and one Pod LIST even if discovery advertises several ReplicaSet
versions or a CRD reuses either kind name. If the matching served version is
absent, that candidate type contributes no rows rather than silently choosing
a different API version.

## Error Handling

Candidate LIST failures keep their current best-effort behavior: an unavailable
or forbidden ReplicaSet or Pod API contributes no rows but does not fail the
entire relation request. The selected Deployment identity is still verified by
an exact GET and UID comparison before candidate collection.

## Tests

- A unit test supplies a catalog containing ReplicaSet, Pod, unrelated built-in
  resources, Events, same-kind CRDs, and multiple ReplicaSet versions, and
  asserts that an exact `apps/Deployment` selects only its matching ReplicaSet
  version and `core/v1/Pod`.
- Unit coverage proves a same-named non-`apps` Deployment and every other
  target kind retain the unchanged generic catalog sweep.
- An adapter-level request test gives an unrelated CRD LIST a non-completing
  response and asserts Deployment relations still complete, with zero CRD LIST
  hits and exactly one ReplicaSet and one Pod LIST hit.
- Existing owner-traversal tests continue to prove strict controller UID
  matching and the Deployment -> ReplicaSet -> Pod result.
- Focused backend and server relation suites verify the optimized selection did
  not alter the wire response.

## Success Criteria

- After discovery/catalog resolution, opening an `apps/Deployment` Pods tab
  causes only the Deployment identity GET, one matching-version ReplicaSet
  LIST, and one Pod LIST, independent of catalog/CRD size.
- No label-only, name-only, or non-controller relationship is accepted.
- Other resource relation behavior remains unchanged.
