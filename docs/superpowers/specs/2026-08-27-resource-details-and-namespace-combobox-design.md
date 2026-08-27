# Resource Details and Namespace Combobox Design

## Goal

Make Pod and Deployment details load promptly, and replace the free-form namespace filter with a searchable combobox backed by the complete Kubernetes Namespace list.

## Confirmed UX

- The namespace control is a searchable combobox.
- Its selectable values are the complete, authoritative `core/v1 Namespace` list from Kubernetes.
- Users may only select an existing namespace.
- An empty selection means all namespaces.
- Context-default namespace is not exposed and has no active UI meaning.
- New workload windows start with all namespaces selected.

## Root Cause

The current `resource.detail` kernel path reads the selected object and then waits for generic owner traversal before returning. The real Kubernetes adapter builds that traversal by serially listing every namespaced API type in the discovery catalog. On clusters with many resource types, slow aggregated APIs, or partially unavailable APIs, a Pod or Deployment detail response can remain pending for a long time. Existing small fake and recorded-cluster tests do not reproduce that latency.

## Detail Loading Architecture

Split primary detail data from related-resource traversal.

1. `resource.detail` returns the exact object identity, overview sections, YAML, bounded event enrichment, capabilities, and projection without waiting for relation traversal. Event API reads have a fixed one-second total budget; forbidden, unavailable, or timed-out event reads return the primary detail with an event-unavailable condition instead of delaying or failing the pane.
2. A separate related-resources query accepts the same exact `ResourceIdentity` and returns normalized related groups.
3. The UI issues the related query lazily when the Related tab is first selected.
4. Detail and related caches are keyed by the complete stable identity, including context, GVK, namespace, name, and UID.
5. A selection change or context switch cannot bind an older response to a newer selection.

Pod and Deployment details therefore become usable after their primary object read and bounded event enrichment finish. A slow or failed relation query affects only the Related tab.

## Related Wire Contract

The independent request kind is `resource.relations`. It uses `ResourceRefRequest { identity }` and returns `ResourceRelationsResponse { identity, revision, groups }`. The response echoes the complete identity. The client applies it only when its request ID, echoed context/GVK/namespace/name/UID, and connection generation match the pending entry; every mismatch is discarded without populating the cache.

For wire compatibility, `ResourceDetailResponse.related` remains present and gains a deserialization default. New servers serialize it as an empty list; new clients ignore any eagerly populated legacy value and use `resource.relations`. An optional, defaulted `eventsCondition` distinguishes available events from bounded/unavailable enrichment without breaking legacy payloads. An older server that reports `unsupportedMessage` for the new request produces an isolated Related-tab unavailable state. Protocol fixtures and loopback tests cover old-client/new-server and new-client/old-server behavior.

## Related Tab States

The Related tab has explicit independent states:

- Not requested: no query has been sent.
- Loading: one query is in flight for the selected identity.
- Loaded: normalized related groups are shown; an empty result is a valid loaded state.
- Failed: a safe error message and retry action are shown without removing the primary detail.

Repeated rendering must not create duplicate queries. Successful relation results have a 30-second freshness window; selecting Related after expiry starts one replacement request while retaining the old rows as stale content. A failed entry exposes Retry, which replaces the failure and starts exactly one request. Leaving the tab does not cancel an in-flight identity-bound request; changing selection, retiring a context, or losing transport cancels or discards it through the generation checks above.

## Namespace Data Flow

When at least one namespaced resource window is open, the application retains one shared cluster-scoped `core/v1 Namespace` subscription for the active context. The resulting rows are normalized into a sorted, deduplicated namespace-name list and included in the resource feed used by all windows.

Namespace demand comes from every open Service window and every resource window whose built-in or selected custom GVK is declared namespaced. A custom-resource picker with no selected GVK does not demand or render the control. Changing a custom selection from cluster-scoped to namespaced adds demand; the reverse removes its demand.

The feed exposes `NamespaceCatalogState::{NotDemanded, Loading, Ready(Vec<String>), Unavailable(SafeError)}` rather than a bare list. Snapshot pages remain internal while loading and become selectable only after the complete snapshot marker is applied. Watch additions/deletions update the ready list; resync returns it to Loading, and transport loss clears server-issued names and returns demanded catalogs to Loading.

The control is a searchable egui combobox:

- Opening the popup exposes a search field and matching namespace options.
- Selecting an option sets `NamespaceScope::Namespace(name)`.
- Clearing the selection sets `NamespaceScope::AllNamespaces`.
- The closed control shows the selected namespace or an all-namespaces placeholder when empty.
- Search text is UI scratch state scoped per window and is not persisted as workspace intent.

The shared Namespace subscription is retired when no namespaced window needs it and is rebuilt for context changes and reconnects through the existing subscription reconciliation path.

## Compatibility

`NamespaceScope::ContextDefault` remains deserializable so older workspace snapshots remain readable. Snapshot restoration normalizes that legacy value to `AllNamespaces` at the snapshot-to-workspace boundary, before demand calculation or subscription reconciliation; current UI actions never create or display context-default scope. Existing explicit namespaces remain unchanged. Defaults for both resource and Service windows become `AllNamespaces`.

The protocol change for related resources follows the existing typed query/request/response pattern. Older unsupported servers yield a Related-tab failure state while primary detail remains functional.

## Error Handling

- A primary detail error is reported in the detail pane instead of leaving an indefinite loading state. A blocked or failed Event API is bounded and produces primary detail plus an Events-unavailable condition.
- A related query error is isolated to the Related tab and is retryable.
- If Namespace listing is forbidden or unavailable, the combobox is disabled with an unavailable status. It never falls back to accepting arbitrary text.
- Transport loss clears server-issued primary details, related results, namespace data, and their in-flight request state; normal recovery repopulates demanded data.

## Testing

Tests are added before implementation and cover:

1. Pod and Deployment primary detail responses complete without waiting for a deliberately blocked relation query, and complete within the bounded enrichment budget when Event APIs block or fail.
2. Selecting Related sends exactly one identity-bound related query, renders loading, and then renders loaded or failed state independently of primary detail.
3. Late related responses cannot attach to a same-name/different-UID selection, a different connection generation, or a retired context; retry and 30-second stale-cache replacement issue exactly one request.
4. The Namespace subscription supplies a complete sorted candidate list to every namespaced window only after the complete snapshot marker; partial pages never appear as ready candidates.
5. Combobox search narrows options, selection applies an explicit namespace, and clearing applies `AllNamespaces`.
6. New windows and restored legacy `ContextDefault` snapshots resolve to all namespaces.
7. Namespace state and search scratch remain independent across multiple windows.
8. Cluster-scoped resource windows do not demand or render the namespace control; Services do demand it, and custom-resource cluster/namespaced GVK changes reconcile demand correctly.
9. Protocol compatibility fixtures cover defaulted legacy `related` data and unsupported `resource.relations` handling.

Targeted protocol, backend, server-loopback, client-state, workspace-state, and egui UI tests run alongside the existing workspace and detail suites.

## Out of Scope

- Free-form namespace entry.
- A context-default namespace option.
- Redesigning other resource filters.
- General optimization of every discovery or owner-traversal operation beyond separating it from primary detail loading.
