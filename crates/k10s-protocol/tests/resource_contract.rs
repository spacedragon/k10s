//! Stable wire contracts for normalized resource, subscription, and metrics
//! payloads.
//!
//! These tests pin the exact JSON shapes shared by the server and both
//! clients. No Kubernetes-specific types may appear: every payload is a
//! backend-owned normalized view model.

use std::collections::BTreeMap;

use k10s_protocol::{
    BackendRevision, GroupVersionKind, InfrastructureWatchSpec, MetricsAvailability, PodMetrics,
    RequestId, ResourceCapabilities, ResourceDetailResponse, ResourceGone, ResourceIdentity,
    ResourceListResponse, ResourceListRow, ResourceScope, ResourceSnapshotPage, ServerFrame,
    ServerKind, SubscriptionSelector, WorkloadKind, decode_server_frame,
};
use serde_json::{Value, json};

fn round_trip<T: serde::Serialize + serde::de::DeserializeOwned>(value: &T) -> Value {
    let encoded = serde_json::to_value(value).expect("payload must serialize");
    let decoded: T = serde_json::from_value(encoded.clone()).expect("payload must deserialize");
    let reencoded = serde_json::to_value(&decoded).expect("payload must re-serialize");
    assert_eq!(encoded, reencoded, "round trip must be stable");
    encoded
}

#[test]
fn infrastructure_watch_uses_the_typed_subscription_selector() {
    let selector = SubscriptionSelector::Infrastructure(InfrastructureWatchSpec {
        context: "dev-local".into(),
    });
    assert_eq!(
        round_trip(&selector),
        json!({"kind": "infrastructure", "context": "dev-local"})
    );
}

#[test]
fn resource_identity_uses_stable_camel_case_keys() {
    let identity = ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        },
        namespace: Some("default".into()),
        name: "web-frontend".into(),
        uid: "uid-1".into(),
    };
    let encoded = round_trip(&identity);
    assert_eq!(
        encoded,
        json!({
            "context": "dev-local",
            "gvk": {"group": "apps", "version": "v1", "kind": "Deployment"},
            "namespace": "default",
            "name": "web-frontend",
            "uid": "uid-1",
        })
    );
}

#[test]
fn cluster_scoped_identity_omits_namespace() {
    let identity = ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "Node".into(),
        },
        namespace: None,
        name: "dev-node-1".into(),
        uid: "uid-node".into(),
    };
    let encoded = serde_json::to_value(&identity).unwrap();
    assert!(encoded.get("namespace").is_none());
    let decoded: ResourceIdentity = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded.scope(), ResourceScope::Cluster);
}

#[test]
fn identity_scope_distinguishes_namespaced_and_cluster() {
    let namespaced = ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Pod"),
        namespace: Some("kube-system".into()),
        name: "pod-1".into(),
        uid: "uid-pod".into(),
    };
    let clustered = ResourceIdentity {
        namespace: None,
        ..namespaced.clone()
    };
    assert_eq!(namespaced.scope(), ResourceScope::Namespaced);
    assert_eq!(clustered.scope(), ResourceScope::Cluster);
}

#[test]
fn every_designed_workload_kind_round_trips() {
    let kinds = [
        WorkloadKind::Deployment,
        WorkloadKind::ReplicaSet,
        WorkloadKind::StatefulSet,
        WorkloadKind::DaemonSet,
        WorkloadKind::Job,
        WorkloadKind::CronJob,
        WorkloadKind::Pod,
    ];
    for kind in kinds {
        let encoded = serde_json::to_value(kind).unwrap();
        let decoded: WorkloadKind = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, kind);
    }
    assert_eq!(
        serde_json::to_value(WorkloadKind::StatefulSet).unwrap(),
        json!("statefulSet")
    );
}

#[test]
fn workload_kind_is_derived_from_group_version_kind() {
    for (group, version, kind, expected) in [
        ("apps", "v1", "Deployment", Some(WorkloadKind::Deployment)),
        ("apps", "v1", "ReplicaSet", Some(WorkloadKind::ReplicaSet)),
        ("apps", "v1", "StatefulSet", Some(WorkloadKind::StatefulSet)),
        ("apps", "v1", "DaemonSet", Some(WorkloadKind::DaemonSet)),
        ("batch", "v1", "Job", Some(WorkloadKind::Job)),
        ("batch", "v1", "CronJob", Some(WorkloadKind::CronJob)),
        ("", "v1", "Pod", Some(WorkloadKind::Pod)),
        ("monitoring.example.com", "v1", "Dashboard", None),
        ("", "v1", "Node", None),
    ] {
        let gvk = GroupVersionKind {
            group: group.into(),
            version: version.into(),
            kind: kind.into(),
        };
        assert_eq!(WorkloadKind::from_gvk(&gvk), expected, "{gvk:?}");
    }
}

#[test]
fn snapshot_frames_stream_with_subscription_metadata() {
    let page = ResourceSnapshotPage {
        revision: BackendRevision::new(7),
        rows: vec![ResourceListRow {
            identity: ResourceIdentity {
                context: "dev-local".into(),
                gvk: GroupVersionKind::core("v1", "Pod"),
                namespace: Some("default".into()),
                name: "web-1".into(),
                uid: "uid-web".into(),
            },
            revision: BackendRevision::new(7),
            labels: BTreeMap::from([("app".to_owned(), "web".to_owned())]),
            summary: "Running".into(),
            created_at: "2026-08-21T00:00:00Z".into(),
        }],
    };

    let begin = ServerFrame {
        kind: ServerKind::SnapshotBegin,
        request_id: None,
        subscription_id: Some("sub-1".into()),
        sequence: Some(1),
        payload: serde_json::to_value(k10s_protocol::SnapshotBegin { total_chunks: 2 }).unwrap(),
    };
    let chunk = ServerFrame {
        kind: ServerKind::SnapshotChunk,
        request_id: None,
        subscription_id: Some("sub-1".into()),
        sequence: Some(2),
        payload: json!({"chunkIndex": 0, "data": page}),
    };
    let end = ServerFrame {
        kind: ServerKind::SnapshotEnd,
        request_id: None,
        subscription_id: Some("sub-1".into()),
        sequence: Some(3),
        payload: json!({"checksum": "fnv-64:deadbeef"}),
    };

    for frame in [begin, chunk, end] {
        let text = serde_json::to_string(&frame).unwrap();
        let decoded = decode_server_frame(serde_json::from_str(&text).unwrap()).unwrap();
        assert_eq!(
            decoded.subscription_id.as_ref().map(|id| id.as_str()),
            Some("sub-1")
        );
        decoded.decode_payload().expect("snapshot payload decodes");
    }
}

#[test]
fn snapshot_frames_require_subscription_id_and_sequence() {
    for frame in [
        json!({"kind":"snapshotBegin","subscriptionId":"s","payload":{"totalChunks":1}}),
        json!({"kind":"snapshotChunk","sequence":1,"payload":{"chunkIndex":0,"data":{}}}),
        json!({"kind":"snapshotEnd","subscriptionId":"s","payload":{"checksum":"x"}}),
    ] {
        let error = decode_server_frame(frame).unwrap_err();
        assert_eq!(error.code, k10s_protocol::ErrorCode::InvalidRequest);
    }
}

#[test]
fn backend_revisions_are_monotonic_by_construction() {
    let first = BackendRevision::new(1000);
    let second = BackendRevision::new(1001);
    assert!(first < second);
    assert_eq!(first.get(), 1000);
    assert_eq!(serde_json::to_value(second).unwrap(), json!(1001));
    let decoded: BackendRevision = serde_json::from_value(json!(1001)).unwrap();
    assert_eq!(decoded, second);
    // Revisions display as their numeric string for envelopes and logs.
    assert_eq!(second.to_string(), "1001");
}

#[test]
fn metrics_availability_is_exactly_tri_state() {
    for (availability, expected) in [
        (MetricsAvailability::Available, "available"),
        (MetricsAvailability::Partial, "partial"),
        (MetricsAvailability::Unavailable, "unavailable"),
    ] {
        assert_eq!(serde_json::to_value(availability).unwrap(), json!(expected));
        let decoded: MetricsAvailability = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(decoded, availability);
    }

    let full = PodMetrics {
        availability: MetricsAvailability::Available,
        cpu_millicores: Some(250),
        memory_bytes: Some(134_217_728),
        collected_at: Some("2026-08-21T00:00:00Z".into()),
    };
    assert_eq!(round_trip(&full), full_json());

    // Partial metrics keep missing values absent rather than zero.
    let partial = PodMetrics {
        availability: MetricsAvailability::Partial,
        cpu_millicores: Some(120),
        memory_bytes: None,
        collected_at: Some("2026-08-21T00:00:00Z".into()),
    };
    let encoded = serde_json::to_value(&partial).unwrap();
    assert!(encoded.get("memoryBytes").is_none());
}

fn full_json() -> Value {
    json!({
        "availability": "available",
        "cpuMillicores": 250,
        "memoryBytes": 134217728,
        "collectedAt": "2026-08-21T00:00:00Z",
    })
}

#[test]
fn resource_gone_event_decodes_from_event_frames() {
    let gone = ResourceGone {
        identity: ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            namespace: Some("default".into()),
            name: "web-3".into(),
            uid: "uid-web-3".into(),
        },
        revision: BackendRevision::new(1042),
    };
    let frame = ServerFrame {
        kind: ServerKind::Event,
        request_id: None,
        subscription_id: Some("sub-1".into()),
        sequence: Some(9),
        payload: json!({
            "kind": "resource.gone",
            "revision": "1042",
            "payload": gone,
        }),
    };
    let text = serde_json::to_string(&frame).unwrap();
    let decoded = decode_server_frame(serde_json::from_str(&text).unwrap()).unwrap();
    let event = match decoded.decode_payload().unwrap() {
        k10s_protocol::ServerPayload::Event(event) => event,
        other => panic!("expected event, got {other:?}"),
    };
    assert_eq!(event.event_kind, "resource.gone");
    assert_eq!(event.revision.as_deref(), Some("1042"));
    let parsed: ResourceGone = serde_json::from_value(event.payload).unwrap();
    assert_eq!(parsed.identity.name, "web-3");
    assert_eq!(parsed.revision, BackendRevision::new(1042));
}

#[test]
fn list_rows_carry_normalized_identity_labels_summary_and_timestamp() {
    let row = ResourceListRow {
        identity: ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            namespace: Some("default".into()),
            name: "api-server".into(),
            uid: "uid-api".into(),
        },
        revision: BackendRevision::new(1010),
        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
        summary: "2/2 ready".into(),
        created_at: "2026-08-21T00:05:00Z".into(),
    };
    let encoded = round_trip(&row);
    assert_eq!(encoded["summary"], json!("2/2 ready"));
    assert_eq!(encoded["revision"], json!(1010));
    assert_eq!(encoded["createdAt"], json!("2026-08-21T00:05:00Z"));
    assert_eq!(encoded["identity"]["name"], json!("api-server"));
    assert_eq!(encoded["labels"], json!({"app": "api"}));
}

#[test]
fn detail_response_contains_sections_owner_references_and_capabilities() {
    let response = ResourceDetailResponse {
        identity: ResourceIdentity {
            context: "dev-local".into(),
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "ReplicaSet".into(),
            },
            namespace: Some("default".into()),
            name: "web-frontend-7d9f8".into(),
            uid: "uid-rs".into(),
        },
        revision: BackendRevision::new(1011),
        created_at: "2026-08-21T00:06:00Z".into(),
        owner_references: vec![k10s_protocol::OwnerReference {
            gvk: GroupVersionKind {
                group: "apps".into(),
                version: "v1".into(),
                kind: "Deployment".into(),
            },
            name: "web-frontend".into(),
            uid: "uid-deploy".into(),
            controller: true,
        }],
        sections: vec![k10s_protocol::DetailSection {
            title: "Overview".into(),
            rows: vec![k10s_protocol::DetailRow {
                label: "Replicas".into(),
                value: "3 desired".into(),
            }],
        }],
        events: vec![k10s_protocol::EventRow {
            reason: "Started".into(),
            message: "replicaset reached 20 desired".into(),
            count: 1,
            last_seen: "2026-08-21T00:06:45Z".into(),
        }],
        related: vec![k10s_protocol::RelatedGroup {
            title: "Pods".into(),
            gvk: GroupVersionKind::core("v1", "Pod"),
            rows: Vec::new(),
        }],
        capabilities: ResourceCapabilities {
            can_edit_yaml: true,
            can_delete: true,
            can_scale: false,
            can_view_logs: false,
            can_exec: false,
        },
        manifest: "apiVersion: apps/v1\nkind: ReplicaSet\nmetadata:\n  name: web-frontend-7d9f8\n"
            .into(),
    };
    let encoded = round_trip(&response);
    assert_eq!(encoded["ownerReferences"][0]["controller"], json!(true));
    assert_eq!(
        encoded["sections"][0]["rows"][0]["label"],
        json!("Replicas")
    );
    assert_eq!(encoded["events"][0]["reason"], json!("Started"));
    assert_eq!(encoded["events"][0]["count"], json!(1));
    assert_eq!(encoded["related"][0]["title"], json!("Pods"));
    assert_eq!(encoded["related"][0]["gvk"]["kind"], json!("Pod"));
    assert_eq!(encoded["capabilities"]["canScale"], json!(false));
}

#[test]
fn resource_list_response_is_fully_normalized() {
    let response = ResourceListResponse {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Pod"),
        namespace: Some("default".into()),
        revision: BackendRevision::new(1024),
        rows: Vec::new(),
        generated_at: "2026-08-21T00:07:00Z".into(),
        capabilities: ResourceCapabilities::default(),
    };
    let encoded = round_trip(&response);
    assert_eq!(encoded["namespace"], json!("default"));
    assert_eq!(encoded["revision"], json!(1024));
    assert!(encoded["capabilities"]["canEditYaml"].is_boolean());
}

#[test]
fn response_frames_carry_resource_payloads_with_request_metadata() {
    let response = ResourceListResponse {
        context: "dev-local".into(),
        gvk: GroupVersionKind {
            group: "batch".into(),
            version: "v1".into(),
            kind: "Job".into(),
        },
        namespace: None,
        revision: BackendRevision::new(5),
        rows: Vec::new(),
        generated_at: "2026-08-21T00:08:00Z".into(),
        capabilities: ResourceCapabilities::default(),
    };
    let frame = ServerFrame::response(RequestId::from("req-9"), response);
    let text = serde_json::to_string(&frame).unwrap();
    let decoded = decode_server_frame(serde_json::from_str(&text).unwrap()).unwrap();
    assert_eq!(decoded.kind, ServerKind::Response);
    let parsed: ResourceListResponse = decoded.decode_response_payload().unwrap();
    assert_eq!(parsed.gvk.kind, "Job");
}
