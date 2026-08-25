//! Table-driven normalization coverage for the real kube-rs adapter's list
//! read path.
//!
//! Every case runs a recorded API-server LIST response through the adapter
//! and kernel — no live cluster — and asserts that typed built-ins and
//! dynamic objects normalize into the same view models the fake adapter
//! produces: stable identity, preserved labels, controller owner references,
//! exact creation timestamps, and honest per-kind summaries.

use k10s_backend::testkit::RecordedApiServer;
use k10s_backend::{
    BackendKernel, ContextInfo, Gvk, KernelQueryResult, KubeAdapter, KubernetesAccess, Query,
    QueryResult, ResourceListData,
};
use k10s_protocol::{GroupVersionKind, ResourceListResponse};

const CONTEXT: &str = "norm-cluster";

/// One expected normalized row of a table case.
struct ExpectedRow {
    name: &'static str,
    uid: &'static str,
    namespace: Option<&'static str>,
    labels: &'static [(&'static str, &'static str)],
    summary: &'static str,
    /// Creation timestamp prefix; the instant must survive normalization
    /// exactly (no date guessing or re-basing).
    created_at_prefix: &'static str,
    /// Optional controlling owner reference as (kind, name).
    owner: Option<(&'static str, &'static str)>,
}

/// One table row: a kind, its recorded list cut, and the rows it must
/// normalize into.
struct Case {
    label: &'static str,
    gvk: Gvk,
    plural: &'static str,
    namespace: Option<&'static str>,
    list_body: &'static str,
    expect: Vec<ExpectedRow>,
}

impl Case {
    fn list_path(&self) -> String {
        let prefix = if self.gvk.group.is_empty() {
            format!("/api/{}", self.gvk.version)
        } else {
            format!("/apis/{}/{}", self.gvk.group, self.gvk.version)
        };
        match self.namespace {
            Some(namespace) => format!("{prefix}/namespaces/{namespace}/{}", self.plural),
            None => format!("{prefix}/{}", self.plural),
        }
    }
}

fn gvk(group: &str, version: &str, kind: &str) -> Gvk {
    Gvk::new(group, version, kind)
}

fn recorded_adapter(server: &RecordedApiServer) -> KubeAdapter {
    let client = server.clone().into_client("default");
    KubeAdapter::with_cluster_clients(
        vec![ContextInfo {
            name: CONTEXT.into(),
            cluster: "recorded-apiserver".into(),
            namespace: Some("default".into()),
            is_current: true,
            availability: k10s_protocol::ContextAvailability::Available,
            unavailable_reason: None,
        }],
        [(CONTEXT, client)],
    )
    .expect("adapter builds around the recorded server")
}

/// Run one case's LIST through the adapter seam and return the normalized
/// backend data.
async fn run_case(case: &Case) -> ResourceListData {
    let server = RecordedApiServer::standard();
    server.set_response(&case.list_path(), 200, case.list_body);
    let adapter = recorded_adapter(&server);
    match adapter
        .query(Query::ResourceList {
            context: CONTEXT.into(),
            gvk: case.gvk.clone(),
            namespace: case.namespace.map(str::to_owned),
        })
        .await
        .expect("resource list succeeds")
    {
        QueryResult::ResourceList(data) => data,
        other => panic!(
            "{}: adapter must normalize a resource list, got {other:?}",
            case.label
        ),
    }
}

/// Run one case's LIST through the full kernel-to-wire mapping.
async fn run_case_wire(case: &Case) -> ResourceListResponse {
    let server = RecordedApiServer::standard();
    server.set_response(&case.list_path(), 200, case.list_body);
    let kernel = BackendKernel::new(recorded_adapter(&server));
    match kernel
        .query(Query::ResourceList {
            context: CONTEXT.into(),
            gvk: case.gvk.clone(),
            namespace: case.namespace.map(str::to_owned),
        })
        .await
        .expect("resource list succeeds")
    {
        KernelQueryResult::ResourceList(result) => result.wire_payload(),
        other => panic!(
            "{}: kernel must map into a resource list, got {other:?}",
            case.label
        ),
    }
}

fn assert_rows(case: &Case, data: &ResourceListData) {
    assert_eq!(data.context, CONTEXT, "{}: context echoes back", case.label);
    assert_eq!(&data.gvk, &case.gvk, "{}: gvk echoes back", case.label);
    assert_eq!(
        data.namespace.as_deref(),
        case.namespace,
        "{}: namespace restriction echoes back",
        case.label
    );
    assert_eq!(
        data.rows.len(),
        case.expect.len(),
        "{}: row count drifts",
        case.label
    );

    let mut previous: Option<&k10s_backend::ResourceRecord> = None;
    for (row, expected) in data.rows.iter().zip(case.expect.iter()) {
        // Rows publish sorted by stable identity.
        if let Some(previous) = previous {
            assert!(
                row.reference >= previous.reference,
                "{}: rows must arrive sorted by stable identity",
                case.label
            );
        }
        previous = Some(row);

        let reference = &row.reference;
        assert_eq!(reference.name, expected.name, "{}: name", case.label);
        assert_eq!(
            reference.uid, expected.uid,
            "{}: uid of {}",
            case.label, expected.name
        );
        assert_eq!(
            reference.namespace.as_deref(),
            expected.namespace,
            "{}: namespace of {}",
            case.label,
            expected.name
        );
        assert_eq!(
            reference.context, CONTEXT,
            "{}: identity carries the context",
            case.label
        );
        assert_eq!(
            reference.gvk.kind, case.gvk.kind,
            "{}: identity carries the kind",
            case.label
        );

        let labels: std::collections::BTreeMap<String, String> = expected
            .labels
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        assert_eq!(
            &row.labels, &labels,
            "{}: labels of {}",
            case.label, expected.name
        );

        assert_eq!(
            row.summary, expected.summary,
            "{}: summary of {}",
            case.label, expected.name
        );
        assert!(
            row.created_at.starts_with(expected.created_at_prefix),
            "{}: created_at of {} drifted: {}",
            case.label,
            expected.name,
            row.created_at
        );

        match expected.owner {
            None => assert!(
                row.owner_references.is_empty(),
                "{}: unexpected owners on {}",
                case.label,
                expected.name
            ),
            Some((kind, name)) => {
                let owner = row
                    .owner_references
                    .iter()
                    .find(|owner| owner.controller && owner.name == name)
                    .unwrap_or_else(|| panic!("{}: missing controller owner {name}", case.label));
                assert_eq!(owner.gvk.kind, kind, "{}: owner kind of {name}", case.label);
                assert!(!owner.uid.is_empty(), "{}: owner uid of {name}", case.label);
            }
        }
    }
}

fn assert_wire_shape(case: &Case, payload: &ResourceListResponse) {
    assert_eq!(payload.context, CONTEXT, "{}: wire context", case.label);
    assert_eq!(
        payload.gvk,
        GroupVersionKind {
            group: case.gvk.group.clone(),
            version: case.gvk.version.clone(),
            kind: case.gvk.kind.clone(),
        },
        "{}: wire gvk",
        case.label
    );
    assert_eq!(
        payload.namespace.as_deref(),
        case.namespace,
        "{}: wire namespace",
        case.label
    );
    assert_eq!(
        payload.rows.len(),
        case.expect.len(),
        "{}: wire row count",
        case.label
    );
    for (row, expected) in payload.rows.iter().zip(case.expect.iter()) {
        assert_eq!(
            row.identity.name, expected.name,
            "{}: wire name",
            case.label
        );
        assert_eq!(row.identity.uid, expected.uid, "{}: wire uid", case.label);
        assert_eq!(
            row.summary, expected.summary,
            "{}: wire summary",
            case.label
        );
        assert!(
            row.created_at.starts_with(expected.created_at_prefix),
            "{}: wire created_at of {}",
            case.label,
            expected.name
        );
    }
    // The opaque Kubernetes resourceVersion never reaches the wire.
    let serialized = serde_json::to_string(payload).expect("wire payload serializes");
    assert!(
        !serialized.contains("resourceVersion"),
        "{}: rv leaked",
        case.label
    );
    assert!(
        !serialized.contains("resource_version"),
        "{}: rv leaked",
        case.label
    );
}

#[tokio::test]
async fn deployments_normalize_ready_counts() {
    let case = Case {
        label: "deployments",
        gvk: gvk("apps", "v1", "Deployment"),
        plural: "deployments",
        namespace: Some("default"),
        list_body: r#"{"kind":"DeploymentList","apiVersion":"apps/v1","metadata":{"resourceVersion":"41"},"items":[
          {"metadata":{"name":"web","uid":"uid-web","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z","labels":{"app":"web"}},
           "spec":{"replicas":3},"status":{"readyReplicas":3}},
          {"metadata":{"name":"api","uid":"uid-api","namespace":"default","creationTimestamp":"2026-08-20T12:30:00Z"},
           "spec":{"replicas":2},"status":{"readyReplicas":0}}
        ]}"#,
        expect: vec![
            ExpectedRow {
                name: "api",
                uid: "uid-api",
                namespace: Some("default"),
                labels: &[],
                summary: "0/2 ready",
                created_at_prefix: "2026-08-20T12:30:00",
                owner: None,
            },
            ExpectedRow {
                name: "web",
                uid: "uid-web",
                namespace: Some("default"),
                labels: &[("app", "web")],
                summary: "3/3 ready",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            },
        ],
    };
    let data = run_case(&case).await;
    assert_rows(&case, &data);
    let payload = run_case_wire(&case).await;
    assert_wire_shape(&case, &payload);
}

#[tokio::test]
async fn statefulsets_and_daemonsets_normalize_ready_counts() {
    let cases = [
        Case {
            label: "statefulsets",
            gvk: gvk("apps", "v1", "StatefulSet"),
            plural: "statefulsets",
            namespace: Some("default"),
            list_body: r#"{"kind":"StatefulSetList","apiVersion":"apps/v1","items":[
              {"metadata":{"name":"db","uid":"uid-db","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z"},
               "spec":{"replicas":3},"status":{"readyReplicas":3}}
            ]}"#,
            expect: vec![ExpectedRow {
                name: "db",
                uid: "uid-db",
                namespace: Some("default"),
                labels: &[],
                summary: "3/3 ready",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            }],
        },
        Case {
            label: "daemonsets",
            gvk: gvk("apps", "v1", "DaemonSet"),
            plural: "daemonsets",
            namespace: Some("default"),
            list_body: r#"{"kind":"DaemonSetList","apiVersion":"apps/v1","items":[
              {"metadata":{"name":"node-exporter","uid":"uid-ne","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z"},
               "status":{"desiredNumberScheduled":5,"numberReady":4}}
            ]}"#,
            expect: vec![ExpectedRow {
                name: "node-exporter",
                uid: "uid-ne",
                namespace: Some("default"),
                labels: &[],
                summary: "4/5 ready",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            }],
        },
    ];
    for case in &cases {
        let data = run_case(case).await;
        assert_rows(case, &data);
        let payload = run_case_wire(case).await;
        assert_wire_shape(case, &payload);
    }
}

#[tokio::test]
async fn pods_normalize_phase_and_crashloop() {
    let case = Case {
        label: "pods",
        gvk: gvk("", "v1", "Pod"),
        plural: "pods",
        namespace: Some("default"),
        list_body: r#"{"kind":"PodList","apiVersion":"v1","items":[
          {"metadata":{"name":"init-loop","uid":"uid-init-loop","namespace":"default","creationTimestamp":"2026-08-21T03:00:00Z"},
           "status":{"phase":"Pending",
             "initContainerStatuses":[{"state":{"waiting":{"reason":"CrashLoopBackOff"}}}],
             "containerStatuses":[{"state":{"waiting":{"reason":"PodInitializing"}}}]}},
          {"metadata":{"name":"running","uid":"uid-running","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z","labels":{"tier":"frontend"}},
           "status":{"phase":"Running"}},
          {"metadata":{"name":"looping","uid":"uid-looping","namespace":"default","creationTimestamp":"2026-08-21T02:00:00Z",
             "ownerReferences":[{"apiVersion":"apps/v1","kind":"ReplicaSet","name":"web-rs","uid":"uid-web-rs","controller":true}]},
           "status":{"phase":"Running","containerStatuses":[{"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}
        ]}"#,
        expect: vec![
            ExpectedRow {
                name: "init-loop",
                uid: "uid-init-loop",
                namespace: Some("default"),
                labels: &[],
                summary: "CrashLoopBackOff",
                created_at_prefix: "2026-08-21T03:00:00",
                owner: None,
            },
            ExpectedRow {
                name: "looping",
                uid: "uid-looping",
                namespace: Some("default"),
                labels: &[],
                summary: "CrashLoopBackOff",
                created_at_prefix: "2026-08-21T02:00:00",
                owner: Some(("ReplicaSet", "web-rs")),
            },
            ExpectedRow {
                name: "running",
                uid: "uid-running",
                namespace: Some("default"),
                labels: &[("tier", "frontend")],
                summary: "Running",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            },
        ],
    };
    let data = run_case(&case).await;
    assert_rows(&case, &data);
    let payload = run_case_wire(&case).await;
    assert_wire_shape(&case, &payload);
}

#[tokio::test]
async fn jobs_normalize_conditions() {
    let case = Case {
        label: "jobs",
        gvk: gvk("batch", "v1", "Job"),
        plural: "jobs",
        namespace: Some("default"),
        list_body: r#"{"kind":"JobList","apiVersion":"batch/v1","items":[
          {"metadata":{"name":"index","uid":"uid-index","namespace":"default","creationTimestamp":"2026-08-21T01:00:00Z"},
           "status":{"active":2}},
          {"metadata":{"name":"migrate","uid":"uid-migrate","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z"},
           "status":{"conditions":[{"type":"Complete","status":"True"}]}}
        ]}"#,
        expect: vec![
            ExpectedRow {
                name: "index",
                uid: "uid-index",
                namespace: Some("default"),
                labels: &[],
                summary: "Running",
                created_at_prefix: "2026-08-21T01:00:00",
                owner: None,
            },
            ExpectedRow {
                name: "migrate",
                uid: "uid-migrate",
                namespace: Some("default"),
                labels: &[],
                summary: "Complete",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            },
        ],
    };
    let data = run_case(&case).await;
    assert_rows(&case, &data);

    // A failed condition normalizes honestly too.
    let failed = Case {
        label: "job-failed",
        gvk: case.gvk.clone(),
        plural: "jobs",
        namespace: Some("default"),
        list_body: r#"{"kind":"JobList","apiVersion":"batch/v1","items":[
          {"metadata":{"name":"backup","uid":"uid-backup","namespace":"default","creationTimestamp":"2026-08-21T00:30:00Z"},
           "status":{"conditions":[{"type":"Failed","status":"True"}]}}
        ]}"#,
        expect: vec![ExpectedRow {
            name: "backup",
            uid: "uid-backup",
            namespace: Some("default"),
            labels: &[],
            summary: "Failed",
            created_at_prefix: "2026-08-21T00:30:00",
            owner: None,
        }],
    };
    let data = run_case(&failed).await;
    assert_rows(&failed, &data);
    let payload = run_case_wire(&case).await;
    assert_wire_shape(&case, &payload);
}

#[tokio::test]
async fn cronjobs_normalize_suspend_state() {
    let case = Case {
        label: "cronjobs",
        gvk: gvk("batch", "v1", "CronJob"),
        plural: "cronjobs",
        namespace: Some("default"),
        list_body: r#"{"kind":"CronJobList","apiVersion":"batch/v1","items":[
          {"metadata":{"name":"hourly","uid":"uid-hourly","namespace":"default","creationTimestamp":"2026-08-21T00:30:00Z"},
           "spec":{"schedule":"0 * * * *"},"status":{"active":[{"kind":"Job","name":"hourly-1","uid":"uid-h1"}]}},
          {"metadata":{"name":"nightly","uid":"uid-nightly","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z"},
           "spec":{"schedule":"0 0 * * *","suspend":true},"status":{}}
        ]}"#,
        expect: vec![
            ExpectedRow {
                name: "hourly",
                uid: "uid-hourly",
                namespace: Some("default"),
                labels: &[],
                summary: "Running",
                created_at_prefix: "2026-08-21T00:30:00",
                owner: None,
            },
            ExpectedRow {
                name: "nightly",
                uid: "uid-nightly",
                namespace: Some("default"),
                labels: &[],
                summary: "Suspended",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            },
        ],
    };
    let data = run_case(&case).await;
    assert_rows(&case, &data);
    let payload = run_case_wire(&case).await;
    assert_wire_shape(&case, &payload);
}

#[tokio::test]
async fn nodes_normalize_readiness() {
    let case = Case {
        label: "nodes",
        gvk: gvk("", "v1", "Node"),
        plural: "nodes",
        namespace: None,
        list_body: r#"{"kind":"NodeList","apiVersion":"v1","items":[
          {"metadata":{"name":"node-a","uid":"uid-node-a","creationTimestamp":"2026-08-21T00:00:00Z","labels":{"kubernetes.io/os":"linux"}},
           "status":{"conditions":[{"type":"Ready","status":"True"}]}},
          {"metadata":{"name":"node-b","uid":"uid-node-b","creationTimestamp":"2026-08-21T00:30:00Z"},
           "status":{"conditions":[{"type":"Ready","status":"False"}]}}
        ]}"#,
        expect: vec![
            ExpectedRow {
                name: "node-a",
                uid: "uid-node-a",
                namespace: None,
                labels: &[("kubernetes.io/os", "linux")],
                summary: "Ready",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            },
            ExpectedRow {
                name: "node-b",
                uid: "uid-node-b",
                namespace: None,
                labels: &[],
                summary: "NotReady",
                created_at_prefix: "2026-08-21T00:30:00",
                owner: None,
            },
        ],
    };
    let data = run_case(&case).await;
    assert_rows(&case, &data);
    let payload = run_case_wire(&case).await;
    assert_wire_shape(&case, &payload);
}

#[tokio::test]
async fn storage_kinds_normalize_phases_without_inventing_status() {
    let cases = [
        // PVC phases come straight from status.phase; quantities are never
        // parsed or guessed at.
        Case {
            label: "pvcs",
            gvk: gvk("", "v1", "PersistentVolumeClaim"),
            plural: "persistentvolumeclaims",
            namespace: Some("default"),
            list_body: r#"{"kind":"PersistentVolumeClaimList","apiVersion":"v1","items":[
              {"metadata":{"name":"data","uid":"uid-data","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z"},
               "spec":{"resources":{"requests":{"storage":"10Gi"}},"storageClassName":"fast"},
               "status":{"phase":"Bound","capacity":{"storage":"8Gi"}}}
            ]}"#,
            expect: vec![ExpectedRow {
                name: "data",
                uid: "uid-data",
                namespace: Some("default"),
                labels: &[],
                summary: "Bound",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            }],
        },
        Case {
            label: "pvs",
            gvk: gvk("", "v1", "PersistentVolume"),
            plural: "persistentvolumes",
            namespace: None,
            list_body: r#"{"kind":"PersistentVolumeList","apiVersion":"v1","items":[
              {"metadata":{"name":"pv-a","uid":"uid-pv-a","creationTimestamp":"2026-08-21T00:00:00Z"},
               "spec":{"capacity":{"storage":"8Gi"}},"status":{"phase":"Available"}},
              {"metadata":{"name":"pv-b","uid":"uid-pv-b","creationTimestamp":"2026-08-21T01:00:00Z"},
               "spec":{},"status":{"phase":"Released"}}
            ]}"#,
            expect: vec![
                ExpectedRow {
                    name: "pv-a",
                    uid: "uid-pv-a",
                    namespace: None,
                    labels: &[],
                    summary: "Available",
                    created_at_prefix: "2026-08-21T00:00:00",
                    owner: None,
                },
                ExpectedRow {
                    name: "pv-b",
                    uid: "uid-pv-b",
                    namespace: None,
                    labels: &[],
                    summary: "Released",
                    created_at_prefix: "2026-08-21T01:00:00",
                    owner: None,
                },
            ],
        },
        // StorageClasses carry no meaningful phase: the summary stays empty
        // instead of fabricating status.
        Case {
            label: "storageclasses",
            gvk: gvk("storage.k8s.io", "v1", "StorageClass"),
            plural: "storageclasses",
            namespace: None,
            list_body: r#"{"kind":"StorageClassList","apiVersion":"storage.k8s.io/v1","items":[
              {"metadata":{"name":"fast","uid":"uid-fast","creationTimestamp":"2026-08-21T00:00:00Z","labels":{"tier":"gold"}},
               "provisioner":"kubernetes.io/aws-ebs"}
            ]}"#,
            expect: vec![ExpectedRow {
                name: "fast",
                uid: "uid-fast",
                namespace: None,
                labels: &[("tier", "gold")],
                summary: "",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            }],
        },
    ];
    for case in &cases {
        let data = run_case(case).await;
        assert_rows(case, &data);
        let payload = run_case_wire(case).await;
        assert_wire_shape(case, &payload);
    }
}

#[tokio::test]
async fn dynamic_objects_normalize_standard_metadata_only() {
    let cases = [
        // Namespaced CRD objects: standard metadata only, empty summary even
        // though the object carries spec/status payloads.
        Case {
            label: "dynamic-namespaced",
            gvk: gvk("k10s.example.com", "v1alpha1", "Gadget"),
            plural: "gadgets",
            namespace: Some("default"),
            list_body: r#"{"kind":"GadgetList","apiVersion":"k10s.example.com/v1alpha1","items":[
              {"metadata":{"name":"spinner","uid":"uid-spinner","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z","labels":{"size":"small"},
                 "ownerReferences":[{"apiVersion":"k10s.example.com/v1alpha1","kind":"GadgetCluster","name":"gc","uid":"uid-gc","controller":true}]},
               "spec":{"rpm":1200},"status":{"phase":"Spinning"}}
            ]}"#,
            expect: vec![ExpectedRow {
                name: "spinner",
                uid: "uid-spinner",
                namespace: Some("default"),
                labels: &[("size", "small")],
                summary: "",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: Some(("GadgetCluster", "gc")),
            }],
        },
        // Cluster-scoped dynamic objects normalize through the same path.
        Case {
            label: "dynamic-cluster",
            gvk: gvk("apiextensions.k8s.io", "v1", "CustomResourceDefinition"),
            plural: "customresourcedefinitions",
            namespace: None,
            list_body: r#"{"kind":"CustomResourceDefinitionList","apiVersion":"apiextensions.k8s.io/v1","items":[
              {"metadata":{"name":"gadgets.k10s.example.com","uid":"uid-crd","creationTimestamp":"2026-08-21T00:00:00Z"},
               "spec":{"group":"k10s.example.com"},"status":{"conditions":[{"type":"Established","status":"True"}]}}
            ]}"#,
            expect: vec![ExpectedRow {
                name: "gadgets.k10s.example.com",
                uid: "uid-crd",
                namespace: None,
                labels: &[],
                summary: "",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            }],
        },
    ];
    for case in &cases {
        let data = run_case(case).await;
        assert_rows(case, &data);
        let payload = run_case_wire(case).await;
        assert_wire_shape(case, &payload);
        // Dynamic normalizers never leak raw spec/status vocabulary into the
        // view model: only standard metadata shapes reach the wire.
        let serialized =
            serde_json::to_string(payload.rows.first().unwrap()).expect("rows serialize");
        for marker in ["\"spec\"", "\"status\"", "\"Spinning\"", "\"Established\""] {
            assert!(
                !serialized.contains(marker),
                "{}: raw object vocabulary leaked: {marker}",
                case.label
            );
        }
    }
}
