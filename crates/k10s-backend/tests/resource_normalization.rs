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
async fn pods_normalize_phase_and_container_waiting_reasons() {
    let case = Case {
        label: "pods",
        gvk: gvk("", "v1", "Pod"),
        plural: "pods",
        namespace: Some("default"),
        list_body: r#"{"kind":"PodList","apiVersion":"v1","items":[
          {"metadata":{"name":"image-pull","uid":"uid-image-pull","namespace":"default","creationTimestamp":"2026-08-21T04:00:00Z"},
           "status":{"phase":"Pending","containerStatuses":[{"state":{"waiting":{"reason":"ImagePullBackOff"}}}]}},
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
                name: "image-pull",
                uid: "uid-image-pull",
                namespace: Some("default"),
                labels: &[],
                summary: "ImagePullBackOff",
                created_at_prefix: "2026-08-21T04:00:00",
                owner: None,
            },
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

/// Recorded core/v1 Service list cut exercising every projection dimension:
/// declared port kinds, omitted-`targetPort` defaulting, node ports, UDP and
/// SCTP visibility, traffic policies, headless and ExternalName types.
#[tokio::test]
async fn services_normalize_declared_ports_and_structured_projections() {
    let case = Case {
        label: "services",
        gvk: gvk("", "v1", "Service"),
        plural: "services",
        namespace: Some("default"),
        list_body: r#"{"kind":"ServiceList","apiVersion":"v1","items":[
          {"metadata":{"name":"api","uid":"uid-api","namespace":"default","creationTimestamp":"2026-08-21T00:00:00Z"},
           "spec":{"type":"ClusterIP","clusterIP":"10.96.0.20","clusterIPs":["10.96.0.20"],
             "selector":{"app":"api"},"sessionAffinity":"ClientIP",
             "ports":[{"name":"https","port":443,"targetPort":"https","protocol":"TCP","appProtocol":"https"},
                      {"name":"metrics","port":9100,"targetPort":9100,"protocol":"UDP"}]}},
          {"metadata":{"name":"web","uid":"uid-web","namespace":"default","creationTimestamp":"2026-08-20T12:00:00Z"},
           "spec":{"type":"NodePort","clusterIP":"10.96.0.10","clusterIPs":["10.96.0.10"],
             "externalTrafficPolicy":"Local","internalTrafficPolicy":"Cluster",
             "ports":[{"name":"http","port":80,"protocol":"TCP","nodePort":31000},
                      {"name":"dns","port":53,"protocol":"SCTP"}]}},
          {"metadata":{"name":"headless","uid":"uid-headless","namespace":"default","creationTimestamp":"2026-08-19T00:00:00Z"},
           "spec":{"clusterIP":"None","clusterIPs":["None"],"ports":[{"name":"data","port":7000,"targetPort":7001}]}},
          {"metadata":{"name":"external","uid":"uid-external","namespace":"default","creationTimestamp":"2026-08-18T00:00:00Z"},
           "spec":{"type":"ExternalName","externalName":"example.com"}}
        ]}"#,
        expect: vec![
            ExpectedRow {
                name: "api",
                uid: "uid-api",
                namespace: Some("default"),
                labels: &[],
                summary: "ClusterIP",
                created_at_prefix: "2026-08-21T00:00:00",
                owner: None,
            },
            ExpectedRow {
                name: "external",
                uid: "uid-external",
                namespace: Some("default"),
                labels: &[],
                summary: "example.com",
                created_at_prefix: "2026-08-18T00:00:00",
                owner: None,
            },
            ExpectedRow {
                name: "headless",
                uid: "uid-headless",
                namespace: Some("default"),
                labels: &[],
                // An omitted type means the ClusterIP default.
                summary: "ClusterIP",
                created_at_prefix: "2026-08-19T00:00:00",
                owner: None,
            },
            ExpectedRow {
                name: "web",
                uid: "uid-web",
                namespace: Some("default"),
                labels: &[],
                summary: "NodePort",
                created_at_prefix: "2026-08-20T12:00:00",
                owner: None,
            },
        ],
    };
    let data = run_case(&case).await;
    assert_rows(&case, &data);
    let payload = run_case_wire(&case).await;
    assert_wire_shape(&case, &payload);

    use k10s_protocol::{TargetPort, TransportProtocol};
    let projection_of =
        |payload: &ResourceListResponse, name: &str| -> k10s_protocol::ServiceProjection {
            let row = payload
                .rows
                .iter()
                .find(|row| row.identity.name == name)
                .unwrap_or_else(|| panic!("{name} row present"));
            match &row.projection {
                Some(k10s_protocol::ResourceProjection::Service(service)) => service.clone(),
                other => panic!("{name}: expected a Service projection, got {other:?}"),
            }
        };

    // Named TCP port with a named targetPort and app protocol; the UDP port
    // stays visible read-only.
    let api = projection_of(&payload, "api");
    assert_eq!(api.service_type, "ClusterIP");
    assert_eq!(api.cluster_ips, ["10.96.0.20"]);
    assert_eq!(api.selector.get("app").map(String::as_str), Some("api"));
    assert_eq!(api.session_affinity.as_deref(), Some("ClientIP"));
    assert_eq!(api.external_name, None);
    assert_eq!(api.ports.len(), 2);
    let https = &api.ports[0];
    assert_eq!(https.name.as_deref(), Some("https"));
    assert_eq!(https.service_port, 443);
    assert_eq!(
        https.target_port,
        TargetPort::Name {
            name: "https".into()
        }
    );
    assert_eq!(https.protocol, TransportProtocol::Tcp);
    assert_eq!(https.app_protocol.as_deref(), Some("https"));
    let metrics = &api.ports[1];
    assert_eq!(metrics.protocol, TransportProtocol::Udp);
    assert_eq!(metrics.target_port, TargetPort::Number { number: 9_100 });

    // NodePort with traffic policies; an omitted targetPort normalizes to
    // the Service port number, and SCTP ports stay visible.
    let web = projection_of(&payload, "web");
    assert_eq!(web.service_type, "NodePort");
    assert_eq!(web.external_traffic_policy.as_deref(), Some("Local"));
    assert_eq!(web.internal_traffic_policy.as_deref(), Some("Cluster"));
    let http = &web.ports[0];
    assert_eq!(
        http.target_port,
        TargetPort::Number { number: 80 },
        "omitted targetPort normalizes to the Service port"
    );
    assert_eq!(http.node_port, Some(31_000));
    assert_eq!(http.protocol, TransportProtocol::Tcp);
    assert_eq!(web.ports[1].protocol, TransportProtocol::Sctp);
    assert_eq!(web.ports[1].target_port, TargetPort::Number { number: 53 });
    assert_eq!(web.ports[1].node_port, None);

    // Headless Services keep their literal None cluster IP.
    let headless = projection_of(&payload, "headless");
    assert_eq!(headless.cluster_ips, ["None"]);
    assert_eq!(
        headless.ports[0].target_port,
        TargetPort::Number { number: 7_001 }
    );

    // ExternalName Services carry the external name instead of IPs.
    let external = projection_of(&payload, "external");
    assert_eq!(external.service_type, "ExternalName");
    assert_eq!(external.external_name.as_deref(), Some("example.com"));
    assert!(external.cluster_ips.is_empty());

    // The wire payload never leaks the raw Kubernetes object or
    // credential-bearing fields; structured projection keys are expected.
    for row in &payload.rows {
        let serialized = serde_json::to_string(row).expect("row serializes");
        for marker in ["\"spec\":", "\"status\":", "kubeconfig", "token"] {
            assert!(
                !serialized.contains(marker),
                "{}: raw field leaked: {marker}",
                row.identity.name
            );
        }
    }
}

/// The fake adapter exposes Services with structured projections while rows
/// of every other kind keep `projection: None`.
#[tokio::test]
async fn fake_services_carry_projections_and_other_kinds_do_not() {
    use k10s_backend::FakeKubernetes;

    let adapter = FakeKubernetes::standard();
    let kernel = BackendKernel::new(adapter);
    let result = kernel
        .query(Query::ResourceList {
            context: "dev-local".into(),
            gvk: Gvk::core("v1", "Service"),
            namespace: Some("default".into()),
        })
        .await
        .expect("service list succeeds");
    let KernelQueryResult::ResourceList(list) = result else {
        panic!("expected a resource list, got {result:?}")
    };
    let payload = list.wire_payload();
    assert_eq!(payload.rows.len(), 2, "dev-local seeds two Services");

    let web = payload
        .rows
        .iter()
        .find(|row| row.identity.name == "web-frontend")
        .expect("web-frontend service seeded");
    let Some(k10s_protocol::ResourceProjection::Service(projection)) = &web.projection else {
        panic!(
            "web-frontend carries a Service projection, got {:?}",
            web.projection
        )
    };
    assert_eq!(projection.service_type, "ClusterIP");
    assert_eq!(projection.ports.len(), 1);
    assert_eq!(projection.ports[0].service_port, 80);
    assert_eq!(
        projection.ports[0].target_port,
        k10s_protocol::TargetPort::Number { number: 8_080 }
    );

    let api = payload
        .rows
        .iter()
        .find(|row| row.identity.name == "api-server")
        .expect("api-server service seeded");
    let Some(k10s_protocol::ResourceProjection::Service(projection)) = &api.projection else {
        panic!("api-server carries a Service projection")
    };
    assert_eq!(projection.ports.len(), 2, "declared UDP port stays visible");

    // Rows of kinds without a designed projection keep `projection: None`.
    let result = kernel
        .query(Query::ResourceList {
            context: "dev-local".into(),
            gvk: Gvk::new("apps", "v1", "Deployment"),
            namespace: Some("default".into()),
        })
        .await
        .expect("deployment list succeeds");
    let KernelQueryResult::ResourceList(list) = result else {
        panic!("expected a resource list")
    };
    for row in list.wire_payload().rows {
        assert!(
            row.projection.is_none(),
            "{} must not carry a projection",
            row.identity.name
        );
    }
}

/// Backend-owned structured projections map field-for-field onto the frozen
/// protocol shapes. Constructing every internal variant here also keeps the
/// kernel's mapping match exhaustive as the port grows.
#[test]
fn typed_detail_projections_map_exhaustively_to_wire_shapes() {
    use std::collections::BTreeMap;

    use k10s_backend::port::{
        ContainerImageProjection, ContainerStateProjection, ContainerTerminationProjection,
        DeploymentProjection, PodContainerProjection, PodProjection, ReplicaSetProjection,
        ResourceConditionProjection, ResourceProjection, ResourceRecord, ResourceRef,
    };
    use k10s_protocol::{
        ContainerImageProjection as WireContainerImage,
        ContainerStateProjection as WireContainerState,
        ContainerTerminationProjection as WireTermination, DeploymentProjection as WireDeployment,
        PodContainerProjection as WirePodContainer, PodProjection as WirePod,
        ReplicaSetProjection as WireReplicaSet, ResourceConditionProjection as WireCondition,
        ResourceProjection as WireProjection,
    };

    fn record(kind: &str, projection: ResourceProjection) -> ResourceRecord {
        ResourceRecord {
            reference: ResourceRef {
                context: CONTEXT.into(),
                gvk: match kind {
                    "Pod" => Gvk::core("v1", kind),
                    _ => Gvk::new("apps", "v1", kind),
                },
                namespace: Some("default".into()),
                name: kind.to_ascii_lowercase(),
                uid: format!("uid-{kind}"),
            },
            revision: 7,
            labels: BTreeMap::new(),
            summary: "structured".into(),
            created_at: "2026-08-21T00:00:00Z".into(),
            owner_references: Vec::new(),
            events: Vec::new(),
            events_condition: k10s_backend::RecordEventsCondition::Available,
            manifest: String::new(),
            projection: Some(projection),
        }
    }

    let condition = ResourceConditionProjection {
        condition_type: "Ready".into(),
        status: "False".into(),
        reason: Some("ContainersNotReady".into()),
        message: Some("one container is waiting".into()),
        last_transition_time: Some("2026-08-21T00:01:00Z".into()),
    };
    let last_termination = ContainerTerminationProjection {
        exit_code: 137,
        reason: Some("OOMKilled".into()),
    };
    let pod = PodProjection {
        phase: Some("Running".into()),
        ready_containers: Some(1),
        total_containers: Some(3),
        restart_count: Some(4),
        containers: vec![
            PodContainerProjection {
                name: "running".into(),
                image: Some("example/running:v1".into()),
                state: Some(ContainerStateProjection::Running),
                ready: Some(true),
                restart_count: Some(0),
                last_termination: None,
            },
            PodContainerProjection {
                name: "waiting".into(),
                image: Some("example/waiting:v2".into()),
                state: Some(ContainerStateProjection::Waiting {
                    reason: Some("CrashLoopBackOff".into()),
                }),
                ready: Some(false),
                restart_count: Some(4),
                last_termination: Some(last_termination.clone()),
            },
            PodContainerProjection {
                name: "terminated".into(),
                image: None,
                state: Some(ContainerStateProjection::Terminated(
                    ContainerTerminationProjection {
                        exit_code: 0,
                        reason: Some("Completed".into()),
                    },
                )),
                ready: Some(false),
                restart_count: Some(0),
                last_termination: None,
            },
        ],
        conditions: vec![condition.clone()],
        node_name: Some("worker-a".into()),
        pod_ip: Some("10.42.0.7".into()),
        labels: BTreeMap::from([("app".into(), "web".into())]),
        annotations: BTreeMap::from([("example.io/trace".into(), "enabled".into())]),
        created_at: Some("2026-08-21T00:00:00Z".into()),
    };

    let deployment = DeploymentProjection {
        desired_replicas: Some(4),
        ready_replicas: Some(3),
        updated_replicas: Some(2),
        available_replicas: Some(3),
        strategy: Some("RollingUpdate".into()),
        selector: BTreeMap::from([("app".into(), "web".into())]),
        max_surge: Some("25%".into()),
        max_unavailable: Some("1".into()),
        conditions: vec![condition],
        template_containers: vec![ContainerImageProjection {
            name: "web".into(),
            image: Some("example/web:v3".into()),
        }],
        template_labels: BTreeMap::from([("app".into(), "web".into())]),
        template_annotations: BTreeMap::from([("checksum/config".into(), "abc".into())]),
        labels: BTreeMap::from([("managed-by".into(), "k10s".into())]),
        annotations: BTreeMap::from([("example.io/owner".into(), "platform".into())]),
        created_at: Some("2026-08-20T00:00:00Z".into()),
    };

    let replica_set = ReplicaSetProjection {
        revision: 12,
        replicas: Some(4),
        ready_replicas: Some(3),
        created_at: Some("2026-08-20T01:00:00Z".into()),
    };

    let kernel = BackendKernel::new(k10s_backend::FakeKubernetes::standard());
    let payload = kernel.snapshot_page(
        7,
        &[
            record("Pod", ResourceProjection::Pod(pod)),
            record("Deployment", ResourceProjection::Deployment(deployment)),
            record("ReplicaSet", ResourceProjection::ReplicaSet(replica_set)),
        ],
    );

    assert_eq!(
        payload.rows[0].projection,
        Some(WireProjection::Pod(WirePod {
            phase: Some("Running".into()),
            ready_containers: Some(1),
            total_containers: Some(3),
            restart_count: Some(4),
            containers: vec![
                WirePodContainer {
                    name: "running".into(),
                    image: Some("example/running:v1".into()),
                    state: Some(WireContainerState::Running),
                    ready: Some(true),
                    restart_count: Some(0),
                    last_termination: None,
                },
                WirePodContainer {
                    name: "waiting".into(),
                    image: Some("example/waiting:v2".into()),
                    state: Some(WireContainerState::Waiting {
                        reason: Some("CrashLoopBackOff".into()),
                    }),
                    ready: Some(false),
                    restart_count: Some(4),
                    last_termination: Some(WireTermination {
                        exit_code: 137,
                        reason: Some("OOMKilled".into()),
                    }),
                },
                WirePodContainer {
                    name: "terminated".into(),
                    image: None,
                    state: Some(WireContainerState::Terminated(WireTermination {
                        exit_code: 0,
                        reason: Some("Completed".into()),
                    })),
                    ready: Some(false),
                    restart_count: Some(0),
                    last_termination: None,
                },
            ],
            conditions: vec![WireCondition {
                condition_type: "Ready".into(),
                status: "False".into(),
                reason: Some("ContainersNotReady".into()),
                message: Some("one container is waiting".into()),
                last_transition_time: Some("2026-08-21T00:01:00Z".into()),
            }],
            node_name: Some("worker-a".into()),
            pod_ip: Some("10.42.0.7".into()),
            labels: BTreeMap::from([("app".into(), "web".into())]),
            annotations: BTreeMap::from([("example.io/trace".into(), "enabled".into())]),
            created_at: Some("2026-08-21T00:00:00Z".into()),
        }))
    );
    assert_eq!(
        payload.rows[1].projection,
        Some(WireProjection::Deployment(WireDeployment {
            desired_replicas: Some(4),
            ready_replicas: Some(3),
            updated_replicas: Some(2),
            available_replicas: Some(3),
            strategy: Some("RollingUpdate".into()),
            selector: BTreeMap::from([("app".into(), "web".into())]),
            max_surge: Some("25%".into()),
            max_unavailable: Some("1".into()),
            conditions: vec![WireCondition {
                condition_type: "Ready".into(),
                status: "False".into(),
                reason: Some("ContainersNotReady".into()),
                message: Some("one container is waiting".into()),
                last_transition_time: Some("2026-08-21T00:01:00Z".into()),
            }],
            template_containers: vec![WireContainerImage {
                name: "web".into(),
                image: Some("example/web:v3".into()),
            }],
            template_labels: BTreeMap::from([("app".into(), "web".into())]),
            template_annotations: BTreeMap::from([("checksum/config".into(), "abc".into())]),
            labels: BTreeMap::from([("managed-by".into(), "k10s".into())]),
            annotations: BTreeMap::from([("example.io/owner".into(), "platform".into())]),
            created_at: Some("2026-08-20T00:00:00Z".into()),
        }))
    );
    assert_eq!(
        payload.rows[2].projection,
        Some(WireProjection::ReplicaSet(WireReplicaSet {
            revision: 12,
            replicas: Some(4),
            ready_replicas: Some(3),
            created_at: Some("2026-08-20T01:00:00Z".into()),
        }))
    );
}
