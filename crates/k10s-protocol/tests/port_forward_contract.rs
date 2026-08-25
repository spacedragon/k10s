//! Stable wire contracts for normalized Service projections and bounded
//! port-forward session payloads.
//!
//! These tests pin the exact JSON shapes shared by the server and both
//! clients. No Kubernetes-specific types may appear: every payload is a
//! backend-owned normalized view model.

use std::collections::BTreeMap;

use k10s_protocol::{
    BackendRevision, CAPABILITY_SERVICE_PORT_FORWARD, GroupVersionKind, PortForwardFailureCategory,
    PortForwardListResponse, PortForwardPodTarget, PortForwardSession, PortForwardSessionEvent,
    PortForwardSessionId, PortForwardSessionState, PortForwardStartRequest,
    REQUEST_PORT_FORWARD_LIST, REQUEST_PORT_FORWARD_START, REQUEST_PORT_FORWARD_STOP,
    ResourceChanged, ResourceDetailResponse, ResourceIdentity, ResourceListRow, ResourceProjection,
    ResourceSnapshotPage, ServerFrame, ServerKind, ServicePort, ServiceProjection,
    SubscriptionSelector, TargetPort, TransportProtocol, decode_server_frame,
};
use serde_json::{Value, json};

fn round_trip<T: serde::Serialize + serde::de::DeserializeOwned>(value: &T) -> Value {
    let encoded = serde_json::to_value(value).expect("payload must serialize");
    let decoded: T = serde_json::from_value(encoded.clone()).expect("payload must deserialize");
    let reencoded = serde_json::to_value(&decoded).expect("payload must re-serialize");
    assert_eq!(encoded, reencoded, "round trip must be stable");
    encoded
}

fn service_identity() -> ResourceIdentity {
    ResourceIdentity {
        context: "dev-local".into(),
        gvk: GroupVersionKind::core("v1", "Service"),
        namespace: Some("default".into()),
        name: "web-frontend".into(),
        uid: "uid-svc-1".into(),
    }
}

#[test]
fn protocol_minor_bumps_without_changing_major() {
    assert_eq!(k10s_protocol::PROTOCOL_MAJOR, 1);
    assert_eq!(k10s_protocol::PROTOCOL_MINOR, 2);
}

#[test]
fn transport_protocol_covers_tcp_udp_and_sctp() {
    for (protocol, expected) in [
        (TransportProtocol::Tcp, "tcp"),
        (TransportProtocol::Udp, "udp"),
        (TransportProtocol::Sctp, "sctp"),
    ] {
        assert_eq!(serde_json::to_value(protocol).unwrap(), json!(expected));
        let decoded: TransportProtocol = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(decoded, protocol);
    }
}

#[test]
fn target_port_selects_by_name_or_number() {
    assert_eq!(
        round_trip(&TargetPort::Number { number: 8080 }),
        json!({"kind": "number", "number": 8080})
    );
    assert_eq!(
        round_trip(&TargetPort::Name { name: "web".into() }),
        json!({"kind": "name", "name": "web"})
    );
}

#[test]
fn service_ports_serialize_declared_fields_with_optional_omissions() {
    let named = ServicePort {
        name: Some("http".into()),
        service_port: 80,
        target_port: TargetPort::Number { number: 8080 },
        node_port: Some(31000),
        protocol: TransportProtocol::Tcp,
        app_protocol: Some("http".into()),
    };
    assert_eq!(
        round_trip(&named),
        json!({
            "name": "http",
            "servicePort": 80,
            "targetPort": {"kind": "number", "number": 8080},
            "nodePort": 31000,
            "protocol": "tcp",
            "appProtocol": "http",
        })
    );

    let minimal = ServicePort {
        name: None,
        service_port: 5353,
        target_port: TargetPort::Name { name: "dns".into() },
        node_port: None,
        protocol: TransportProtocol::Udp,
        app_protocol: None,
    };
    let encoded = round_trip(&minimal);
    assert_eq!(
        encoded,
        json!({
            "servicePort": 5353,
            "targetPort": {"kind": "name", "name": "dns"},
            "protocol": "udp",
        })
    );
}

#[test]
fn service_projection_serializes_as_a_tagged_resource_projection() {
    let projection = ServiceProjection {
        service_type: "ClusterIP".into(),
        cluster_ips: vec!["10.96.0.10".into()],
        selector: BTreeMap::from([("app".to_owned(), "web".to_owned())]),
        external_name: None,
        session_affinity: Some("None".into()),
        external_traffic_policy: Some("Cluster".into()),
        internal_traffic_policy: None,
        ports: vec![ServicePort {
            name: Some("http".into()),
            service_port: 80,
            target_port: TargetPort::Number { number: 8080 },
            node_port: None,
            protocol: TransportProtocol::Tcp,
            app_protocol: None,
        }],
    };
    let wrapped = ResourceProjection::Service(projection.clone());
    let encoded = round_trip(&wrapped);
    assert_eq!(encoded["kind"], json!("service"));
    assert_eq!(encoded["serviceType"], json!("ClusterIP"));
    assert_eq!(encoded["clusterIps"], json!(["10.96.0.10"]));
    assert_eq!(encoded["selector"], json!({"app": "web"}));
    assert!(encoded.get("externalName").is_none());
    assert_eq!(encoded["sessionAffinity"], json!("None"));
    assert_eq!(encoded["externalTrafficPolicy"], json!("Cluster"));
    assert!(encoded.get("internalTrafficPolicy").is_none());
    assert_eq!(encoded["ports"][0]["servicePort"], json!(80));

    let decoded: ResourceProjection = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded, wrapped);

    // Traffic-policy strings render only when present; external names too.
    let external = ResourceProjection::Service(ServiceProjection {
        service_type: "ExternalName".into(),
        cluster_ips: Vec::new(),
        selector: BTreeMap::new(),
        external_name: Some("example.com".into()),
        session_affinity: None,
        external_traffic_policy: None,
        internal_traffic_policy: None,
        ports: Vec::new(),
    });
    let encoded = serde_json::to_value(&external).unwrap();
    assert_eq!(encoded["externalName"], json!("example.com"));
    assert!(encoded.get("sessionAffinity").is_none());
}

#[test]
fn legacy_list_rows_without_projections_still_decode() {
    let legacy = json!({
        "identity": {
            "context": "dev-local",
            "gvk": {"group": "", "version": "v1", "kind": "Service"},
            "namespace": "default",
            "name": "web",
            "uid": "uid-1"
        },
        "revision": 1000,
        "labels": {},
        "summary": "ClusterIP",
        "createdAt": "2026-08-21T00:00:00Z"
    });
    let decoded: ResourceListRow = serde_json::from_value(legacy).unwrap();
    assert_eq!(decoded.projection, None);
}

#[test]
fn populated_row_projections_flow_through_snapshot_pages_and_deltas() {
    let row = ResourceListRow {
        identity: service_identity(),
        revision: BackendRevision::new(1004),
        labels: BTreeMap::from([("app".to_owned(), "web".to_owned())]),
        summary: "ClusterIP".into(),
        created_at: "2026-08-21T00:00:00Z".into(),
        projection: Some(ResourceProjection::Service(ServiceProjection {
            service_type: "ClusterIP".into(),
            cluster_ips: vec!["10.96.0.10".into()],
            selector: BTreeMap::from([("app".to_owned(), "web".to_owned())]),
            external_name: None,
            session_affinity: None,
            external_traffic_policy: None,
            internal_traffic_policy: None,
            ports: vec![ServicePort {
                name: Some("http".into()),
                service_port: 80,
                target_port: TargetPort::Number { number: 80 },
                node_port: None,
                protocol: TransportProtocol::Tcp,
                app_protocol: None,
            }],
        })),
    };

    let page = ResourceSnapshotPage {
        revision: BackendRevision::new(1004),
        rows: vec![row.clone()],
    };
    let encoded = round_trip(&page);
    assert_eq!(encoded["rows"][0]["projection"]["kind"], json!("service"));
    assert_eq!(
        encoded["rows"][0]["projection"]["ports"][0]["servicePort"],
        json!(80)
    );

    let changed = ResourceChanged {
        identity: row.identity.clone(),
        row: row.clone(),
    };
    let frame = ServerFrame {
        kind: ServerKind::Event,
        request_id: None,
        subscription_id: Some("sub-1".into()),
        sequence: Some(12),
        payload: json!({
            "kind": "resource.changed",
            "revision": "1004",
            "payload": changed,
        }),
    };
    let text = serde_json::to_string(&frame).unwrap();
    let decoded = decode_server_frame(serde_json::from_str(&text).unwrap()).unwrap();
    match decoded.decode_payload().unwrap() {
        k10s_protocol::ServerPayload::Event(event) => {
            let parsed: ResourceChanged = serde_json::from_value(event.payload).unwrap();
            let projection = parsed.row.projection.expect("delta carries the projection");
            assert_eq!(
                projection,
                ResourceProjection::Service(ServiceProjection {
                    service_type: "ClusterIP".into(),
                    cluster_ips: vec!["10.96.0.10".into()],
                    selector: BTreeMap::from([("app".to_owned(), "web".to_owned())]),
                    external_name: None,
                    session_affinity: None,
                    external_traffic_policy: None,
                    internal_traffic_policy: None,
                    ports: vec![ServicePort {
                        name: Some("http".into()),
                        service_port: 80,
                        target_port: TargetPort::Number { number: 80 },
                        node_port: None,
                        protocol: TransportProtocol::Tcp,
                        app_protocol: None,
                    }],
                })
            );
        }
        other => panic!("expected event, got {other:?}"),
    }
}

#[test]
fn legacy_detail_responses_without_projections_still_decode() {
    let legacy = json!({
        "identity": {
            "context": "dev-local",
            "gvk": {"group": "", "version": "v1", "kind": "Service"},
            "namespace": "default",
            "name": "web",
            "uid": "uid-1"
        },
        "revision": 1000,
        "createdAt": "2026-08-21T00:00:00Z",
        "ownerReferences": [],
        "sections": [],
        "events": [],
        "related": [],
        "capabilities": {
            "canEditYaml": true,
            "canDelete": true,
            "canScale": false,
            "canViewLogs": false,
            "canExec": false
        },
        "manifest": "apiVersion: v1\nkind: Service\n"
    });
    let decoded: ResourceDetailResponse = serde_json::from_value(legacy).unwrap();
    assert_eq!(decoded.projection, None);
}

#[test]
fn detail_responses_carry_populated_service_projections() {
    let mut response = ResourceDetailResponse {
        identity: service_identity(),
        revision: BackendRevision::new(1005),
        created_at: "2026-08-21T00:00:00Z".into(),
        owner_references: Vec::new(),
        sections: Vec::new(),
        events: Vec::new(),
        related: Vec::new(),
        capabilities: k10s_protocol::ResourceCapabilities::default(),
        manifest: String::new(),
        projection: None,
    };
    response.projection = Some(ResourceProjection::Service(ServiceProjection {
        service_type: "NodePort".into(),
        cluster_ips: vec!["10.96.0.11".into()],
        selector: BTreeMap::new(),
        external_name: None,
        session_affinity: None,
        external_traffic_policy: None,
        internal_traffic_policy: None,
        ports: Vec::new(),
    }));
    let encoded = round_trip(&response);
    assert_eq!(encoded["projection"]["serviceType"], json!("NodePort"));

    let decoded: ResourceDetailResponse =
        serde_json::from_value(encoded).expect("populated projection decodes");
    assert!(decoded.projection.is_some());
}

#[test]
fn port_forward_requests_use_the_documented_kind_strings() {
    assert_eq!(REQUEST_PORT_FORWARD_START, "portForward.start");
    assert_eq!(REQUEST_PORT_FORWARD_STOP, "portForward.stop");
    assert_eq!(REQUEST_PORT_FORWARD_LIST, "portForward.list");
    assert_eq!(CAPABILITY_SERVICE_PORT_FORWARD, "service.portForward");
}

#[test]
fn start_requests_carry_exact_service_identity_and_port_selector() {
    let named = PortForwardStartRequest {
        service: service_identity(),
        port: k10s_protocol::PortForwardPortSelector::Name {
            name: "http".into(),
        },
        local_port: 0,
    };
    let encoded = round_trip(&named);
    assert_eq!(encoded["service"]["uid"], json!("uid-svc-1"));
    assert_eq!(
        encoded["service"]["gvk"],
        json!({"group": "", "version": "v1", "kind": "Service"})
    );
    assert_eq!(encoded["port"], json!({"kind": "name", "name": "http"}));
    assert_eq!(encoded["localPort"], json!(0));

    let numeric = PortForwardStartRequest {
        port: k10s_protocol::PortForwardPortSelector::Number { number: 8443 },
        local_port: u16::MAX,
        ..named.clone()
    };
    let encoded = round_trip(&numeric);
    assert_eq!(encoded["port"], json!({"kind": "number", "number": 8443}));
    assert_eq!(encoded["localPort"], json!(65535));
    assert_eq!(
        serde_json::from_value::<PortForwardStartRequest>(
            json!({"localPort": 0, "port": numeric.port, "service": named.service})
        )
        .unwrap()
        .local_port,
        0
    );

    assert!(named.validate().is_ok());

    let wrong_kind = PortForwardStartRequest {
        service: ResourceIdentity {
            gvk: GroupVersionKind::core("v1", "Pod"),
            ..service_identity()
        },
        ..named.clone()
    };
    assert!(wrong_kind.validate().is_err());

    let missing_uid = PortForwardStartRequest {
        service: ResourceIdentity {
            uid: String::new(),
            ..service_identity()
        },
        ..named
    };
    assert!(missing_uid.validate().is_err());
}

#[test]
fn session_snapshots_are_complete_and_typed() {
    let active = PortForwardSession {
        id: PortForwardSessionId::try_new("pf-1").unwrap(),
        service: service_identity(),
        service_port: 80,
        pod: PortForwardPodTarget {
            namespace: "default".into(),
            name: "web-frontend-7d9f8-abcde".into(),
            uid: "uid-pod-1".into(),
        },
        pod_port: 8080,
        local_addr: "127.0.0.1:45621".into(),
        state: PortForwardSessionState::Active,
        failure: None,
        revision: 3,
    };
    let encoded = round_trip(&active);
    assert_eq!(encoded["id"], json!("pf-1"));
    assert_eq!(encoded["localAddr"], json!("127.0.0.1:45621"));
    assert_eq!(encoded["state"], json!("active"));
    assert_eq!(encoded["pod"]["name"], json!("web-frontend-7d9f8-abcde"));
    assert_eq!(encoded["podPort"], json!(8080));
    assert_eq!(encoded["servicePort"], json!(80));
    assert!(encoded.get("failure").is_none());

    let failed = PortForwardSession {
        id: PortForwardSessionId::try_new("pf-2").unwrap(),
        state: PortForwardSessionState::Failed,
        failure: Some(k10s_protocol::PortForwardFailure {
            category: PortForwardFailureCategory::UnavailableEndpoint,
            message: "no ready endpoint is currently available".into(),
        }),
        revision: 4,
        ..active.clone()
    };
    let encoded = round_trip(&failed);
    assert_eq!(encoded["state"], json!("failed"));
    assert_eq!(encoded["failure"]["category"], json!("unavailableEndpoint"));
    assert_eq!(
        encoded["failure"]["message"],
        json!("no ready endpoint is currently available")
    );

    for (state, expected) in [
        (PortForwardSessionState::Starting, "starting"),
        (PortForwardSessionState::Stopping, "stopping"),
        (PortForwardSessionState::Stopped, "stopped"),
    ] {
        assert_eq!(serde_json::to_value(state).unwrap(), json!(expected));
    }
}

#[test]
fn empty_session_ids_never_decode() {
    let raw = json!("");
    let decoded: Result<PortForwardSessionId, _> = serde_json::from_value(raw);
    assert!(decoded.is_err());
    let decoded: PortForwardSessionId = serde_json::from_value(json!("pf-valid")).unwrap();
    assert_eq!(decoded.as_str(), "pf-valid");
}

#[test]
fn stop_and_list_payloads_round_trip() {
    let stop = k10s_protocol::PortForwardStopRequest {
        session_id: PortForwardSessionId::try_new("pf-1").unwrap(),
    };
    assert_eq!(round_trip(&stop), json!({"sessionId": "pf-1"}));

    let stopped = k10s_protocol::PortForwardStopResponse {
        session: Some(PortForwardSession {
            id: PortForwardSessionId::try_new("pf-1").unwrap(),
            service: service_identity(),
            service_port: 80,
            pod: PortForwardPodTarget {
                namespace: "default".into(),
                name: "web-1".into(),
                uid: "uid-pod-1".into(),
            },
            pod_port: 8080,
            local_addr: "127.0.0.1:45621".into(),
            state: PortForwardSessionState::Stopped,
            failure: None,
            revision: 9,
        }),
    };
    let encoded = round_trip(&stopped);
    assert_eq!(encoded["session"]["state"], json!("stopped"));

    let idempotent = k10s_protocol::PortForwardStopResponse { session: None };
    assert!(round_trip(&idempotent).get("session").is_none());

    let list = PortForwardListResponse {
        revision: 10,
        sessions: vec![PortForwardSession {
            id: PortForwardSessionId::try_new("pf-2").unwrap(),
            service: service_identity(),
            service_port: 443,
            pod: PortForwardPodTarget {
                namespace: "default".into(),
                name: "web-2".into(),
                uid: "uid-pod-2".into(),
            },
            pod_port: 8443,
            local_addr: "127.0.0.1:45622".into(),
            state: PortForwardSessionState::Active,
            failure: None,
            revision: 10,
        }],
    };
    let encoded = round_trip(&list);
    assert_eq!(encoded["sessions"][0]["podPort"], json!(8443));
    let decoded: PortForwardListResponse = serde_json::from_value(json!({"sessions": []})).unwrap();
    assert!(decoded.sessions.is_empty());
    assert_eq!(decoded.revision, 0, "older peers default the watermark");
}

#[test]
fn failure_categories_use_the_safe_stable_strings() {
    for (category, expected) in [
        (
            PortForwardFailureCategory::UnavailableEndpoint,
            "unavailableEndpoint",
        ),
        (PortForwardFailureCategory::Forbidden, "forbidden"),
        (PortForwardFailureCategory::LocalPortInUse, "localPortInUse"),
        (
            PortForwardFailureCategory::VanishedResource,
            "vanishedResource",
        ),
        (
            PortForwardFailureCategory::UnsupportedService,
            "unsupportedService",
        ),
        (
            PortForwardFailureCategory::ContextTransition,
            "contextTransition",
        ),
        (
            PortForwardFailureCategory::TransportClosed,
            "transportClosed",
        ),
    ] {
        assert_eq!(serde_json::to_value(category).unwrap(), json!(expected));
        let decoded: PortForwardFailureCategory = serde_json::from_value(json!(expected)).unwrap();
        assert_eq!(decoded, category);
    }
}

#[test]
fn sessions_subscription_uses_a_dedicated_selector_and_snapshot_events() {
    let selector = SubscriptionSelector::PortForwardSessions;
    assert_eq!(
        round_trip(&selector),
        json!({"kind": "portForwardSessions"})
    );

    let event = PortForwardSessionEvent {
        revision: 11,
        session: PortForwardSession {
            id: PortForwardSessionId::try_new("pf-3").unwrap(),
            service: service_identity(),
            service_port: 80,
            pod: PortForwardPodTarget {
                namespace: "default".into(),
                name: "web-3".into(),
                uid: "uid-pod-3".into(),
            },
            pod_port: 8080,
            local_addr: "127.0.0.1:45623".into(),
            state: PortForwardSessionState::Failed,
            failure: Some(k10s_protocol::PortForwardFailure {
                category: PortForwardFailureCategory::ContextTransition,
                message: "the context switched while the forward was starting".into(),
            }),
            revision: 11,
        },
    };
    let encoded = round_trip(&event);
    assert_eq!(encoded["revision"], json!(11));
    assert_eq!(encoded["session"]["id"], json!("pf-3"));
}
