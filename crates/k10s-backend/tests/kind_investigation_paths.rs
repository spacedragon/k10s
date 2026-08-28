//! Opt-in, read-only acceptance smoke for issue #169 investigation paths.

use k10s_backend::{Gvk, KubeAdapter, KubernetesAccess, Query, QueryResult, ResourceRef};

fn gvk(group: &str, version: &str, kind: &str) -> Gvk {
    Gvk {
        group: group.into(),
        version: version.into(),
        kind: kind.into(),
    }
}

#[tokio::test]
#[ignore = "live read-only cluster: set K10S_LIVE_CONTEXT=kind-bunyip"]
async fn validates_issue_169_investigation_paths_on_live_kind() {
    let context =
        std::env::var("K10S_LIVE_CONTEXT").expect("set K10S_LIVE_CONTEXT to the live kind context");
    let adapter = KubeAdapter::from_kubeconfig(None).expect("desktop kubeconfig must load");

    let healthy = find_row(
        &adapter,
        &context,
        gvk("", "v1", "Pod"),
        |summary| summary.contains("Running"),
        "healthy Pod",
    )
    .await;
    let image_pull = find_row(
        &adapter,
        &context,
        gvk("", "v1", "Pod"),
        |summary| summary.contains("ImagePullBackOff"),
        "ImagePullBackOff Pod",
    )
    .await;
    let completed_job = find_row(
        &adapter,
        &context,
        gvk("batch", "v1", "Job"),
        |summary| summary.contains("Complete") || summary.contains("succeeded"),
        "completed Job",
    )
    .await;
    let stateful_set = find_row(
        &adapter,
        &context,
        gvk("apps", "v1", "StatefulSet"),
        |_| true,
        "StatefulSet",
    )
    .await;

    for (label, reference) in [
        ("healthy Pod", healthy),
        ("ImagePullBackOff Pod", image_pull),
        ("completed Job", completed_job),
        ("StatefulSet", stateful_set),
    ] {
        let QueryResult::ResourceDetail(detail) = adapter
            .query(Query::ResourceDetail {
                reference: reference.clone(),
            })
            .await
            .unwrap_or_else(|error| panic!("{label} detail must resolve: {error}"))
        else {
            panic!("{label} detail returned the wrong result");
        };
        assert_eq!(
            detail.reference.uid, reference.uid,
            "{label} identity drift"
        );
        assert!(
            detail.manifest.contains(&reference.name),
            "{label} manifest must describe the selected live object"
        );
    }
}

async fn find_row(
    adapter: &KubeAdapter,
    context: &str,
    kind: Gvk,
    matches: impl Fn(&str) -> bool,
    label: &str,
) -> ResourceRef {
    let QueryResult::ResourceList(list) = adapter
        .query(Query::ResourceList {
            context: context.to_owned(),
            gvk: kind,
            namespace: None,
        })
        .await
        .unwrap_or_else(|error| panic!("{label} list must resolve: {error}"))
    else {
        panic!("{label} list returned the wrong result");
    };
    list.rows
        .into_iter()
        .find(|row| matches(&row.summary))
        .unwrap_or_else(|| panic!("live cluster has no {label}"))
        .reference
}
