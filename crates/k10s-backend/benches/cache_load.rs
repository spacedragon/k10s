//! Medium-large backend cache/watch executable budget gate.

use std::time::{Duration, Instant};

use k10s_backend::{BackendEvent, Gvk, KubernetesAccess, Query, QueryResult, Subscribe};

const OBJECTS: usize = 50_000;
const NODES: usize = 1_000;
const BURST: usize = 10_000;
const BUILD_CEILING: Duration = Duration::from_secs(60);
const BURST_CEILING: Duration = Duration::from_secs(30);
const RELIST_CEILING: Duration = Duration::from_secs(30);

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let started = Instant::now();
    let fake = k10s_backend::FakeKubernetes::with_capacity(OBJECTS, NODES);
    assert!(started.elapsed() < BUILD_CEILING);
    let gvk = Gvk::core("v1", "Pod");
    let QueryResult::ResourceList(list) = runtime
        .block_on(fake.query(Query::ResourceList {
            context: "dev-local".into(),
            gvk: gvk.clone(),
            namespace: None,
        }))
        .unwrap()
    else {
        panic!("pod list")
    };
    assert_eq!(list.rows.len(), OBJECTS * 3 / 8);
    let names: Vec<_> = list
        .rows
        .iter()
        .take(BURST)
        .map(|row| (row.reference.namespace.clone(), row.reference.name.clone()))
        .collect();

    let mut watch = runtime
        .block_on(fake.subscribe(Subscribe::ResourceWatch {
            context: "dev-local".into(),
            gvk: gvk.clone(),
            namespace: None,
        }))
        .unwrap();
    let mut events = watch.take_events().unwrap();
    let snapshot = runtime.block_on(events.recv()).unwrap();
    assert!(matches!(snapshot, BackendEvent::Snapshot(_)));

    let burst_started = Instant::now();
    for (namespace, name) in &names {
        assert!(
            fake.touch_resource("dev-local", &gvk, namespace.as_deref(), name)
                .is_some()
        );
        let event = runtime.block_on(events.recv()).unwrap();
        assert!(matches!(event, BackendEvent::Changed(_)));
    }
    let burst_elapsed = burst_started.elapsed();
    assert_eq!(names.len(), BURST);
    assert!(
        burst_elapsed < BURST_CEILING,
        "10k watch burst took {burst_elapsed:?}"
    );

    // Repeated full subscriptions model supervised relist cuts. Every fresh
    // watch must begin with one complete 18,750-row snapshot and tear down.
    let relist_started = Instant::now();
    for _ in 0..20 {
        let mut relist = runtime
            .block_on(fake.subscribe(Subscribe::ResourceWatch {
                context: "dev-local".into(),
                gvk: gvk.clone(),
                namespace: None,
            }))
            .unwrap();
        let mut receiver = relist.take_events().unwrap();
        let BackendEvent::Snapshot(snapshot) = runtime.block_on(receiver.recv()).unwrap() else {
            panic!("fresh watch starts with snapshot")
        };
        assert_eq!(snapshot.rows.len(), OBJECTS * 3 / 8);
    }
    let relist_elapsed = relist_started.elapsed();
    assert!(
        relist_elapsed < RELIST_CEILING,
        "20 relists took {relist_elapsed:?}"
    );
    println!(
        "cache_load OK: records={} burst={BURST} burst={burst_elapsed:?} relists=20 relist={relist_elapsed:?}",
        fake.total_records()
    );
}
