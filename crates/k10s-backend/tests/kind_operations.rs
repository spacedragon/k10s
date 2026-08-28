//! Ignored end-to-end verification of real Kubernetes operations.
//!
//! Run `tests/kind/cluster.sh up` first, then:
//! `cargo test --locked -p k10s-backend --test kind_operations -- --ignored --nocapture`.

use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use k10s_backend::operation::OutcomeData;
use k10s_backend::{
    BackendError, BackendEvent, Command, Gvk, KubeAdapter, KubernetesAccess, OperationState,
    Propagation, Query, QueryResult, ResourceRef, StreamInput, StreamKind, StreamOrigin,
    StreamRouteKind, Subscribe,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CONTEXT: &str = "k10s-operations";
const NAMESPACE: &str = "k10s-operations";
static KIND_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn kubeconfig() -> PathBuf {
    std::env::var_os("K10S_KIND_KUBECONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/kind/.kubeconfig")
        })
}

fn admin_context() -> String {
    let name = std::env::var("K10S_KIND_CLUSTER").unwrap_or_else(|_| "k10s-read-path".into());
    format!("kind-{name}")
}

fn kubectl(args: &[&str]) -> String {
    let output = ProcessCommand::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig())
        .arg("--context")
        .arg(admin_context())
        .args(args)
        .output()
        .expect("kubectl is installed by the kind harness");
    assert!(
        output.status.success(),
        "kubectl {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn adapter() -> KubeAdapter {
    KubeAdapter::from_kubeconfig(Some(&kubeconfig())).expect("kind kubeconfig is valid")
}

async fn reference(adapter: &KubeAdapter, gvk: Gvk, name: &str) -> ResourceRef {
    let QueryResult::ResourceList(list) = adapter
        .query(Query::ResourceList {
            context: CONTEXT.into(),
            gvk,
            namespace: Some(NAMESPACE.into()),
        })
        .await
        .unwrap()
    else {
        panic!("resource list returned the wrong result")
    };
    list.rows
        .into_iter()
        .find(|row| row.reference.name == name)
        .unwrap_or_else(|| panic!("fixture {name} is missing"))
        .reference
}

async fn terminal(adapter: &KubeAdapter, id: &str) -> (OperationState, Option<String>) {
    for _ in 0..200 {
        let QueryResult::OperationStatus(status) = adapter
            .query(Query::OperationStatus {
                operation_ids: vec![id.into()],
            })
            .await
            .unwrap()
        else {
            panic!("operation status returned the wrong result")
        };
        if let Some(record) = status.operations.first()
            && matches!(
                record.state,
                OperationState::Succeeded | OperationState::Failed | OperationState::OutcomeUnknown
            )
        {
            return (record.state, record.detail.clone());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("operation {id} did not settle")
}

async fn run(adapter: &KubeAdapter, label: &str, command: Command) {
    let id = adapter.execute(command).await.unwrap();
    let (state, detail) = terminal(adapter, id.as_str()).await;
    assert_eq!(
        state,
        OperationState::Succeeded,
        "{label} operation failed: {detail:?}"
    );
}

struct DropResponseProxy {
    armed: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
    kubectl_proxy: Child,
    kubeconfig: PathBuf,
}

impl Drop for DropResponseProxy {
    fn drop(&mut self) {
        self.task.abort();
        let _ = self.kubectl_proxy.kill();
        let _ = self.kubectl_proxy.wait();
        let _ = fs::remove_file(&self.kubeconfig);
    }
}

impl DropResponseProxy {
    async fn start() -> Self {
        let upstream_port = free_port();
        let kubectl_proxy = ProcessCommand::new("kubectl")
            .arg("--kubeconfig")
            .arg(kubeconfig())
            .arg("--context")
            .arg(CONTEXT)
            .args([
                "proxy",
                &format!("--port={upstream_port}"),
                "--accept-hosts=.*",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("kubectl proxy starts");
        let upstream = format!("127.0.0.1:{upstream_port}");
        for _ in 0..100 {
            if TcpStream::connect(&upstream).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let armed = Arc::new(AtomicBool::new(false));
        let task_armed = armed.clone();
        let task_upstream = upstream.clone();
        let task = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let upstream = task_upstream.clone();
                let armed = task_armed.clone();
                tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();
                    let dropping = Arc::new(AtomicBool::new(false));
                    let request_dropping = dropping.clone();
                    let mut request = tokio::spawn(async move {
                        let mut buffer = [0_u8; 16 * 1024];
                        loop {
                            let count = client_read.read(&mut buffer).await?;
                            if count == 0 {
                                return std::io::Result::Ok(());
                            }
                            let mutation = buffer[..count].windows(6).any(|part| {
                                part == b"PATCH " || part == b"POST /" || part == b"DELETE"
                            });
                            let drop_response = mutation && armed.swap(false, Ordering::SeqCst);
                            if drop_response {
                                request_dropping.store(true, Ordering::SeqCst);
                            }
                            server_write.write_all(&buffer[..count]).await?;
                            server_write.flush().await?;
                            if drop_response {
                                // The small operation payload fits in this write. Give the
                                // apiserver time to commit it, then discard its response.
                                tokio::time::sleep(Duration::from_millis(150)).await;
                                return Ok(());
                            }
                        }
                    });
                    let mut response = tokio::spawn(async move {
                        let mut buffer = [0_u8; 16 * 1024];
                        loop {
                            let count = server_read.read(&mut buffer).await?;
                            if count == 0 || dropping.load(Ordering::SeqCst) {
                                return std::io::Result::Ok(());
                            }
                            client_write.write_all(&buffer[..count]).await?;
                            client_write.flush().await?;
                        }
                    });
                    tokio::select! {
                        _ = &mut request => response.abort(),
                        _ = &mut response => request.abort(),
                    }
                });
            }
        });

        let path = std::env::temp_dir().join(format!(
            "k10s-kind-proxy-{}-{}.yaml",
            std::process::id(),
            upstream_port
        ));
        fs::write(
            &path,
            format!(
                "apiVersion: v1\nkind: Config\nclusters:\n- name: proxy\n  cluster:\n    server: http://{address}\ncontexts:\n- name: {CONTEXT}\n  context:\n    cluster: proxy\n    user: proxy\n    namespace: {NAMESPACE}\ncurrent-context: {CONTEXT}\nusers:\n- name: proxy\n  user: {{}}\n"
            ),
        )
        .unwrap();
        Self {
            armed,
            task,
            kubectl_proxy,
            kubeconfig: path,
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn dropped_mutation_response_is_unknown_until_authoritative_refresh() {
    let _guard = KIND_LOCK.lock().await;
    let proxy = DropResponseProxy::start().await;
    let adapter = KubeAdapter::from_kubeconfig(Some(&proxy.kubeconfig)).unwrap();
    // Prime discovery before arming the one-shot failure. Only the mutation
    // response is severed; its prerequisite GET remains authoritative.
    adapter
        .query(Query::ResourceTypes {
            context: CONTEXT.into(),
        })
        .await
        .unwrap();
    let deployment = reference(
        &adapter,
        Gvk::new("apps", "v1", "Deployment"),
        "operations-web",
    )
    .await;
    let current: u32 = kubectl(&[
        "-n",
        NAMESPACE,
        "get",
        "deployment/operations-web",
        "-o",
        "jsonpath={.spec.replicas}",
    ])
    .parse()
    .unwrap();
    let desired = if current == 1 { 2 } else { 1 };
    proxy.arm();
    let id = adapter
        .execute(Command::Scale {
            context: CONTEXT.into(),
            gvk: deployment.gvk.clone(),
            namespace: deployment.namespace.clone(),
            name: deployment.name.clone(),
            uid: deployment.uid.clone(),
            replicas: desired,
            idempotency_key: "kind-unknown-scale".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        terminal(&adapter, id.as_str()).await.0,
        OperationState::OutcomeUnknown
    );

    for _ in 0..100 {
        let observed = kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "deployment/operations-web",
            "-o",
            "jsonpath={.spec.replicas}",
        ]);
        if observed == desired.to_string() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "deployment/operations-web",
            "-o",
            "jsonpath={.spec.replicas}"
        ]),
        desired.to_string(),
        "an authoritative refresh reconciles the unknown write outcome"
    );
    assert!(matches!(
        adapter
            .execute(Command::Scale {
                context: CONTEXT.into(),
                gvk: deployment.gvk.clone(),
                namespace: deployment.namespace.clone(),
                name: deployment.name.clone(),
                uid: deployment.uid.clone(),
                replicas: desired,
                idempotency_key: "kind-retry-before-refresh".into(),
            })
            .await,
        Err(BackendError::Conflict(_))
    ));
    let QueryResult::ResourceDetail(_) = adapter
        .query(Query::ResourceDetail {
            reference: deployment.clone(),
        })
        .await
        .unwrap()
    else {
        panic!("authoritative detail refresh returned the wrong result")
    };
    run(
        &adapter,
        "post-refresh scale",
        Command::Scale {
            context: CONTEXT.into(),
            gvk: deployment.gvk.clone(),
            namespace: deployment.namespace.clone(),
            name: deployment.name.clone(),
            uid: deployment.uid.clone(),
            replicas: desired,
            idempotency_key: "kind-retry-after-refresh".into(),
        },
    )
    .await;
    assert_eq!(
        terminal(&adapter, id.as_str()).await.0,
        OperationState::OutcomeUnknown,
        "refresh releases admission but never rewrites history as success"
    );
}

fn free_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn live_mutations_cover_validation_conflict_scale_restart_and_delete() {
    let _guard = KIND_LOCK.lock().await;
    // The outcome-unknown test may have just changed replica count. Wait for
    // the controller's status writes to settle before binding an exact
    // resourceVersion into the guarded apply document.
    kubectl(&[
        "-n",
        NAMESPACE,
        "rollout",
        "status",
        "deployment/operations-web",
        "--timeout=60s",
    ]);
    let adapter = adapter();
    let deployment = reference(
        &adapter,
        Gvk::new("apps", "v1", "Deployment"),
        "operations-web",
    )
    .await;
    let mut resource_version = kubectl(&[
        "-n",
        NAMESPACE,
        "get",
        "deployment/operations-web",
        "-o",
        "jsonpath={.metadata.resourceVersion}",
    ]);
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let observed = kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "deployment/operations-web",
            "-o",
            "jsonpath={.metadata.resourceVersion}",
        ]);
        if observed == resource_version {
            break;
        }
        resource_version = observed;
    }
    let replicas: u32 = kubectl(&[
        "-n",
        NAMESPACE,
        "get",
        "deployment/operations-web",
        "-o",
        "jsonpath={.spec.replicas}",
    ])
    .parse()
    .unwrap();
    let yaml = format!(
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: operations-web\n  namespace: {NAMESPACE}\n  uid: {}\n  resourceVersion: '{resource_version}'\nspec:\n  selector:\n    matchLabels:\n      app: operations-web\n  template:\n    metadata:\n      labels:\n        app: operations-web\n    spec:\n      containers:\n        - name: shell\n          image: busybox:1.36.1\n          command: [sh, -c, 'echo k10s-applied-ready; sleep 3600']\n",
        deployment.uid
    );
    let QueryResult::YamlValidation(validation) = adapter
        .query(Query::ValidateApply {
            context: CONTEXT.into(),
            yaml: yaml.clone(),
        })
        .await
        .unwrap()
    else {
        panic!("validation returned the wrong result")
    };
    let OutcomeData::Valid { ticket } = validation.outcome else {
        panic!("live server-side dry-run rejected the fixture")
    };
    let hash = k10s_protocol::buffer_hash(&yaml);
    run(
        &adapter,
        "apply",
        Command::Apply {
            context: CONTEXT.into(),
            yaml,
            idempotency_key: "kind-apply".into(),
            ticket_id: ticket.id,
            buffer_hash: hash,
            target: deployment.clone(),
        },
    )
    .await;
    assert_eq!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "deployment/operations-web",
            "-o",
            "jsonpath={.spec.replicas}"
        ]),
        replicas.to_string()
    );
    assert_eq!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "deployment/operations-web",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].command[2]}"
        ]),
        "echo k10s-applied-ready; sleep 3600",
        "apply changes the intended pod command"
    );

    let mut stale = deployment.clone();
    stale.uid = "replaced-uid".into();
    assert!(matches!(
        adapter
            .execute(Command::Restart {
                target: stale,
                idempotency_key: "kind-stale".into()
            })
            .await,
        Err(BackendError::Conflict(_))
    ));

    run(
        &adapter,
        "scale",
        Command::Scale {
            context: CONTEXT.into(),
            gvk: deployment.gvk.clone(),
            namespace: deployment.namespace.clone(),
            name: deployment.name.clone(),
            uid: deployment.uid.clone(),
            replicas: if replicas == 1 { 2 } else { 1 },
            idempotency_key: "kind-scale".into(),
        },
    )
    .await;
    kubectl(&[
        "-n",
        NAMESPACE,
        "rollout",
        "status",
        "deployment/operations-web",
        "--timeout=60s",
    ]);
    let previous_restart = kubectl(&[
        "-n",
        NAMESPACE,
        "get",
        "deployment/operations-web",
        "-o",
        "jsonpath={.spec.template.metadata.annotations.kubectl\\.kubernetes\\.io/restartedAt}",
    ]);
    run(
        &adapter,
        "restart",
        Command::Restart {
            target: deployment,
            idempotency_key: "kind-restart".into(),
        },
    )
    .await;
    let restart = kubectl(&[
        "-n",
        NAMESPACE,
        "get",
        "deployment/operations-web",
        "-o",
        "jsonpath={.spec.template.metadata.annotations.kubectl\\.kubernetes\\.io/restartedAt}",
    ]);
    assert!(!restart.is_empty(), "restart writes the rollout annotation");
    assert_ne!(restart, previous_restart, "restart advances rollout state");

    let _ = ProcessCommand::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig())
        .arg("--context")
        .arg(admin_context())
        .args([
            "-n",
            NAMESPACE,
            "patch",
            "configmap/delete-dependent",
            "--type=merge",
            "-p",
            r#"{"metadata":{"finalizers":null}}"#,
        ])
        .status();
    let _ = ProcessCommand::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig())
        .arg("--context")
        .arg(admin_context())
        .args([
            "-n",
            NAMESPACE,
            "delete",
            "configmap/delete-me",
            "configmap/delete-dependent",
            "--ignore-not-found",
            "--wait=true",
        ])
        .status();
    kubectl(&[
        "-n",
        NAMESPACE,
        "create",
        "configmap",
        "delete-me",
        "--from-literal=value=fixture",
    ]);
    kubectl(&[
        "-n",
        NAMESPACE,
        "create",
        "configmap",
        "delete-dependent",
        "--from-literal=value=dependent",
    ]);
    let owner_uid = kubectl(&[
        "-n",
        NAMESPACE,
        "get",
        "configmap/delete-me",
        "-o",
        "jsonpath={.metadata.uid}",
    ]);
    let dependent_patch = format!(
        r#"{{"metadata":{{"finalizers":["k10s.dev/hold"],"ownerReferences":[{{"apiVersion":"v1","kind":"ConfigMap","name":"delete-me","uid":"{owner_uid}","blockOwnerDeletion":true}}]}}}}"#
    );
    kubectl(&[
        "-n",
        NAMESPACE,
        "patch",
        "configmap/delete-dependent",
        "--type=merge",
        "-p",
        &dependent_patch,
    ]);
    let target = reference(&adapter, Gvk::core("v1", "ConfigMap"), "delete-me").await;
    run(
        &adapter,
        "delete",
        Command::Delete {
            target,
            propagation: Propagation::Foreground,
            idempotency_key: "kind-foreground-delete".into(),
        },
    )
    .await;
    assert!(
        !kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "configmap/delete-me",
            "-o",
            "jsonpath={.metadata.deletionTimestamp}",
        ])
        .is_empty()
    );
    assert!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "configmap/delete-me",
            "-o",
            "jsonpath={.metadata.finalizers}",
        ])
        .contains("foregroundDeletion")
    );
    assert_eq!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "configmap/delete-dependent",
            "-o",
            "jsonpath={.metadata.finalizers[0]}",
        ]),
        "k10s.dev/hold",
        "foreground deletion waits for a blocked dependent"
    );
    kubectl(&[
        "-n",
        NAMESPACE,
        "patch",
        "configmap/delete-dependent",
        "--type=merge",
        "-p",
        r#"{"metadata":{"finalizers":null}}"#,
    ]);
    for _ in 0..100 {
        if kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "configmap/delete-me",
            "--ignore-not-found",
        ])
        .is_empty()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "configmap/delete-me",
            "--ignore-not-found"
        ])
        .is_empty()
    );
    assert!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "configmap/delete-dependent",
            "--ignore-not-found"
        ])
        .is_empty()
    );

    let _ = ProcessCommand::new("kubectl")
        .arg("--kubeconfig")
        .arg(kubeconfig())
        .arg("--context")
        .arg(admin_context())
        .args([
            "-n",
            "k10s-operations-forbidden",
            "delete",
            "deployment/forbidden",
            "--ignore-not-found",
            "--wait=true",
        ])
        .status();
    kubectl(&[
        "-n",
        "k10s-operations-forbidden",
        "create",
        "deployment",
        "forbidden",
        "--image=busybox:1.36.1",
    ]);
    let forbidden_uid = kubectl(&[
        "-n",
        "k10s-operations-forbidden",
        "get",
        "deployment/forbidden",
        "-o",
        "jsonpath={.metadata.uid}",
    ]);
    assert_eq!(
        adapter
            .execute(Command::Restart {
                target: ResourceRef {
                    context: CONTEXT.into(),
                    gvk: Gvk::new("apps", "v1", "Deployment"),
                    namespace: Some("k10s-operations-forbidden".into()),
                    name: "forbidden".into(),
                    uid: forbidden_uid,
                },
                idempotency_key: "kind-forbidden-restart".into(),
            })
            .await
            .unwrap_err(),
        BackendError::Forbidden
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires tests/kind/cluster.sh up"]
async fn live_job_cronjob_logs_exec_and_rbac_paths_are_real() {
    let _guard = KIND_LOCK.lock().await;
    let adapter = adapter();
    let cronjob = reference(
        &adapter,
        Gvk::new("batch", "v1", "CronJob"),
        "operations-cron",
    )
    .await;
    run(
        &adapter,
        "cronjob suspend",
        Command::SetCronJobSuspended {
            target: cronjob.clone(),
            suspended: true,
            idempotency_key: "kind-suspend".into(),
        },
    )
    .await;
    assert_eq!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "cronjob/operations-cron",
            "-o",
            "jsonpath={.spec.suspend}",
        ]),
        "true",
        "suspend mutates CronJob state"
    );
    run(
        &adapter,
        "cronjob resume",
        Command::SetCronJobSuspended {
            target: cronjob.clone(),
            suspended: false,
            idempotency_key: "kind-resume".into(),
        },
    )
    .await;
    assert_eq!(
        kubectl(&[
            "-n",
            NAMESPACE,
            "get",
            "cronjob/operations-cron",
            "-o",
            "jsonpath={.spec.suspend}",
        ]),
        "false",
        "resume restores CronJob state"
    );
    run(
        &adapter,
        "job creation",
        Command::CreateJob {
            source: cronjob,
            idempotency_key: "kind-create-job".into(),
        },
    )
    .await;
    assert!(!kubectl(&["-n", NAMESPACE, "get", "jobs", "-o", "name"]).is_empty());

    let pod = reference(&adapter, Gvk::core("v1", "Pod"), "operations-shell").await;
    let logs = StreamKind::Logs {
        context: CONTEXT.into(),
        namespace: NAMESPACE.into(),
        pod: pod.name.clone(),
        uid: pod.uid.clone(),
        container: "shell".into(),
        tail_lines: Some(20),
        since_seconds: Some(300),
        previous: false,
        timestamps: false,
        follow: false,
    };
    let QueryResult::StreamTicket(log_ticket) = adapter
        .query(Query::StreamTicket { stream: logs })
        .await
        .unwrap()
    else {
        panic!("log ticket")
    };
    let mut handle = adapter
        .subscribe(Subscribe::StreamRedeem {
            ticket_id: log_ticket.ticket_id,
            route: StreamRouteKind::Logs,
        })
        .await
        .unwrap();
    let mut events = handle.take_events().unwrap();
    let mut text = String::new();
    while let Ok(Ok(BackendEvent::Stream(chunk))) =
        tokio::time::timeout(Duration::from_secs(3), events.recv()).await
    {
        text.push_str(&chunk.text);
        if text.contains("k10s-log-ready") {
            break;
        }
    }
    assert!(text.contains("k10s-log-ready"), "actual logs: {text:?}");

    let exec = StreamKind::Exec {
        context: CONTEXT.into(),
        namespace: NAMESPACE.into(),
        pod: pod.name,
        uid: pod.uid,
        container: "shell".into(),
        command: vec![
            "sh".into(),
            "-c".into(),
            "read value; echo exec-$value".into(),
        ],
        tty: false,
    };
    let QueryResult::StreamTicket(exec_ticket) = adapter
        .query(Query::StreamTicket { stream: exec })
        .await
        .unwrap()
    else {
        panic!("exec ticket")
    };
    let ticket_id = exec_ticket.ticket_id;
    let mut handle = adapter
        .subscribe(Subscribe::StreamRedeem {
            ticket_id: ticket_id.clone(),
            route: StreamRouteKind::Exec,
        })
        .await
        .unwrap();
    let mut events = handle.take_events().unwrap();
    adapter
        .stream_input(&ticket_id, StreamInput::Stdin("verified\n".into()))
        .await
        .unwrap();
    let mut text = String::new();
    while let Ok(Ok(BackendEvent::Stream(chunk))) =
        tokio::time::timeout(Duration::from_secs(5), events.recv()).await
    {
        if chunk.origin == StreamOrigin::Stdout {
            text.push_str(&chunk.text);
        }
        if chunk.exit_code.is_some() {
            break;
        }
    }
    assert!(
        text.contains("exec-verified"),
        "actual exec output: {text:?}"
    );

    assert_eq!(
        adapter
            .query(Query::ResourceList {
                context: CONTEXT.into(),
                gvk: Gvk::core("v1", "Pod"),
                namespace: Some("k10s-operations-forbidden".into()),
            })
            .await
            .unwrap_err(),
        BackendError::Forbidden
    );
}
