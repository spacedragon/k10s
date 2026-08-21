//! Behavior tests for the backend kernel and the KubernetesAccess port.
//!
//! These tests exercise the kernel as the sole protocol-facing interface and
//! never reach into fake-adapter internal collections.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use k10s_backend::{
    BackendError, BackendKernel, Command, FakeKubernetes, KubernetesAccess, OperationId, Query,
    QueryResult, Subscribe, SubscriptionHandle,
};
use k10s_protocol::{
    RequestId, ServerFrame, ServerKind, decode_server_frame, validate_bootstrap_response,
};

#[tokio::test]
async fn unsupported_queries_return_typed_capability_errors() {
    let kernel = BackendKernel::new(FakeKubernetes::standard());
    let err = kernel
        .query(Query::ValidateApply {
            context: "dev-local".into(),
            yaml: "kind: ConfigMap".into(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        err,
        BackendError::Unsupported {
            capability: "validate.apply".into()
        }
    );
}

#[tokio::test]
async fn unsupported_commands_return_typed_capability_errors() {
    let kernel = BackendKernel::new(FakeKubernetes::standard());
    let err = kernel
        .execute(Command::Scale {
            context: "dev-local".into(),
            kind: "Deployment".into(),
            namespace: "default".into(),
            name: "api".into(),
            replicas: 3,
        })
        .await
        .unwrap_err();

    assert_eq!(
        err,
        BackendError::Unsupported {
            capability: "execute".into()
        }
    );
}

#[tokio::test]
async fn unsupported_subscriptions_return_typed_capability_errors() {
    let kernel = BackendKernel::new(FakeKubernetes::standard());
    let err = kernel
        .subscribe(Subscribe::ResourceList {
            context: "dev-local".into(),
            kind: "Pod".into(),
            namespace: None,
        })
        .await
        .unwrap_err();

    assert_eq!(
        err,
        BackendError::Unsupported {
            capability: "resource.list".into()
        }
    );
}

#[tokio::test]
async fn bootstrap_status_subscription_is_opaque() {
    let kernel = BackendKernel::new(FakeKubernetes::standard());
    let handle = kernel.subscribe(Subscribe::BootstrapStatus).await.unwrap();
    assert!(!handle.id.is_empty());
}

#[tokio::test]
async fn bootstrap_result_carries_server_instance_id() {
    let kernel = BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "instance-1");
    let result = kernel.query(Query::Bootstrap).await.unwrap();
    let serialized = result.serialized();
    assert!(serialized.contains("instance-1"));
    assert_eq!(result.context_names(), ["dev-local", "prod-readonly"]);
}

#[tokio::test]
async fn bootstrap_wire_payload_decodes_through_protocol_validator() {
    let kernel = BackendKernel::new(FakeKubernetes::standard());
    let result = kernel.query(Query::Bootstrap).await.unwrap();

    // The wire payload must decode through the protocol validator.
    let wire = result.wire_payload();
    let value = serde_json::to_value(&wire).unwrap();
    assert!(validate_bootstrap_response(&value).is_ok());

    // It must also round-trip through a ServerFrame response envelope.
    let frame = ServerFrame::response(RequestId::from("req-1"), &wire);
    let frame_value = serde_json::to_value(&frame).unwrap();
    let decoded = decode_server_frame(frame_value).unwrap();
    assert_eq!(decoded.kind, ServerKind::Response);
    assert!(decoded.request_id().is_some());
}

#[tokio::test]
async fn distinct_kernels_get_distinct_instance_ids() {
    let a = BackendKernel::new(FakeKubernetes::standard());
    let b = BackendKernel::new(FakeKubernetes::standard());
    assert_ne!(a.server_instance_id(), b.server_instance_id());
}

#[tokio::test]
async fn deterministic_instance_id_injection() {
    let a = BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "fixed-id");
    let b = BackendKernel::new_with_instance_id(FakeKubernetes::standard(), "fixed-id");
    assert_eq!(a.server_instance_id(), b.server_instance_id());
    assert_eq!(a.server_instance_id(), "fixed-id");
}

/// An adapter that never responds in time, used to verify deadline enforcement.
#[derive(Debug)]
struct SlowAdapter;

impl KubernetesAccess for SlowAdapter {
    fn query<'a>(
        &'a self,
        _req: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            unreachable!("deadline must fire first")
        })
    }

    fn execute<'a>(
        &'a self,
        _cmd: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("execute")) })
    }

    fn subscribe<'a>(
        &'a self,
        _req: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("subscribe")) })
    }
}

#[tokio::test]
async fn kernel_enforces_query_deadlines() {
    let kernel = BackendKernel::new(SlowAdapter);
    let err = kernel
        .query_with_deadline(Query::Bootstrap, Some(Duration::from_millis(50)))
        .await
        .unwrap_err();
    assert_eq!(err, BackendError::Timeout);
}

/// An adapter that implements execute, proving the port is the sole seam.
#[derive(Debug)]
struct ExecAdapter;

impl KubernetesAccess for ExecAdapter {
    fn query<'a>(
        &'a self,
        _req: Query,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResult, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("query")) })
    }

    fn execute<'a>(
        &'a self,
        _cmd: Command,
    ) -> Pin<Box<dyn Future<Output = Result<OperationId, BackendError>> + Send + 'a>> {
        Box::pin(async { Ok(OperationId::new("op-1")) })
    }

    fn subscribe<'a>(
        &'a self,
        _req: Subscribe,
    ) -> Pin<Box<dyn Future<Output = Result<SubscriptionHandle, BackendError>> + Send + 'a>> {
        Box::pin(async { Err(BackendError::unsupported("subscribe")) })
    }
}

#[tokio::test]
async fn execute_returns_operation_id_through_kernel() {
    let kernel = BackendKernel::new(ExecAdapter);
    let id = kernel
        .execute(Command::Delete {
            context: "dev-local".into(),
            kind: "Pod".into(),
            namespace: "default".into(),
            name: "api".into(),
            idempotency_key: "idem-1".into(),
        })
        .await
        .unwrap();
    assert_eq!(id.as_str(), "op-1");
}
