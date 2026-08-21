//! Contract test for the backend kernel's bootstrap behavior.
//!
//! Verifies that the kernel is the sole protocol-facing interface and that
//! fake-adapter data never escapes as fixture types.

use k10s_backend::{BackendKernel, FakeKubernetes, Query};

#[tokio::test]
async fn bootstrap_hides_credentials_and_reports_fake_contexts() {
    let kernel = BackendKernel::new(FakeKubernetes::standard());
    let result = kernel.query(Query::Bootstrap).await.unwrap();
    assert_eq!(result.context_names(), ["dev-local", "prod-readonly"]);
    assert!(!result.serialized().contains("token"));
}
