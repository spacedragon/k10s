//! Exact Service-to-Pod port-forward resolution contracts.
//!
//! The [`PortForwardConnector`] is the sole seam for resolving one declared
//! TCP Service port to exactly one ready backing Pod and opening opaque
//! byte streams to it. Resolution binds every session to exact Service and
//! Pod UIDs; streams never expose Kubernetes client types outside this
//! crate and are never serialized onto the control protocol.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::port::BackendError;

/// Which declared Service port a start request forwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortForwardPortSelection {
    /// Select by declared Service port name.
    Name(String),
    /// Select by declared Service port number.
    Number(u16),
}

/// One bounded port-forward start request before any resolution.
///
/// Carries the exact core/v1 Service identity; the UID must match the live
/// object or resolution fails without binding anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortForwardRequest {
    /// Kubernetes context to resolve within.
    pub context: String,
    /// Service namespace.
    pub namespace: String,
    /// Service name.
    pub service_name: String,
    /// Immutable Service UID the live object must carry.
    pub service_uid: String,
    /// Which declared Service port to forward.
    pub port: PortForwardPortSelection,
}

/// A resolved, pinned forward target owned by the backend.
///
/// Only backend-owned values cross the seam: no Kubernetes types, no
/// sockets. Sessions pin these values; later endpoint changes never
/// retarget them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPortForward {
    /// Context the Service and Pod were resolved in.
    pub context: String,
    /// Namespace of both objects.
    pub namespace: String,
    /// Verified live Service UID.
    pub service_uid: String,
    /// Selected backing Pod name.
    pub pod_name: String,
    /// Verified Pod UID.
    pub pod_uid: String,
    /// Numeric target port on the Pod.
    pub pod_port: u16,
}

/// Safe rejection categories produced by resolution and connection.
///
/// These mirror the protocol failure categories so the server can surface
/// typed failures without parsing error text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionCategory {
    /// No ready endpoint exists for the requested Service port.
    UnavailableEndpoint,
    /// Kubernetes authorization denied a required call.
    Forbidden,
    /// The Service or Pod identity changed or disappeared.
    VanishedResource,
    /// The Service type or port cannot be forwarded.
    UnsupportedService,
    /// The upstream stream failed before or during transfer.
    TransportClosed,
}

/// An opaque bidirectional byte stream to one pinned Pod port.
///
/// The concrete transport stays inside the backend crate.
pub struct PortForwardStream(Box<dyn PortForwardIo>);

impl PortForwardStream {
    pub(crate) fn new(io: Box<dyn PortForwardIo>) -> Self {
        Self(io)
    }
}

impl std::fmt::Debug for PortForwardStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PortForwardStream")
    }
}

impl AsyncRead for PortForwardStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for PortForwardStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// Object-safe supertrait bounding the boxed transport.
pub(crate) trait PortForwardIo: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> PortForwardIo for T {}

/// Behavior-level seam behind [`PortForwardConnector`].
pub trait PortForwardSeam: Send + Sync + std::fmt::Debug {
    /// Resolve one request to an exact pinned Pod target.
    fn resolve<'a>(
        &'a self,
        request: PortForwardRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedPortForward, BackendError>> + Send + 'a>>;

    /// Open one fresh byte stream to the pinned Pod port.
    fn connect<'a>(
        &'a self,
        resolved: &'a ResolvedPortForward,
    ) -> Pin<Box<dyn Future<Output = Result<PortForwardStream, BackendError>> + Send + 'a>>;
}

/// Cloneable handle to the backend's port-forward seam.
///
/// Clones share one implementation; the connector itself carries no state
/// beyond the seam.
#[derive(Clone)]
pub struct PortForwardConnector {
    seam: Arc<dyn PortForwardSeam>,
}

impl PortForwardConnector {
    /// Wrap one seam implementation.
    pub fn new(seam: Arc<dyn PortForwardSeam>) -> Self {
        Self { seam }
    }

    /// Resolve one start request to an exact Service-UID- and Pod-UID-bound
    /// target. Failures never bind local resources.
    pub async fn resolve_service_port(
        &self,
        request: PortForwardRequest,
    ) -> Result<ResolvedPortForward, BackendError> {
        self.seam.resolve(request).await
    }

    /// Open one fresh byte stream to the pinned remote port.
    ///
    /// Each accepted local connection calls this once; a failed stream
    /// affects only its own connection.
    pub async fn connect(
        &self,
        resolved: &ResolvedPortForward,
    ) -> Result<PortForwardStream, BackendError> {
        self.seam.connect(resolved).await
    }
}

impl std::fmt::Debug for PortForwardConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PortForwardConnector")
    }
}
