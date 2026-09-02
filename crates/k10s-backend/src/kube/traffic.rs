//! Low-overhead byte counters around the kube-rs HTTP transport.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http_body::{Body as HttpBody, Frame};
use kube::client::Body;
use tower::{BoxError, Layer, Service};

#[derive(Debug, Default)]
pub(super) struct TrafficCounters {
    uploaded: AtomicU64,
    downloaded: AtomicU64,
    requests: AtomicU64,
    active: AtomicU64,
}

impl TrafficCounters {
    pub(super) fn totals(&self) -> TrafficTotals {
        TrafficTotals {
            uploaded: self.uploaded.load(Ordering::Relaxed),
            downloaded: self.downloaded.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct TrafficTotals {
    uploaded: u64,
    downloaded: u64,
    requests: u64,
    active: u64,
}

impl TrafficTotals {
    pub(super) fn sample(
        self,
        context: String,
        previous: Self,
        elapsed: Duration,
    ) -> k10s_protocol::TrafficSample {
        let millis = elapsed.as_millis().max(1) as u64;
        let per_second =
            |current: u64, old: u64| current.saturating_sub(old).saturating_mul(1_000) / millis;
        k10s_protocol::TrafficSample {
            context,
            captured_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            upload_bytes_per_second: per_second(self.uploaded, previous.uploaded),
            download_bytes_per_second: per_second(self.downloaded, previous.downloaded),
            uploaded_bytes_total: self.uploaded,
            downloaded_bytes_total: self.downloaded,
            requests_total: self.requests,
            active_requests: self.active,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct TrafficRegistry {
    counters: Arc<std::sync::Mutex<HashMap<String, Arc<TrafficCounters>>>>,
}

impl TrafficRegistry {
    pub(super) fn counters(&self, context: &str) -> Arc<TrafficCounters> {
        Arc::clone(
            self.counters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(context.to_owned())
                .or_default(),
        )
    }
}

pub(super) struct TrafficLayer {
    counters: Arc<TrafficCounters>,
}

impl TrafficLayer {
    pub(super) fn new(counters: Arc<TrafficCounters>) -> Self {
        Self { counters }
    }
}

impl<S> Layer<S> for TrafficLayer {
    type Service = TrafficService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TrafficService {
            inner,
            counters: Arc::clone(&self.counters),
        }
    }
}

pub(super) struct TrafficService<S> {
    inner: S,
    counters: Arc<TrafficCounters>,
}

impl<S, B> Service<http::Request<Body>> for TrafficService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Response = http::Response<CountingBody<B>>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        use http_body::Body as _;
        let size = request.body().size_hint();
        let uploaded = size.exact().unwrap_or_else(|| size.lower());
        self.counters
            .uploaded
            .fetch_add(uploaded, Ordering::Relaxed);
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        self.counters.active.fetch_add(1, Ordering::Relaxed);
        let counters = Arc::clone(&self.counters);
        let future = self.inner.call(request);
        Box::pin(async move {
            match future.await.map_err(Into::into) {
                Ok(response) => Ok(response.map(|body| CountingBody::new(body, counters))),
                Err(error) => {
                    counters.active.fetch_sub(1, Ordering::Relaxed);
                    Err(error)
                }
            }
        })
    }
}

pub(super) struct CountingBody<B> {
    inner: Pin<Box<B>>,
    counters: Arc<TrafficCounters>,
}

impl<B> CountingBody<B> {
    fn new(inner: B, counters: Arc<TrafficCounters>) -> Self {
        Self {
            inner: Box::pin(inner),
            counters,
        }
    }
}

impl<B> HttpBody for CountingBody<B>
where
    B: HttpBody<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.counters
                        .downloaded
                        .fetch_add(data.len() as u64, Ordering::Relaxed);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl<B> Drop for CountingBody<B> {
    fn drop(&mut self) {
        self.counters.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt as _, Full};
    use tower::{ServiceExt as _, service_fn};

    #[tokio::test]
    async fn counts_request_and_consumed_response_bytes() {
        let counters = Arc::new(TrafficCounters::default());
        let service = service_fn(|_: http::Request<Body>| async {
            Ok::<_, BoxError>(http::Response::new(Full::new(Bytes::from_static(b"hello"))))
        });
        let response = TrafficLayer::new(Arc::clone(&counters))
            .layer(service)
            .oneshot(http::Request::new(Body::from(b"abc".to_vec())))
            .await
            .unwrap();
        assert_eq!(counters.totals().active, 1);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "hello"
        );
        let totals = counters.totals();
        assert_eq!(totals.uploaded, 3);
        assert_eq!(totals.downloaded, 5);
        assert_eq!(totals.requests, 1);
        assert_eq!(totals.active, 0);
    }

    #[test]
    fn computes_saturating_interval_rates() {
        let current = TrafficTotals {
            uploaded: 300,
            downloaded: 700,
            requests: 2,
            active: 1,
        };
        let previous = TrafficTotals {
            uploaded: 100,
            downloaded: 300,
            requests: 1,
            active: 0,
        };
        let sample = current.sample("dev".into(), previous, Duration::from_millis(500));
        assert_eq!(sample.upload_bytes_per_second, 400);
        assert_eq!(sample.download_bytes_per_second, 800);
    }
}
