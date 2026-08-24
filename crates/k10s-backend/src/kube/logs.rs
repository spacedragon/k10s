//! Exact-identity, bounded Kubernetes Pod log streaming.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures_util::{AsyncBufReadExt, AsyncReadExt};
use k8s_openapi::api::core::v1::Pod;
use kube::ResourceExt;
use kube::api::{Api, LogParams};
use tokio::sync::broadcast;

use crate::port::{
    BackendError, BackendEvent, QueryResult, StreamGrant, StreamKind, StreamRouteKind,
    SubscriptionHandle,
};
use crate::stream::{STREAM_QUEUE_CAPACITY, StreamChunk, StreamOrigin};

use super::KubeAdapter;

const CAPACITY: usize = 32;
const TTL: Duration = Duration::from_secs(60);
const MAX_TAIL_LINES: i64 = 10_000;
const MAX_SINCE_SECONDS: i64 = 31 * 24 * 60 * 60;
const MAX_LOG_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Debug)]
struct Ticket {
    stream: StreamKind,
    issued: Instant,
}

#[derive(Debug, Default)]
struct Inner {
    tickets: HashMap<String, Ticket>,
    order: VecDeque<String>,
    instance_id: String,
}

#[derive(Debug)]
pub(super) struct StreamTickets(Mutex<Inner>);

impl StreamTickets {
    pub(super) fn new() -> Self {
        Self(Mutex::new(Inner {
            tickets: HashMap::new(),
            order: VecDeque::new(),
            instance_id: uuid::Uuid::new_v4().to_string(),
        }))
    }

    pub(super) fn issue(&self, stream: StreamKind) -> String {
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = format!("{}:{}", inner.instance_id, uuid::Uuid::new_v4());
        inner.tickets.insert(
            id.clone(),
            Ticket {
                stream,
                issued: Instant::now(),
            },
        );
        inner.order.push_back(id.clone());
        while inner.tickets.len() > CAPACITY {
            if let Some(oldest) = inner.order.pop_front() {
                inner.tickets.remove(&oldest);
            }
        }
        id
    }

    pub(super) fn redeem_for(
        &self,
        id: &str,
        route: StreamRouteKind,
    ) -> Result<StreamKind, BackendError> {
        let mut inner = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(ticket) = inner.tickets.get(id) else {
            return Err(BackendError::Conflict(
                "the stream ticket is unknown or already used".into(),
            ));
        };
        if ticket.issued.elapsed() > TTL {
            inner.tickets.remove(id);
            inner.order.retain(|candidate| candidate != id);
            return Err(BackendError::Conflict(
                "the stream ticket has expired".into(),
            ));
        }
        let expected = match &ticket.stream {
            StreamKind::Logs { .. } => StreamRouteKind::Logs,
            StreamKind::Exec { .. } => StreamRouteKind::Exec,
        };
        if route != expected {
            return Err(BackendError::Conflict(
                "the stream ticket was issued for a different route".into(),
            ));
        }
        let ticket = inner.tickets.remove(id).expect("checked above");
        inner.order.retain(|candidate| candidate != id);
        Ok(ticket.stream)
    }
}

impl KubeAdapter {
    pub(super) async fn issue_stream_ticket(
        &self,
        stream: StreamKind,
    ) -> Result<QueryResult, BackendError> {
        if matches!(stream, StreamKind::Exec { .. }) {
            return self.issue_exec_ticket(stream).await;
        }
        let StreamKind::Logs {
            context,
            namespace,
            pod,
            uid: requested_uid,
            container,
            tail_lines,
            since_seconds,
            timestamps,
            follow,
        } = stream
        else {
            unreachable!()
        };
        if tail_lines.is_some_and(|value| !(0..=MAX_TAIL_LINES).contains(&value))
            || since_seconds.is_some_and(|value| !(0..=MAX_SINCE_SECONDS).contains(&value))
        {
            return Err(BackendError::Conflict(
                "log history limits exceed the configured bounds".into(),
            ));
        }
        let client = self.cluster_client(&context).await?;
        let api: Api<Pod> = Api::namespaced(client, &namespace);
        let current = api.get(&pod).await.map_err(stream_error)?;
        let observed_uid = current
            .uid()
            .ok_or_else(|| BackendError::Conflict("the pod has no UID".into()))?;
        if !requested_uid.is_empty() && requested_uid != observed_uid {
            return Err(BackendError::Conflict("the pod was replaced".into()));
        }
        if !has_container(&current, &container) {
            return Err(BackendError::NotFound);
        }

        // A one-line, non-following request validates the pods/log RBAC
        // subresource without retaining data or opening a live session.
        let probe = LogParams {
            container: Some(container.clone()),
            tail_lines: Some(1),
            ..LogParams::default()
        };
        drop(api.log_stream(&pod, &probe).await.map_err(stream_error)?);

        let bound = StreamKind::Logs {
            context,
            namespace,
            pod,
            uid: observed_uid,
            container,
            tail_lines,
            since_seconds,
            timestamps,
            follow,
        };
        let id = self.stream_tickets.issue(bound.clone());
        Ok(QueryResult::StreamTicket(StreamGrant {
            ticket_id: id,
            stream: bound,
        }))
    }

    pub(super) async fn redeem_stream_ticket(
        &self,
        ticket_id: String,
        route: StreamRouteKind,
    ) -> Result<SubscriptionHandle, BackendError> {
        if route == StreamRouteKind::Exec {
            return self.redeem_exec_ticket(ticket_id).await;
        }
        let bound = self.stream_tickets.redeem_for(&ticket_id, route)?;
        let StreamKind::Logs {
            context,
            namespace,
            pod,
            uid,
            container,
            tail_lines,
            since_seconds,
            timestamps,
            follow,
        } = &bound
        else {
            unreachable!()
        };
        let client = self.cluster_client(context).await?;
        let api: Api<Pod> = Api::namespaced(client, namespace);
        let current = api.get(pod).await.map_err(stream_error)?;
        if current.uid().as_deref() != Some(uid.as_str()) {
            return Err(BackendError::Conflict(
                "the pod was replaced after ticket issuance".into(),
            ));
        }
        if !has_container(&current, container) {
            return Err(BackendError::NotFound);
        }
        let params = LogParams {
            container: Some(container.clone()),
            follow: *follow,
            since_seconds: *since_seconds,
            tail_lines: *tail_lines,
            timestamps: *timestamps,
            ..LogParams::default()
        };
        let mut reader = api.log_stream(pod, &params).await.map_err(stream_error)?;
        let (sender, receiver) = broadcast::channel(STREAM_QUEUE_CAPACITY);
        tokio::spawn(async move { pump(&mut reader, sender).await });
        Ok(SubscriptionHandle::with_events("kube-log-stream", receiver).with_stream(bound))
    }
}

async fn pump<R: futures_util::AsyncBufRead + Unpin>(
    reader: &mut R,
    sender: broadcast::Sender<BackendEvent>,
) {
    let mut bytes = Vec::new();
    loop {
        bytes.clear();
        let mut limited = (&mut *reader).take(MAX_LOG_CHUNK_BYTES as u64);
        tokio::select! {
            _ = sender.closed() => break,
            read = limited.read_until(b'\n', &mut bytes) => match read {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let text = String::from_utf8_lossy(&bytes).into_owned();
                    if sender.send(BackendEvent::Stream(StreamChunk { origin: StreamOrigin::Stdout, text, exit_code: None })).is_err() { break; }
                }
            }
        }
    }
}

pub(super) fn has_container(pod: &Pod, name: &str) -> bool {
    pod.spec.as_ref().is_some_and(|spec| {
        spec.containers
            .iter()
            .any(|container| container.name == name)
            || spec
                .init_containers
                .as_ref()
                .is_some_and(|items| items.iter().any(|container| container.name == name))
            || spec
                .ephemeral_containers
                .as_ref()
                .is_some_and(|items| items.iter().any(|container| container.name == name))
    })
}

fn stream_error(error: kube::Error) -> BackendError {
    match error {
        kube::Error::Api(status) if status.code == 403 => BackendError::Forbidden,
        kube::Error::Api(status) if status.code == 404 => BackendError::NotFound,
        _ => BackendError::Internal("kubernetes log stream unavailable".into()),
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_util::{AsyncBufRead, AsyncRead};

    use super::*;

    fn log_stream(pod: &str) -> StreamKind {
        StreamKind::Logs {
            context: "dev".into(),
            namespace: "default".into(),
            pod: pod.into(),
            uid: format!("uid-{pod}"),
            container: "app".into(),
            tail_lines: Some(200),
            since_seconds: None,
            timestamps: true,
            follow: true,
        }
    }

    #[test]
    fn ticket_store_is_opaque_single_use_and_evicts_the_oldest_entry() {
        let tickets = StreamTickets::new();
        let oldest = tickets.issue(log_stream("sensitive-pod-name"));
        assert!(!oldest.contains("sensitive-pod-name"));

        let mut newest = String::new();
        for index in 1..=CAPACITY {
            newest = tickets.issue(log_stream(&format!("pod-{index}")));
        }

        assert!(matches!(
            tickets.redeem_for(&oldest, StreamRouteKind::Logs),
            Err(BackendError::Conflict(_))
        ));
        assert!(matches!(
            tickets.redeem_for(&newest, StreamRouteKind::Logs),
            Ok(StreamKind::Logs { .. })
        ));
        assert!(matches!(
            tickets.redeem_for(&newest, StreamRouteKind::Logs),
            Err(BackendError::Conflict(_))
        ));
    }

    struct PendingReader;

    impl AsyncRead for PendingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Pending
        }
    }
    impl AsyncBufRead for PendingReader {
        fn poll_fill_buf(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<std::io::Result<&[u8]>> {
            Poll::Pending
        }
        fn consume(self: Pin<&mut Self>, _: usize) {}
    }

    #[tokio::test]
    async fn receiver_loss_cancels_even_a_stalled_upstream_reader() {
        let (sender, receiver) = broadcast::channel(2);
        let task = tokio::spawn(async move { pump(&mut PendingReader, sender).await });
        drop(receiver);
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn invalid_utf8_is_replaced_without_dropping_neighboring_bytes() {
        let mut reader = futures_util::io::BufReader::new(futures_util::io::Cursor::new(vec![
            b'a', 0xff, b'b', b'\n',
        ]));
        let (sender, mut receiver) = broadcast::channel(2);
        pump(&mut reader, sender).await;
        let BackendEvent::Stream(chunk) = receiver.try_recv().unwrap() else {
            panic!("stream")
        };
        assert_eq!(chunk.text, "a�b\n");
    }

    #[tokio::test]
    async fn a_log_line_without_newlines_is_split_at_the_hard_chunk_bound() {
        let input = vec![b'x'; MAX_LOG_CHUNK_BYTES + 17];
        let mut reader = futures_util::io::BufReader::new(futures_util::io::Cursor::new(input));
        let (sender, mut receiver) = broadcast::channel(4);
        pump(&mut reader, sender).await;
        let BackendEvent::Stream(first) = receiver.try_recv().unwrap() else {
            panic!("stream")
        };
        let BackendEvent::Stream(second) = receiver.try_recv().unwrap() else {
            panic!("stream")
        };
        assert_eq!(first.text.len(), MAX_LOG_CHUNK_BYTES);
        assert_eq!(second.text.len(), 17);
    }
}
