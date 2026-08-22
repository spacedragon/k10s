use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use k10s_backend::{
    BackendError, BackendEvent, BackendKernel, Gvk, KernelQueryResult, Query,
    Subscribe as BackendSubscribe,
};
use k10s_protocol::{
    ClientKind, ClientPayload, ErrorCode, ErrorFrame, ErrorScope, RequestId, ResourceIdentity,
    ResourceListRequest, ResourceRefRequest, ResumeStatus, Retryability, ServerFrame, ServerKind,
    SessionId, ShutdownNotice, SnapshotBegin, SnapshotChunk, SnapshotEnd, Subscribed,
    SubscriptionId, SubscriptionSelector, Welcome, decode_client_frame,
};
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::auth::{AuthenticationError, authenticate};
use crate::config::ServerConfig;
use crate::lifecycle::{DrainSignals, MutationGate};
use crate::outbound::{EnqueueError, Priority, Scheduler};

/// Rows carried by one bounded snapshot chunk frame.
const RESOURCE_ROWS_PER_CHUNK: usize = 16;

/// A request failure that is not attributable to the backend adapter.
enum RequestFailure {
    Backend(BackendError),
    Malformed(String),
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_socket(
    socket: WebSocket,
    config: Arc<ServerConfig>,
    kernel: Arc<BackendKernel>,
    unauthenticated: OwnedSemaphorePermit,
    authenticated_slots: Arc<tokio::sync::Semaphore>,
    gate: Arc<MutationGate>,
    signals: crate::lifecycle::DrainSignals,
    tasks: Arc<crate::lifecycle::ConnectionTasks>,
) {
    let DrainSignals { drain, force } = signals;
    let (mut sink, mut stream) = socket.split();
    let outbound = Scheduler::new(
        config.outbound_queue_capacity,
        (config.outbound_queue_capacity / 4).max(1),
    );
    let writer_outbound = outbound.clone();
    let child = CancellationToken::new();
    let writer_cancel = child.clone();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = writer_cancel.cancelled() => break,
                item = writer_outbound.recv() => match item {
                    Some(item) => {
                        if let Some(gap) = item.gap {
                            let frame = ServerFrame { kind: ServerKind::ResyncRequired, request_id: None, subscription_id: None, sequence: Some(gap.actual), payload: serde_json::json!({"reason": format!("revision gap: expected {}, received {}", gap.expected, gap.actual)}) };
                            let text = serde_json::to_string(&frame).expect("resync frame serializes");
                            let send = sink.send(Message::Text(text.into()));
                            if tokio::select! { () = writer_cancel.cancelled() => true, result = send => result.is_err() } { break; }
                        }
                        let send = sink.send(item.message);
                        if tokio::select! { () = writer_cancel.cancelled() => true, result = send => result.is_err() } { break; }
                    },
                    None => break,
                }
            }
        }
    });
    tasks.track(&writer);
    let first = tokio::select! {
        biased;
        () = force.cancelled() => return close_and_join(outbound, child, writer, "server shutdown", config.graceful_flush_timeout).await,
        () = drain.cancelled() => return close_and_join(outbound, child, writer, "server shutdown", config.graceful_flush_timeout).await,
        first = tokio::time::timeout(config.hello_timeout, stream.next()) => first,
    };
    let hello = match first {
        Ok(Some(Ok(Message::Text(text)))) => match decode_client_frame(&text) {
            Ok(frame) if frame.kind == ClientKind::Hello => match frame.decode_payload() {
                Ok(ClientPayload::Hello(hello)) => hello,
                _ => {
                    return close_and_join(
                        outbound,
                        child,
                        writer,
                        "invalid hello",
                        config.graceful_flush_timeout,
                    )
                    .await;
                }
            },
            _ => {
                return close_and_join(
                    outbound,
                    child,
                    writer,
                    "hello required",
                    config.graceful_flush_timeout,
                )
                .await;
            }
        },
        _ => {
            return close_and_join(
                outbound,
                child,
                writer,
                "hello timeout",
                config.graceful_flush_timeout,
            )
            .await;
        }
    };
    let negotiated = match authenticate(&config, &hello) {
        Ok(value) => value,
        Err(error) => {
            return terminal_auth_error_and_close(
                outbound,
                child,
                writer,
                error,
                config.graceful_flush_timeout,
            )
            .await;
        }
    };
    let authenticated = match authenticated_slots.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return close_and_join(
                outbound,
                child,
                writer,
                "authenticated connection limit",
                config.graceful_flush_timeout,
            )
            .await;
        }
    };
    drop(unauthenticated);
    let session_id = SessionId::new(uuid::Uuid::new_v4().to_string());
    tracing::info!(
        session_id = %session_id.as_str(),
        queue_pressure = outbound.len(),
        "control session authenticated"
    );
    let negotiated_protocol = negotiated.protocol;
    let negotiated_capabilities = negotiated.capabilities;
    let welcome = Welcome {
        protocol: negotiated_protocol,
        capabilities: negotiated_capabilities.clone(),
        session_id: session_id.clone(),
        server_instance_id: kernel.server_instance_id().to_owned(),
        resume_status: ResumeStatus::Fresh,
    };
    if send_frame(
        &outbound,
        ServerFrame {
            kind: ServerKind::Welcome,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(welcome).expect("welcome serializes"),
        },
        Priority::P0,
    )
    .is_err()
    {
        return close_and_join(
            outbound,
            child,
            writer,
            "outbound overload",
            config.graceful_flush_timeout,
        )
        .await;
    }
    let requests: Arc<std::sync::Mutex<HashMap<RequestId, CancellationToken>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let mut request_tasks = JoinSet::new();
    let last_sent_sequence: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let subscription_cancel = child.child_token();
    let mut subscription_cancels: HashMap<SubscriptionId, CancellationToken> = HashMap::new();
    let mut last_acked_sequence = 0_u64;
    let mut noticed = false;
    let mut drain_grace: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    loop {
        let next = tokio::select! {
            biased;
            () = force.cancelled() => break,
            () = drain.cancelled(), if !noticed => {
                noticed = true;
                tracing::info!(
                    session_id = %session_id.as_str(),
                    drain_window_ms = config.drain_grace_timeout.as_millis() as u64,
                    "control session entering drain window"
                );
                let notice = ServerFrame {
                    kind: ServerKind::ShutdownNotice,
                    request_id: None,
                    subscription_id: None,
                    sequence: None,
                    payload: serde_json::to_value(ShutdownNotice {
                        reason: "server shutdown".to_owned(),
                        retry_after: Some(config.drain_grace_timeout.as_secs().max(1)),
                    })
                    .expect("shutdown notice serializes"),
                };
                if send_frame(&outbound, notice, Priority::P0).is_err() { overload_close(&outbound); }
                drain_grace = Some(Box::pin(tokio::time::sleep(config.drain_grace_timeout)));
                continue;
            }
            () = async { drain_grace.as_mut().expect("grace window armed").await }, if drain_grace.is_some() => break,
            next = stream.next() => next,
            completed = request_tasks.join_next(), if !request_tasks.is_empty() => {
                let _ = completed;
                continue;
            }
        };
        let Some(Ok(message)) = next else { break };
        let Message::Text(text) = message else {
            if matches!(message, Message::Close(_)) {
                break;
            } else {
                continue;
            }
        };
        let frame = match decode_client_frame(&text) {
            Ok(frame) => frame,
            Err(error) => {
                if send_error(&outbound, ErrorTarget::Session, error.code, error.message).is_err() {
                    overload_close(&outbound);
                    break;
                }
                continue;
            }
        };
        match frame.decode_payload() {
            Ok(ClientPayload::Request(request)) => {
                let Some(request_id) = frame.request_id else {
                    continue;
                };
                if !gate.is_open() && request.request_kind != "bootstrap" {
                    let rejection = ErrorFrame::new(
                        ErrorCode::Cancelled,
                        "server is shutting down",
                        Retryability::AfterReconnect,
                        ErrorScope::Request,
                        request_id.as_str(),
                    );
                    tracing::info!(
                        session_id = %session_id.as_str(),
                        request_id = %request_id.as_str(),
                        correlation_id = %request_id.as_str(),
                        "mutation rejected during drain"
                    );
                    if send_error_frame(&outbound, Some(request_id.clone()), None, rejection)
                        .is_err()
                    {
                        overload_close(&outbound);
                        break;
                    }
                    continue;
                }
                let request_cancel = child.child_token();
                {
                    let mut active = requests
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if active.len() >= config.outbound_queue_capacity.max(1) {
                        drop(active);
                        overload_close(&outbound);
                        break;
                    }
                    if let Some(previous) =
                        active.insert(request_id.clone(), request_cancel.clone())
                    {
                        previous.cancel();
                    }
                }
                let task_kernel = kernel.clone();
                let task_outbound = outbound.clone();
                let task_requests = requests.clone();
                let task_protocol = negotiated_protocol;
                let task_capabilities = negotiated_capabilities.clone();
                let task_session_id = session_id.clone();
                let queue_capacity = config.outbound_queue_capacity;
                let request_span = tracing::info_span!(
                    "control_request",
                    session_id = %session_id.as_str(),
                    request_id = %request_id.as_str(),
                    correlation_id = %request_id.as_str(),
                    queue_capacity,
                );
                request_tasks.spawn(
                    async move {
                        let parsed = parse_resource_query(&request.request_kind, request.payload);
                        let result: Result<KernelQueryResult, RequestFailure> = match parsed {
                            Ok(Some(query)) => {
                                let deadline = request.deadline.map(Duration::from_millis);
                                tokio::select! {
                                    () = request_cancel.cancelled() =>
                                        Err(RequestFailure::Backend(BackendError::Cancelled)),
                                    result = task_kernel.query_with_deadline(query, deadline) =>
                                        result.map_err(RequestFailure::Backend),
                                }
                            }
                            Ok(None) => Err(RequestFailure::Backend(BackendError::unsupported(
                                request.request_kind,
                            ))),
                            Err(message) => Err(RequestFailure::Malformed(message)),
                        };
                        let sent = match result {
                            Ok(KernelQueryResult::Bootstrap(value)) => {
                                let mut payload = value.wire_payload();
                                payload.protocol = task_protocol;
                                payload.capabilities = task_capabilities;
                                send_frame(
                                    &task_outbound,
                                    ServerFrame::response(request_id.clone(), payload),
                                    Priority::P1,
                                )
                            }
                            Ok(KernelQueryResult::ResourceList(value)) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(KernelQueryResult::ResourceDetail(value)) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(KernelQueryResult::ResourceMetrics(value)) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Err(RequestFailure::Malformed(message)) => send_error(
                                &task_outbound,
                                ErrorTarget::Request(request_id.clone()),
                                ErrorCode::InvalidRequest,
                                message,
                            ),
                            Err(RequestFailure::Backend(error)) => send_backend_error(
                                &task_outbound,
                                ErrorTarget::Request(request_id.clone()),
                                &task_session_id,
                                error,
                            ),
                        };
                        if sent.is_err() {
                            task_outbound.overload_close(Message::Close(Some(CloseFrame {
                                code: 1013,
                                reason: "outbound overload".into(),
                            })));
                        }
                        task_requests
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&request_id);
                    }
                    .instrument(request_span),
                );
            }
            Ok(ClientPayload::CancelRequest(_)) => {
                if let Some(request_id) = frame.request_id
                    && let Some(token) = requests
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(&request_id)
                {
                    token.cancel();
                }
            }
            Ok(ClientPayload::Subscribe(selector)) => {
                let Some(subscription_id) = frame.subscription_id else {
                    continue;
                };
                let selector_kind = selector.0.get("kind").and_then(serde_json::Value::as_str);
                let outcome = if selector_kind.is_none() {
                    Err((
                        ErrorCode::InvalidRequest,
                        "invalid subscription payload".into(),
                    ))
                } else if selector_kind == Some("bootstrapStatus") {
                    match kernel.subscribe(BackendSubscribe::BootstrapStatus).await {
                        Ok(_) => Ok(None),
                        Err(error) => Err(backend_rejection(&error)),
                    }
                } else if selector_kind == Some("resource") {
                    match serde_json::from_value::<SubscriptionSelector>(selector.0.clone()) {
                        Ok(SubscriptionSelector::Resource(spec)) => {
                            match kernel
                                .subscribe(BackendSubscribe::ResourceWatch {
                                    context: spec.context,
                                    gvk: Gvk {
                                        group: spec.gvk.group,
                                        version: spec.gvk.version,
                                        kind: spec.gvk.kind,
                                    },
                                    namespace: spec.namespace,
                                })
                                .await
                            {
                                Ok(handle) => Ok(Some(handle)),
                                Err(error) => Err(backend_rejection(&error)),
                            }
                        }
                        _ => Err((
                            ErrorCode::InvalidRequest,
                            "invalid resource subscription payload".into(),
                        )),
                    }
                } else {
                    Err((
                        ErrorCode::UnsupportedMessage,
                        "unsupported subscription".into(),
                    ))
                };
                match outcome {
                    Ok(mut handle) => {
                        if let Some(previous) = subscription_cancels.remove(&subscription_id) {
                            previous.cancel();
                        }
                        let task_cancel =
                            handle.as_ref().map(|_| subscription_cancel.child_token());
                        if let Some(cancel) = &task_cancel {
                            subscription_cancels.insert(subscription_id.clone(), cancel.clone());
                        }
                        let Some(sequence) = allocate_sequence(&last_sent_sequence) else {
                            if send_error(
                                &outbound,
                                ErrorTarget::Session,
                                ErrorCode::Internal,
                                "connection sequence exhausted".into(),
                            )
                            .is_err()
                            {
                                overload_close(&outbound);
                            }
                            break;
                        };
                        let subscribed = ServerFrame {
                            kind: ServerKind::Subscribed,
                            request_id: None,
                            subscription_id: Some(subscription_id.clone()),
                            sequence: Some(sequence),
                            payload: serde_json::to_value(Subscribed)
                                .expect("subscribed payload serializes"),
                        };
                        if send_frame(&outbound, subscribed, Priority::P1).is_err() {
                            overload_close(&outbound);
                            break;
                        }
                        if let Some(mut handle) = handle.take() {
                            let task_outbound = outbound.clone();
                            let task_kernel = Arc::clone(&kernel);
                            let task_cancel = task_cancel.expect("resource watch has cancellation");
                            let task_counter = Arc::clone(&last_sent_sequence);
                            let forwarder_span = tracing::info_span!(
                                "control_subscription",
                                session_id = %session_id.as_str(),
                                subscription_id = %subscription_id.as_str(),
                                backend_subscription_id = %handle.id,
                            );
                            request_tasks.spawn(
                                async move {
                                    stream_backend_events(
                                        &task_outbound,
                                        &task_kernel,
                                        &subscription_id,
                                        handle.take_events(),
                                        &task_counter,
                                        &task_cancel,
                                    )
                                    .await;
                                }
                                .instrument(forwarder_span),
                            );
                        }
                    }
                    Err((code, message)) => {
                        if send_error(
                            &outbound,
                            ErrorTarget::Subscription(subscription_id),
                            code,
                            message,
                        )
                        .is_err()
                        {
                            overload_close(&outbound);
                            break;
                        }
                    }
                }
            }
            Ok(ClientPayload::Unsubscribe(_)) => {
                if let Some(subscription_id) = frame.subscription_id
                    && let Some(cancel) = subscription_cancels.remove(&subscription_id)
                {
                    cancel.cancel();
                }
            }
            Ok(ClientPayload::Ack(ack)) => {
                let cursor = ack.last_acked_sequence;
                let sent_so_far = last_sent_sequence.load(Ordering::Acquire);
                let valid = frame.sequence.is_none_or(|sequence| sequence == cursor)
                    && cursor >= last_acked_sequence
                    && cursor <= sent_so_far;
                if !valid {
                    if send_error(
                        &outbound,
                        ErrorTarget::Session,
                        ErrorCode::InvalidRequest,
                        "invalid acknowledgement cursor".into(),
                    )
                    .is_err()
                    {
                        overload_close(&outbound);
                        break;
                    }
                    continue;
                }
                last_acked_sequence = cursor;
                tracing::debug!(
                    session_id = %session_id.as_str(),
                    last_acked_sequence,
                    last_sent_sequence = sent_so_far,
                    "control acknowledgement advanced"
                );
            }
            Ok(ClientPayload::Ping(_)) => {
                let pong = ServerFrame {
                    kind: ServerKind::Pong,
                    request_id: None,
                    subscription_id: None,
                    sequence: None,
                    payload: serde_json::json!({}),
                };
                if send_frame(&outbound, pong, Priority::P1).is_err() {
                    overload_close(&outbound);
                    break;
                }
            }
            Ok(_) => {
                if send_error(
                    &outbound,
                    error_target(&frame),
                    ErrorCode::UnsupportedMessage,
                    "unsupported behavior".into(),
                )
                .is_err()
                {
                    overload_close(&outbound);
                    break;
                }
            }
            Err(error) => {
                if send_error(&outbound, error_target(&frame), error.code, error.message).is_err() {
                    overload_close(&outbound);
                    break;
                }
            }
        }
    }
    for token in requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .values()
    {
        token.cancel();
    }
    subscription_cancel.cancel();
    while request_tasks.join_next().await.is_some() {}
    drop(authenticated);
    finish_writer(outbound, child, writer, config.graceful_flush_timeout).await;
}

/// Outcome of mapping a request kind and payload onto a backend query.
///
/// `Ok(None)` marks the bootstrap request, which needs no payload parsing.
fn parse_resource_query(kind: &str, payload: serde_json::Value) -> Result<Option<Query>, String> {
    match kind {
        "bootstrap" => Ok(Some(Query::Bootstrap)),
        "resource.list" => serde_json::from_value::<ResourceListRequest>(payload)
            .map(|parsed| {
                Some(Query::ResourceList {
                    context: parsed.context,
                    gvk: Gvk {
                        group: parsed.gvk.group,
                        version: parsed.gvk.version,
                        kind: parsed.gvk.kind,
                    },
                    namespace: parsed.namespace,
                })
            })
            .map_err(|error| format!("invalid resource.list payload: {error}")),
        "resource.detail" => serde_json::from_value::<ResourceRefRequest>(payload)
            .map(|parsed| {
                Some(Query::ResourceDetail {
                    reference: backend_reference(parsed.identity),
                })
            })
            .map_err(|error| format!("invalid resource.detail payload: {error}")),
        "resource.metrics" => serde_json::from_value::<ResourceRefRequest>(payload)
            .map(|parsed| {
                Some(Query::ResourceMetrics {
                    reference: backend_reference(parsed.identity),
                })
            })
            .map_err(|error| format!("invalid resource.metrics payload: {error}")),
        _ => Ok(None),
    }
}

fn backend_reference(identity: ResourceIdentity) -> k10s_backend::ResourceRef {
    k10s_backend::ResourceRef {
        context: identity.context,
        gvk: Gvk {
            group: identity.gvk.group,
            version: identity.gvk.version,
            kind: identity.gvk.kind,
        },
        namespace: identity.namespace,
        name: identity.name,
        uid: identity.uid,
    }
}

/// Allocate the next monotonic connection sequence.
fn allocate_sequence(counter: &AtomicU64) -> Option<u64> {
    let mut observed = counter.load(Ordering::Acquire);
    loop {
        let next = observed.checked_add(1)?;
        match counter.compare_exchange(observed, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Some(next),
            Err(current) => observed = current,
        }
    }
}

/// Map a backend failure onto a safe subscription-scoped error.
fn backend_rejection(error: &BackendError) -> (ErrorCode, String) {
    match error {
        BackendError::Unsupported { capability } => (
            ErrorCode::UnsupportedMessage,
            format!("unsupported capability: {capability}"),
        ),
        BackendError::NotFound => (
            ErrorCode::NotFound,
            "context or resource not found".to_owned(),
        ),
        BackendError::Timeout => (ErrorCode::Timeout, "request timed out".to_owned()),
        BackendError::Cancelled => (ErrorCode::Cancelled, "request was cancelled".to_owned()),
        BackendError::Internal(_) => (ErrorCode::Internal, "internal server error".to_owned()),
    }
}

/// Forward one resource watch from the backend to the client.
///
/// Snapshots stream as lossless bounded `snapshot*` frames; deltas ride the
/// bounded P2 scheduler coalesced by resource identity. A lagging backend
/// consumer demands a resync rather than silently dropping deltas.
async fn stream_backend_events(
    outbound: &Scheduler,
    kernel: &BackendKernel,
    subscription_id: &SubscriptionId,
    events: Option<tokio::sync::broadcast::Receiver<BackendEvent>>,
    sequence_counter: &AtomicU64,
    cancel: &CancellationToken,
) {
    let mut events = match events {
        Some(events) => events,
        None => return,
    };
    loop {
        let event = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            event = events.recv() => event,
        };
        match event {
            Ok(BackendEvent::Snapshot(data)) => {
                if stream_snapshot(outbound, kernel, subscription_id, data, sequence_counter)
                    .await
                    .is_err()
                {
                    overload_close(outbound);
                    break;
                }
            }
            Ok(BackendEvent::Changed(record)) => {
                let revision = record.revision;
                let delta = kernel.changed_delta(&record);
                let resource = record.reference.coalescing_key();
                match enqueue_delta(
                    outbound,
                    subscription_id,
                    &resource,
                    k10s_protocol::RESOURCE_EVENT_CHANGED,
                    revision,
                    &delta,
                    sequence_counter,
                ) {
                    Ok(()) => {}
                    Err(DeltaAdmission::Dropped) => {
                        demand_resync(outbound);
                        break;
                    }
                    Err(DeltaAdmission::Overloaded) => {
                        overload_close(outbound);
                        break;
                    }
                }
            }
            Ok(BackendEvent::Gone {
                reference,
                revision,
            }) => {
                let delta = kernel.gone_delta(&reference, revision);
                let resource = reference.coalescing_key();
                match enqueue_delta(
                    outbound,
                    subscription_id,
                    &resource,
                    k10s_protocol::RESOURCE_EVENT_GONE,
                    revision,
                    &delta,
                    sequence_counter,
                ) {
                    Ok(()) => {}
                    Err(DeltaAdmission::Dropped) => {
                        demand_resync(outbound);
                        break;
                    }
                    Err(DeltaAdmission::Overloaded) => {
                        overload_close(outbound);
                        break;
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                tracing::warn!(
                    subscription_id = %subscription_id.as_str(),
                    dropped,
                    "subscription consumer lagged; demanding resync"
                );
                demand_resync(outbound);
                break;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Stream a snapshot as bounded `snapshotBegin`/`snapshotChunk`/
/// `snapshotEnd` frames with contiguous connection sequences and a
/// deterministic checksum over the chunk pages.
async fn stream_snapshot(
    outbound: &Scheduler,
    kernel: &BackendKernel,
    subscription_id: &SubscriptionId,
    data: k10s_backend::ResourceListData,
    sequence_counter: &AtomicU64,
) -> Result<(), EnqueueError> {
    let total_chunks = data.rows.len().div_ceil(RESOURCE_ROWS_PER_CHUNK).max(1);
    let begin_payload = SnapshotBegin {
        total_chunks: total_chunks as u32,
    };
    send_sequenced(
        outbound,
        subscription_id,
        ServerKind::SnapshotBegin,
        serde_json::to_value(begin_payload).expect("begin payload serializes"),
        sequence_counter,
    )
    .await?;

    // An empty selection still streams exactly one empty page so the client
    // can complete reassembly.
    let pages: Vec<&[k10s_backend::ResourceRecord]> = if data.rows.is_empty() {
        vec![&data.rows[..]]
    } else {
        data.rows.chunks(RESOURCE_ROWS_PER_CHUNK).collect()
    };
    let mut checksum: u64 = FNV_OFFSET_BASIS;
    for (index, chunk) in pages.into_iter().enumerate() {
        let page = kernel.snapshot_page(data.revision, chunk);
        let page_bytes = serde_json::to_vec(&page).expect("snapshot page serializes");
        for byte in &page_bytes {
            checksum = (checksum ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
        }
        let payload = SnapshotChunk {
            chunk_index: index as u32,
            data: serde_json::to_value(page).expect("snapshot page serializes"),
        };
        send_sequenced(
            outbound,
            subscription_id,
            ServerKind::SnapshotChunk,
            serde_json::to_value(payload).expect("chunk payload serializes"),
            sequence_counter,
        )
        .await?;
    }

    let end_payload = SnapshotEnd {
        checksum: format!("fnv-64:{checksum:016x}"),
    };
    send_sequenced(
        outbound,
        subscription_id,
        ServerKind::SnapshotEnd,
        serde_json::to_value(end_payload).expect("end payload serializes"),
        sequence_counter,
    )
    .await
}

/// Why a delta could not be admitted to the bounded scheduler.
enum DeltaAdmission {
    /// The P2 partition rejected the delta; the watch must resynchronize.
    Dropped,
    /// The session is out of queue capacity and must close.
    Overloaded,
}

/// Enqueue one resource delta on the bounded P2 scheduler, coalesced by
/// resource identity. A dropped delta leaves the client's revision stream
/// permanently behind, so the caller must demand a resync instead of
/// continuing silently.
fn enqueue_delta(
    outbound: &Scheduler,
    subscription_id: &SubscriptionId,
    resource: &str,
    event_kind: &'static str,
    revision: u64,
    payload: &impl serde::Serialize,
    sequence_counter: &AtomicU64,
) -> Result<(), DeltaAdmission> {
    let Some(sequence) = allocate_sequence(sequence_counter) else {
        return Err(DeltaAdmission::Overloaded);
    };
    let frame = ServerFrame {
        kind: ServerKind::Event,
        request_id: None,
        subscription_id: Some(subscription_id.clone()),
        sequence: Some(sequence),
        payload: serde_json::json!({
            "kind": event_kind,
            "revision": revision.to_string(),
            "payload": payload,
        }),
    };
    let text = serde_json::to_string(&frame).expect("server frame serializes");
    match outbound.enqueue_p2(resource, revision, Message::Text(text.into())) {
        Ok(()) => Ok(()),
        Err(EnqueueError::Coalesced) => Err(DeltaAdmission::Dropped),
        Err(EnqueueError::Overloaded) => Err(DeltaAdmission::Overloaded),
    }
}

/// Tell the client its revision stream can no longer be trusted. If the
/// connection cannot even carry the notice, close it as overloaded.
fn demand_resync(outbound: &Scheduler) {
    let frame = ServerFrame {
        kind: ServerKind::ResyncRequired,
        request_id: None,
        subscription_id: None,
        sequence: None,
        payload: serde_json::json!({"reason": "resource deltas were dropped"}),
    };
    if send_frame(outbound, frame, Priority::P0).is_err() {
        overload_close(outbound);
    }
}

/// Enqueue one sequenced snapshot frame at lossless priority.
fn send_sequenced<'a>(
    outbound: &'a Scheduler,
    subscription_id: &'a SubscriptionId,
    kind: ServerKind,
    payload: serde_json::Value,
    sequence_counter: &'a AtomicU64,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), EnqueueError>> + Send + 'a>> {
    Box::pin(async move {
        let Some(sequence) = allocate_sequence(sequence_counter) else {
            return Err(EnqueueError::Overloaded);
        };
        let frame = ServerFrame {
            kind,
            request_id: None,
            subscription_id: Some(subscription_id.clone()),
            sequence: Some(sequence),
            payload,
        };
        let text = serde_json::to_string(&frame).expect("server frame serializes");
        outbound.enqueue(Priority::P1, Message::Text(text.into()))
    })
}

/// FNV-1a 64-bit constants for the deterministic snapshot checksum.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

async fn terminal_auth_error_and_close(
    outbound: Scheduler,
    child: CancellationToken,
    writer: tokio::task::JoinHandle<()>,
    error: AuthenticationError,
    flush_timeout: Duration,
) {
    let mut terminal = ErrorFrame::new(
        error.code(),
        error.safe_reason(),
        Retryability::Never,
        ErrorScope::Session,
        "authentication",
    );
    if let AuthenticationError::IncompatibleProtocol { client_major } = error {
        terminal = terminal.with_details(serde_json::json!({
            "clientProtocolMajor": client_major,
            "serverProtocolMajor": k10s_protocol::PROTOCOL_MAJOR,
        }));
    }
    let _ = send_frame(
        &outbound,
        ServerFrame {
            kind: ServerKind::Error,
            request_id: None,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(terminal).expect("terminal error serializes"),
        },
        Priority::P0,
    );

    let wait_for_writer = async {
        while !outbound.is_empty() {
            tokio::task::yield_now().await;
        }
    };
    let _ = tokio::time::timeout(flush_timeout, wait_for_writer).await;
    close_and_join(outbound, child, writer, error.safe_reason(), flush_timeout).await;
}

fn overload_close(outbound: &Scheduler) {
    tracing::warn!(
        queue_pressure = outbound.len(),
        "closing overloaded control session"
    );
    outbound.overload_close(Message::Close(Some(CloseFrame {
        code: 1013,
        reason: "outbound overload".into(),
    })));
}

fn send_frame(
    outbound: &Scheduler,
    frame: ServerFrame,
    priority: Priority,
) -> Result<(), EnqueueError> {
    let text = serde_json::to_string(&frame).expect("server frame serializes");
    outbound.enqueue(priority, Message::Text(text.into()))
}

fn send_error_frame(
    outbound: &Scheduler,
    request_id: Option<RequestId>,
    subscription_id: Option<SubscriptionId>,
    error: ErrorFrame,
) -> Result<(), EnqueueError> {
    send_frame(
        outbound,
        ServerFrame {
            kind: ServerKind::Error,
            request_id,
            subscription_id,
            sequence: None,
            payload: serde_json::to_value(error).expect("error serializes"),
        },
        Priority::P1,
    )
}

#[derive(Debug, Clone)]
enum ErrorTarget {
    Session,
    Request(RequestId),
    Subscription(SubscriptionId),
}

impl ErrorTarget {
    fn correlation(&self) -> &str {
        match self {
            Self::Session => "session",
            Self::Request(id) => id.as_str(),
            Self::Subscription(id) => id.as_str(),
        }
    }
}

fn error_target(frame: &k10s_protocol::ClientFrame) -> ErrorTarget {
    frame.request_id.clone().map_or_else(
        || {
            frame
                .subscription_id
                .clone()
                .map_or(ErrorTarget::Session, ErrorTarget::Subscription)
        },
        ErrorTarget::Request,
    )
}

fn send_error(
    outbound: &Scheduler,
    target: ErrorTarget,
    code: ErrorCode,
    message: String,
) -> Result<(), EnqueueError> {
    let correlation = target.correlation().to_owned();
    let (request_id, subscription_id, scope) = match target {
        ErrorTarget::Session => (None, None, ErrorScope::Session),
        ErrorTarget::Request(id) => (Some(id), None, ErrorScope::Request),
        ErrorTarget::Subscription(id) => (None, Some(id), ErrorScope::Subscription),
    };
    let error = ErrorFrame::new(code, message, Retryability::Never, scope, correlation);
    send_frame(
        outbound,
        ServerFrame {
            kind: ServerKind::Error,
            request_id,
            subscription_id,
            sequence: None,
            payload: serde_json::to_value(error).expect("error serializes"),
        },
        Priority::P1,
    )
}

fn send_backend_error(
    outbound: &Scheduler,
    target: ErrorTarget,
    session_id: &SessionId,
    error: BackendError,
) -> Result<(), EnqueueError> {
    let (code, safe_message) = match error {
        BackendError::NotFound => (
            ErrorCode::NotFound,
            "context or resource not found".to_owned(),
        ),
        BackendError::Timeout => (ErrorCode::Timeout, "request timed out".to_owned()),
        BackendError::Cancelled => (ErrorCode::Cancelled, "request was cancelled".to_owned()),
        BackendError::Unsupported { capability } => (
            ErrorCode::UnsupportedMessage,
            format!("unsupported capability: {capability}"),
        ),
        BackendError::Internal(_) => {
            let correlation = target.correlation();
            let request = match &target {
                ErrorTarget::Request(id) => id.as_str(),
                _ => "-",
            };
            let subscription = match &target {
                ErrorTarget::Subscription(id) => id.as_str(),
                _ => "-",
            };
            tracing::error!(
                session_id = %session_id.as_str(),
                request_id = %request,
                subscription_id = %subscription,
                correlation_id = %correlation,
                diagnostic = "backend adapter returned an internal error",
                "control backend failure"
            );
            (ErrorCode::Internal, "internal server error".to_owned())
        }
    };
    send_error(outbound, target, code, safe_message)
}

async fn close_and_join(
    outbound: Scheduler,
    child: CancellationToken,
    writer: tokio::task::JoinHandle<()>,
    reason: &'static str,
    flush_timeout: Duration,
) {
    let _ = outbound.enqueue(
        Priority::P0,
        Message::Close(Some(CloseFrame {
            code: 1008,
            reason: reason.into(),
        })),
    );
    finish_writer(outbound, child, writer, flush_timeout).await;
}

async fn finish_writer(
    outbound: Scheduler,
    child: CancellationToken,
    mut writer: tokio::task::JoinHandle<()>,
    flush_timeout: Duration,
) {
    outbound.close();
    if tokio::time::timeout(flush_timeout, &mut writer)
        .await
        .is_ok()
    {
        child.cancel();
        return;
    }
    child.cancel();
    if tokio::time::timeout(flush_timeout, &mut writer)
        .await
        .is_err()
    {
        writer.abort();
        let _ = writer.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn changed_record(name: &str) -> k10s_backend::ResourceRecord {
        k10s_backend::ResourceRecord {
            reference: k10s_backend::ResourceRef {
                context: "dev-local".into(),
                gvk: Gvk {
                    group: String::new(),
                    version: "v1".into(),
                    kind: "Pod".into(),
                },
                namespace: Some("default".into()),
                name: name.into(),
                uid: format!("uid-{name}"),
            },
            revision: 2000,
            labels: BTreeMap::new(),
            summary: "Running".into(),
            created_at: "2026-08-21T00:00:00Z".into(),
            owner_references: Vec::new(),
        }
    }

    async fn drain_frames(scheduler: &Scheduler) -> Vec<ServerFrame> {
        scheduler.close();
        let mut frames = Vec::new();
        while let Some(item) = scheduler.recv().await {
            if let Message::Text(text) = item.message {
                frames.push(serde_json::from_str(&text).unwrap());
            }
        }
        frames
    }

    #[tokio::test]
    async fn p2_admission_failure_demands_an_out_of_band_resync() {
        let scheduler = Scheduler::new(8, 2);
        let kernel = BackendKernel::new(k10s_backend::FakeKubernetes::standard());
        let subscription_id = SubscriptionId::new("sub-test");
        let counter = AtomicU64::new(0);
        let cancel = CancellationToken::new();
        let (sender, receiver) = tokio::sync::broadcast::channel(32);

        // Ten distinct resource keys overflow the six-slot P2 partition;
        // nothing drains because no writer runs.
        for index in 0..10_u32 {
            sender
                .send(BackendEvent::Changed(changed_record(&format!(
                    "pod-{index:02}"
                ))))
                .expect("broadcast has capacity");
        }
        drop(sender);

        stream_backend_events(
            &scheduler,
            &kernel,
            &subscription_id,
            Some(receiver),
            &counter,
            &cancel,
        )
        .await;

        let frames = drain_frames(&scheduler).await;
        assert!(
            matches!(frames.first(), Some(frame) if frame.kind == ServerKind::ResyncRequired),
            "an admission drop must notify the client: {frames:?}"
        );
        let notice = frames.first().expect("notice present");
        assert_eq!(
            notice.sequence, None,
            "the notice is out-of-band and must not preempt sequenced traffic with a sequence"
        );
        let deltas: Vec<u64> = frames
            .iter()
            .filter(|frame| frame.kind == ServerKind::Event)
            .filter_map(|frame| frame.sequence)
            .collect();
        assert_eq!(
            deltas,
            vec![1, 2, 3, 4, 5, 6],
            "admitted deltas keep their allocated sequences"
        );
    }

    #[tokio::test]
    async fn admission_drop_recovery_converges_a_real_client_state() {
        let scheduler = Scheduler::new(8, 2);
        let kernel = BackendKernel::new(k10s_backend::FakeKubernetes::standard());
        let subscription_id = SubscriptionId::new("resource-1");
        let counter = AtomicU64::new(0);
        let cancel = CancellationToken::new();
        let (sender, receiver) = tokio::sync::broadcast::channel(32);
        for index in 0..10_u32 {
            sender
                .send(BackendEvent::Changed(changed_record(&format!(
                    "pod-{index:02}"
                ))))
                .expect("broadcast has capacity");
        }
        drop(sender);
        stream_backend_events(
            &scheduler,
            &kernel,
            &subscription_id,
            Some(receiver),
            &counter,
            &cancel,
        )
        .await;
        let mut wire = drain_frames(&scheduler).await;

        // Drive the produced frames plus simulated recovery traffic through
        // the real client state machine: it must converge without ever
        // reporting a sequence gap.
        use k10s_ui::client::{ClientConfig, ClientPhase, ClientState, ConnectTarget};
        let mut client = ClientState::new(ClientConfig::default());
        client
            .connect(ConnectTarget::new(
                "ws://localhost/api/v1/control",
                "secret",
            ))
            .unwrap();
        let _hello = client.take_outbound();
        let welcome_payload = Welcome {
            protocol: k10s_protocol::ProtocolVersion {
                major: k10s_protocol::PROTOCOL_MAJOR,
                minor: k10s_protocol::PROTOCOL_MINOR,
            },
            capabilities: vec![],
            session_id: SessionId::new("session-1"),
            server_instance_id: "instance-1".into(),
            resume_status: ResumeStatus::Fresh,
        };
        client
            .apply(ServerFrame {
                kind: ServerKind::Welcome,
                request_id: None,
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(welcome_payload).unwrap(),
            })
            .unwrap();
        assert_eq!(client.phase(), ClientPhase::Ready);
        let handle = client
            .subscribe_resource("dev-local", "", "v1", "Pod", None)
            .unwrap();
        assert_eq!(handle.id().as_str(), "resource-1");
        let _queued_subscribe = client.take_outbound();

        // Simulated recovery tail: the rebuilt subscription lands on a jumped
        // sequence followed by a complete fresh snapshot.
        let mut recovery = vec![
            ServerFrame {
                kind: ServerKind::Subscribed,
                request_id: None,
                subscription_id: Some(handle.id().clone()),
                sequence: Some(8),
                payload: serde_json::to_value(Subscribed).unwrap(),
            },
            ServerFrame {
                kind: ServerKind::SnapshotBegin,
                request_id: None,
                subscription_id: Some(handle.id().clone()),
                sequence: Some(9),
                payload: serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
            },
        ];
        let page = kernel.snapshot_page(
            4_100,
            &[k10s_backend::ResourceRecord {
                reference: k10s_backend::ResourceRef {
                    context: "dev-local".into(),
                    gvk: Gvk::core("v1", "Pod"),
                    namespace: Some("default".into()),
                    name: "pod-a".into(),
                    uid: "uid-pod-a".into(),
                },
                revision: 4_100,
                labels: BTreeMap::new(),
                summary: "Running".into(),
                created_at: "2026-08-21T00:00:00Z".into(),
                owner_references: Vec::new(),
            }],
        );
        recovery.push(ServerFrame {
            kind: ServerKind::SnapshotChunk,
            request_id: None,
            subscription_id: Some(handle.id().clone()),
            sequence: Some(10),
            payload: serde_json::to_value(SnapshotChunk {
                chunk_index: 0,
                data: serde_json::to_value(page).unwrap(),
            })
            .unwrap(),
        });
        recovery.push(ServerFrame {
            kind: ServerKind::SnapshotEnd,
            request_id: None,
            subscription_id: Some(handle.id().clone()),
            sequence: Some(11),
            payload: serde_json::to_value(SnapshotEnd {
                checksum: "fnv-64:0000000000000000".into(),
            })
            .unwrap(),
        });
        wire.extend(recovery);

        for frame in wire {
            client
                .apply(frame)
                .unwrap_or_else(|error| panic!("recovery frame must converge cleanly: {error:?}"));
        }
        let snapshot = client
            .take_resource_snapshot(handle.id())
            .expect("snapshot reassembled after resync");
        assert_eq!(snapshot.rows.len(), 1);
        assert_eq!(client.last_acked_sequence(), Some(11));
    }
}
