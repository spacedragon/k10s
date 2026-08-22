use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use k10s_backend::{
    BackendError, BackendKernel, KernelQueryResult, Query, Subscribe as BackendSubscribe,
};
use k10s_protocol::{
    ClientKind, ClientPayload, ErrorCode, ErrorFrame, ErrorScope, RequestId, ResumeStatus,
    Retryability, ServerFrame, ServerKind, SessionId, ShutdownNotice, Subscribed, SubscriptionId,
    Welcome, decode_client_frame,
};
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::auth::{AuthenticationError, authenticate};
use crate::config::ServerConfig;
use crate::lifecycle::MutationGate;
use crate::outbound::{EnqueueError, Priority, Scheduler};

pub(crate) async fn serve_socket(
    socket: WebSocket,
    config: Arc<ServerConfig>,
    kernel: Arc<BackendKernel>,
    unauthenticated: OwnedSemaphorePermit,
    authenticated_slots: Arc<tokio::sync::Semaphore>,
    gate: Arc<MutationGate>,
    drain: CancellationToken,
) {
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
    let first = tokio::select! {
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
    let mut last_sent_sequence = 0_u64;
    let mut last_acked_sequence = 0_u64;
    let mut noticed = false;
    let mut drain_grace: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    loop {
        let next = tokio::select! {
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
                        let query = async {
                            if request.request_kind == "bootstrap" {
                                task_kernel
                                    .query_with_deadline(
                                        Query::Bootstrap,
                                        request.deadline.map(Duration::from_millis),
                                    )
                                    .await
                            } else {
                                Err(BackendError::unsupported(request.request_kind))
                            }
                        };
                        let result = tokio::select! {
                            () = request_cancel.cancelled() => Err(BackendError::Cancelled),
                            result = query => result,
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
                            Err(error) => send_backend_error(
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
                if selector_kind.is_none() {
                    if send_error(
                        &outbound,
                        ErrorTarget::Subscription(subscription_id),
                        ErrorCode::InvalidRequest,
                        "invalid subscription payload".into(),
                    )
                    .is_err()
                    {
                        overload_close(&outbound);
                        break;
                    }
                } else if selector_kind == Some("bootstrapStatus") {
                    match kernel.subscribe(BackendSubscribe::BootstrapStatus).await {
                        Ok(_) => {
                            let Some(sequence) = last_sent_sequence.checked_add(1) else {
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
                                subscription_id: Some(subscription_id),
                                sequence: Some(sequence),
                                payload: serde_json::to_value(Subscribed)
                                    .expect("subscribed payload serializes"),
                            };
                            if send_frame(&outbound, subscribed, Priority::P1).is_err() {
                                overload_close(&outbound);
                                break;
                            }
                            last_sent_sequence = sequence;
                        }
                        Err(error) => {
                            if send_backend_error(
                                &outbound,
                                ErrorTarget::Subscription(subscription_id),
                                &session_id,
                                error,
                            )
                            .is_err()
                            {
                                overload_close(&outbound);
                                break;
                            }
                        }
                    }
                } else {
                    if send_error(
                        &outbound,
                        ErrorTarget::Subscription(subscription_id),
                        ErrorCode::UnsupportedMessage,
                        "unsupported subscription".into(),
                    )
                    .is_err()
                    {
                        overload_close(&outbound);
                        break;
                    }
                }
            }
            Ok(ClientPayload::Ack(ack)) => {
                let cursor = ack.last_acked_sequence;
                let valid = frame.sequence.is_none_or(|sequence| sequence == cursor)
                    && cursor >= last_acked_sequence
                    && cursor <= last_sent_sequence;
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
                    last_sent_sequence,
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
    while request_tasks.join_next().await.is_some() {}
    drop(authenticated);
    finish_writer(outbound, child, writer, config.graceful_flush_timeout).await;
}

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
