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
    Retryability, ServerFrame, ServerKind, SessionId, Welcome, decode_client_frame,
};
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::auth::authenticate;
use crate::config::ServerConfig;
use crate::outbound::{EnqueueError, Priority, Scheduler};

pub(crate) async fn serve_socket(
    socket: WebSocket,
    config: Arc<ServerConfig>,
    kernel: Arc<BackendKernel>,
    unauthenticated: OwnedSemaphorePermit,
    authenticated_slots: Arc<tokio::sync::Semaphore>,
    shutdown: CancellationToken,
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
        () = shutdown.cancelled() => return close_and_join(outbound, child, writer, "server shutdown", config.graceful_flush_timeout).await,
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
        Err(reason) => {
            return close_and_join(
                outbound,
                child,
                writer,
                reason,
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
    loop {
        let next = tokio::select! {
            () = shutdown.cancelled() => {
                let notice = ServerFrame { kind: ServerKind::ShutdownNotice, request_id: None, subscription_id: None, sequence: None, payload: serde_json::json!({"reason":"server shutdown"}) };
                if send_frame(&outbound, notice, Priority::P0).is_err() { overload_close(&outbound); }
                break;
            }
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
                if send_error(&outbound, None, error.code, error.message).is_err() {
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
                                Some(request_id.clone()),
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
                if selector.0.get("kind").and_then(serde_json::Value::as_str)
                    == Some("bootstrapStatus")
                {
                    match kernel.subscribe(BackendSubscribe::BootstrapStatus).await {
                        Ok(_) => {
                            let subscribed = ServerFrame {
                                kind: ServerKind::Subscribed,
                                request_id: None,
                                subscription_id: Some(subscription_id),
                                sequence: Some(1),
                                payload: serde_json::json!({}),
                            };
                            if send_frame(&outbound, subscribed, Priority::P1).is_err() {
                                overload_close(&outbound);
                                break;
                            }
                        }
                        Err(error) => {
                            if send_backend_error(&outbound, None, &session_id, error).is_err() {
                                overload_close(&outbound);
                                break;
                            }
                        }
                    }
                } else {
                    if send_error(
                        &outbound,
                        None,
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
                    frame.request_id,
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
                if send_error(&outbound, frame.request_id, error.code, error.message).is_err() {
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

fn send_error(
    outbound: &Scheduler,
    request_id: Option<RequestId>,
    code: ErrorCode,
    message: String,
) -> Result<(), EnqueueError> {
    let correlation = request_id.as_ref().map_or("session", RequestId::as_str);
    let error = ErrorFrame::new(
        code,
        message,
        Retryability::Never,
        if request_id.is_some() {
            ErrorScope::Request
        } else {
            ErrorScope::Session
        },
        correlation,
    );
    send_frame(
        outbound,
        ServerFrame {
            kind: ServerKind::Error,
            request_id,
            subscription_id: None,
            sequence: None,
            payload: serde_json::to_value(error).expect("error serializes"),
        },
        Priority::P1,
    )
}

fn send_backend_error(
    outbound: &Scheduler,
    request_id: Option<RequestId>,
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
            let request = request_id.as_ref().map_or("-", RequestId::as_str);
            tracing::error!(
                session_id = %session_id.as_str(),
                request_id = %request,
                correlation_id = %request,
                diagnostic = "backend adapter returned an internal error",
                "control backend failure"
            );
            (ErrorCode::Internal, "internal server error".to_owned())
        }
    };
    send_error(outbound, request_id, code, safe_message)
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
