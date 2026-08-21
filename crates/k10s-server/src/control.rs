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
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::auth::authenticate;
use crate::config::ServerConfig;
use crate::outbound::{Outbound, Priority};

pub(crate) async fn serve_socket(
    socket: WebSocket,
    config: Arc<ServerConfig>,
    kernel: Arc<BackendKernel>,
    unauthenticated: OwnedSemaphorePermit,
    authenticated_slots: Arc<tokio::sync::Semaphore>,
) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel(config.outbound_queue_capacity.max(1));
    let child = CancellationToken::new();
    let writer_cancel = child.clone();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = writer_cancel.cancelled() => break,
                message = rx.recv() => match message {
                    Some(message) => if sink.send(message).await.is_err() { break; },
                    None => break,
                }
            }
        }
    });
    let outbound = Outbound::new(tx);
    let first = tokio::time::timeout(config.hello_timeout, stream.next()).await;
    let hello = match first {
        Ok(Some(Ok(Message::Text(text)))) => match decode_client_frame(&text) {
            Ok(frame) if frame.kind == ClientKind::Hello => match frame.decode_payload() {
                Ok(ClientPayload::Hello(hello)) => hello,
                _ => return close_and_join(outbound, child, writer, "invalid hello").await,
            },
            _ => return close_and_join(outbound, child, writer, "hello required").await,
        },
        _ => return close_and_join(outbound, child, writer, "hello timeout").await,
    };
    let negotiated = match authenticate(&config, &hello) {
        Ok(value) => value,
        Err(reason) => return close_and_join(outbound, child, writer, reason).await,
    };
    let authenticated = match authenticated_slots.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return close_and_join(outbound, child, writer, "authenticated connection limit").await;
        }
    };
    drop(unauthenticated);
    let session_id = SessionId::new(format!("session-{}", kernel.server_instance_id()));
    let welcome = Welcome {
        protocol: negotiated.protocol,
        capabilities: negotiated.capabilities,
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
        return close_and_join(outbound, child, writer, "outbound overload").await;
    }
    let requests: Arc<std::sync::Mutex<HashMap<RequestId, CancellationToken>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    while let Some(Ok(message)) = stream.next().await {
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
                let _ = send_error(&outbound, None, error.code, error.message);
                continue;
            }
        };
        match frame.decode_payload() {
            Ok(ClientPayload::Request(request)) => {
                let Some(request_id) = frame.request_id else {
                    continue;
                };
                let request_cancel = child.child_token();
                requests
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(request_id.clone(), request_cancel.clone());
                let task_kernel = kernel.clone();
                let task_outbound = outbound.clone();
                let task_requests = requests.clone();
                let queue_capacity = config.outbound_queue_capacity;
                let request_span = tracing::info_span!(
                    "control_request",
                    session_id = %session_id.as_str(),
                    request_id = %request_id.as_str(),
                    correlation_id = %request_id.as_str(),
                    queue_capacity,
                );
                tokio::spawn(
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
                            Ok(KernelQueryResult::Bootstrap(value)) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Err(error) => {
                                send_backend_error(&task_outbound, request_id.clone(), error)
                            }
                        };
                        if sent.is_err() {
                            let _ = task_outbound.send(
                                Message::Close(Some(CloseFrame {
                                    code: 1013,
                                    reason: "outbound overload".into(),
                                })),
                                Priority::P0,
                            );
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
                                break;
                            }
                        }
                        Err(error) => {
                            let _ =
                                send_error(&outbound, None, ErrorCode::Internal, error.to_string());
                        }
                    }
                } else {
                    let _ = send_error(
                        &outbound,
                        None,
                        ErrorCode::UnsupportedMessage,
                        "unsupported subscription".into(),
                    );
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
                    break;
                }
            }
            Ok(_) => {
                let _ = send_error(
                    &outbound,
                    frame.request_id,
                    ErrorCode::UnsupportedMessage,
                    "unsupported behavior".into(),
                );
            }
            Err(error) => {
                let _ = send_error(&outbound, frame.request_id, error.code, error.message);
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
    drop(authenticated);
    child.cancel();
    drop(outbound);
    let _ = writer.await;
}

fn send_frame(
    outbound: &Outbound,
    frame: ServerFrame,
    priority: Priority,
) -> Result<(), &'static str> {
    let text = serde_json::to_string(&frame).expect("server frame serializes");
    outbound.send(Message::Text(text.into()), priority)
}

fn send_error(
    outbound: &Outbound,
    request_id: Option<RequestId>,
    code: ErrorCode,
    message: String,
) -> Result<(), &'static str> {
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
    outbound: &Outbound,
    request_id: RequestId,
    error: BackendError,
) -> Result<(), &'static str> {
    let code = match error {
        BackendError::Timeout => ErrorCode::Timeout,
        BackendError::Cancelled => ErrorCode::Cancelled,
        BackendError::Unsupported { .. } => ErrorCode::UnsupportedMessage,
        BackendError::Internal(_) => ErrorCode::Internal,
    };
    send_error(outbound, Some(request_id), code, error.to_string())
}

async fn close_and_join(
    outbound: Outbound,
    child: CancellationToken,
    writer: tokio::task::JoinHandle<()>,
    reason: &'static str,
) {
    let _ = outbound.send(
        Message::Close(Some(CloseFrame {
            code: 1008,
            reason: reason.into(),
        })),
        Priority::P0,
    );
    drop(outbound);
    let _ = writer.await;
    child.cancel();
}
