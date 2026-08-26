//! Shared implementation of the dedicated logs/exec stream sockets.
//!
//! Every upgrade is guarded by the Plan 1 unauthenticated-connection
//! semaphore and admission barrier. The mandatory first frame is an
//! authenticated `hello`; only afterwards is the single-use stream ticket
//! redeemed through [`k10s_backend::BackendKernel::subscribe`] into the
//! kernel-owned Stream Hub. Separate frame and assembled-message limits
//! apply across fragmentation before authentication or payload dispatch,
//! per-stream queues stay bounded, and exceeding the inbound rate budget or
//! lagging behind closes the socket with an explicit overload error.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use k10s_backend::{
    BackendError, BackendEvent, BackendKernel, StreamInput, Subscribe as BackendSubscribe,
};
use k10s_protocol::{
    ErrorCode, StreamServerMessage, StreamType, decode_resize_payload, decode_stream_payload,
    encode_stream_payload, payload_kind,
};
use tokio::sync::OwnedSemaphorePermit;

use crate::config::ServerConfig;

/// Which dedicated stream route a socket serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamRoute {
    /// `/api/v1/logs`.
    Logs,
    /// `/api/v1/exec`.
    Exec,
}

impl StreamRoute {
    fn stream_type(self) -> StreamType {
        match self {
            Self::Logs => StreamType::Logs,
            Self::Exec => StreamType::Exec,
        }
    }

    fn backend_route(self) -> k10s_backend::StreamRouteKind {
        match self {
            Self::Logs => k10s_backend::StreamRouteKind::Logs,
            Self::Exec => k10s_backend::StreamRouteKind::Exec,
        }
    }
}

/// Build one JSON error status frame.
fn error_message(code: ErrorCode, message: &str) -> Message {
    text_message(&StreamServerMessage::Error {
        code,
        message: message.to_owned(),
    })
}

async fn close_overloaded_stream<S, I, SendError, ReceiveError>(
    sink: &mut S,
    inbound: &mut I,
    error: &str,
    reason: &str,
    flush_timeout: Duration,
) where
    S: futures_util::Sink<Message, Error = SendError> + Unpin,
    I: futures_util::Stream<Item = Result<Message, ReceiveError>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + flush_timeout;
    let close = async {
        let _ = sink.send(error_message(ErrorCode::Internal, error)).await;
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        let _ = sink
            .send(Message::Close(Some(CloseFrame {
                code: 1013,
                reason: reason.into(),
            })))
            .await;
        if tokio::time::Instant::now() >= deadline {
            return;
        }

        // A flooding peer can still have data in flight when the close frame
        // is sent. Keep consuming it until the peer acknowledges the close so
        // dropping the TCP socket cannot turn the explicit close into a reset.
        loop {
            if tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => return,
                frame = inbound.next() => match frame {
                    Some(Ok(Message::Close(_))) => {
                        if tokio::time::Instant::now() < deadline {
                            let _ = sink.close().await;
                        }
                        return;
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(_)) | None => return,
                }
            }
        }
    };
    tokio::select! {
        biased;
        () = tokio::time::sleep_until(deadline) => {}
        () = close => {}
    }
}

/// Serialize one JSON status frame.
fn text_message(message: &StreamServerMessage) -> Message {
    let raw = serde_json::to_string(message).expect("stream status frames serialize");
    Message::Text(raw.into())
}

/// Map a failed ticket redemption onto a safe typed stream error.
fn redemption_error(error: &BackendError) -> Message {
    match error {
        BackendError::Forbidden => {
            error_message(ErrorCode::Unauthorized, "access denied by policy")
        }
        BackendError::NotFound => {
            error_message(ErrorCode::NotFound, "context or resource not found")
        }
        BackendError::Conflict(reason) => error_message(ErrorCode::Conflict, reason),
        BackendError::Unsupported { .. } => error_message(
            ErrorCode::UnsupportedMessage,
            "unsupported capability: stream.redeem",
        ),
        _ => error_message(ErrorCode::Internal, "internal server error"),
    }
}

/// Serve one dedicated stream socket until either side ends it.
///
/// The unauthenticated-control permit is released the moment the `hello`
/// authenticates; from then on only the dedicated stream-cap permit is
/// held, so live streams cannot starve control authentication.
pub(crate) async fn serve_stream(
    socket: WebSocket,
    config: Arc<ServerConfig>,
    kernel: Arc<BackendKernel>,
    route: StreamRoute,
    signals: crate::lifecycle::DrainSignals,
    unauthenticated_permit: OwnedSemaphorePermit,
    _stream_permit: OwnedSemaphorePermit,
) {
    let (mut sink, mut inbound) = socket.split();
    if signals.force.is_cancelled() || signals.drain.is_cancelled() {
        let _ = sink
            .send(Message::Close(Some(CloseFrame {
                code: 1001,
                reason: "server shutdown".into(),
            })))
            .await;
        return;
    }

    // The mandatory first frame must arrive within the hello timeout.
    let first = tokio::time::timeout(config.stream_hello_timeout, inbound.next()).await;
    let Ok(Some(Ok(Message::Text(hello_text)))) = first else {
        return;
    };
    // The first frame must be a hello. Fields are extracted leniently so a
    // missing token is an authentication failure rather than a decode
    // error: authentication always precedes ticket examination.
    let Ok(hello_value) = serde_json::from_str::<serde_json::Value>(&hello_text) else {
        let _ = sink
            .send(error_message(
                ErrorCode::InvalidRequest,
                "expected a hello frame",
            ))
            .await;
        let _ = sink.send(Message::Close(None)).await;
        return;
    };
    if hello_value.get("kind").and_then(serde_json::Value::as_str) != Some("hello") {
        let _ = sink
            .send(error_message(
                ErrorCode::InvalidRequest,
                "expected a hello frame",
            ))
            .await;
        let _ = sink.send(Message::Close(None)).await;
        return;
    }
    let protocol_major = hello_value
        .get("protocolMajor")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default() as u16;
    let access_token = hello_value
        .get("accessToken")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let stream_ticket = hello_value
        .get("streamTicket")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    // Authenticate the token before anything about the ticket is examined.
    if !crate::auth::const_time_eq(access_token.as_bytes(), config.access_token.as_bytes()) {
        let _ = sink
            .send(error_message(
                ErrorCode::Unauthorized,
                "authentication failed",
            ))
            .await;
        let _ = sink.send(Message::Close(None)).await;
        return;
    }
    // Authenticated: the stream no longer needs the shared unauthenticated
    // pool; the dedicated stream-cap permit carries it for its lifetime.
    drop(unauthenticated_permit);
    if protocol_major != k10s_protocol::PROTOCOL_MAJOR {
        let _ = sink
            .send(error_message(
                ErrorCode::IncompatibleProtocol,
                "incompatible protocol major",
            ))
            .await;
        let _ = sink.send(Message::Close(None)).await;
        return;
    }

    // Redeem the single-use ticket in the kernel-owned Stream Hub behind
    // the backend subscription seam.
    let mut handle = match kernel
        .subscribe(BackendSubscribe::StreamRedeem {
            ticket_id: stream_ticket.clone(),
            route: route.backend_route(),
        })
        .await
    {
        Ok(handle) => handle,
        Err(error) => {
            let _ = sink.send(redemption_error(&error)).await;
            let _ = sink.send(Message::Close(None)).await;
            return;
        }
    };
    let Some(bound) = handle.take_bound_stream() else {
        let _ = sink
            .send(error_message(
                ErrorCode::Internal,
                "stream session lost its binding",
            ))
            .await;
        let _ = sink.send(Message::Close(None)).await;
        return;
    };
    let (tty, container) = match &bound {
        k10s_backend::StreamKind::Exec { container, tty, .. } => (*tty, container.clone()),
        k10s_backend::StreamKind::Logs { container, .. } => (false, container.clone()),
    };
    let ready = sink
        .send(text_message(&StreamServerMessage::Ready {
            stream_type: route.stream_type(),
            tty,
            container,
        }))
        .await;
    if ready.is_err() {
        return;
    }
    let Some(mut events) = handle.take_events() else {
        return;
    };

    // Inbound rate budget: a fixed one-second byte window.
    let mut window_start = Instant::now();
    let mut window_bytes = 0_usize;
    let mut outbound_window_start = Instant::now();
    let mut outbound_window_bytes = 0_usize;

    loop {
        tokio::select! {
            biased;
            () = signals.force.cancelled() => return,
            () = signals.drain.cancelled() => {
                let _ = sink.send(Message::Close(Some(CloseFrame {
                    code: 1001,
                    reason: "server shutdown".into(),
                }))).await;
                return;
            }
            event = events.recv() => match event {
                Ok(BackendEvent::Stream(chunk)) => {
                    if let Some(exit_code) = chunk.exit_code {
                        let _ = sink
                            .send(text_message(&StreamServerMessage::Exit { exit_code }))
                            .await;
                        let _ = sink.send(Message::Close(None)).await;
                        return;
                    }
                    if !admit_rate(
                        &mut outbound_window_start,
                        &mut outbound_window_bytes,
                        chunk.text.len(),
                        config.stream_rate_budget_bytes_per_sec,
                    ) {
                        tracing::warn!("stream outbound rate budget exceeded; closing");
                        close_overloaded_stream(
                            &mut sink,
                            &mut inbound,
                            "stream rate budget exceeded",
                            "rate budget exceeded",
                            config.graceful_flush_timeout,
                        )
                        .await;
                        return;
                    }
                    let frame =
                        encode_stream_payload(chunk.origin.payload_kind(), chunk.text.as_bytes());
                    if sink.send(Message::Binary(frame.into())).await.is_err() {
                        return;
                    }
                }
                // Bounded per-stream queue overflow: explicit closure, never
                // silent loss.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                    tracing::warn!(dropped, "stream consumer lagged; closing");
                    close_overloaded_stream(
                        &mut sink,
                        &mut inbound,
                        "stream queue overload",
                        "stream queue overload",
                        config.graceful_flush_timeout,
                    )
                    .await;
                    return;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                Ok(_) => continue,
            },
            frame = inbound.next() => match frame {
                Some(Ok(Message::Binary(raw))) => {
                    if !admit_rate(&mut window_start, &mut window_bytes, raw.len(), config.stream_rate_budget_bytes_per_sec) {
                        close_overloaded_stream(
                            &mut sink,
                            &mut inbound,
                            "inbound rate budget exceeded; closing to prevent overload",
                            "rate budget exceeded",
                            config.graceful_flush_timeout,
                        )
                        .await;
                        return;
                    }
                    let Ok(payload) = decode_stream_payload(&raw) else {
                        let _ = sink
                            .send(error_message(
                                ErrorCode::InvalidRequest,
                                "invalid stream payload header",
                            ))
                            .await;
                        let _ = sink.send(Message::Close(None)).await;
                        return;
                    };
                    if route == StreamRoute::Logs {
                        // The logs route accepts no client payload after the
                        // hello; anything else closes the socket.
                        let _ = sink
                            .send(error_message(
                                ErrorCode::InvalidRequest,
                                "the logs route accepts no client payloads",
                            ))
                            .await;
                        let _ = sink.send(Message::Close(None)).await;
                        return;
                    }
                    match payload.kind {
                        payload_kind::STDIN => {
                            let Ok(text) = std::str::from_utf8(payload.data) else {
                                let _ = sink
                                    .send(error_message(
                                        ErrorCode::InvalidRequest,
                                        "stdin must be utf-8 text",
                                    ))
                                    .await;
                                let _ = sink.send(Message::Close(None)).await;
                                return;
                            };
                            if kernel
                                .stream_input(&stream_ticket, StreamInput::Stdin(text.to_owned()))
                                .await
                                .is_err()
                            {
                                let _ = sink
                                    .send(error_message(
                                        ErrorCode::Conflict,
                                        "the stream session is not active",
                                    ))
                                    .await;
                                let _ = sink.send(Message::Close(None)).await;
                                return;
                            }
                        }
                        payload_kind::RESIZE => {
                            let Some((cols, rows)) = decode_resize_payload(payload.data) else {
                                let _ = sink
                                    .send(error_message(
                                        ErrorCode::InvalidRequest,
                                        "resize payloads are two big-endian u32 values",
                                    ))
                                    .await;
                                let _ = sink.send(Message::Close(None)).await;
                                return;
                            };
                            if kernel
                                .stream_input(&stream_ticket, StreamInput::Resize { cols, rows })
                                .await
                                .is_err()
                            {
                                let _ = sink
                                    .send(error_message(
                                        ErrorCode::Conflict,
                                        "the stream session is not active",
                                    ))
                                    .await;
                                let _ = sink.send(Message::Close(None)).await;
                                return;
                            }
                            let status = sink
                                .send(text_message(&StreamServerMessage::Status {
                                    message: format!("resized to {cols}x{rows}"),
                                }))
                                .await;
                            if status.is_err() {
                                return;
                            }
                        }
                        _ => {
                            let _ = sink
                                .send(error_message(
                                    ErrorCode::InvalidRequest,
                                    "unsupported client payload kind on this route",
                                ))
                                .await;
                            let _ = sink.send(Message::Close(None)).await;
                            return;
                        }
                    }
                }
                Some(Ok(Message::Text(_))) => {
                    let _ = sink
                        .send(error_message(
                            ErrorCode::InvalidRequest,
                            "only the first hello may be a text frame",
                        ))
                        .await;
                    let _ = sink.send(Message::Close(None)).await;
                    return;
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(_)) => continue,
            },
        }
    }
}

/// Fixed-window inbound byte budget admission.
fn admit_rate(
    window_start: &mut Instant,
    window_bytes: &mut usize,
    incoming: usize,
    budget: usize,
) -> bool {
    let now = Instant::now();
    if now.duration_since(*window_start) >= Duration::from_secs(1) {
        *window_start = now;
        *window_bytes = 0;
    }
    *window_bytes = window_bytes.saturating_add(incoming);
    *window_bytes <= budget.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn overload_close_deadline_covers_a_blocked_sink() {
        let mut sink = Box::pin(futures_util::sink::unfold(
            (),
            |(), _message: Message| async move {
                std::future::pending::<Result<(), std::convert::Infallible>>().await
            },
        ));
        let mut inbound =
            futures_util::stream::empty::<Result<Message, std::convert::Infallible>>();

        let completed = tokio::time::timeout(
            Duration::from_millis(100),
            close_overloaded_stream(
                &mut sink,
                &mut inbound,
                "stream rate budget exceeded",
                "rate budget exceeded",
                Duration::from_millis(1),
            ),
        )
        .await;

        assert!(
            completed.is_ok(),
            "the helper's own deadline must bound a sink that never becomes writable"
        );
    }

    #[tokio::test]
    async fn overload_close_deadline_preempts_a_continuously_ready_flood() {
        let mut sink = futures_util::sink::drain::<Message>();
        let polls = Arc::new(AtomicUsize::new(0));
        let stream_polls = polls.clone();
        let mut inbound = futures_util::stream::poll_fn(move |_| {
            let poll = stream_polls.fetch_add(1, Ordering::AcqRel);
            if poll == 0 {
                std::thread::sleep(Duration::from_millis(10));
            }
            if poll < 8 {
                std::task::Poll::Ready(Some(Ok::<_, std::convert::Infallible>(Message::Binary(
                    vec![1].into(),
                ))))
            } else {
                std::task::Poll::Pending
            }
        });

        close_overloaded_stream(
            &mut sink,
            &mut inbound,
            "stream rate budget exceeded",
            "rate budget exceeded",
            Duration::from_millis(1),
        )
        .await;

        assert!(
            polls.load(Ordering::Acquire) <= 2,
            "the deadline must win before a continuously-ready stream is drained unchecked"
        );
    }

    #[tokio::test]
    async fn overload_close_emits_error_and_1013_then_drains_through_peer_close() {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut sink = Box::pin(futures_util::sink::unfold(
            sent.clone(),
            |sent, message| async move {
                sent.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(message);
                Ok::<_, std::convert::Infallible>(sent)
            },
        ));
        let mut inbound = futures_util::stream::iter([
            Ok::<_, std::convert::Infallible>(Message::Binary(vec![1].into())),
            Ok(Message::Binary(vec![2].into())),
            Ok(Message::Close(None)),
        ]);

        close_overloaded_stream(
            &mut sink,
            &mut inbound,
            "stream rate budget exceeded",
            "rate budget exceeded",
            Duration::from_secs(1),
        )
        .await;

        {
            let sent = sent
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(sent.len(), 2, "one error and one close frame");
            let Message::Text(error) = &sent[0] else {
                panic!("overload must emit a safe text error first");
            };
            let error: serde_json::Value = serde_json::from_str(error).unwrap();
            assert_eq!(error["kind"], "error");
            assert_eq!(error["code"], "internal");
            assert_eq!(error["message"], "stream rate budget exceeded");
            assert!(matches!(
                &sent[1],
                Message::Close(Some(CloseFrame { code: 1013, reason }))
                    if reason.as_str() == "rate budget exceeded"
            ));
        }
        assert!(
            inbound.next().await.is_none(),
            "overload closure must drain through the peer's close acknowledgment"
        );
    }
}
