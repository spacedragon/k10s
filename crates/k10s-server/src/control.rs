use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use k10s_backend::{
    BackendError, BackendEvent, BackendKernel, Command, Gvk, KernelQueryResult, Query, StreamKind,
    Subscribe as BackendSubscribe,
};
use k10s_protocol::{
    ClientKind, ClientPayload, ContextPermissionsRequest, ContextSwitchRequest, DeletePropagation,
    DeleteRequest, ErrorCode, ErrorFrame, ErrorScope, InfrastructureRequest, OperationAccepted,
    OperationId, OperationStatusRequest, OperationUpdate, REQUEST_CONTEXT_PERMISSIONS,
    REQUEST_CONTEXT_SWITCH, RequestId, ResourceIdentity, ResourceListRequest, ResourceRefRequest,
    ResourceTypesRequest, ResumeStatus, Retryability, ScaleRequest, ServerFrame, ServerKind,
    SessionId, ShutdownNotice, SnapshotBegin, SnapshotChunk, SnapshotEnd, Subscribed,
    SubscriptionId, SubscriptionSelector, Welcome, YamlApplyRequest, YamlValidateRequest,
    decode_client_frame,
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

/// Coordinates one generation of resource-watch forwarders for a session.
/// The first recovery demand advances the generation and cancels every old
/// forwarder, so a delayed old-generation demand cannot invalidate recovery
/// requests after the first barrier has left the scheduler queue.
#[derive(Clone)]
struct WatchRecovery {
    state: Arc<Mutex<WatchRecoveryState>>,
}

struct WatchRecoveryState {
    generation: u128,
    cancel: CancellationToken,
}

struct WatchGeneration {
    id: u128,
    cancel: CancellationToken,
    recovery: WatchRecovery,
}

struct ActiveWatchSubscription {
    task_id: u128,
    cancel: CancellationToken,
}

impl WatchRecovery {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(WatchRecoveryState {
                generation: 0,
                cancel: CancellationToken::new(),
            })),
        }
    }

    fn register(&self) -> WatchGeneration {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        WatchGeneration {
            id: state.generation,
            cancel: state.cancel.clone(),
            recovery: self.clone(),
        }
    }

    fn demand(&self, generation: u128) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if generation != state.generation {
            return false;
        }
        state.cancel.cancel();
        state.generation = state.generation.wrapping_add(1);
        state.cancel = CancellationToken::new();
        true
    }
}

impl WatchGeneration {
    fn demand(&self) -> bool {
        self.recovery.demand(self.id)
    }
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
    // Accepted operation IDs correlated to their submitting request ID so
    // every forwarded operation update can be traced with both identifiers.
    let operation_correlations: Arc<std::sync::Mutex<HashMap<String, String>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let mut request_tasks = JoinSet::new();
    let last_sent_sequence: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let subscription_cancel = child.child_token();
    let watch_recovery = WatchRecovery::new();
    let mut watch_subscriptions: HashMap<SubscriptionId, ActiveWatchSubscription> = HashMap::new();
    let (forwarder_done_tx, mut forwarder_done_rx) =
        tokio::sync::mpsc::unbounded_channel::<(SubscriptionId, u128)>();
    let mut next_forwarder_id = 1_u128;
    let mut last_acked_sequence = 0_u64;
    let mut noticed = false;

    // Every authenticated session streams background operation updates:
    // they arrive through the backend's operations subscription and leave
    // as sequenced `operationUpdate` frames on the lossless reserve.
    if let Ok(mut handle) = kernel.subscribe(BackendSubscribe::Operations).await
        && let Some(mut events) = handle.take_events()
    {
        let fwd_outbound = outbound.clone();
        // Bound to the subscription-generation token so session teardown
        // can join this forwarder deterministically.
        let fwd_cancel = subscription_cancel.child_token();
        let fwd_correlations = operation_correlations.clone();
        let fwd_counter = Arc::clone(&last_sent_sequence);
        let ops_span = tracing::info_span!(
            "control_operations",
            session_id = %session_id.as_str(),
            backend_subscription_id = %handle.id,
        );
        request_tasks.spawn(
            async move {
                loop {
                    tokio::select! {
                        biased;
                        () = fwd_cancel.cancelled() => break,
                        event = events.recv() => match event {
                            Ok(BackendEvent::Operation(update)) => {
                                let correlation = fwd_correlations
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .get(&update.id)
                                    .cloned();
                                let outcome = forward_operation_update(
                                    &fwd_outbound,
                                    &fwd_counter,
                                    &update,
                                    correlation.as_deref(),
                                );
                                if outcome.is_err() {
                                    overload_close(&fwd_outbound);
                                    break;
                                }
                            }
                            Ok(_) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                                tracing::warn!(
                                    dropped,
                                    "operations consumer lagged; demanding resync"
                                );
                                // Dropped updates are unrecoverable on this
                                // stream: the client must refresh every
                                // nonterminal operation by ID, so demand a
                                // full resync losslessly.
                                let reason = "operation updates were dropped".to_owned();
                                let outcome = fwd_outbound.enqueue_p0_sequenced(|| {
                                    let sequence = allocate_sequence(&fwd_counter)
                                        .ok_or(EnqueueError::Overloaded)?;
                                    let frame = ServerFrame {
                                        kind: ServerKind::ResyncRequired,
                                        request_id: None,
                                        subscription_id: None,
                                        sequence: Some(sequence),
                                        payload: serde_json::json!({ "reason": reason }),
                                    };
                                    let text =
                                        serde_json::to_string(&frame).expect("resync frame serializes");
                                    Ok((sequence, Message::Text(text.into())))
                                });
                                if outcome.is_err() {
                                    overload_close(&fwd_outbound);
                                    break;
                                }
                                // Keep forwarding: later updates still reach
                                // the client, and the resync answer repairs
                                // everything dropped before it.
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        },
                    }
                }
            }
            .instrument(ops_span),
        );
    }
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
            completed = forwarder_done_rx.recv() => {
                if let Some((subscription_id, task_id)) = completed
                    && watch_subscriptions
                        .get(&subscription_id)
                        .is_some_and(|active| active.task_id == task_id)
                {
                    watch_subscriptions.remove(&subscription_id);
                }
                continue;
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
                let task_correlations = operation_correlations.clone();
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
                        let parsed = parse_request(
                    &request.request_kind,
                    &request.payload,
                    request.idempotency_key.clone(),
                );
                        let result: Result<RequestOutcome, RequestFailure> = match parsed {
                            Ok(Some(ParsedRequest::Query(query))) => {
                                let deadline = request.deadline.map(Duration::from_millis);
                                tokio::select! {
                                    () = request_cancel.cancelled() =>
                                        Err(RequestFailure::Backend(BackendError::Cancelled)),
                                    result = task_kernel.query_with_deadline(query, deadline) =>
                                        result.map(RequestOutcome::Kernel).map_err(RequestFailure::Backend),
                                }
                            }
                            Ok(Some(ParsedRequest::Execute(command))) => {
                                let deadline = request.deadline.map(Duration::from_millis);
                                tokio::select! {
                                    () = request_cancel.cancelled() =>
                                        Err(RequestFailure::Backend(BackendError::Cancelled)),
                                    result = task_kernel.execute_with_deadline(command, deadline) => {
                                        result.map(|operation_id| {
                                            task_correlations
                                                .lock()
                                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                                .insert(
                                                    operation_id.as_str().to_owned(),
                                                    request_id.as_str().to_owned(),
                                                );
                                            RequestOutcome::Applied(OperationAccepted {
                                                operation_id: OperationId::new(operation_id.as_str()),
                                            })
                                        })
                                        .map_err(RequestFailure::Backend)
                                    }
                                }
                            }
                            Ok(None) => Err(RequestFailure::Backend(BackendError::unsupported(
                                request.request_kind,
                            ))),
                            Err(message) => Err(RequestFailure::Malformed(message)),
                        };
                        let sent = match result {
                            Ok(RequestOutcome::Kernel(KernelQueryResult::Bootstrap(value))) => {
                                let mut payload = value.wire_payload();
                                payload.protocol = task_protocol;
                                payload.capabilities = task_capabilities;
                                send_frame(
                                    &task_outbound,
                                    ServerFrame::response(request_id.clone(), payload),
                                    Priority::P1,
                                )
                            }
                            Ok(RequestOutcome::Kernel(KernelQueryResult::ResourceList(value))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Kernel(KernelQueryResult::ResourceDetail(value))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Kernel(KernelQueryResult::ResourceMetrics(value))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Kernel(KernelQueryResult::ResourceTypes(value))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Kernel(KernelQueryResult::ContextSwitch(
                                value,
                            ))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Kernel(KernelQueryResult::ContextPermissions(
                                value,
                            ))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Kernel(KernelQueryResult::Infrastructure(value))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Kernel(KernelQueryResult::YamlValidate(value))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Kernel(KernelQueryResult::StreamTicket(value))) => {
                                send_frame(
                                    &task_outbound,
                                    ServerFrame::response(request_id.clone(), value.wire_payload()),
                                    Priority::P1,
                                )
                            }
                            Ok(RequestOutcome::Kernel(KernelQueryResult::OperationStatus(
                                value,
                            ))) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value.wire_payload()),
                                Priority::P1,
                            ),
                            Ok(RequestOutcome::Applied(value)) => send_frame(
                                &task_outbound,
                                ServerFrame::response(request_id.clone(), value),
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
                let selector_kind = selector
                    .0
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                let outcome = match serde_json::from_value::<SubscriptionSelector>(selector.0) {
                    Ok(SubscriptionSelector::BootstrapStatus) => {
                        match kernel.subscribe(BackendSubscribe::BootstrapStatus).await {
                            Ok(_) => Ok(None),
                            Err(error) => Err(backend_rejection(&error)),
                        }
                    }
                    Ok(SubscriptionSelector::Resource(spec)) => {
                        if !watch_subscriptions.contains_key(&subscription_id)
                            && watch_subscriptions.len()
                                >= config.max_resource_subscriptions_per_session
                        {
                            Err((
                                ErrorCode::Conflict,
                                format!(
                                    "watch subscription limit is {}",
                                    config.max_resource_subscriptions_per_session
                                ),
                            ))
                        } else {
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
                    }
                    Ok(SubscriptionSelector::Infrastructure(spec)) => {
                        if !watch_subscriptions.contains_key(&subscription_id)
                            && watch_subscriptions.len()
                                >= config.max_resource_subscriptions_per_session
                        {
                            Err((
                                ErrorCode::Conflict,
                                format!(
                                    "watch subscription limit is {}",
                                    config.max_resource_subscriptions_per_session
                                ),
                            ))
                        } else {
                            match kernel
                                .subscribe(BackendSubscribe::Infrastructure {
                                    context: spec.context,
                                })
                                .await
                            {
                                Ok(handle) => Ok(Some(handle)),
                                Err(error) => Err(backend_rejection(&error)),
                            }
                        }
                    }
                    Err(_) if selector_kind.is_none() => Err((
                        ErrorCode::InvalidRequest,
                        "invalid subscription payload".into(),
                    )),
                    Err(_)
                        if matches!(
                            selector_kind.as_deref(),
                            Some("bootstrapStatus" | "resource" | "infrastructure")
                        ) =>
                    {
                        Err((
                            ErrorCode::InvalidRequest,
                            "invalid subscription payload".into(),
                        ))
                    }
                    Err(_) => Err((
                        ErrorCode::UnsupportedMessage,
                        "unsupported subscription".into(),
                    )),
                };
                match outcome {
                    Ok(mut handle) => {
                        if let Some(previous) = watch_subscriptions.remove(&subscription_id) {
                            previous.cancel.cancel();
                        }
                        let task_cancel =
                            handle.as_ref().map(|_| subscription_cancel.child_token());
                        let task_generation = handle.as_ref().map(|_| watch_recovery.register());
                        let task_id = handle.as_ref().map(|_| {
                            let task_id = next_forwarder_id;
                            next_forwarder_id = next_forwarder_id.wrapping_add(1);
                            task_id
                        });
                        if let (Some(cancel), Some(task_id)) = (&task_cancel, task_id) {
                            watch_subscriptions.insert(
                                subscription_id.clone(),
                                ActiveWatchSubscription {
                                    task_id,
                                    cancel: cancel.clone(),
                                },
                            );
                        }
                        if send_sequenced(
                            &outbound,
                            &subscription_id,
                            ServerKind::Subscribed,
                            serde_json::to_value(Subscribed)
                                .expect("subscribed payload serializes"),
                            &last_sent_sequence,
                        )
                        .await
                        .is_err()
                        {
                            overload_close(&outbound);
                            break;
                        }
                        if let Some(mut handle) = handle.take() {
                            let task_outbound = outbound.clone();
                            let task_kernel = Arc::clone(&kernel);
                            let task_cancel = task_cancel.expect("resource watch has cancellation");
                            let task_counter = Arc::clone(&last_sent_sequence);
                            let task_generation =
                                task_generation.expect("resource watch has a generation");
                            let task_id = task_id.expect("resource watch has a task ID");
                            let task_done = forwarder_done_tx.clone();
                            let completed_subscription_id = subscription_id.clone();
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
                                        task_generation,
                                    )
                                    .await;
                                    let _ = task_done.send((completed_subscription_id, task_id));
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
                    && let Some(active) = watch_subscriptions.remove(&subscription_id)
                {
                    active.cancel.cancel();
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

/// Outcome of mapping a request kind and payload onto backend behavior.
enum ParsedRequest {
    /// A behavior-level query.
    Query(Query),
    /// A behavior-level mutation routed through the command port. The
    /// envelope-level idempotency key travels inside every command.
    Execute(Command),
}

/// Outcome of one dispatched control request.
#[allow(clippy::large_enum_variant)]
enum RequestOutcome {
    /// A completed kernel query.
    Kernel(KernelQueryResult),
    /// An accepted background operation.
    Applied(OperationAccepted),
}

/// Map a request kind and payload onto a backend behavior.
///
/// `Ok(None)` marks the bootstrap request, which needs no payload parsing.
/// Mutations receive the envelope-level idempotency key; when absent, the
/// ticket ID (for applies) keeps legacy envelopes safe.
fn parse_request(
    kind: &str,
    payload: &serde_json::Value,
    idempotency_key: Option<String>,
) -> Result<Option<ParsedRequest>, String> {
    match kind {
        "bootstrap" => Ok(Some(ParsedRequest::Query(Query::Bootstrap))),
        "yaml.validate" => serde_json::from_value::<YamlValidateRequest>(payload.clone())
            .map(|parsed| {
                Some(ParsedRequest::Query(Query::ValidateApply {
                    context: parsed.context,
                    yaml: parsed.yaml,
                }))
            })
            .map_err(|error| format!("invalid yaml.validate payload: {error}")),
        k10s_protocol::REQUEST_YAML_APPLY => {
            serde_json::from_value::<YamlApplyRequest>(payload.clone())
                .map(|apply| {
                    Some(ParsedRequest::Execute(Command::Apply {
                        context: apply.context.clone(),
                        yaml: apply.yaml,
                        idempotency_key: idempotency_key.unwrap_or_else(|| apply.ticket_id.clone()),
                        ticket_id: apply.ticket_id,
                        buffer_hash: apply.buffer_hash,
                        target: backend_reference(apply.target),
                    }))
                })
                .map_err(|error| format!("invalid yaml.apply payload: {error}"))
        }
        k10s_protocol::REQUEST_WORKLOAD_SCALE => {
            // A missing key cannot be synthesized from context/name alone:
            // same-name objects in different namespaces or kinds, and
            // legitimate later mutations, would be misread as replays.
            let Some(idempotency_key) = idempotency_key.filter(|key| !key.trim().is_empty()) else {
                return Err(
                    "workload.scale requires a non-empty envelope-level idempotencyKey".to_owned(),
                );
            };
            serde_json::from_value::<ScaleRequest>(payload.clone())
                .map(|scale| {
                    let key = idempotency_key;
                    Some(ParsedRequest::Execute(Command::Scale {
                        context: scale.context,
                        gvk: Gvk {
                            group: scale.gvk.group,
                            version: scale.gvk.version,
                            kind: scale.gvk.kind,
                        },
                        namespace: scale.namespace,
                        name: scale.name,
                        uid: scale.uid,
                        replicas: scale.replicas,
                        idempotency_key: key,
                    }))
                })
                .map_err(|error| {
                    format!(
                        "invalid {kind} payload: {error}",
                        kind = k10s_protocol::REQUEST_WORKLOAD_SCALE
                    )
                })
        }
        k10s_protocol::REQUEST_WORKLOAD_DELETE => {
            let Some(idempotency_key) = idempotency_key.filter(|key| !key.trim().is_empty()) else {
                return Err(
                    "workload.delete requires a non-empty envelope-level idempotencyKey".to_owned(),
                );
            };
            serde_json::from_value::<DeleteRequest>(payload.clone())
                .map(|delete| {
                    let key = idempotency_key;
                    let propagation = match delete.propagation {
                        DeletePropagation::Background => k10s_backend::Propagation::Background,
                        DeletePropagation::Foreground => k10s_backend::Propagation::Foreground,
                        DeletePropagation::Orphan => k10s_backend::Propagation::Orphan,
                    };
                    Some(ParsedRequest::Execute(Command::Delete {
                        target: backend_reference(delete.identity),
                        propagation,
                        idempotency_key: key,
                    }))
                })
                .map_err(|error| {
                    format!(
                        "invalid {kind} payload: {error}",
                        kind = k10s_protocol::REQUEST_WORKLOAD_DELETE
                    )
                })
        }
        k10s_protocol::REQUEST_OPERATION_STATUS => {
            serde_json::from_value::<OperationStatusRequest>(payload.clone())
                .map(|status| {
                    Some(ParsedRequest::Query(Query::OperationStatus {
                        operation_ids: status
                            .operation_ids
                            .iter()
                            .map(|id| id.as_str().to_owned())
                            .collect(),
                    }))
                })
                .map_err(|error| {
                    format!(
                        "invalid {kind} payload: {error}",
                        kind = k10s_protocol::REQUEST_OPERATION_STATUS
                    )
                })
        }
        "resource.list" => serde_json::from_value::<ResourceListRequest>(payload.clone())
            .map(|parsed| {
                Some(ParsedRequest::Query(Query::ResourceList {
                    context: parsed.context,
                    gvk: Gvk {
                        group: parsed.gvk.group,
                        version: parsed.gvk.version,
                        kind: parsed.gvk.kind,
                    },
                    namespace: parsed.namespace,
                }))
            })
            .map_err(|error| format!("invalid resource.list payload: {error}")),
        "resource.detail" => serde_json::from_value::<ResourceRefRequest>(payload.clone())
            .map(|parsed| {
                Some(ParsedRequest::Query(Query::ResourceDetail {
                    reference: backend_reference(parsed.identity),
                }))
            })
            .map_err(|error| format!("invalid resource.detail payload: {error}")),
        "resource.metrics" => serde_json::from_value::<ResourceRefRequest>(payload.clone())
            .map(|parsed| {
                Some(ParsedRequest::Query(Query::ResourceMetrics {
                    reference: backend_reference(parsed.identity),
                }))
            })
            .map_err(|error| format!("invalid resource.metrics payload: {error}")),
        "resource.types" => serde_json::from_value::<ResourceTypesRequest>(payload.clone())
            .map(|parsed| {
                Some(ParsedRequest::Query(Query::ResourceTypes {
                    context: parsed.context,
                }))
            })
            .map_err(|error| format!("invalid resource.types payload: {error}")),
        REQUEST_CONTEXT_SWITCH => serde_json::from_value::<ContextSwitchRequest>(payload.clone())
            .map(|parsed| Some(ParsedRequest::Query(Query::ContextSwitch { to: parsed.to })))
            .map_err(|error| format!("invalid {REQUEST_CONTEXT_SWITCH} payload: {error}")),
        REQUEST_CONTEXT_PERMISSIONS => {
            serde_json::from_value::<ContextPermissionsRequest>(payload.clone())
                .map(|parsed| {
                    Some(ParsedRequest::Query(Query::ContextPermissions {
                        context: parsed.context,
                        probes: parsed
                            .probes
                            .into_iter()
                            .map(|probe| k10s_backend::PermissionProbe {
                                verb: probe.verb,
                                resource: probe.resource,
                                group: probe.group,
                                namespace: probe.namespace,
                            })
                            .collect(),
                    }))
                })
                .map_err(|error| format!("invalid {REQUEST_CONTEXT_PERMISSIONS} payload: {error}"))
        }
        "infrastructure.get" => serde_json::from_value::<InfrastructureRequest>(payload.clone())
            .map(|parsed| {
                Some(ParsedRequest::Query(Query::Infrastructure {
                    context: parsed.context,
                }))
            })
            .map_err(|error| format!("invalid infrastructure.get payload: {error}")),
        k10s_protocol::REQUEST_STREAM_TICKET => {
            serde_json::from_value::<k10s_protocol::StreamTicketRequest>(payload.clone())
                .map(|parsed| {
                    let target = &parsed.target;
                    let stream = match parsed.stream_type {
                        k10s_protocol::StreamType::Logs => StreamKind::Logs {
                            context: target.context.clone(),
                            namespace: target.namespace.clone(),
                            pod: target.pod.clone(),
                            container: target.container.clone(),
                        },
                        k10s_protocol::StreamType::Exec => StreamKind::Exec {
                            context: target.context.clone(),
                            namespace: target.namespace.clone(),
                            pod: target.pod.clone(),
                            container: target.container.clone(),
                            tty: parsed.tty,
                        },
                    };
                    Some(ParsedRequest::Query(Query::StreamTicket { stream }))
                })
                .map_err(|error| {
                    format!(
                        "invalid {kind} payload: {error}",
                        kind = k10s_protocol::REQUEST_STREAM_TICKET
                    )
                })
        }
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
        BackendError::Conflict(reason) => (ErrorCode::Conflict, format!("conflict: {reason}")),
        BackendError::Forbidden => (
            ErrorCode::Unauthorized,
            "access denied by policy".to_owned(),
        ),
        BackendError::Timeout => (ErrorCode::Timeout, "request timed out".to_owned()),
        BackendError::Cancelled => (ErrorCode::Cancelled, "request was cancelled".to_owned()),
        BackendError::Internal(_) => (ErrorCode::Internal, "internal server error".to_owned()),
    }
}

/// Forward one resource or infrastructure watch from the backend to the client.
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
    generation: WatchGeneration,
) {
    let mut events = match events {
        Some(events) => events,
        None => return,
    };
    loop {
        let event = tokio::select! {
            biased;
            () = generation.cancel.cancelled() => break,
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
                        demand_resync(outbound, sequence_counter, &generation);
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
                        demand_resync(outbound, sequence_counter, &generation);
                        break;
                    }
                    Err(DeltaAdmission::Overloaded) => {
                        overload_close(outbound);
                        break;
                    }
                }
            }
            Ok(BackendEvent::Infrastructure(snapshot)) => {
                let context = snapshot.context().to_owned();
                let revision = snapshot.revision();
                let payload = kernel.infrastructure_update(snapshot);
                match enqueue_infrastructure(
                    outbound,
                    subscription_id,
                    &context,
                    revision,
                    &payload,
                    sequence_counter,
                ) {
                    Ok(()) => {}
                    Err(EnqueueError::Coalesced) => {
                        tracing::debug!(
                            subscription_id = %subscription_id.as_str(),
                            context,
                            "infrastructure telemetry dropped under bounded P2 pressure"
                        );
                    }
                    Err(EnqueueError::Overloaded) => {
                        overload_close(outbound);
                        break;
                    }
                }
            }
            Ok(BackendEvent::Stream(_)) => {
                // Stream chunks never ride the control scheduler; they are
                // forwarded by the dedicated logs/exec sockets only.
            }
            Ok(BackendEvent::Operation(update)) => {
                let _ = forward_operation_update(outbound, sequence_counter, &update, None);
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                tracing::warn!(
                    subscription_id = %subscription_id.as_str(),
                    dropped,
                    "subscription consumer lagged; demanding resync"
                );
                demand_resync(outbound, sequence_counter, &generation);
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
/// subscription and resource identity. A dropped delta leaves the client's
/// revision stream permanently behind, so the caller must demand a resync
/// instead of continuing silently.
fn enqueue_delta(
    outbound: &Scheduler,
    subscription_id: &SubscriptionId,
    resource: &str,
    event_kind: &'static str,
    revision: u64,
    payload: &impl serde::Serialize,
    sequence_counter: &AtomicU64,
) -> Result<(), DeltaAdmission> {
    match outbound.enqueue_p2_sequenced(subscription_id.as_str(), resource, |queued_sequence| {
        let sequence = match queued_sequence {
            Some(sequence) => sequence,
            None => allocate_sequence(sequence_counter).ok_or(EnqueueError::Overloaded)?,
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
        Ok((sequence, Message::Text(text.into())))
    }) {
        Ok(()) => Ok(()),
        Err(EnqueueError::Coalesced) => Err(DeltaAdmission::Dropped),
        Err(EnqueueError::Overloaded) => Err(DeltaAdmission::Overloaded),
    }
}

/// Enqueue one infrastructure telemetry update on the existing bounded P2
/// partition, coalesced by subscription and context. Unlike resource
/// revisions, telemetry may be dropped under pressure; the UI retains and
/// explicitly marks its last timestamp rather than inventing a newer value.
fn enqueue_infrastructure(
    outbound: &Scheduler,
    subscription_id: &SubscriptionId,
    context: &str,
    revision: u64,
    payload: &impl serde::Serialize,
    sequence_counter: &AtomicU64,
) -> Result<(), EnqueueError> {
    outbound.enqueue_p2_sequenced(subscription_id.as_str(), context, |queued_sequence| {
        let sequence = match queued_sequence {
            Some(sequence) => sequence,
            None => allocate_sequence(sequence_counter).ok_or(EnqueueError::Overloaded)?,
        };
        let frame = ServerFrame {
            kind: ServerKind::Event,
            request_id: None,
            subscription_id: Some(subscription_id.clone()),
            sequence: Some(sequence),
            payload: serde_json::json!({
                "kind": k10s_protocol::INFRASTRUCTURE_EVENT_UPDATED,
                "revision": revision.to_string(),
                "payload": payload,
            }),
        };
        let text = serde_json::to_string(&frame).expect("server frame serializes");
        Ok((sequence, Message::Text(text.into())))
    })
}

/// Tell the client its revision stream can no longer be trusted. If the
/// connection cannot even carry the notice, close it as overloaded.
fn demand_resync(outbound: &Scheduler, sequence_counter: &AtomicU64, generation: &WatchGeneration) {
    if !generation.demand() {
        return;
    }
    if outbound
        .enqueue_p2_barrier(|| {
            let sequence = allocate_sequence(sequence_counter).ok_or(EnqueueError::Overloaded)?;
            let frame = ServerFrame {
                kind: ServerKind::ResyncRequired,
                request_id: None,
                subscription_id: None,
                sequence: Some(sequence),
                payload: serde_json::json!({"reason": "resource deltas were dropped"}),
            };
            let text = serde_json::to_string(&frame).expect("resync frame serializes");
            Ok((sequence, Message::Text(text.into())))
        })
        .is_err()
    {
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
        outbound.enqueue_sequenced(|| {
            let sequence = allocate_sequence(sequence_counter).ok_or(EnqueueError::Overloaded)?;
            let frame = ServerFrame {
                kind,
                request_id: None,
                subscription_id: Some(subscription_id.clone()),
                sequence: Some(sequence),
                payload,
            };
            let text = serde_json::to_string(&frame).expect("server frame serializes");
            Ok((sequence, Message::Text(text.into())))
        })
    })
}

/// Forward one backend operation event as a sequenced `operationUpdate`
/// frame on the lossless P0 reserve. The frame carries its connection
/// sequence and is traced with both the operation ID and, when known, the
/// correlation ID of the request that submitted it.
fn forward_operation_update(
    outbound: &Scheduler,
    sequence_counter: &AtomicU64,
    update: &k10s_backend::OperationEvent,
    correlation_id: Option<&str>,
) -> Result<(), EnqueueError> {
    let payload = OperationUpdate {
        operation_id: OperationId::new(update.id.clone()),
        status: update.state.wire(),
        progress: update.progress.map(|(completed, total)| {
            serde_json::to_value(k10s_protocol::OperationProgress { completed, total })
                .expect("progress serializes")
        }),
    };
    let result = outbound.enqueue_p0_sequenced(|| {
        let sequence = allocate_sequence(sequence_counter).ok_or(EnqueueError::Overloaded)?;
        let frame = ServerFrame {
            kind: ServerKind::OperationUpdate,
            request_id: None,
            subscription_id: None,
            sequence: Some(sequence),
            payload: serde_json::to_value(&payload).expect("operation update serializes"),
        };
        let text = serde_json::to_string(&frame).expect("server frame serializes");
        Ok((sequence, Message::Text(text.into())))
    });
    if result.is_ok() {
        tracing::info!(
            operation_id = %update.id,
            correlation_id = correlation_id.unwrap_or("unknown"),
            status = ?update.state,
            "operation update forwarded"
        );
    }
    result
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
        BackendError::Conflict(reason) => (ErrorCode::Conflict, format!("conflict: {reason}")),
        BackendError::Forbidden => (
            ErrorCode::Unauthorized,
            "access denied by policy".to_owned(),
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
            events: Vec::new(),
            manifest: String::new(),
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

    fn ready_client() -> k10s_ui::client::ClientState {
        use k10s_ui::client::{ClientConfig, ClientState, ConnectTarget};

        let mut client = ClientState::new(ClientConfig::default());
        client
            .connect(ConnectTarget::new(
                "ws://localhost/api/v1/control",
                "secret",
            ))
            .unwrap();
        let _hello = client.take_outbound();
        client
            .apply(ServerFrame {
                kind: ServerKind::Welcome,
                request_id: None,
                subscription_id: None,
                sequence: None,
                payload: serde_json::to_value(Welcome {
                    protocol: k10s_protocol::ProtocolVersion {
                        major: k10s_protocol::PROTOCOL_MAJOR,
                        minor: k10s_protocol::PROTOCOL_MINOR,
                    },
                    capabilities: vec![],
                    session_id: SessionId::new("session-1"),
                    server_instance_id: "instance-1".into(),
                    resume_status: ResumeStatus::Fresh,
                })
                .unwrap(),
            })
            .unwrap();
        client
    }

    #[tokio::test]
    async fn p2_admission_failure_sequences_resync_after_reliable_and_queued_frames() {
        let scheduler = Scheduler::new(8, 2);
        let kernel = BackendKernel::new(k10s_backend::FakeKubernetes::standard());
        let subscription_id = SubscriptionId::new("sub-test");
        let counter = AtomicU64::new(0);
        let cancel = CancellationToken::new();
        let recovery = WatchRecovery::new();
        let generation = recovery.register();
        let (sender, receiver) = tokio::sync::broadcast::channel(32);
        let mut client = ready_client();
        let pending = client.begin(k10s_ui::client::Query::Bootstrap).unwrap();
        let _request = client.take_outbound();
        send_frame(
            &scheduler,
            ServerFrame::response(
                pending.id().clone(),
                k10s_protocol::BootstrapResponse::fixture(),
            ),
            Priority::P1,
        )
        .unwrap();

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
            generation,
        )
        .await;

        let frames = drain_frames(&scheduler).await;
        let kinds: Vec<_> = frames.iter().map(|frame| frame.kind).collect();
        assert_eq!(
            kinds,
            [
                ServerKind::Response,
                ServerKind::Event,
                ServerKind::Event,
                ServerKind::Event,
                ServerKind::Event,
                ServerKind::Event,
                ServerKind::Event,
                ServerKind::ResyncRequired,
            ],
            "reliable work completes before the sequenced P2 tail is invalidated"
        );
        let sequenced: Vec<_> = frames.iter().filter_map(|frame| frame.sequence).collect();
        assert_eq!(
            sequenced,
            vec![1, 2, 3, 4, 5, 6, 7],
            "every connection sequence reaches the wire contiguously"
        );
        for frame in frames {
            let wire = serde_json::to_value(&frame).unwrap();
            let decoded = k10s_protocol::decode_server_frame(wire)
                .expect("every emitted frame satisfies the public contract");
            client
                .apply(decoded)
                .expect("pending response and sequenced resync apply in wire order");
        }
        assert!(client.server_state_invalid());
    }

    #[tokio::test]
    async fn same_resource_coalescing_preserves_the_connection_sequence() {
        let scheduler = Scheduler::new(8, 2);
        let kernel = BackendKernel::new(k10s_backend::FakeKubernetes::standard());
        let counter = AtomicU64::new(1);
        let cancel = CancellationToken::new();
        let recovery = WatchRecovery::new();
        let generation = recovery.register();
        let (sender, receiver) = tokio::sync::broadcast::channel(8);
        let mut first = changed_record("pod-same");
        first.revision = 2_000;
        sender.send(BackendEvent::Changed(first)).unwrap();
        let mut second = changed_record("pod-same");
        second.revision = 2_001;
        sender.send(BackendEvent::Changed(second)).unwrap();
        drop(sender);

        stream_backend_events(
            &scheduler,
            &kernel,
            &SubscriptionId::new("resource-1"),
            Some(receiver),
            &counter,
            &cancel,
            generation,
        )
        .await;
        let frames = drain_frames(&scheduler).await;
        assert_eq!(frames.len(), 1, "the full-row replacement coalesces");
        assert_eq!(
            frames[0].sequence,
            Some(2),
            "the queued slot keeps its original connection sequence"
        );

        let mut client = ready_client();
        let subscription = client
            .subscribe_resource("dev-local", "", "v1", "Pod", Some("default".into()))
            .unwrap();
        let _subscribe = client.take_outbound();
        client
            .apply(ServerFrame {
                kind: ServerKind::Subscribed,
                request_id: None,
                subscription_id: Some(subscription.id().clone()),
                sequence: Some(1),
                payload: serde_json::to_value(Subscribed).unwrap(),
            })
            .unwrap();
        let _ack = client.take_outbound();
        client
            .apply(frames.into_iter().next().unwrap())
            .expect("coalesced full-row delta stays contiguous for the real client");
        assert_eq!(client.last_acked_sequence(), Some(2));
    }

    #[tokio::test]
    async fn infrastructure_updates_coalesce_by_context_without_a_sequence_gap() {
        let scheduler = Scheduler::new(8, 2);
        let subscription_id = SubscriptionId::new("infrastructure-1");
        let counter = AtomicU64::new(0);

        enqueue_infrastructure(
            &scheduler,
            &subscription_id,
            "dev-local",
            2_000,
            &serde_json::json!({"metrics": "available"}),
            &counter,
        )
        .unwrap();
        enqueue_infrastructure(
            &scheduler,
            &subscription_id,
            "dev-local",
            2_001,
            &serde_json::json!({"metrics": "partial"}),
            &counter,
        )
        .unwrap();

        let frames = drain_frames(&scheduler).await;
        assert_eq!(frames.len(), 1, "one context occupies one bounded P2 slot");
        assert_eq!(frames[0].sequence, Some(1));
        assert_eq!(frames[0].payload["revision"], "2001");
        assert_eq!(frames[0].payload["payload"]["metrics"], "partial");
    }

    #[tokio::test]
    async fn sequenced_snapshot_does_not_overtake_a_queued_delta() {
        let scheduler = Scheduler::new(8, 2);
        let subscription_id = SubscriptionId::new("resource-1");
        let counter = AtomicU64::new(0);

        assert!(matches!(
            enqueue_delta(
                &scheduler,
                &subscription_id,
                "pod/a",
                k10s_protocol::RESOURCE_EVENT_CHANGED,
                2_000,
                &serde_json::json!({"name": "pod-a"}),
                &counter,
            ),
            Ok(())
        ));
        send_sequenced(
            &scheduler,
            &subscription_id,
            ServerKind::SnapshotBegin,
            serde_json::to_value(SnapshotBegin { total_chunks: 1 }).unwrap(),
            &counter,
        )
        .await
        .expect("snapshot is admitted");

        let frames = drain_frames(&scheduler).await;
        assert_eq!(
            frames
                .iter()
                .map(|frame| (frame.kind, frame.sequence))
                .collect::<Vec<_>>(),
            [
                (ServerKind::Event, Some(1)),
                (ServerKind::SnapshotBegin, Some(2)),
            ],
            "priority must not reorder the connection sequence"
        );
    }

    #[tokio::test]
    async fn overlapping_subscriptions_keep_distinct_delta_slots() {
        let scheduler = Scheduler::new(8, 2);
        let counter = AtomicU64::new(0);
        for subscription_id in ["resource-all", "resource-default"] {
            assert!(matches!(
                enqueue_delta(
                    &scheduler,
                    &SubscriptionId::new(subscription_id),
                    "pod/a",
                    k10s_protocol::RESOURCE_EVENT_CHANGED,
                    2_000,
                    &serde_json::json!({"name": "pod-a"}),
                    &counter,
                ),
                Ok(())
            ));
        }

        let frames = drain_frames(&scheduler).await;
        assert_eq!(frames.len(), 2, "each subscription owns a delta slot");
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.subscription_id.as_ref().unwrap().as_str())
                .collect::<Vec<_>>(),
            ["resource-all", "resource-default"]
        );
        assert_eq!(
            frames
                .iter()
                .filter_map(|frame| frame.sequence)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[tokio::test]
    async fn recovery_generation_suppresses_old_demand_after_barrier_dequeue() {
        let scheduler = Scheduler::new(8, 2);
        let counter = AtomicU64::new(0);
        let mut client = ready_client();
        let recovery = WatchRecovery::new();
        let first_generation = recovery.register();
        let second_generation = recovery.register();

        demand_resync(&scheduler, &counter, &first_generation);
        assert!(first_generation.cancel.is_cancelled());
        assert!(second_generation.cancel.is_cancelled());
        let first = scheduler.recv().await.expect("first recovery barrier");
        let Message::Text(first) = first.message else {
            panic!("recovery barrier is text");
        };
        let first: ServerFrame = serde_json::from_str(&first).unwrap();
        client.apply(first).expect("first recovery begins");
        let recovery_request = client.take_outbound().expect("recovery bootstrap request");
        assert_eq!(recovery_request.kind, ClientKind::Request);
        let recovery_request_id = recovery_request.request_id.unwrap();

        // A second old-generation forwarder can demand recovery after the
        // writer has dequeued the first barrier. It must not create another
        // notice that would clear the first generation's pending request.
        demand_resync(&scheduler, &counter, &second_generation);
        assert!(
            scheduler.is_empty(),
            "the old recovery generation stays deduplicated after dequeue"
        );
        client
            .apply(ServerFrame::response(
                recovery_request_id,
                k10s_protocol::BootstrapResponse::fixture(),
            ))
            .expect("delayed first-generation response remains correlated");
    }

    #[tokio::test]
    async fn admission_drop_recovery_converges_a_real_client_state() {
        let scheduler = Scheduler::new(8, 2);
        let kernel = BackendKernel::new(k10s_backend::FakeKubernetes::standard());
        let subscription_id = SubscriptionId::new("resource-1");
        let counter = AtomicU64::new(0);
        let cancel = CancellationToken::new();
        let recovery = WatchRecovery::new();
        let generation = recovery.register();
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
            generation,
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

        // Simulated recovery tail: the rebuilt subscription continues on the
        // next sequence followed by a complete fresh snapshot.
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
                events: Vec::new(),
                manifest: String::new(),
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
