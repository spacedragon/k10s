//! Exact-identity, bounded Kubernetes Pod exec sessions.

use std::collections::HashMap;
use std::sync::Mutex;

use futures_util::SinkExt;
use k8s_openapi::api::core::v1::Pod;
use kube::ResourceExt;
use kube::api::{Api, AttachParams, TerminalSize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};

use crate::port::{
    BackendError, BackendEvent, QueryResult, StreamGrant, StreamInput, StreamKind, StreamRouteKind,
    SubscriptionHandle,
};
use crate::stream::{STREAM_QUEUE_CAPACITY, StreamChunk, StreamOrigin};

use super::KubeAdapter;

const INPUT_CAPACITY: usize = 32;
const SESSION_CAPACITY: usize = 32;
const MAX_COMMAND_ARGS: usize = 64;
const MAX_COMMAND_BYTES: usize = 16 * 1024;
const OUTPUT_CHUNK_BYTES: usize = 16 * 1024;

#[derive(Debug)]
enum ExecInput {
    Stdin(String),
    Resize { cols: u16, rows: u16 },
}

/// Bounded input authorities for live remote exec sessions.
#[derive(Debug, Default)]
pub(super) struct ExecSessions(Mutex<HashMap<String, mpsc::Sender<ExecInput>>>);

impl ExecSessions {
    fn insert(&self, id: String, sender: mpsc::Sender<ExecInput>) -> Result<(), BackendError> {
        let mut sessions = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if sessions.len() >= SESSION_CAPACITY {
            tracing::warn!(capacity = SESSION_CAPACITY, "exec session budget exhausted");
            return Err(BackendError::Conflict(
                "the exec session budget is exhausted".into(),
            ));
        }
        sessions.insert(id, sender);
        Ok(())
    }

    fn remove(&self, id: &str) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }

    pub(super) async fn send(&self, id: &str, input: StreamInput) -> Result<(), BackendError> {
        let sender = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
            .ok_or_else(|| BackendError::Conflict("the stream session is not active".into()))?;
        let input = match input {
            StreamInput::Stdin(text) => ExecInput::Stdin(text),
            StreamInput::Resize { cols, rows } => ExecInput::Resize {
                cols: u16::try_from(cols).map_err(|_| {
                    BackendError::Conflict("terminal width exceeds Kubernetes bounds".into())
                })?,
                rows: u16::try_from(rows).map_err(|_| {
                    BackendError::Conflict("terminal height exceeds Kubernetes bounds".into())
                })?,
            },
        };
        sender.try_send(input).map_err(|_| {
            tracing::warn!(capacity = INPUT_CAPACITY, "exec input queue full or closed");
            BackendError::Conflict("the exec input queue is full or closed".into())
        })
    }
}

impl KubeAdapter {
    pub(super) async fn issue_exec_ticket(
        &self,
        stream: StreamKind,
    ) -> Result<QueryResult, BackendError> {
        let StreamKind::Exec {
            context,
            namespace,
            pod,
            uid: requested_uid,
            container,
            command,
            tty,
        } = stream
        else {
            unreachable!()
        };
        validate_command(&command)?;
        let client = self.cluster_client(&context).await?;
        let api: Api<Pod> = Api::namespaced(client, &namespace);
        let current = api.get(&pod).await.map_err(exec_error)?;
        let observed_uid = current
            .uid()
            .ok_or_else(|| BackendError::Conflict("the pod has no UID".into()))?;
        if !requested_uid.is_empty() && requested_uid != observed_uid {
            return Err(BackendError::Conflict("the pod was replaced".into()));
        }
        if !super::logs::has_container(&current, &container) {
            return Err(BackendError::NotFound);
        }
        let bound = StreamKind::Exec {
            context,
            namespace,
            pod,
            uid: observed_uid,
            container,
            command,
            tty,
        };
        let ticket_id = self.stream_tickets.issue(bound.clone());
        Ok(QueryResult::StreamTicket(StreamGrant {
            ticket_id,
            stream: bound,
        }))
    }

    pub(super) async fn redeem_exec_ticket(
        &self,
        ticket_id: String,
    ) -> Result<SubscriptionHandle, BackendError> {
        let bound = self
            .stream_tickets
            .redeem_for(&ticket_id, StreamRouteKind::Exec)?;
        let StreamKind::Exec {
            context,
            namespace,
            pod,
            uid,
            container,
            command,
            tty,
        } = &bound
        else {
            unreachable!()
        };
        let client = self.cluster_client(context).await?;
        let api: Api<Pod> = Api::namespaced(client, namespace);
        let current = api.get(pod).await.map_err(exec_error)?;
        if current.uid().as_deref() != Some(uid.as_str()) {
            return Err(BackendError::Conflict(
                "the pod was replaced after ticket issuance".into(),
            ));
        }
        if !super::logs::has_container(&current, container) {
            return Err(BackendError::NotFound);
        }

        let params = AttachParams::default()
            .container(container)
            .stdin(true)
            .stdout(true)
            .stderr(!*tty)
            .tty(*tty)
            .max_stdin_buf_size(OUTPUT_CHUNK_BYTES)
            .max_stdout_buf_size(OUTPUT_CHUNK_BYTES)
            .max_stderr_buf_size(OUTPUT_CHUNK_BYTES);
        let mut attached = api
            .exec(pod, command.clone(), &params)
            .await
            .map_err(exec_error)?;
        let stdin = attached.stdin().ok_or_else(|| {
            BackendError::Internal("kubernetes exec omitted the stdin channel".into())
        })?;
        let stdout = attached.stdout().ok_or_else(|| {
            BackendError::Internal("kubernetes exec omitted the stdout channel".into())
        })?;
        let stderr = if *tty { None } else { attached.stderr() };
        let resize = if *tty { attached.terminal_size() } else { None };
        let status = attached.take_status().ok_or_else(|| {
            BackendError::Internal("kubernetes exec omitted the status channel".into())
        })?;
        let (sender, receiver) = broadcast::channel(STREAM_QUEUE_CAPACITY);
        let (input_sender, input_receiver) = mpsc::channel(INPUT_CAPACITY);
        if let Err(error) = self.exec_sessions.insert(ticket_id.clone(), input_sender) {
            attached.abort();
            return Err(error);
        }
        let sessions = self.exec_sessions.clone();
        let session_id = ticket_id.clone();
        let tty = *tty;
        tokio::spawn(async move {
            run_exec(
                attached,
                stdin,
                stdout,
                stderr,
                resize,
                status,
                input_receiver,
                sender,
                tty,
            )
            .await;
            sessions.remove(&session_id);
        });
        Ok(SubscriptionHandle::with_events("kube-exec-stream", receiver).with_stream(bound))
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_exec<In, Out, ErrOut, Resize, StatusFuture>(
    attached: kube::api::AttachedProcess,
    mut stdin: In,
    stdout: Out,
    stderr: Option<ErrOut>,
    mut resize: Option<Resize>,
    status: StatusFuture,
    mut inputs: mpsc::Receiver<ExecInput>,
    sender: broadcast::Sender<BackendEvent>,
    tty: bool,
) where
    In: tokio::io::AsyncWrite + Unpin + Send + 'static,
    Out: AsyncRead + Unpin + Send + 'static,
    ErrOut: AsyncRead + Unpin + Send + 'static,
    Resize: futures_util::Sink<TerminalSize> + Unpin + Send + 'static,
    StatusFuture: std::future::Future<Output = Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Status>>
        + Send,
{
    let stdout_origin = if tty {
        StreamOrigin::TtyOutput
    } else {
        StreamOrigin::Stdout
    };
    let stdout_task = tokio::spawn(pump_output(stdout, sender.clone(), stdout_origin));
    let stderr_task = stderr
        .map(|reader| tokio::spawn(pump_output(reader, sender.clone(), StreamOrigin::Stderr)));
    tokio::pin!(status);
    let status = loop {
        tokio::select! {
            biased;
            _ = sender.closed() => {
                tracing::debug!(tty, "exec consumer disconnected; aborting upstream");
                attached.abort();
                stdout_task.abort();
                if let Some(task) = &stderr_task { task.abort(); }
                return;
            }
            status = &mut status => break status,
            Some(input) = inputs.recv() => match input {
                ExecInput::Stdin(text) => {
                    if !forward_stdin(&mut stdin, text.as_bytes(), &sender).await {
                        attached.abort();
                        stdout_task.abort();
                        if let Some(task) = &stderr_task { task.abort(); }
                        return;
                    }
                }
                ExecInput::Resize { cols, rows } => {
                    let Some(channel) = resize.as_mut() else { continue };
                    if !forward_resize(channel, TerminalSize { width: cols, height: rows }, &sender).await {
                        attached.abort();
                        stdout_task.abort();
                        if let Some(task) = &stderr_task { task.abort(); }
                        return;
                    }
                }
            }
        }
    };
    drop(stdin);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), stdout_task).await;
    if let Some(task) = stderr_task {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
    }
    let exit_code = status.as_ref().map_or(1, status_exit_code);
    let origin = if tty {
        StreamOrigin::TtyOutput
    } else {
        StreamOrigin::Stdout
    };
    let _ = sender.send(BackendEvent::Stream(StreamChunk {
        origin,
        text: String::new(),
        exit_code: Some(exit_code),
    }));
    attached.abort();
    tracing::debug!(exit_code, tty, "exec session ended");
}

async fn forward_stdin<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    sender: &broadcast::Sender<BackendEvent>,
) -> bool {
    tokio::select! {
        _ = sender.closed() => false,
        result = async {
            writer.write_all(bytes).await?;
            writer.flush().await
        } => result.is_ok(),
    }
}

async fn forward_resize<S: futures_util::Sink<TerminalSize> + Unpin>(
    channel: &mut S,
    size: TerminalSize,
    sender: &broadcast::Sender<BackendEvent>,
) -> bool {
    tokio::select! {
        _ = sender.closed() => false,
        result = channel.send(size) => result.is_ok(),
    }
}

async fn pump_output<R: AsyncRead + Unpin>(
    mut reader: R,
    sender: broadcast::Sender<BackendEvent>,
    origin: StreamOrigin,
) {
    let mut buffer = vec![0_u8; OUTPUT_CHUNK_BYTES];
    let mut undecoded = Vec::new();
    loop {
        tokio::select! {
            _ = sender.closed() => break,
            read = reader.read(&mut buffer) => match read {
                Ok(0) => {
                    if let Some(text) = super::logs::decode_utf8(&mut undecoded, true)
                        && sender.send(BackendEvent::Stream(StreamChunk { origin, text, exit_code: None })).is_err()
                    {
                        break;
                    }
                    break;
                }
                Err(_) => break,
                Ok(count) => {
                    undecoded.extend_from_slice(&buffer[..count]);
                    if let Some(text) = super::logs::decode_utf8(&mut undecoded, false)
                        && sender.send(BackendEvent::Stream(StreamChunk { origin, text, exit_code: None })).is_err()
                    {
                        break;
                    }
                }
            }
        }
    }
}

fn validate_command(command: &[String]) -> Result<(), BackendError> {
    let bytes = command.iter().map(String::len).sum::<usize>();
    if command.is_empty()
        || command[0].is_empty()
        || command.len() > MAX_COMMAND_ARGS
        || bytes > MAX_COMMAND_BYTES
    {
        return Err(BackendError::Conflict(
            "the exec command is empty or exceeds configured bounds".into(),
        ));
    }
    Ok(())
}

fn status_exit_code(status: &k8s_openapi::apimachinery::pkg::apis::meta::v1::Status) -> i32 {
    if status.status.as_deref() == Some("Success") {
        return 0;
    }
    status
        .details
        .as_ref()
        .and_then(|details| details.causes.as_ref())
        .and_then(|causes| {
            causes
                .iter()
                .find(|cause| cause.reason.as_deref() == Some("ExitCode"))
        })
        .and_then(|cause| cause.message.as_deref())
        .and_then(|message| message.parse().ok())
        .unwrap_or(1)
}

fn exec_error(error: kube::Error) -> BackendError {
    match error {
        kube::Error::Api(status) if status.code == 403 => BackendError::Forbidden,
        kube::Error::Api(status) if status.code == 404 => BackendError::NotFound,
        kube::Error::UpgradeConnection(kube::client::UpgradeConnectionError::ProtocolSwitch(
            status,
        )) if status.as_u16() == 403 => BackendError::Forbidden,
        kube::Error::UpgradeConnection(kube::client::UpgradeConnectionError::ProtocolSwitch(
            status,
        )) if status.as_u16() == 404 => BackendError::NotFound,
        _ => BackendError::Internal("kubernetes exec session unavailable".into()),
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Status, StatusCause, StatusDetails};

    #[test]
    fn command_and_exit_status_are_bounded_and_typed() {
        assert!(validate_command(&[]).is_err());
        assert!(validate_command(&["/bin/sh".into()]).is_ok());
        let status = Status {
            status: Some("Failure".into()),
            reason: Some("NonZeroExitCode".into()),
            details: Some(StatusDetails {
                causes: Some(vec![StatusCause {
                    reason: Some("ExitCode".into()),
                    message: Some("127".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(status_exit_code(&status), 127);
    }

    #[test]
    fn live_session_registry_has_an_exact_hard_capacity() {
        let sessions = ExecSessions::default();
        let mut receivers = Vec::new();
        for index in 0..SESSION_CAPACITY {
            let (sender, receiver) = mpsc::channel(1);
            sessions.insert(format!("session-{index}"), sender).unwrap();
            receivers.push(receiver);
        }
        let (sender, _receiver) = mpsc::channel(1);
        assert!(matches!(
            sessions.insert("overflow".into(), sender),
            Err(BackendError::Conflict(_))
        ));
        sessions.remove("session-0");
        let (sender, _receiver) = mpsc::channel(1);
        sessions.insert("replacement".into(), sender).unwrap();
        drop(receivers);
    }

    #[tokio::test]
    async fn input_queue_and_terminal_dimensions_fail_closed_at_their_bounds() {
        let sessions = ExecSessions::default();
        let (sender, _receiver) = mpsc::channel(INPUT_CAPACITY);
        sessions.insert("exec".into(), sender).unwrap();
        for _ in 0..INPUT_CAPACITY {
            sessions
                .send("exec", StreamInput::Stdin("x".into()))
                .await
                .unwrap();
        }
        assert!(matches!(
            sessions
                .send("exec", StreamInput::Stdin("overflow".into()))
                .await,
            Err(BackendError::Conflict(_))
        ));

        let (sender, _receiver) = mpsc::channel(1);
        sessions.remove("exec");
        sessions.insert("resize".into(), sender).unwrap();
        assert!(matches!(
            sessions
                .send(
                    "resize",
                    StreamInput::Resize {
                        cols: u32::from(u16::MAX) + 1,
                        rows: 24,
                    },
                )
                .await,
            Err(BackendError::Conflict(_))
        ));
    }

    struct PendingWriter;

    impl tokio::io::AsyncWrite for PendingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn consumer_disconnect_cancels_backpressured_stdin() {
        let (sender, receiver) = broadcast::channel(1);
        let mut writer = PendingWriter;
        let forwarding = forward_stdin(&mut writer, b"blocked", &sender);
        drop(receiver);
        assert!(
            !tokio::time::timeout(std::time::Duration::from_millis(100), forwarding)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn output_preserves_utf8_across_arbitrary_reads() {
        let mut input = vec![b'x'; OUTPUT_CHUNK_BYTES - 1];
        input.extend_from_slice("🦀done".as_bytes());
        let reader = &input[..];
        let (sender, mut receiver) = broadcast::channel(4);
        pump_output(reader, sender, StreamOrigin::Stdout).await;

        let mut output = String::new();
        while let Ok(BackendEvent::Stream(chunk)) = receiver.try_recv() {
            output.push_str(&chunk.text);
        }
        assert_eq!(
            output,
            format!("{}🦀done", "x".repeat(OUTPUT_CHUNK_BYTES - 1))
        );
    }
}
