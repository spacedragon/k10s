//! Exec credential failure classification and safe operator diagnostics.

use kube::client::AuthError;

use crate::port::BackendError;

const MAX_DIAGNOSTIC_BYTES: usize = 2 * 1024;

pub(super) fn classify_kube_error(error: &kube::Error) -> Option<String> {
    match error {
        kube::Error::Auth(error) => classify_exec_auth_error(error),
        _ => None,
    }
}

pub(super) fn context_unavailable(error: &kube::Error) -> Option<BackendError> {
    let kube::Error::Service(error) = error else {
        return None;
    };
    let marker = error.downcast_ref::<super::auth_observer::ContextUnavailableMarker>()?;
    Some(BackendError::ContextUnavailable {
        context: marker.context.clone(),
        reason: marker.reason.clone(),
    })
}

pub(super) fn classify_exec_auth_error(error: &AuthError) -> Option<String> {
    let reason = match error {
        AuthError::MissingCommand => "credential plugin command is missing".to_owned(),
        AuthError::AuthExecStart(error) => format!(
            "credential plugin could not start ({})",
            match error.kind() {
                std::io::ErrorKind::NotFound => "command not found",
                std::io::ErrorKind::PermissionDenied => "permission denied",
                _ => "operating system error",
            }
        ),
        AuthError::AuthExecRun { status, out, .. } => {
            let status = status
                .code()
                .map_or_else(|| "terminated by signal".into(), |code| code.to_string());
            let stderr = sanitize_stderr(&out.stderr);
            if stderr.is_empty() {
                format!("credential plugin exited with status {status}")
            } else {
                format!("credential plugin exited with status {status}: {stderr}")
            }
        }
        AuthError::AuthExecParse(_) => "credential plugin returned invalid output".to_owned(),
        AuthError::AuthExecSerialize(_) => {
            "credential plugin input could not be serialized".to_owned()
        }
        AuthError::AuthExec(_) => "credential plugin execution failed".to_owned(),
        AuthError::ExecPluginFailed => {
            "credential plugin response did not contain credentials".to_owned()
        }
        AuthError::ExecMissingClusterInfo => {
            "credential plugin requires unavailable cluster information".to_owned()
        }
        _ => return None,
    };
    Some(reason)
}

fn sanitize_stderr(stderr: &[u8]) -> String {
    let normalized = String::from_utf8_lossy(stderr)
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    let mut redact_next = false;
    let mut words = Vec::new();
    for word in normalized.split_whitespace() {
        if redact_next {
            words.push("[redacted]".to_owned());
            redact_next = false;
            continue;
        }
        if word.eq_ignore_ascii_case("bearer") {
            words.push("Bearer".to_owned());
            redact_next = true;
            continue;
        }
        if looks_like_jwt(word) || looks_like_pem(word) {
            words.push("[redacted]".to_owned());
            continue;
        }
        if let Some((key, _)) = word.split_once('=')
            && is_sensitive_key(key)
        {
            words.push(format!("{key}=[redacted]"));
            continue;
        }
        words.push(word.to_owned());
    }
    truncate_utf8(&words.join(" "), MAX_DIAGNOSTIC_BYTES)
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    [
        "token",
        "password",
        "secret",
        "credential",
        "apikey",
        "api_key",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

fn looks_like_jwt(word: &str) -> bool {
    let mut segments = word.split('.');
    matches!(
        (segments.next(), segments.next(), segments.next(), segments.next()),
        (Some(header), Some(payload), Some(signature), None)
            if header.len() >= 8 && payload.len() >= 8 && signature.len() >= 8
    )
}

fn looks_like_pem(word: &str) -> bool {
    word.contains("PRIVATE") || word.contains("CERTIFICATE") || word.starts_with("-----BEGIN")
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", value[..end].trim_end())
}

#[cfg(test)]
mod tests {
    use std::process::{ExitStatus, Output};

    use super::{MAX_DIAGNOSTIC_BYTES, classify_exec_auth_error};

    #[cfg(unix)]
    fn failed_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(17 << 8)
    }

    #[test]
    #[cfg(unix)]
    fn exec_auth_failures_are_safe() {
        let stdout_secret = "stdout-secret-a45f";
        let stderr = format!(
            "denied\u{1b}[31m Bearer bearer-secret TOKEN=assignment-secret {} {}",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.signature-value",
            "x".repeat(MAX_DIAGNOSTIC_BYTES * 2)
        );
        let error = kube::client::AuthError::AuthExecRun {
            cmd: "command-secret".into(),
            status: failed_status(),
            out: Output {
                status: failed_status(),
                stdout: stdout_secret.as_bytes().to_vec(),
                stderr: stderr.into_bytes(),
            },
        };

        let reason = classify_exec_auth_error(&error).expect("exec failure classifies");
        assert!(reason.contains("status 17: denied"));
        assert!(reason.contains("[redacted]"));
        for secret in [
            stdout_secret,
            "bearer-secret",
            "assignment-secret",
            "command-secret",
            "eyJhbGciOiJIUzI1NiJ9",
        ] {
            assert!(!reason.contains(secret), "secret leaked: {secret}");
        }
        assert!(reason.len() <= MAX_DIAGNOSTIC_BYTES + 96);
        assert!(reason.is_char_boundary(reason.len()));
    }

    #[test]
    fn non_exec_auth_errors_are_not_context_failures() {
        let error = kube::client::AuthError::UnrefreshableTokenResponse;
        assert_eq!(classify_exec_auth_error(&error), None);
    }
}
