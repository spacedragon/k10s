use k10s_protocol::{ErrorCode, Hello, PROTOCOL_MAJOR, PROTOCOL_MINOR, ProtocolVersion};

use crate::config::ServerConfig;

#[derive(Debug)]
pub(crate) struct Negotiated {
    pub(crate) protocol: ProtocolVersion,
    pub(crate) capabilities: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthenticationError {
    Unauthorized,
    IncompatibleProtocol { client_major: u16 },
}

impl AuthenticationError {
    pub(crate) fn code(self) -> ErrorCode {
        match self {
            Self::Unauthorized => ErrorCode::Unauthorized,
            Self::IncompatibleProtocol { .. } => ErrorCode::IncompatibleProtocol,
        }
    }

    pub(crate) fn safe_reason(self) -> &'static str {
        match self {
            Self::Unauthorized => "authentication failed",
            Self::IncompatibleProtocol { .. } => "incompatible protocol major",
        }
    }
}

pub(crate) fn authenticate(
    config: &ServerConfig,
    hello: &Hello,
) -> Result<Negotiated, AuthenticationError> {
    if hello.access_token.as_bytes() != config.access_token.as_bytes() {
        return Err(AuthenticationError::Unauthorized);
    }
    if hello.protocol_major != PROTOCOL_MAJOR {
        return Err(AuthenticationError::IncompatibleProtocol {
            client_major: hello.protocol_major,
        });
    }
    Ok(Negotiated {
        protocol: ProtocolVersion {
            major: PROTOCOL_MAJOR,
            minor: hello.protocol_minor.min(PROTOCOL_MINOR),
        },
        capabilities: hello
            .capabilities
            .iter()
            .filter(|item| config.capabilities.contains(item))
            .cloned()
            .collect(),
    })
}
