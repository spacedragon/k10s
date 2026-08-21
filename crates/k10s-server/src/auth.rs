use k10s_protocol::{Hello, PROTOCOL_MAJOR, PROTOCOL_MINOR, ProtocolVersion};

use crate::config::ServerConfig;

#[derive(Debug)]
pub(crate) struct Negotiated {
    pub(crate) protocol: ProtocolVersion,
    pub(crate) capabilities: Vec<String>,
}

pub(crate) fn authenticate(
    config: &ServerConfig,
    hello: &Hello,
) -> Result<Negotiated, &'static str> {
    if hello.access_token.as_bytes() != config.access_token.as_bytes() {
        return Err("authentication failed");
    }
    if hello.protocol_major != PROTOCOL_MAJOR {
        return Err("incompatible protocol major");
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
