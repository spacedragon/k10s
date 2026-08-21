//! Target-neutral wire and route contract for the k10s control protocol.
//!
//! This crate must stay free of platform-specific dependencies such as
//! kube-rs or Tokio; it is shared by the native and WASM clients as well as
//! the server.

pub const PROTOCOL_MAJOR: u16 = 1;
pub const PROTOCOL_MINOR: u16 = 1;
