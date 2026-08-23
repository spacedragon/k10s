use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use k10s_server::{StandaloneConfig, StandaloneConfigError};

#[test]
fn rejects_non_loopback_without_a_token_before_bind() {
    let result = StandaloneConfig::new(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 8080)),
        None,
        PathBuf::from("dist"),
    );

    assert_eq!(result, Err(StandaloneConfigError::TokenRequired));
}

#[test]
fn permits_loopback_without_a_token_and_non_loopback_with_one() {
    let loopback = StandaloneConfig::new(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
        None,
        PathBuf::from("dist"),
    )
    .unwrap();
    assert_eq!(loopback.bind_addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));

    let public = StandaloneConfig::new(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 8080)),
        Some("explicit-secret".to_owned()),
        PathBuf::from("dist"),
    )
    .unwrap();
    assert_eq!(public.access_token(), "explicit-secret");
    assert!(!format!("{public:?}").contains("explicit-secret"));
}

#[test]
fn token_is_never_accepted_from_a_url() {
    let result = StandaloneConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        Some("secret".to_owned()),
        PathBuf::from("dist?token=secret"),
    );

    assert_eq!(result, Err(StandaloneConfigError::InvalidDistDirectory));
}
