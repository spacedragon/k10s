use std::fs;
use std::path::Path;

use k10s_backend::BackendMode;
use k10s_desktop::launch_embedded_server_with_mode;

#[test]
fn desktop_uses_fresh_high_entropy_loopback_credentials() {
    let mut first = launch_embedded_server_with_mode(&BackendMode::Fake).unwrap();
    let mut second = launch_embedded_server_with_mode(&BackendMode::Fake).unwrap();
    for server in [&first, &second] {
        assert!(server.local_addr().ip().is_loopback());
        assert_eq!(
            server.access_token().len(),
            43,
            "32 bytes, base64url no-pad"
        );
        assert!(
            server
                .access_token()
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' })
        );
        assert!(!server.control_url().contains(server.access_token()));
    }
    assert_ne!(first.access_token(), second.access_token());
    first.shutdown().unwrap();
    second.shutdown().unwrap();
}

#[test]
fn native_packager_outputs_are_present_when_requested() {
    let Some(package_dir) = std::env::var_os("K10S_PACKAGE_DIR") else {
        return;
    };
    let package_dir = Path::new(&package_dir);
    let package_dir = if package_dir.is_absolute() {
        package_dir.to_owned()
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(package_dir)
    };
    let entries = fs::read_dir(&package_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    let has = |suffix: &str| {
        entries.iter().any(|path| {
            path.to_string_lossy()
                .to_ascii_lowercase()
                .ends_with(suffix)
        })
    };
    match std::env::consts::OS {
        "linux" => {
            assert!(has(".deb"), "missing Debian package in {entries:?}");
            assert!(has(".appimage"), "missing AppImage in {entries:?}");
        }
        "windows" => {
            assert!(has(".msi"), "missing MSI in {entries:?}");
            assert!(has(".exe"), "missing NSIS installer in {entries:?}");
        }
        "macos" => {
            assert!(has(".app"), "missing app bundle in {entries:?}");
            assert!(has(".dmg"), "missing DMG in {entries:?}");
        }
        other => panic!("unsupported packaging host {other}"),
    }
}
