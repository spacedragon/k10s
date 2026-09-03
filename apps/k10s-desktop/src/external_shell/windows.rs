//! Windows secure-storage primitives. Handle-based owner/ACL and reparse checks
//! are kept here so non-Windows builds never weaken their Unix counterpart.

use std::io;
use std::path::Path;

use super::{KubectlExecCommand, StorageError};

pub(super) fn ensure_private_parent(path: &Path) -> Result<(), StorageError> {
    if !path.exists() {
        std::fs::create_dir(path)?;
    }
    validate_private_parent(path)
}

pub(super) fn validate_private_parent(path: &Path) -> Result<(), StorageError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        Err(StorageError::InvalidParent)
    }
}

pub(super) fn create_private_directory(path: &Path) -> Result<(), StorageError> {
    std::fs::create_dir(path)?;
    validate_launch_directory(path)
}

pub(super) fn validate_launch_directory(path: &Path) -> Result<(), StorageError> {
    validate_private_parent(path)
}

pub(super) fn create_private_file(
    path: &Path,
    bytes: &[u8],
    _executable: bool,
) -> Result<(), StorageError> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

pub(super) fn remove_regular_file(path: &Path) -> Result<(), StorageError> {
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(StorageError::InvalidParent);
    }
    std::fs::remove_file(path)?;
    Ok(())
}

pub(super) fn remove_empty_directory(path: &Path) -> Result<(), StorageError> {
    validate_launch_directory(path)?;
    std::fs::remove_dir(path)?;
    Ok(())
}

pub(super) fn render_with_cleanup(
    command: &KubectlExecCommand<'_>,
    manifest: &Path,
    directory: &Path,
) -> Result<Vec<u8>, io::Error> {
    let mut body = command.render_powershell().map_err(io::Error::other)?;
    let exit = "exit $K10sStatus\r\n";
    let manifest = manifest.to_string_lossy().replace('\'', "''");
    let directory = directory.to_string_lossy().replace('\'', "''");
    let cleanup = format!(
        "Remove-Item -LiteralPath $PSCommandPath -Force -ErrorAction SilentlyContinue\r\nRemove-Item -LiteralPath '{manifest}' -Force -ErrorAction SilentlyContinue\r\nRemove-Item -LiteralPath '{directory}' -Force -ErrorAction SilentlyContinue\r\nexit $K10sStatus\r\n"
    );
    body = body.strip_suffix(exit).unwrap_or(&body).to_owned() + &cleanup;
    let mut encoded = vec![0xEF, 0xBB, 0xBF];
    encoded.extend_from_slice(body.as_bytes());
    Ok(encoded)
}

pub(super) fn launch(script: &super::TemporaryShellScript) -> Result<(), StorageError> {
    use std::os::windows::process::CommandExt;
    const CREATE_NEW_CONSOLE: u32 = 0x0000_0010;
    let result = std::process::Command::new("powershell.exe")
        .args([
            "-NoLogo",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(script.path())
        .creation_flags(CREATE_NEW_CONSOLE)
        .spawn();
    match result {
        Ok(_) => Ok(()),
        Err(_) => {
            let _ = script.cleanup();
            Err(StorageError::NoTerminalLauncher)
        }
    }
}
