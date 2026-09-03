use std::io;
use std::path::Path;

use super::StorageError;

fn metadata_is_private_directory(path: &Path) -> Result<bool, io::Error> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path)?;
    Ok(metadata.file_type().is_dir()
        && metadata.uid() == rustix::process::geteuid().as_raw()
        && metadata.mode() & 0o777 == 0o700)
}

pub(super) fn ensure_private_parent(path: &Path) -> Result<(), StorageError> {
    use std::os::unix::fs::DirBuilderExt;
    match std::fs::symlink_metadata(path) {
        Ok(_) => validate_private_parent(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700);
            builder.create(path)?;
            validate_private_parent(path)
        }
        Err(error) => Err(error.into()),
    }
}
pub(super) fn validate_private_parent(path: &Path) -> Result<(), StorageError> {
    if metadata_is_private_directory(path).unwrap_or(false) {
        Ok(())
    } else {
        Err(StorageError::InvalidParent)
    }
}
pub(super) fn create_private_directory(path: &Path) -> Result<(), StorageError> {
    use std::os::unix::fs::DirBuilderExt;
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path)?;
    validate_launch_directory(path)
}
pub(super) fn validate_launch_directory(path: &Path) -> Result<(), StorageError> {
    if metadata_is_private_directory(path).unwrap_or(false) {
        Ok(())
    } else {
        Err(StorageError::InvalidParent)
    }
}
pub(super) fn create_private_file(
    path: &Path,
    bytes: &[u8],
    executable: bool,
) -> Result<(), StorageError> {
    use std::io::Write;
    let parent = path.parent().ok_or(StorageError::InvalidParent)?;
    validate_launch_directory(parent)?;
    let directory = std::fs::File::open(parent)?;
    let name = path.file_name().ok_or(StorageError::InvalidParent)?;
    let mode = if executable { 0o700 } else { 0o600 };
    let fd = rustix::fs::openat(
        &directory,
        name,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(mode),
    )
    .map_err(io::Error::from)?;
    let mut file = std::fs::File::from(fd);
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
pub(super) fn remove_regular_file(path: &Path) -> Result<(), StorageError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
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
