use super::StorageError;
use rustix::fd::OwnedFd;
use rustix::fs::{AtFlags, Mode, OFlags};
use std::io;
use std::path::Path;

fn open_directory(path: &Path) -> Result<OwnedFd, StorageError> {
    rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)
    .map_err(Into::into)
}
fn private_directory(fd: &OwnedFd) -> Result<(), StorageError> {
    let stat = rustix::fs::fstat(fd).map_err(io::Error::from)?;
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::Directory
        || stat.st_uid != rustix::process::geteuid().as_raw()
        || stat.st_mode & 0o777 != 0o700
    {
        return Err(StorageError::InvalidParent);
    }
    Ok(())
}
fn open_private_directory(path: &Path) -> Result<OwnedFd, StorageError> {
    let fd = open_directory(path)?;
    private_directory(&fd)?;
    Ok(fd)
}
pub(super) fn ensure_private_parent(path: &Path) -> Result<(), StorageError> {
    use std::os::unix::fs::DirBuilderExt;
    match open_private_directory(path) {
        Ok(_) => Ok(()),
        Err(StorageError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700);
            builder.create(path)?;
            validate_private_parent(path)
        }
        Err(error) => Err(error),
    }
}
pub(super) fn validate_private_parent(path: &Path) -> Result<(), StorageError> {
    open_private_directory(path).map(drop)
}
pub(super) fn create_private_directory(path: &Path) -> Result<(), StorageError> {
    let parent = open_private_directory(path.parent().ok_or(StorageError::InvalidParent)?)?;
    let name = path.file_name().ok_or(StorageError::InvalidParent)?;
    rustix::fs::mkdirat(&parent, name, Mode::from_raw_mode(0o700)).map_err(io::Error::from)?;
    let child = rustix::fs::openat(
        &parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    private_directory(&child)
}
pub(super) fn validate_launch_directory(path: &Path) -> Result<(), StorageError> {
    let parent = open_private_directory(path.parent().ok_or(StorageError::InvalidParent)?)?;
    let child = rustix::fs::openat(
        &parent,
        path.file_name().ok_or(StorageError::InvalidParent)?,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    private_directory(&child)
}
pub(super) fn create_private_file<F: FnOnce()>(
    path: &Path,
    bytes: &[u8],
    executable: bool,
    created: F,
) -> Result<(), StorageError> {
    use std::io::Write;
    let directory = open_private_directory(path.parent().ok_or(StorageError::InvalidParent)?)?;
    let name = path.file_name().ok_or(StorageError::InvalidParent)?;
    let mode = if executable { 0o700 } else { 0o600 };
    let fd = rustix::fs::openat(
        &directory,
        name,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(mode),
    )
    .map_err(io::Error::from)?;
    created();
    let mut file = std::fs::File::from(fd);
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
pub(super) fn remove_regular_file(path: &Path) -> Result<(), StorageError> {
    validate_regular_file(path)?;
    let directory = open_private_directory(path.parent().ok_or(StorageError::InvalidParent)?)?;
    let name = path.file_name().ok_or(StorageError::InvalidParent)?;
    rustix::fs::unlinkat(&directory, name, AtFlags::empty()).map_err(io::Error::from)?;
    Ok(())
}
pub(super) fn validate_regular_file(path: &Path) -> Result<(), StorageError> {
    let directory = open_private_directory(path.parent().ok_or(StorageError::InvalidParent)?)?;
    let name = path.file_name().ok_or(StorageError::InvalidParent)?;
    let stat =
        rustix::fs::statat(&directory, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::RegularFile
        || stat.st_uid != rustix::process::geteuid().as_raw()
    {
        return Err(StorageError::InvalidParent);
    }
    Ok(())
}
pub(super) fn remove_empty_directory(path: &Path) -> Result<(), StorageError> {
    let parent = open_private_directory(path.parent().ok_or(StorageError::InvalidParent)?)?;
    let name = path.file_name().ok_or(StorageError::InvalidParent)?;
    let child = rustix::fs::openat(
        &parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    private_directory(&child)?;
    let opened = rustix::fs::fstat(&child).map_err(io::Error::from)?;
    let linked =
        rustix::fs::statat(&parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    if opened.st_dev != linked.st_dev || opened.st_ino != linked.st_ino {
        return Err(StorageError::InvalidParent);
    }
    rustix::fs::unlinkat(&parent, name, AtFlags::REMOVEDIR).map_err(io::Error::from)?;
    Ok(())
}
