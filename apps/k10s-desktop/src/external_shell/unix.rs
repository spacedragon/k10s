use super::StorageError;
use rustix::fs::{AtFlags, Mode, OFlags};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(super) struct PrivateParent {
    fd: Arc<std::fs::File>,
    path: PathBuf,
}
#[derive(Clone, Debug)]
pub(super) struct PrivateChild {
    parent: PrivateParent,
    fd: Arc<std::fs::File>,
    name: String,
    path: PathBuf,
}

fn private_directory(file: &std::fs::File) -> Result<(), StorageError> {
    let stat = rustix::fs::fstat(file).map_err(io::Error::from)?;
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::Directory
        || stat.st_uid != rustix::process::geteuid().as_raw()
        || stat.st_mode & 0o777 != 0o700
    {
        return Err(StorageError::InvalidParent);
    }
    Ok(())
}
fn open_directory(path: &Path) -> Result<std::fs::File, StorageError> {
    let fd = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let file = std::fs::File::from(fd);
    private_directory(&file)?;
    Ok(file)
}
pub(super) fn ensure_private_parent(path: &Path) -> Result<PrivateParent, StorageError> {
    use std::os::unix::fs::DirBuilderExt;
    let file = match open_directory(path) {
        Ok(file) => file,
        Err(StorageError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            let mut builder = std::fs::DirBuilder::new();
            builder.mode(0o700);
            builder.create(path)?;
            open_directory(path)?
        }
        Err(error) => return Err(error),
    };
    Ok(PrivateParent {
        fd: Arc::new(file),
        path: path.to_owned(),
    })
}
pub(super) fn parent_path(parent: &PrivateParent) -> &Path {
    &parent.path
}
pub(super) fn validate_private_parent(parent: &PrivateParent) -> Result<(), StorageError> {
    private_directory(&parent.fd)?;
    let linked = open_directory(&parent.path)?;
    let retained = rustix::fs::fstat(&parent.fd).map_err(io::Error::from)?;
    let current = rustix::fs::fstat(&linked).map_err(io::Error::from)?;
    if retained.st_dev != current.st_dev || retained.st_ino != current.st_ino {
        return Err(StorageError::InvalidParent);
    }
    Ok(())
}
pub(super) fn create_private_directory(
    parent: &PrivateParent,
    name: &str,
) -> Result<PrivateChild, StorageError> {
    validate_private_parent(parent)?;
    rustix::fs::mkdirat(&parent.fd, name, Mode::from_raw_mode(0o700)).map_err(io::Error::from)?;
    match open_child(parent, name) {
        Ok(child) => Ok(child),
        Err(error) => match rollback_created_directory(parent, name) {
            Ok(()) => Err(error),
            Err(_) => Err(StorageError::RollbackFailed),
        },
    }
}
fn rollback_created_directory(parent: &PrivateParent, name: &str) -> Result<(), StorageError> {
    rustix::fs::unlinkat(&parent.fd, name, AtFlags::REMOVEDIR)
        .map_err(|_| StorageError::RollbackFailed)?;
    Ok(())
}
pub(super) fn open_child(parent: &PrivateParent, name: &str) -> Result<PrivateChild, StorageError> {
    validate_private_parent(parent)?;
    let fd = rustix::fs::openat(
        &parent.fd,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let file = std::fs::File::from(fd);
    private_directory(&file)?;
    let opened = rustix::fs::fstat(&file).map_err(io::Error::from)?;
    let linked =
        rustix::fs::statat(&parent.fd, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    if opened.st_dev != linked.st_dev || opened.st_ino != linked.st_ino {
        return Err(StorageError::InvalidParent);
    }
    Ok(PrivateChild {
        parent: parent.clone(),
        fd: Arc::new(file),
        name: name.to_owned(),
        path: parent.path.join(name),
    })
}
pub(super) fn child_path(child: &PrivateChild) -> &Path {
    &child.path
}
pub(super) fn validate_launch_directory(child: &PrivateChild) -> Result<(), StorageError> {
    validate_private_parent(&child.parent)?;
    private_directory(&child.fd)?;
    let opened = rustix::fs::fstat(&child.fd).map_err(io::Error::from)?;
    let linked = rustix::fs::statat(
        &child.parent.fd,
        child.name.as_str(),
        AtFlags::SYMLINK_NOFOLLOW,
    )
    .map_err(io::Error::from)?;
    if opened.st_dev != linked.st_dev || opened.st_ino != linked.st_ino {
        return Err(StorageError::InvalidParent);
    }
    Ok(())
}
pub(super) fn create_private_file<F: FnOnce()>(
    child: &PrivateChild,
    name: &str,
    bytes: &[u8],
    executable: bool,
    created: F,
) -> Result<(), StorageError> {
    use std::io::Write;
    validate_launch_directory(child)?;
    let mode = if executable { 0o700 } else { 0o600 };
    let fd = rustix::fs::openat(
        &child.fd,
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
pub(super) fn read_regular_file(child: &PrivateChild, name: &str) -> Result<Vec<u8>, StorageError> {
    use std::io::Read;
    let fd = rustix::fs::openat(
        &child.fd,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let mut file = std::fs::File::from(fd);
    validate_open_regular(child, name, &file)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}
fn validate_open_regular(
    child: &PrivateChild,
    name: &str,
    file: &std::fs::File,
) -> Result<(), StorageError> {
    let opened = rustix::fs::fstat(file).map_err(io::Error::from)?;
    let linked =
        rustix::fs::statat(&child.fd, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    if rustix::fs::FileType::from_raw_mode(opened.st_mode) != rustix::fs::FileType::RegularFile
        || opened.st_uid != rustix::process::geteuid().as_raw()
        || opened.st_dev != linked.st_dev
        || opened.st_ino != linked.st_ino
    {
        return Err(StorageError::InvalidParent);
    }
    Ok(())
}
pub(super) fn validate_regular_file(child: &PrivateChild, name: &str) -> Result<(), StorageError> {
    open_regular(child, name).map(drop)
}
fn open_regular(child: &PrivateChild, name: &str) -> Result<std::fs::File, StorageError> {
    let fd = rustix::fs::openat(
        &child.fd,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)?;
    let file = std::fs::File::from(fd);
    validate_open_regular(child, name, &file)?;
    Ok(file)
}
pub(super) fn remove_regular_file(child: &PrivateChild, name: &str) -> Result<(), StorageError> {
    let _retained_identity = open_regular(child, name)?;
    rustix::fs::unlinkat(&child.fd, name, AtFlags::empty()).map_err(io::Error::from)?;
    Ok(())
}
pub(super) fn remove_empty_directory(child: &PrivateChild) -> Result<(), StorageError> {
    validate_launch_directory(child)?;
    let opened = rustix::fs::fstat(&child.fd).map_err(io::Error::from)?;
    let linked = rustix::fs::statat(
        &child.parent.fd,
        child.name.as_str(),
        AtFlags::SYMLINK_NOFOLLOW,
    )
    .map_err(io::Error::from)?;
    if opened.st_dev != linked.st_dev || opened.st_ino != linked.st_ino {
        return Err(StorageError::InvalidParent);
    }
    rustix::fs::unlinkat(&child.parent.fd, child.name.as_str(), AtFlags::REMOVEDIR)
        .map_err(io::Error::from)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failed_directory_rollback_is_typed() {
        let root = std::env::temp_dir().join(format!("k10s-unix-rollback-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let parent = ensure_private_parent(&root).unwrap();
        rustix::fs::mkdirat(&parent.fd, "child", Mode::from_raw_mode(0o700)).unwrap();
        std::fs::write(root.join("child/blocker"), "x").unwrap();
        assert!(matches!(
            rollback_created_directory(&parent, "child"),
            Err(StorageError::RollbackFailed)
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
}
