//! Windows secure-storage primitives. Handle-based owner/ACL and reparse checks
//! are kept here so non-Windows builds never weaken their Unix counterpart.
#![allow(unsafe_code)]

use std::io;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SDDL_REVISION_1, SE_FILE_OBJECT,
};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACL, ACL_SIZE_INFORMATION, AclSizeInformation, DACL_SECURITY_INFORMATION,
    EqualSid, GetAce, GetAclInformation, GetTokenInformation, OWNER_SECURITY_INFORMATION, PSID,
    SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    CREATE_NEW, CreateDirectoryW, CreateFileW, FILE_ALL_ACCESS, FILE_ATTRIBUTE_DIRECTORY,
    FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT, FILE_ATTRIBUTE_TAG_INFO,
    FILE_DISPOSITION_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO,
    FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    FileAttributeTagInfo, FileDispositionInfo, FileIdInfo, GetFileInformationByHandleEx,
    OPEN_EXISTING, SetFileInformationByHandle,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

use super::{KubectlExecCommand, StorageError};

struct Handle(HANDLE);
impl std::fmt::Debug for Handle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Handle(REDACTED)")
    }
}
impl Drop for Handle {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}
#[derive(Clone)]
pub(super) struct PrivateParent {
    handle: Arc<Handle>,
    path: PathBuf,
    id: FILE_ID_INFO,
}
impl std::fmt::Debug for PrivateParent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateParent")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
#[derive(Clone)]
pub(super) struct PrivateChild {
    parent: PrivateParent,
    handle: Arc<Handle>,
    path: PathBuf,
    name: String,
    id: FILE_ID_INFO,
}
impl std::fmt::Debug for PrivateChild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateChild")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
fn file_id(handle: HANDLE) -> Result<FILE_ID_INFO, StorageError> {
    let mut id = FILE_ID_INFO::default();
    if unsafe {
        GetFileInformationByHandleEx(
            handle,
            FileIdInfo,
            (&raw mut id).cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    Ok(id)
}
fn same_id(left: &FILE_ID_INFO, right: &FILE_ID_INFO) -> bool {
    left.VolumeSerialNumber == right.VolumeSerialNumber
        && left.FileId.Identifier == right.FileId.Identifier
}
struct Descriptor(*mut core::ffi::c_void);
impl Drop for Descriptor {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::LocalFree(self.0);
        }
    }
}
fn owner_security_descriptor() -> Result<Descriptor, StorageError> {
    let mut token = std::ptr::null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(io::Error::last_os_error().into());
    }
    let token = Handle(token);
    let mut length = 0;
    unsafe {
        GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &raw mut length);
    }
    let mut bytes = vec![0u8; length as usize];
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            bytes.as_mut_ptr().cast(),
            length,
            &raw mut length,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    let sid = unsafe { (*bytes.as_ptr().cast::<TOKEN_USER>()).User.Sid };
    let mut sid_text = std::ptr::null_mut();
    if unsafe { ConvertSidToStringSidW(sid, &raw mut sid_text) } == 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut count = 0;
    while unsafe { *sid_text.add(count) } != 0 {
        count += 1;
    }
    let sid_string = String::from_utf16(unsafe { std::slice::from_raw_parts(sid_text, count) })
        .map_err(io::Error::other)?;
    unsafe {
        windows_sys::Win32::Foundation::LocalFree(sid_text.cast());
    }
    let sddl: Vec<u16> = format!("O:{sid_string}D:P(A;;FA;;;{sid_string})")
        .encode_utf16()
        .chain(Some(0))
        .collect();
    let mut descriptor = std::ptr::null_mut();
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            SDDL_REVISION_1,
            &raw mut descriptor,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    Ok(Descriptor(descriptor))
}
fn security_attributes(descriptor: &mut Descriptor) -> SECURITY_ATTRIBUTES {
    SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    }
}
fn wide(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}
fn open_no_reparse(path: &Path) -> Result<Handle, StorageError> {
    let handle = raw_open_no_reparse(path)?;
    validate_owner_acl(handle.0)?;
    Ok(handle)
}
fn raw_open_no_reparse(path: &Path) -> Result<Handle, StorageError> {
    let path = wide(path);
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            FILE_READ_ATTRIBUTES | 0x0002_0000 | 0x0001_0000,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error().into());
    }
    let handle = Handle(handle);
    let mut tag = FILE_ATTRIBUTE_TAG_INFO::default();
    let ok = unsafe {
        GetFileInformationByHandleEx(
            handle.0,
            FileAttributeTagInfo,
            (&raw mut tag).cast(),
            size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error().into());
    }
    if tag.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(StorageError::InvalidParent);
    }
    Ok(handle)
}
fn validate_owner_acl(handle: HANDLE) -> Result<(), StorageError> {
    let mut owner: PSID = std::ptr::null_mut();
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut descriptor = std::ptr::null_mut();
    let status = unsafe {
        GetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &raw mut owner,
            std::ptr::null_mut(),
            &raw mut dacl,
            std::ptr::null_mut(),
            &raw mut descriptor,
        )
    };
    if status != 0 || owner.is_null() || dacl.is_null() {
        return Err(StorageError::InvalidParent);
    }
    let _descriptor = Descriptor(descriptor);
    let mut token = std::ptr::null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(io::Error::last_os_error().into());
    }
    let token = Handle(token);
    let mut length = 0;
    unsafe {
        GetTokenInformation(token.0, TokenUser, std::ptr::null_mut(), 0, &raw mut length);
    }
    let mut user = vec![0u8; length as usize];
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenUser,
            user.as_mut_ptr().cast(),
            length,
            &raw mut length,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    let current = unsafe { (*user.as_ptr().cast::<TOKEN_USER>()).User.Sid };
    if unsafe { EqualSid(owner, current) } == 0 {
        return Err(StorageError::InvalidParent);
    }
    let mut info = ACL_SIZE_INFORMATION::default();
    if unsafe {
        GetAclInformation(
            dacl,
            (&raw mut info).cast(),
            size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    } == 0
        || info.AceCount == 0
    {
        return Err(StorageError::InvalidParent);
    }
    for index in 0..info.AceCount {
        let mut ace = std::ptr::null_mut();
        if unsafe { GetAce(dacl, index, &raw mut ace) } == 0 {
            return Err(StorageError::InvalidParent);
        }
        let allowed = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
        if allowed.Header.AceType != 0
            || usize::from(allowed.Header.AceSize) < size_of::<ACCESS_ALLOWED_ACE>()
            || allowed.Mask & FILE_ALL_ACCESS != FILE_ALL_ACCESS
        {
            return Err(StorageError::InvalidParent);
        }
        let sid = unsafe { ace.cast::<u8>().add(8).cast() };
        if unsafe { EqualSid(owner, sid) } == 0 {
            return Err(StorageError::InvalidParent);
        }
    }
    Ok(())
}

pub(super) fn ensure_private_parent(path: &Path) -> Result<PrivateParent, StorageError> {
    if !path.exists() {
        create_owned_directory(path)?;
    }
    let handle = open_no_reparse(path)?;
    let id = file_id(handle.0)?;
    Ok(PrivateParent {
        handle: Arc::new(handle),
        path: path.to_owned(),
        id,
    })
}
pub(super) fn parent_path(parent: &PrivateParent) -> &Path {
    &parent.path
}
pub(super) fn validate_private_parent(parent: &PrivateParent) -> Result<(), StorageError> {
    validate_owner_acl(parent.handle.0)?;
    let reopened = open_no_reparse(&parent.path)?;
    if !same_id(&parent.id, &file_id(reopened.0)?) {
        return Err(StorageError::InvalidParent);
    }
    Ok(())
}
pub(super) fn create_private_directory(
    parent: &PrivateParent,
    name: &str,
) -> Result<PrivateChild, StorageError> {
    validate_private_parent(parent)?;
    let path = parent.path.join(name);
    create_owned_directory(&path)?;
    match open_child(parent, name) {
        Ok(child) => Ok(child),
        Err(error) => {
            let rollback = raw_open_no_reparse(&path).and_then(|handle| {
                validate_private_parent(parent)?;
                delete_handle(handle.0)
            });
            if rollback.is_err() {
                Err(StorageError::RollbackFailed)
            } else {
                Err(error)
            }
        }
    }
}
fn create_owned_directory(path: &Path) -> Result<(), StorageError> {
    let mut descriptor = owner_security_descriptor()?;
    let attributes = security_attributes(&mut descriptor);
    if unsafe { CreateDirectoryW(wide(path).as_ptr(), &raw const attributes) } == 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(())
}

pub(super) fn open_child(parent: &PrivateParent, name: &str) -> Result<PrivateChild, StorageError> {
    validate_private_parent(parent)?;
    let path = parent.path.join(name);
    let handle = open_no_reparse(&path)?;
    let id = file_id(handle.0)?;
    validate_private_parent(parent)?;
    Ok(PrivateChild {
        parent: parent.clone(),
        handle: Arc::new(handle),
        path,
        name: name.to_owned(),
        id,
    })
}
pub(super) fn child_path(child: &PrivateChild) -> &Path {
    &child.path
}
pub(super) fn validate_launch_directory(child: &PrivateChild) -> Result<(), StorageError> {
    validate_private_parent(&child.parent)?;
    validate_owner_acl(child.handle.0)?;
    let reopened = open_no_reparse(&child.path)?;
    if !same_id(&child.id, &file_id(reopened.0)?) {
        return Err(StorageError::InvalidParent);
    }
    Ok(())
}

pub(super) fn create_private_file<F: FnOnce()>(
    child: &PrivateChild,
    name: &str,
    bytes: &[u8],
    _executable: bool,
    created: F,
) -> Result<(), StorageError> {
    use std::io::Write;
    use std::os::windows::io::{FromRawHandle, RawHandle};
    validate_launch_directory(child)?;
    let path = child.path.join(name);
    let mut descriptor = owner_security_descriptor()?;
    let attributes = security_attributes(&mut descriptor);
    let handle = unsafe {
        CreateFileW(
            wide(&path).as_ptr(),
            0x4000_0000,
            FILE_SHARE_READ,
            &raw const attributes,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error().into());
    }
    created();
    let mut file = unsafe { std::fs::File::from_raw_handle(handle as RawHandle) };
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
pub(super) fn read_regular_file(child: &PrivateChild, name: &str) -> Result<Vec<u8>, StorageError> {
    use std::io::Read;
    use std::os::windows::io::{FromRawHandle, RawHandle};
    let handle = open_regular(child, name, 0x8000_0000)?;
    let raw = handle.0;
    std::mem::forget(handle);
    let mut file = unsafe { std::fs::File::from_raw_handle(raw as RawHandle) };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}
pub(super) fn remove_regular_file(child: &PrivateChild, name: &str) -> Result<(), StorageError> {
    let handle = open_regular(child, name, 0x0001_0000)?;
    delete_handle(handle.0)
}
pub(super) fn validate_regular_file(child: &PrivateChild, name: &str) -> Result<(), StorageError> {
    open_regular(child, name, 0).map(drop)
}
fn open_regular(child: &PrivateChild, name: &str, access: u32) -> Result<Handle, StorageError> {
    validate_launch_directory(child)?;
    let path = wide(&child.path.join(name));
    let raw = unsafe {
        CreateFileW(
            path.as_ptr(),
            FILE_READ_ATTRIBUTES | 0x0002_0000 | access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if raw == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error().into());
    }
    let handle = Handle(raw);
    let mut tag = FILE_ATTRIBUTE_TAG_INFO::default();
    if unsafe {
        GetFileInformationByHandleEx(
            handle.0,
            FileAttributeTagInfo,
            (&raw mut tag).cast(),
            size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
    if tag.FileAttributes & (FILE_ATTRIBUTE_REPARSE_POINT | FILE_ATTRIBUTE_DIRECTORY) != 0 {
        return Err(StorageError::InvalidParent);
    }
    validate_owner_acl(handle.0)?;
    validate_launch_directory(child)?;
    Ok(handle)
}

pub(super) fn remove_empty_directory(child: &PrivateChild) -> Result<(), StorageError> {
    validate_launch_directory(child)?;
    let linked = open_no_reparse(&child.parent.path.join(&child.name))?;
    if !same_id(&child.id, &file_id(linked.0)?) {
        return Err(StorageError::InvalidParent);
    }
    delete_handle(child.handle.0)
}
fn delete_handle(handle: HANDLE) -> Result<(), StorageError> {
    let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
    if unsafe {
        SetFileInformationByHandle(
            handle,
            FileDispositionInfo,
            (&raw const disposition).cast(),
            size_of::<FILE_DISPOSITION_INFO>() as u32,
        )
    } == 0
    {
        return Err(io::Error::last_os_error().into());
    }
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
        "$K10sParent = Get-Item -LiteralPath '{directory}' -Force -ErrorAction SilentlyContinue\r\nif ($null -ne $K10sParent -and (($K10sParent.Attributes -band [IO.FileAttributes]::ReparsePoint) -eq 0)) {{\r\nRemove-Item -LiteralPath $PSCommandPath -Force -ErrorAction SilentlyContinue\r\nRemove-Item -LiteralPath '{manifest}' -Force -ErrorAction SilentlyContinue\r\nRemove-Item -LiteralPath '{directory}' -Force -ErrorAction SilentlyContinue\r\n}}\r\nexit $K10sStatus\r\n"
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
