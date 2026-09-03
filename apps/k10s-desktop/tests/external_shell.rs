use std::collections::BTreeMap;
use std::path::PathBuf;

use k10s_backend::{ExecPluginPreparation, KubePreparation, prepare_kube_backend_from_paths};
use k10s_desktop::external_shell::{
    EnvironmentSnapshot, ExternalShellTarget, KubectlExecCommand, KubectlLaunchDescriptor,
    RenderError, TemporaryShellStorage, descriptor_when_terminal_available,
};

fn descriptor() -> KubectlLaunchDescriptor {
    KubectlLaunchDescriptor::new(
        7,
        PathBuf::from("/opt/kube tools/kubectl"),
        "prod '$(touch nope)'".into(),
        vec![PathBuf::from("/tmp/a config"), PathBuf::from("/tmp/b")],
        BTreeMap::from([
            ("HOME".into(), "/home/test user".into()),
            ("KUBECONFIG".into(), "/tmp/a config:/tmp/b".into()),
            ("PATH".into(), "/opt/kube tools:/usr/bin:/bin".into()),
        ]),
        Vec::new(),
    )
    .unwrap()
}

#[cfg(unix)]
#[test]
fn temporary_storage_is_private_unique_and_self_cleaning() {
    use std::os::unix::fs::PermissionsExt;
    let root = std::env::temp_dir().join(format!("k10s-temp-storage-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let first = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let second = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    assert_ne!(first.directory(), second.directory());
    assert_eq!(
        std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(first.directory())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(first.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(first.manifest_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let body = std::fs::read_to_string(first.path()).unwrap();
    assert!(body.contains("rm -f -- \"$0\""));
    assert!(body.contains("manifest.json"));
    first.cleanup().unwrap();
    second.cleanup().unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[cfg(unix)]
#[test]
fn launch_uses_the_exact_adapter_resolved_by_the_probe() {
    use k10s_desktop::external_shell::{TerminalAdapter, launch_with_adapter};
    use std::os::unix::fs::PermissionsExt;
    let root = std::env::temp_dir().join(format!("k10s-exact-adapter-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let script = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let log = root.join("argv");
    let launcher = root.join("terminal resolved once");
    std::fs::write(
        &launcher,
        format!("#!/bin/sh\nprintf '%s' \"$2\" > '{}'\n", log.display()),
    )
    .unwrap();
    std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o700)).unwrap();
    let adapter = TerminalAdapter {
        executable: "/bin/sh".into(),
        arguments_before_script: vec![launcher.display().to_string(), "--".into()],
    };
    launch_with_adapter(&script, &adapter).unwrap();
    for _ in 0..100 {
        if std::fs::metadata(&log).is_ok_and(|metadata| metadata.len() > 0) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(
        std::fs::read_to_string(log).unwrap(),
        script.path().display().to_string()
    );
    script.cleanup().unwrap();
    std::fs::remove_file(root.join("argv")).unwrap();
    std::fs::remove_file(root.join("terminal resolved once")).unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[cfg(unix)]
#[test]
fn launch_failure_diagnostic_is_ordered_concrete_and_path_sanitized() {
    use k10s_desktop::external_shell::{TerminalAdapter, launch_with_adapters};
    let root = std::env::temp_dir().join(format!("k10s-launch-diagnostic-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let script = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let error = launch_with_adapters(
        &script,
        &[
            TerminalAdapter {
                executable: "/private/secret/first terminal".into(),
                arguments_before_script: vec![],
            },
            TerminalAdapter {
                executable: "/private/secret/second-terminal".into(),
                arguments_before_script: vec![],
            },
        ],
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("firstterminal (not found); second-terminal (not found)"));
    assert!(!error.contains("/private/secret"));
    assert!(!script.directory().exists());
    std::fs::remove_dir(root).unwrap();
}

#[cfg(unix)]
#[test]
fn temporary_startup_cleanup_removes_only_expired_valid_children() {
    use std::os::unix::fs::symlink;
    let root = std::env::temp_dir().join(format!("k10s-temp-cleanup-{}", std::process::id()));
    let outside = std::env::temp_dir().join(format!("k10s-temp-outside-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&outside);
    std::fs::write(&outside, "keep").unwrap();
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let expired = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let live = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(expired.manifest_path()).unwrap()).unwrap();
    manifest["created_unix_seconds"] = 1.into();
    std::fs::write(
        expired.manifest_path(),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    symlink(&outside, root.join("AAAAAAAAAAAAAAAAAAAAAAAA")).unwrap();
    let report = storage.cleanup_expired(100_000).unwrap();
    assert_eq!(report.removed, 1);
    assert!(!expired.directory().exists());
    assert!(live.directory().exists());
    assert_eq!(std::fs::read_to_string(&outside).unwrap(), "keep");
    live.cleanup().unwrap();
    std::fs::remove_file(root.join("AAAAAAAAAAAAAAAAAAAAAAAA")).unwrap();
    std::fs::remove_dir(root).unwrap();
    std::fs::remove_file(outside).unwrap();
}

#[cfg(unix)]
#[test]
fn temporary_startup_cleanup_examines_exactly_the_oldest_128() {
    let root = std::env::temp_dir().join(format!("k10s-temp-bound-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let mut launches = Vec::new();
    for index in 0..129_u64 {
        let launch = storage
            .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
            .unwrap();
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(launch.manifest_path()).unwrap()).unwrap();
        manifest["created_unix_seconds"] = (index + 1).into();
        std::fs::write(
            launch.manifest_path(),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        launches.push(launch);
    }
    let report = storage.cleanup_expired(200_000).unwrap();
    assert_eq!(report.examined, 128);
    assert_eq!(report.removed, 128);
    assert!(launches[128].directory().exists());
    launches.pop().unwrap().cleanup().unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[cfg(unix)]
#[test]
fn temporary_cleanup_hard_bounds_hostile_manifests_children_and_parent_scans() {
    let root = std::env::temp_dir().join(format!("k10s-cleanup-bounds-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let huge = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    std::fs::write(huge.manifest_path(), vec![b'x'; 64 * 1024]).unwrap();
    let crowded = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(crowded.manifest_path()).unwrap()).unwrap();
    manifest["created_unix_seconds"] = 1.into();
    std::fs::write(
        crowded.manifest_path(),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    for index in 0..100 {
        std::fs::write(crowded.directory().join(format!("extra-{index}")), "x").unwrap();
    }
    let report = storage.cleanup_expired(100_000).unwrap();
    assert!(report.scanned <= 1024);
    assert!(huge.directory().exists());
    assert!(crowded.directory().exists());
    for index in 0..1024 {
        std::fs::write(root.join(format!("hostile-{index}")), "x").unwrap();
    }
    assert!(matches!(
        storage.cleanup_expired(100_000),
        Err(k10s_desktop::external_shell::StorageError::CleanupBudgetExceeded)
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn temporary_creation_faults_rollback_every_owned_object_and_keep_preexisting_entries() {
    use k10s_desktop::external_shell::StorageFaultPoint;
    let root = std::env::temp_dir().join(format!("k10s-temp-faults-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let base = TemporaryShellStorage::new(root.clone()).unwrap();
    let sentinel = root.join("preexisting");
    std::fs::write(&sentinel, "keep").unwrap();
    for fault in [
        StorageFaultPoint::DirectoryCreate,
        StorageFaultPoint::DirectoryPermissions,
        StorageFaultPoint::ManifestCreate,
        StorageFaultPoint::ManifestWrite,
        StorageFaultPoint::ManifestSync,
        StorageFaultPoint::Render,
        StorageFaultPoint::ScriptCreate,
        StorageFaultPoint::ScriptWrite,
        StorageFaultPoint::ScriptSync,
        StorageFaultPoint::ScriptPermissions,
    ] {
        let storage = base.clone().with_fault_for_test(fault);
        assert!(
            storage
                .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
                .is_err(),
            "{fault:?}"
        );
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "keep");
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1, "{fault:?}");
    }
    std::fs::remove_file(sentinel).unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[cfg(unix)]
#[test]
fn temporary_storage_refuses_parent_path_swap_without_touching_replacement() {
    use std::os::unix::fs::symlink;
    let root = std::env::temp_dir().join(format!("k10s-parent-swap-{}", std::process::id()));
    let retained = root.with_extension("retained");
    let outside = root.with_extension("outside");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&retained);
    let _ = std::fs::remove_dir_all(&outside);
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    std::fs::rename(&root, &retained).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("sentinel"), "keep").unwrap();
    symlink(&outside, &root).unwrap();
    assert!(
        storage
            .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(outside.join("sentinel")).unwrap(),
        "keep"
    );
    assert_eq!(std::fs::read_dir(&retained).unwrap().count(), 0);
    std::fs::remove_file(root).unwrap();
    std::fs::remove_dir_all(retained).unwrap();
    std::fs::remove_dir_all(outside).unwrap();
}

#[cfg(unix)]
#[test]
fn temporary_cleanup_refuses_child_entry_swap_before_unlinking_files() {
    use std::os::unix::fs::symlink;
    let root = std::env::temp_dir().join(format!("k10s-child-swap-{}", std::process::id()));
    let outside = root.with_extension("outside");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&outside);
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let script = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let retained = script.directory().with_extension("retained");
    std::fs::rename(script.directory(), &retained).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("sentinel"), "keep").unwrap();
    symlink(&outside, script.directory()).unwrap();
    assert!(script.cleanup().is_err());
    assert!(retained.join("manifest.json").exists());
    assert_eq!(
        std::fs::read_to_string(outside.join("sentinel")).unwrap(),
        "keep"
    );
    std::fs::remove_file(script.directory()).unwrap();
    std::fs::remove_dir_all(retained).unwrap();
    std::fs::remove_dir_all(outside).unwrap();
    std::fs::remove_dir(root).unwrap();
}

#[cfg(windows)]
#[test]
fn temporary_windows_storage_uses_owner_acl_and_refuses_reparse_lookalikes() {
    use std::os::windows::fs::symlink_dir;
    let root = std::env::temp_dir().join(format!("k10s-windows-storage-{}", std::process::id()));
    let outside = std::env::temp_dir().join(format!("k10s-windows-outside-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&outside);
    std::fs::create_dir(&outside).unwrap();
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let script = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    assert!(
        String::from_utf8_lossy(&std::fs::read(script.path()).unwrap())
            .contains("[IO.FileAttributes]::ReparsePoint")
    );
    assert!(symlink_dir(&outside, root.join("AAAAAAAAAAAAAAAAAAAAAAAA")).is_ok());
    let report = storage.cleanup_expired(u64::MAX).unwrap();
    assert_eq!(report.removed, 1);
    assert!(outside.exists());
    assert!(!script.directory().exists());
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(outside).unwrap();
}

#[cfg(windows)]
#[test]
fn temporary_powershell_runtime_rechecks_parent_and_self_cleans() {
    use std::process::{Command, Stdio};
    let root = std::env::temp_dir().join(format!(
        "k10s-windows-runtime-cleanup-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).unwrap();
    let kubectl = root.join("kubectl.cmd");
    std::fs::write(&kubectl, "@exit /b 41\r\n").unwrap();
    let descriptor = KubectlLaunchDescriptor::new(
        1,
        kubectl,
        "context".into(),
        vec![root.join("config")],
        BTreeMap::from([
            ("PATH".into(), root.display().to_string()),
            (
                "KUBECONFIG".into(),
                root.join("config").display().to_string(),
            ),
        ]),
        Vec::new(),
    )
    .unwrap();
    let storage = TemporaryShellStorage::new(root.join("storage")).unwrap();
    let target = ExternalShellTarget {
        generation: 1,
        namespace: "ns".into(),
        pod: "pod".into(),
        uid: "uid".into(),
        container: "main".into(),
        program: "/bin/sh".into(),
    };
    let script = storage
        .create(&KubectlExecCommand::new(&descriptor, target).unwrap())
        .unwrap();
    let launch_dir = script.directory().to_owned();
    let status = Command::new("powershell.exe")
        .args(["-NoProfile", "-File"])
        .arg(script.path())
        .stdin(Stdio::null())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(41));
    assert!(!launch_dir.exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn temporary_linux_launcher_falls_back_with_exact_argv_and_cleans_total_failure() {
    use k10s_desktop::external_shell::{LaunchAttempt, launch_linux_with};
    let root = std::env::temp_dir().join(format!("k10s-launcher-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let storage = TemporaryShellStorage::new(root.clone()).unwrap();
    let script = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let path = script.path().to_path_buf();
    let mut calls = Vec::new();
    let result = launch_linux_with(&script, |program, args| {
        calls.push((program.to_owned(), args.to_vec()));
        if program == "gnome-terminal" {
            Ok(())
        } else {
            Err(LaunchAttempt::Missing)
        }
    });
    assert!(result.is_ok());
    assert_eq!(
        calls,
        vec![
            (
                "xdg-terminal-exec".into(),
                vec!["--".into(), path.clone().into_os_string()]
            ),
            (
                "x-terminal-emulator".into(),
                vec!["-e".into(), path.clone().into_os_string()]
            ),
            (
                "gnome-terminal".into(),
                vec!["--".into(), path.clone().into_os_string()]
            ),
        ]
    );
    script.cleanup().unwrap();

    let failed = storage
        .create(&KubectlExecCommand::new(&descriptor(), target()).unwrap())
        .unwrap();
    let dir = failed.directory().to_owned();
    assert!(launch_linux_with(&failed, |_, _| Err(LaunchAttempt::Spawn("no".into()))).is_err());
    assert!(!dir.exists());
    std::fs::remove_dir(root).unwrap();
}

fn target() -> ExternalShellTarget {
    ExternalShellTarget {
        generation: 7,
        namespace: "team '$()` %!&|<> 日本".into(),
        pod: "pod '$()` %!&|<> 日本".into(),
        uid: "uid '$()` %!&|<> 日本".into(),
        container: "container '$()` %!&|<> 日本".into(),
        program: "/bin/sh".into(),
    }
}

#[test]
fn descriptor_rejects_environment_outside_the_fixed_allowlist() {
    let error = KubectlLaunchDescriptor::new(
        1,
        "/usr/bin/kubectl".into(),
        "context".into(),
        vec!["/tmp/config".into()],
        BTreeMap::from([("AWS_PROFILE".into(), "prod".into())]),
        Vec::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("AWS_PROFILE"));
}

#[test]
fn descriptor_rejects_secret_shaped_environment() {
    let error = KubectlLaunchDescriptor::new(
        1,
        "/usr/bin/kubectl".into(),
        "context".into(),
        vec!["/tmp/config".into()],
        BTreeMap::from([("TOKEN".into(), "do-not-render".into())]),
        Vec::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("sensitive"));
}

#[test]
fn descriptor_public_constructor_rejects_secret_shaped_allowed_values() {
    let error = KubectlLaunchDescriptor::new(
        1,
        "/usr/bin/kubectl".into(),
        "context".into(),
        vec!["/tmp/config".into()],
        BTreeMap::from([("HOME".into(), "Bearer definitely-a-credential".into())]),
        Vec::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("sensitive"));
}

#[test]
fn descriptor_is_not_published_without_a_terminal_adapter() {
    assert!(descriptor_when_terminal_available(Some(descriptor()), None).is_none());
}

#[cfg(target_os = "macos")]
#[test]
fn terminal_probe_selects_the_exact_macos_adapter() {
    let adapter =
        k10s_desktop::external_shell::probe_system_terminal(&EnvironmentSnapshot::default())
            .unwrap();
    assert_eq!(adapter.executable, PathBuf::from("/usr/bin/open"));
    assert!(adapter.arguments_before_script.is_empty());
}

#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn terminal_probe_selects_the_first_available_linux_adapter() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!("k10s-terminal-probe-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    let terminal = dir.join("gnome-terminal");
    fs::write(&terminal, "#!/bin/sh\n").unwrap();
    fs::set_permissions(&terminal, fs::Permissions::from_mode(0o700)).unwrap();
    let environment = EnvironmentSnapshot::from_unicode(BTreeMap::from([(
        "PATH".into(),
        dir.display().to_string(),
    )]));
    let adapter = k10s_desktop::external_shell::probe_system_terminal(&environment).unwrap();
    assert_eq!(adapter.executable, terminal);
    assert_eq!(adapter.arguments_before_script, ["--"]);
    fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn descriptor_rejects_non_unicode_allowed_environment_and_missing_kubectl() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    let preparation = KubePreparation {
        source_paths: vec!["/tmp/config".into()],
        selected_context: "context".into(),
        exec_plugins: Vec::new(),
        context_exec_plugins: BTreeMap::from([("context".into(), Vec::new())]),
    };
    let invalid = EnvironmentSnapshot::from_os(BTreeMap::from([
        ("PATH".into(), OsString::from_vec(vec![0xff])),
        ("HOME".into(), "/home/test".into()),
    ]));
    assert!(KubectlLaunchDescriptor::from_preparation(1, &preparation, &invalid).is_err());

    let missing = EnvironmentSnapshot::from_unicode(BTreeMap::from([
        ("PATH".into(), "/definitely/empty".into()),
        ("HOME".into(), "/home/test".into()),
    ]));
    assert!(
        KubectlLaunchDescriptor::from_preparation(1, &preparation, &missing)
            .unwrap_err()
            .to_string()
            .contains("kubectl")
    );
}

#[test]
fn render_posix_quotes_every_structured_value() {
    let script = KubectlExecCommand::new(&descriptor(), target())
        .unwrap()
        .render_posix()
        .unwrap();
    assert!(script.contains("'prod '\"'\"'$(touch nope)'\"'\"''"));
    assert!(script.contains("exec -it"));
    assert!(script.contains("jsonpath={.metadata.uid}"));
    assert!(script.contains("K10S_STATUS=$?"));
    assert!(script.contains("exit \"$K10S_STATUS\""));
    assert!(!script.contains("eval "));
}

#[test]
fn render_powershell_quotes_every_structured_value() {
    let script = KubectlExecCommand::new(&descriptor(), target())
        .unwrap()
        .render_powershell()
        .unwrap();
    assert!(script.contains("'prod ''$(touch nope)'''"));
    assert!(script.contains("& $K10sKubectl"));
    assert!(script.contains("$global:LASTEXITCODE = 125"));
    assert!(script.contains("catch { $K10sStatus = 125"));
    assert!(script.contains("exit $K10sStatus"));
    assert!(!script.contains("Invoke-Expression"));
}

#[test]
fn render_rejects_line_break_nul_and_generation_mismatch() {
    for bad in ["bad\nvalue", "bad\rvalue", "bad\0value"] {
        let mut value = target();
        value.pod = bad.into();
        assert!(matches!(
            KubectlExecCommand::new(&descriptor(), value),
            Err(RenderError::InvalidField { field: "pod" })
        ));
    }
    let mut value = target();
    value.generation = 8;
    assert!(matches!(
        KubectlExecCommand::new(&descriptor(), value),
        Err(RenderError::GenerationMismatch)
    ));
}

#[cfg(unix)]
#[test]
fn descriptor_resolves_kubectl_and_exec_plugin_from_one_preparation() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    let dir = std::env::temp_dir().join(format!("k10s-shell-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    for name in ["kubectl", "login-helper"] {
        let path = dir.join(name);
        fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let preparation = KubePreparation {
        source_paths: vec![dir.join("first"), dir.join("second")],
        selected_context: "same-name".into(),
        exec_plugins: vec![ExecPluginPreparation {
            command: "login-helper".into(),
            environment: BTreeMap::new(),
        }],
        context_exec_plugins: BTreeMap::new(),
    };
    let env = EnvironmentSnapshot::from_unicode(BTreeMap::from([
        ("PATH".into(), dir.to_string_lossy().into_owned()),
        ("HOME".into(), "/home/test".into()),
    ]));
    let descriptor = KubectlLaunchDescriptor::from_preparation(9, &preparation, &env).unwrap();
    assert_eq!(descriptor.context, "same-name");
    assert_eq!(descriptor.kubeconfig_sources, preparation.source_paths);
    assert_eq!(descriptor.kubectl, dir.join("kubectl"));
    assert_eq!(descriptor.exec_plugins[0].command, dir.join("login-helper"));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn ordered_kubeconfig_snapshot_keeps_first_same_named_context_authoritative() {
    use std::fs;
    let dir = std::env::temp_dir().join(format!("k10s-merge-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    let first = dir.join("first");
    let second = dir.join("second");
    fs::write(&first, kubeconfig_yaml("one", "first-user", "first-helper")).unwrap();
    fs::write(
        &second,
        kubeconfig_yaml("two", "second-user", "second-helper"),
    )
    .unwrap();
    let prepared = prepare_kube_backend_from_paths(vec![first.clone(), second.clone()]).unwrap();
    let kube = prepared.kube().unwrap();
    assert_eq!(kube.source_paths, [first.clone(), second]);
    assert_eq!(kube.selected_context, "same");
    assert_eq!(kube.exec_plugins[0].command, "first-helper");

    // A later file/environment change cannot alter the already prepared snapshot.
    fs::write(
        &first,
        kubeconfig_yaml("changed", "changed-user", "changed-helper"),
    )
    .unwrap();
    assert_eq!(
        prepared.kube().unwrap().exec_plugins[0].command,
        "first-helper"
    );
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn prepared_snapshot_rebuilds_explicit_context_plugin_without_rereading_sources() {
    let dir = std::env::temp_dir().join(format!("k10s-context-metadata-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir(&dir).unwrap();
    let source = dir.join("config");
    std::fs::write(&source, r#"apiVersion: v1
kind: Config
current-context: first
clusters:
- name: cluster
  cluster: {server: https://example.invalid}
contexts:
- name: first
  context: {cluster: cluster, user: first-user}
- name: second
  context: {cluster: cluster, user: second-user}
users:
- name: first-user
  user: {exec: {apiVersion: client.authentication.k8s.io/v1, interactiveMode: Never, command: first-helper}}
- name: second-user
  user: {exec: {apiVersion: client.authentication.k8s.io/v1, interactiveMode: Never, command: second-helper}}
"#).unwrap();
    let prepared = prepare_kube_backend_from_paths(vec![source.clone()]).unwrap();
    let snapshot = prepared.kube().unwrap().clone();
    std::fs::write(&source, "corrupt after preparation").unwrap();
    let rebuilt = snapshot.for_context("second").unwrap();
    assert_eq!(rebuilt.selected_context, "second");
    assert_eq!(rebuilt.exec_plugins[0].command, "second-helper");
    std::fs::remove_dir_all(dir).unwrap();
}

fn kubeconfig_yaml(cluster: &str, user: &str, command: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: Config
current-context: same
clusters:
- name: {cluster}
  cluster:
    server: https://{cluster}.example.invalid
contexts:
- name: same
  context:
    cluster: {cluster}
    user: {user}
users:
- name: {user}
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      interactiveMode: Never
      command: {command}
"#
    )
}

#[cfg(unix)]
#[test]
fn render_posix_executes_exact_argv_and_preserves_exec_status() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;
    let dir = std::env::temp_dir().join(format!("k10s-render-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    let fake = dir.join("fake kubectl");
    let log = dir.join("argv");
    let env_log = dir.join("env");
    fs::write(&fake, format!("#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\nprintf '%s\\n' \"$PATH\" \"$HOME\" \"$KUBECONFIG\" \"${{SHOULD_NOT_LEAK-unset}}\" > '{}'\ncase \" $* \" in *' get pod '*) cat '{}'; exit 0;; *) exit 23;; esac\n", log.display(), env_log.display(), dir.join("uid").display())).unwrap();
    fs::set_permissions(&fake, fs::Permissions::from_mode(0o700)).unwrap();
    let mut d = descriptor();
    d.kubectl = fake;
    let t = target();
    fs::write(dir.join("uid"), &t.uid).unwrap();
    let script = KubectlExecCommand::new(&d, t.clone())
        .unwrap()
        .render_posix()
        .unwrap();
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg(script)
        .env("SHOULD_NOT_LEAK", "secret")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(23));
    let argv = fs::read_to_string(log).unwrap();
    let expected = [
        "--context",
        d.context.as_str(),
        "--namespace",
        t.namespace.as_str(),
        "get",
        "pod",
        t.pod.as_str(),
        "-o",
        "jsonpath={.metadata.uid}",
        "--context",
        d.context.as_str(),
        "--namespace",
        t.namespace.as_str(),
        "exec",
        "-it",
        t.pod.as_str(),
        "--container",
        t.container.as_str(),
        "--",
        t.program.as_str(),
    ]
    .join("\n")
        + "\n";
    assert_eq!(argv, expected);
    assert_eq!(
        fs::read_to_string(env_log).unwrap(),
        "/opt/kube tools:/usr/bin:/bin\n/home/test user\n/tmp/a config:/tmp/b\nunset\n"
    );
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("kubectl exec failed")
    );
    fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn render_posix_uid_mismatch_never_executes() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::process::Command;
    let dir = std::env::temp_dir().join(format!("k10s-mismatch-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    let fake = dir.join("kubectl");
    let log = dir.join("argv");
    fs::write(
        &fake,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\nprintf replaced-uid\n",
            log.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&fake, fs::Permissions::from_mode(0o700)).unwrap();
    let mut d = descriptor();
    d.kubectl = fake;
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg(
            KubectlExecCommand::new(&d, target())
                .unwrap()
                .render_posix()
                .unwrap(),
        )
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(66));
    assert!(!fs::read_to_string(log).unwrap().contains("exec\n"));
    fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn render_posix_uid_lookup_failure_preserves_status_and_eof_does_not_hang() {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};
    let dir = std::env::temp_dir().join(format!("k10s-lookup-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    let fake = dir.join("kubectl");
    fs::write(&fake, "#!/bin/sh\nexit 41\n").unwrap();
    fs::set_permissions(&fake, fs::Permissions::from_mode(0o700)).unwrap();
    let mut descriptor = descriptor();
    descriptor.kubectl = fake;
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg(
            KubectlExecCommand::new(&descriptor, target())
                .unwrap()
                .render_posix()
                .unwrap(),
        )
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(41));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("Pod UID lookup failed")
    );
    fs::remove_dir_all(dir).unwrap();
}

#[cfg(windows)]
#[test]
fn render_powershell_executes_under_real_windows_powershell_and_preserves_status() {
    use std::fs;
    use std::process::{Command, Stdio};
    let dir = std::env::temp_dir().join(format!("k10s-powershell-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir(&dir).unwrap();
    let fake = dir.join("kubectl.exe");
    let plugin = dir.join("aws.CMD");
    let log = dir.join("argv.txt");
    let source = dir.join("fake.rs");
    fs::write(
        &source,
        format!(
            r#"use std::{{env, fs::OpenOptions, io::Write}};
fn main() {{
 let args: Vec<String> = env::args().skip(1).collect();
 let mut environment: Vec<(String, String)> = env::vars().collect();
 environment.sort();
 let mut log = OpenOptions::new().create(true).append(true).open({:?}).unwrap();
 writeln!(log, "CALL\nARGS={{args:?}}\nENV={{environment:?}}\nEND").unwrap();
 if args.iter().any(|arg| arg == "get") {{
   let pod = args.iter().position(|arg| arg == "pod").and_then(|i| args.get(i + 1)).map(String::as_str).unwrap_or("");
   if pod == "lookup-fail" {{ std::process::exit(41); }}
   if pod == "mismatch" {{ print!("replaced"); }} else {{ print!("uid-1"); }}
   return;
 }}
 std::process::exit(23);
}}"#,
            log.to_string_lossy()
        ),
    )
    .unwrap();
    fs::write(&plugin, "@exit /b 0\r\n").unwrap();
    assert!(
        Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&fake)
            .arg(&source)
            .status()
            .unwrap()
            .success()
    );
    let preparation = KubePreparation {
        source_paths: vec![dir.join("config")],
        selected_context: "context".into(),
        exec_plugins: vec![ExecPluginPreparation {
            command: "aws".into(),
            environment: BTreeMap::new(),
        }],
        context_exec_plugins: BTreeMap::new(),
    };
    let shell_environment = EnvironmentSnapshot::from_unicode(BTreeMap::from([
        ("PATH".into(), dir.display().to_string()),
        ("PATHEXT".into(), ".EXE;.CMD;.BAT;.COM".into()),
        ("USERPROFILE".into(), dir.display().to_string()),
    ]));
    let descriptor =
        KubectlLaunchDescriptor::from_preparation(1, &preparation, &shell_environment).unwrap();
    assert_eq!(descriptor.kubectl, fake);
    assert_eq!(descriptor.exec_plugins[0].command, plugin);
    let exec_target = ExternalShellTarget {
        generation: 1,
        namespace: "ns & 'x'".into(),
        pod: "pod '$(malicious)' & | <> %!".into(),
        uid: "uid-1".into(),
        container: "main".into(),
        program: "/bin/sh".into(),
    };
    let script_path = dir.join("rendered.ps1");
    fs::write(
        &script_path,
        KubectlExecCommand::new(&descriptor, exec_target.clone())
            .unwrap()
            .render_powershell()
            .unwrap(),
    )
    .unwrap();
    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-File"])
        .arg(&script_path)
        .env("SHOULD_NOT_LEAK", "secret")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(23));
    assert_eq!(
        String::from_utf8(output.stderr).unwrap().trim(),
        "Finback shell: kubectl exec failed."
    );
    let environment = descriptor
        .environment
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    let lookup = vec![
        "--context".into(),
        descriptor.context.clone(),
        "--namespace".into(),
        exec_target.namespace.clone(),
        "get".into(),
        "pod".into(),
        exec_target.pod.clone(),
        "-o".into(),
        "jsonpath={.metadata.uid}".into(),
    ];
    let exec = vec![
        "--context".into(),
        descriptor.context.clone(),
        "--namespace".into(),
        exec_target.namespace.clone(),
        "exec".into(),
        "-it".into(),
        exec_target.pod.clone(),
        "--container".into(),
        exec_target.container.clone(),
        "--".into(),
        exec_target.program.clone(),
    ];
    assert_eq!(
        fs::read_to_string(&log).unwrap(),
        format!(
            "CALL\nARGS={lookup:?}\nENV={environment:?}\nEND\nCALL\nARGS={exec:?}\nENV={environment:?}\nEND\n"
        )
    );

    for (pod, expected, diagnostic) in [
        ("lookup-fail", 41, "Finback shell: Pod UID lookup failed."),
        (
            "mismatch",
            66,
            "Finback shell: Pod UID changed; refusing exec.",
        ),
    ] {
        fs::write(&log, "").unwrap();
        let mut selected_target = target();
        selected_target.uid = "uid-1".into();
        selected_target.pod = pod.into();
        fs::write(
            &script_path,
            KubectlExecCommand::new(&descriptor, selected_target)
                .unwrap()
                .render_powershell()
                .unwrap(),
        )
        .unwrap();
        let output = Command::new("powershell.exe")
            .args(["-NoProfile", "-File"])
            .arg(&script_path)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(expected));
        assert_eq!(String::from_utf8(output.stderr).unwrap().trim(), diagnostic);
        let lookup = vec![
            "--context".to_owned(),
            descriptor.context.clone(),
            "--namespace".to_owned(),
            target().namespace,
            "get".to_owned(),
            "pod".to_owned(),
            pod.to_owned(),
            "-o".to_owned(),
            "jsonpath={.metadata.uid}".to_owned(),
        ];
        assert_eq!(
            fs::read_to_string(&log).unwrap(),
            format!("CALL\nARGS={lookup:?}\nENV={environment:?}\nEND\n")
        );
    }
    fs::remove_dir_all(dir).unwrap();
}
