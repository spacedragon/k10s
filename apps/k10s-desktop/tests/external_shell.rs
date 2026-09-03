use std::collections::BTreeMap;
use std::path::PathBuf;

use k10s_backend::{ExecPluginPreparation, KubePreparation};
use k10s_desktop::external_shell::{
    EnvironmentSnapshot, ExternalShellTarget, KubectlExecCommand, KubectlLaunchDescriptor,
    RenderError,
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
    fs::write(&fake, format!("#!/bin/sh\nprintf '%s\\n' \"$@\" >> '{}'\ncase \" $* \" in *' get pod '*) cat '{}'; exit 0;; *) exit 23;; esac\n", log.display(), dir.join("uid").display())).unwrap();
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
    assert!(argv.contains("get\npod\n"));
    assert!(argv.contains("exec\n-it\n"));
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
