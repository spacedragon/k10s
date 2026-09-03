use std::collections::BTreeMap;
use std::path::PathBuf;

use k10s_backend::{ExecPluginPreparation, KubePreparation, prepare_kube_backend_from_paths};
use k10s_desktop::external_shell::{
    EnvironmentSnapshot, ExternalShellTarget, KubectlExecCommand, KubectlLaunchDescriptor,
    RenderError, descriptor_when_terminal_available, probe_system_terminal,
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
    let adapter = probe_system_terminal(&EnvironmentSnapshot::default()).unwrap();
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
    let adapter = probe_system_terminal(&environment).unwrap();
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
    let fake = dir.join("fake-kubectl.exe");
    let log = dir.join("argv.txt");
    let source = dir.join("fake.rs");
    fs::write(
        &source,
        format!(
            r#"use std::{{env, fs::OpenOptions, io::Write}};
fn main() {{
 let args: Vec<String> = env::args().skip(1).collect();
 let mut log = OpenOptions::new().create(true).append(true).open({:?}).unwrap();
 for arg in &args {{ writeln!(log, "ARG={{arg}}").unwrap(); }}
 for key in ["PATH", "USERPROFILE", "KUBECONFIG", "SHOULD_NOT_LEAK"] {{ writeln!(log, "ENV {{key}}={{}}", env::var(key).unwrap_or_else(|_| "<unset>".into())).unwrap(); }}
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
    assert!(
        Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&fake)
            .arg(&source)
            .status()
            .unwrap()
            .success()
    );
    let descriptor = KubectlLaunchDescriptor::new(
        1,
        fake,
        "context".into(),
        vec![dir.join("config")],
        BTreeMap::from([
            ("PATH".into(), std::env::var("PATH").unwrap()),
            ("USERPROFILE".into(), dir.display().to_string()),
            (
                "KUBECONFIG".into(),
                dir.join("config").display().to_string(),
            ),
        ]),
        Vec::new(),
    )
    .unwrap();
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
        KubectlExecCommand::new(&descriptor, exec_target)
            .unwrap()
            .render_powershell()
            .unwrap(),
    )
    .unwrap();
    let status = Command::new("powershell.exe")
        .args(["-NoProfile", "-File"])
        .arg(&script_path)
        .env("SHOULD_NOT_LEAK", "secret")
        .stdin(Stdio::null())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(23));
    let recorded = fs::read_to_string(&log).unwrap();
    assert!(recorded.contains("ARG=exec\nARG=-it\nARG=pod '$(malicious)' & | <> %!\n"));
    assert!(recorded.contains("ENV SHOULD_NOT_LEAK=<unset>"));
    for (name, value) in &descriptor.environment {
        assert!(recorded.contains(&format!("ENV {name}={value}\n")));
    }

    for (pod, expected, must_exec) in [("lookup-fail", 41, false), ("mismatch", 66, false)] {
        fs::write(&log, "").unwrap();
        let mut target = target();
        target.uid = "uid-1".into();
        target.pod = pod.into();
        fs::write(
            &script_path,
            KubectlExecCommand::new(&descriptor, target)
                .unwrap()
                .render_powershell()
                .unwrap(),
        )
        .unwrap();
        let status = Command::new("powershell.exe")
            .args(["-NoProfile", "-File"])
            .arg(&script_path)
            .stdin(Stdio::null())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(expected));
        assert_eq!(
            fs::read_to_string(&log).unwrap().contains("ARG=exec"),
            must_exec
        );
    }
    fs::remove_dir_all(dir).unwrap();
}
