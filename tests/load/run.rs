//! Dependency-free two-pass capacity gate orchestrator.
//!
//! Compile with `rustc tests/load/run.rs -o /tmp/k10s-load` from the repo root.

use std::process::{Command, ExitCode};

fn output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|result| result.status.success())
        .map(|result| String::from_utf8_lossy(&result.stdout).trim().to_owned())
        .unwrap_or_else(|| "unavailable".into())
}

fn run(args: &[&str]) -> bool {
    println!("running: cargo {}", args.join(" "));
    Command::new("cargo")
        .args(args)
        .status()
        .is_ok_and(|status| status.success())
}

fn main() -> ExitCode {
    println!("k10s capacity runner metadata");
    println!("os: {}", output("uname", &["-a"]));
    println!("rust: {}", output("rustc", &["--version", "--verbose"]));
    println!(
        "cpu: {}",
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|text| text
                .lines()
                .find(|line| line.starts_with("model name"))
                .map(str::to_owned))
            .unwrap_or_else(|| "unavailable".into())
    );
    let test_mode = std::env::args().any(|argument| argument == "--test");
    let suffix: &[&str] = if test_mode { &["--", "--test"] } else { &[] };
    for pass in 1..=2 {
        println!("capacity pass {pass}/2");
        let benches: [(&str, &str); 3] = [
            ("k10s-backend", "fake_scale"),
            ("k10s-backend", "cache_load"),
            ("k10s-server", "protocol_load"),
        ];
        for (package, bench) in benches {
            let mut args = vec!["bench", "--locked", "-p", package, "--bench", bench];
            args.extend_from_slice(suffix);
            if !run(&args) {
                eprintln!("capacity pass {pass} failed at {package}/{bench}");
                return ExitCode::FAILURE;
            }
        }
        if !run(&[
            "test",
            "--release",
            "--locked",
            "-p",
            "k10s-server",
            "--test",
            "load_paths",
            "--",
            "--ignored",
            "--test-threads=1",
        ]) {
            eprintln!("capacity pass {pass} failed at k10s-server/load_paths");
            return ExitCode::FAILURE;
        }
    }
    println!("k10s two-pass capacity gate OK");
    ExitCode::SUCCESS
}
