use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

fn collect(root: &Path, dir: &Path, assets: &mut Vec<(String, PathBuf)>) -> io::Result<()> {
    let mut entries = fs::read_dir(dir)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect(root, &path, assets)?;
        } else if path.is_file() {
            let relative = path
                .strip_prefix(root)
                .expect("asset is below dist root")
                .to_string_lossy()
                .replace('\\', "/");
            assets.push((relative, path));
        }
    }
    Ok(())
}

fn literal(value: &str) -> String {
    format!("{value:?}")
}

fn main() -> io::Result<()> {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let dist = manifest.join("../../dist");
    println!("cargo:rerun-if-changed={}", dist.display());

    let mut assets = Vec::new();
    let complete = dist.join("index.html").is_file();
    if complete {
        collect(&dist, &dist, &mut assets)?;
    }

    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("embedded_web.rs");
    let mut generated = fs::File::create(out)?;
    writeln!(generated, "const EMBEDDED_WEB_COMPLETE: bool = {complete};")?;
    writeln!(
        generated,
        "const EMBEDDED_WEB_ASSETS: &[(&str, &[u8])] = &["
    )?;
    for (relative, absolute) in assets {
        writeln!(
            generated,
            "    ({}, include_bytes!({}) as &[u8]),",
            literal(&relative),
            literal(&absolute.to_string_lossy())
        )?;
    }
    writeln!(generated, "];")?;
    Ok(())
}
