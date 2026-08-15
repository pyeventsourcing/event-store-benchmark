// Bakes harness provenance into the es-bench binary at BUILD time, so every run can record
// exactly which harness code (and therefore which adapter code, pinned via Cargo.lock) produced
// it — regardless of whether git is available where the binary later runs. This is what makes
// `session.json`'s provenance reliable instead of the old runtime `git rev-parse` that silently
// recorded "unknown" on boxes without a git checkout.
use std::process::Command;

fn main() {
    // Rebuild when HEAD moves or the runtime override changes, so the baked sha stays current.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-env-changed=ESB_GIT_VERSION");

    let sha = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let short = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    // `--untracked-files=no`: dirty means tracked source differs from the commit, so stray
    // untracked files (e.g. benchmark `results/`) don't perpetually flip the flag.
    let dirty = match git(&["status", "--porcelain", "--untracked-files=no"]) {
        Some(s) => !s.trim().is_empty(),
        None => false,
    };
    let build_ts =
        cmd("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]).unwrap_or_else(|| "unknown".to_string());
    let rustc = std::env::var("RUSTC")
        .ok()
        .and_then(|r| cmd(&r, &["--version"]))
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=ESB_GIT_SHA={sha}");
    println!("cargo:rustc-env=ESB_GIT_SHA_SHORT={short}");
    println!("cargo:rustc-env=ESB_GIT_DIRTY={dirty}");
    println!("cargo:rustc-env=ESB_BUILD_TIMESTAMP={build_ts}");
    println!("cargo:rustc-env=ESB_RUSTC_VERSION={rustc}");
}

fn git(args: &[&str]) -> Option<String> {
    cmd("git", args)
}

fn cmd(bin: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(bin).args(args).output().ok()?;
    if out.status.success() {
        Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        None
    }
}
