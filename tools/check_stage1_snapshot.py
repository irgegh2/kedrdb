#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check Rust stage-1 sources in an isolated fresh copy; never run a DB server."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/stage1-checks"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    sources = [root / name for name in (
        "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "LICENSE",
        ".github/workflows/ci.yml", "tools/check_stage1_snapshot.py",
    )]
    sources += sorted(path for path in (root / "crates").rglob("*") if path.is_file())
    summary = {
        "platform": platform.platform(), "machine": platform.machine(),
        "source_sha256": {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
        "runs": [], "linux_executed": platform.system() == "Linux", "all_passed": False,
        "scope": "stage 1 file I/O foundation; not SQL, WAL, durability certification or database benchmark",
    }
    scratch_path = None
    try:
        with tempfile.TemporaryDirectory(prefix="kedr-stage1-snapshot-") as scratch:
            scratch_path = Path(scratch)
            checkout = Path(scratch) / "source"
            checkout.mkdir()
            for path in sources:
                destination = checkout / path.relative_to(root)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, destination)
            env = os.environ.copy()
            # Inherited targeting or test filters must not change this campaign.
            env.pop("KEDR_TEST_SEED", None)
            env.pop("CARGO_BUILD_TARGET", None)
            env["CARGO_TARGET_DIR"] = str(Path(scratch) / "target")
            checks = [
                ("toolchain", ["rustc", "-Vv"]),
                ("format", ["cargo", "fmt", "--all", "--", "--check"]),
                ("clippy", ["cargo", "clippy", "--workspace", "--all-targets", "--locked", "--offline", "--", "-D", "warnings"]),
                ("debug-tests", ["cargo", "test", "--workspace", "--locked", "--offline"]),
                ("release-tests", ["cargo", "test", "--workspace", "--release", "--locked", "--offline"]),
                ("release-build", ["cargo", "build", "--workspace", "--release", "--locked", "--offline"]),
                ("doctor", [str(Path(scratch) / "target/release/kedr"), "doctor"]),
                ("storage-check", [str(Path(scratch) / "target/release/kedr"), "storage-check", str(checkout)]),
            ]
            for name, command in checks:
                started = time.monotonic()
                result = subprocess.run(command, cwd=checkout, env=env, capture_output=True, text=True, timeout=180)
                (output / f"{name}.txt").write_text(result.stdout + result.stderr)
                summary["runs"].append({"name": name, "command": command, "exit_code": result.returncode, "seconds": round(time.monotonic() - started, 3)})
                print(f"{name}: {'PASS' if result.returncode == 0 else 'FAIL'}", flush=True)
                if result.returncode != 0:
                    raise RuntimeError(f"{name} failed: see {output / (name + '.txt')}")
            leftovers = list(checkout.glob(".kedr-check-*"))
            if leftovers:
                raise RuntimeError("storage-check did not remove its scratch file")
            summary["scratch_file_cleaned"] = True
            summary["all_passed"] = True
    finally:
        summary["temporary_snapshot_removed"] = scratch_path is not None and not scratch_path.exists()
        (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(f"Report: {output / 'summary.json'}")


if __name__ == "__main__":
    main()
