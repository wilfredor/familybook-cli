#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _arch_name(raw: str) -> str:
    value = raw.lower()
    if value in {"x86_64", "amd64"}:
        return "x86_64"
    if value in {"arm64", "aarch64"}:
        return "aarch64"
    raise RuntimeError(f"Unsupported architecture for packaging: {raw}")


def _target_triple() -> str:
    system = platform.system().lower()
    arch = _arch_name(platform.machine())
    if system == "darwin":
        return f"{arch}-apple-darwin"
    if system == "linux":
        return f"{arch}-unknown-linux-gnu"
    if system == "windows":
        return f"{arch}-pc-windows-msvc"
    raise RuntimeError(f"Unsupported OS for packaging: {platform.system()}")


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(cwd) if cwd else None)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Familybook sidecar binary with Nuitka.")
    parser.add_argument("--target-triple", default=None, help="Override target triple for Tauri sidecar naming")
    parser.add_argument("--output-name", default="familybook-backend", help="Base sidecar binary name")
    args = parser.parse_args()

    triple = args.target_triple or _target_triple()
    output_name = args.output_name.strip() or "familybook-backend"

    build_dir = ROOT / ".build"
    nuitka_dir = build_dir / "nuitka"
    ui_release_dir = build_dir / "ui_release"
    binaries_dir = ROOT / "desktop" / "src-tauri" / "binaries"
    binaries_dir.mkdir(parents=True, exist_ok=True)

    _run([sys.executable, str(ROOT / "scripts" / "prepare_ui_release.py"), "--source", str(ROOT / "ui"), "--output", str(ui_release_dir)])

    nuitka_cmd = [
        sys.executable,
        "-m",
        "nuitka",
        str(ROOT / "familybook_app.py"),
        "--onefile",
        "--assume-yes-for-downloads",
        "--remove-output",
        f"--output-dir={nuitka_dir}",
        f"--output-filename={output_name}",
        f"--include-data-dir={ui_release_dir}=ui",
        "--include-package=requests",
        "--nofollow-import-to=pytest,unittest,tkinter,pydoc",
    ]
    _run(nuitka_cmd, cwd=ROOT)

    is_windows = platform.system().lower() == "windows"
    built_binary = nuitka_dir / (output_name + (".exe" if is_windows else ""))
    if not built_binary.exists():
        raise RuntimeError(f"Nuitka did not generate expected binary: {built_binary}")

    sidecar_name = f"{output_name}-{triple}" + (".exe" if triple.endswith("windows-msvc") else "")
    target_binary = binaries_dir / sidecar_name
    shutil.copy2(built_binary, target_binary)

    print(f"Built sidecar: {target_binary}")
    print("Next step: run `cd desktop && npm install && npm run tauri:build`")


if __name__ == "__main__":
    main()
