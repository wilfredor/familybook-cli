#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys


def _cmd_version(cmd: str, args: list[str]) -> str | None:
    path = shutil.which(cmd)
    if not path:
        return None
    try:
        out = subprocess.check_output([path, *args], text=True, stderr=subprocess.STDOUT, timeout=6).strip()
    except Exception:
        return f"{path} (version check failed)"
    return f"{path} -> {out.splitlines()[0]}"


def _module_present(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def main() -> None:
    rows = [
        ("python", sys.executable),
        ("node", _cmd_version("node", ["-v"]) or "MISSING"),
        ("npm", _cmd_version("npm", ["-v"]) or "MISSING"),
        ("cargo", _cmd_version("cargo", ["-V"]) or "MISSING"),
        ("nuitka", "installed" if _module_present("nuitka") else "MISSING"),
        ("Cython", "installed" if _module_present("Cython") else "MISSING"),
        ("rjsmin", "installed" if _module_present("rjsmin") else "MISSING"),
        ("csscompressor", "installed" if _module_present("csscompressor") else "MISSING"),
        ("requests", "installed" if _module_present("requests") else "MISSING"),
    ]
    for key, value in rows:
        print(f"{key:14} {value}")


if __name__ == "__main__":
    main()
