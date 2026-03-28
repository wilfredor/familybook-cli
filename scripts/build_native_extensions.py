#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BUILD_SRC = ROOT / ".build" / "cython_src"

MODULE_MAP = [
    ("familybook_db.py", "familybook_db_native"),
    ("familybook_mirror.py", "familybook_mirror_native"),
    ("familybook_gedcom.py", "familybook_gedcom_native"),
]


def prepare_sources(extension_cls) -> list[Any]:
    if BUILD_SRC.exists():
        shutil.rmtree(BUILD_SRC)
    BUILD_SRC.mkdir(parents=True, exist_ok=True)

    extensions: list[Any] = []
    for source_name, module_name in MODULE_MAP:
        source_path = ROOT / source_name
        if not source_path.exists():
            continue
        target_py = BUILD_SRC / f"{module_name}.py"
        target_py.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")
        extensions.append(extension_cls(module_name, [str(target_py)]))
    return extensions


def main() -> None:
    parser = argparse.ArgumentParser(description="Build optional Cython native modules for hardening.")
    parser.add_argument("--inplace", action="store_true", default=True, help="Build extensions in project root (default: on)")
    parser.parse_args()

    try:
        from Cython.Build import cythonize
        from setuptools import Extension, setup
    except Exception as exc:
        raise SystemExit(f"Missing build dependencies (Cython/setuptools): {exc}")

    extensions = prepare_sources(Extension)
    if not extensions:
        raise SystemExit("No source modules found for Cython build.")

    setup(
        name="familybook_native_extensions",
        ext_modules=cythonize(
            extensions,
            compiler_directives={"language_level": "3", "binding": False},
            annotate=False,
        ),
        script_args=["build_ext", "--inplace"],
    )
    print("Native extension build complete.")


if __name__ == "__main__":
    main()
