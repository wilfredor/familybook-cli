#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

try:
    import csscompressor
except Exception:  # pragma: no cover
    csscompressor = None

try:
    import rjsmin
except Exception:  # pragma: no cover
    rjsmin = None


def _minify_js(raw: str) -> str:
    data = raw.replace("\r\n", "\n")
    if rjsmin is not None:
        return rjsmin.jsmin(data)
    return "\n".join(line for line in data.splitlines() if line.strip())


def _minify_css(raw: str) -> str:
    data = raw.replace("\r\n", "\n")
    if csscompressor is not None:
        return csscompressor.compress(data)
    return "\n".join(line for line in data.splitlines() if line.strip())


def _minify_html(raw: str) -> str:
    data = raw.replace("\r\n", "\n")
    data = re.sub(r">\s+<", "><", data)
    data = re.sub(r"\n{2,}", "\n", data)
    return data.strip() + "\n"


def build_ui_release(source_dir: Path, output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for item in source_dir.iterdir():
        if item.is_dir():
            shutil.copytree(item, output_dir / item.name)
            continue

        target = output_dir / item.name
        if item.suffix == ".js":
            target.write_text(_minify_js(item.read_text(encoding="utf-8")), encoding="utf-8")
        elif item.suffix == ".css":
            target.write_text(_minify_css(item.read_text(encoding="utf-8")), encoding="utf-8")
        elif item.suffix == ".html":
            target.write_text(_minify_html(item.read_text(encoding="utf-8")), encoding="utf-8")
        else:
            shutil.copy2(item, target)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build minified UI bundle for release packaging.")
    parser.add_argument("--source", default="ui", help="Source UI directory (default: ui)")
    parser.add_argument("--output", default=".build/ui_release", help="Output directory (default: .build/ui_release)")
    args = parser.parse_args()

    source_dir = Path(args.source).resolve()
    output_dir = Path(args.output).resolve()
    if not source_dir.exists():
        raise SystemExit(f"UI source directory not found: {source_dir}")
    build_ui_release(source_dir, output_dir)
    print(f"UI release prepared at: {output_dir}")


if __name__ == "__main__":
    main()
