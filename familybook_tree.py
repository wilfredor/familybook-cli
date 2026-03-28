#!/usr/bin/env python3
"""
Entry point dedicado para generar solo el árbol SVG.
"""

from __future__ import annotations

import sys

import familybook


def main() -> None:
    args = [a for a in sys.argv[1:] if not (a == "--tree-svg-path" or a.startswith("--tree-svg-path="))]
    # Fuerza modo árbol único y salida única estándar.
    sys.argv = [sys.argv[0], *args, "--tree-svg", "--tree-only", "--tree-svg-path", "output/family_tree.svg"]
    familybook.main()


if __name__ == "__main__":
    main()
