#!/usr/bin/env python3
"""
Entry point dedicado para generar solo el libro (Markdown/PDF).
"""

from __future__ import annotations

import sys

import familybook


def main() -> None:
    # Reutiliza el parser/lógica central y fuerza modo libro.
    sys.argv = [sys.argv[0], *sys.argv[1:], "--book-only"]
    familybook.main()


if __name__ == "__main__":
    main()

