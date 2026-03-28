#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

with_native=0
if [[ "${1:-}" == "--with-native" ]]; then
  with_native=1
  shift
fi

if [[ ! -d ".venv-build" ]]; then
  python3 -m venv .venv-build
fi

PYTHON_BIN=".venv-build/bin/python"
PIP_BIN=".venv-build/bin/pip"

"$PYTHON_BIN" -m pip install --upgrade pip
"$PIP_BIN" install -r requirements-build.txt
if [[ "$with_native" -eq 1 ]]; then
  "$PYTHON_BIN" scripts/build_native_extensions.py
fi
"$PYTHON_BIN" scripts/build_sidecar.py "$@"

cd desktop
npm install

if [[ "$(uname -s)" == "Darwin" ]]; then
  npm run tauri:build -- --bundles app
  APP_BUNDLE="src-tauri/target/release/bundle/macos/Familybook.app"
  DMG_OUT="src-tauri/target/release/bundle/dmg/Familybook.dmg"
  ../scripts/macos_make_dmg.sh "$APP_BUNDLE" "$DMG_OUT" "Familybook"
else
  npm run tauri:build
fi
