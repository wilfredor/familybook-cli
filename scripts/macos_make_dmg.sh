#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "" || "${2:-}" == "" ]]; then
  echo "Usage: $0 <path/to/AppName.app> <output.dmg> [volume-name]" >&2
  exit 1
fi

APP_PATH="$1"
OUTPUT_DMG="$2"
VOLUME_NAME="${3:-Familybook}"

if [[ ! -d "$APP_PATH" ]]; then
  echo "App bundle not found: $APP_PATH" >&2
  exit 1
fi

OUT_DIR="$(dirname "$OUTPUT_DMG")"
mkdir -p "$OUT_DIR"

TMP_STAGE="$(mktemp -d)"
TMP_DMG="${OUTPUT_DMG%.dmg}.rw.tmp.dmg"

cleanup() {
  rm -rf "$TMP_STAGE" || true
  rm -f "$TMP_DMG" || true
}
trap cleanup EXIT

cp -R "$APP_PATH" "$TMP_STAGE/"
ln -s /Applications "$TMP_STAGE/Applications"

rm -f "$OUTPUT_DMG"
hdiutil create -volname "$VOLUME_NAME" -srcfolder "$TMP_STAGE" -ov -format UDRW "$TMP_DMG"
hdiutil convert "$TMP_DMG" -format UDZO -imagekey zlib-level=9 -o "$OUTPUT_DMG"

echo "DMG created: $OUTPUT_DMG"
