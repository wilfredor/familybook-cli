#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Manual signing helper."
echo "Expected build artifacts under: desktop/src-tauri/target/release/bundle"
echo
echo "macOS example:"
echo '  codesign --force --deep --sign "$APPLE_SIGN_IDENTITY" path/to/Familybook.app'
echo '  xcrun notarytool submit path/to/Familybook.dmg --apple-id "$APPLE_ID" --team-id "$APPLE_TEAM_ID" --password "$APPLE_APP_PASSWORD" --wait'
echo
echo "Windows example:"
echo '  signtool sign /fd SHA256 /tr http://timestamp.digicert.com /td SHA256 /a path\\to\\Familybook.msi'
echo
echo "Linux:"
echo "  Sign package metadata in your repository (deb/rpm) and publish checksums + signatures."
