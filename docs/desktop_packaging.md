# Familybook Desktop Packaging

This project can be shipped as a local desktop app (Linux/macOS/Windows) without exposing raw Python source files in the release bundle.

## Goals
- Keep everything local: UI + API + SQLite on user machine.
- Ship compiled backend binary (Nuitka) as Tauri sidecar.
- Reduce JS/CSS readability in releases via minification.
- Produce installers per OS with optional code signing.

## Build Prerequisites
- Python 3.11+
- Node.js 20+
- Rust stable + Cargo
- Tauri native prerequisites:
  - Linux: `webkit2gtk`, `libappindicator` (or modern tray deps), build-essential
  - macOS: Xcode command line tools
  - Windows: MSVC Build Tools + WebView2 runtime

Quick preflight:
```bash
python3 scripts/check_desktop_prereqs.py
```

## One-command Desktop Build

### Linux / macOS
```bash
./scripts/build_desktop.sh
```
This script auto-creates and uses `.venv-build`, so no system-wide pip install is needed.

With optional Cython hardening:
```bash
./scripts/build_desktop.sh --with-native
```

### Windows (PowerShell)
```powershell
.\scripts\build_desktop.ps1
```

This does:
1. install build Python deps (`requirements-build.txt`);
2. optionally supports Cython-native modules for sensitive code (`scripts/build_native_extensions.py`);
3. minify `ui/` into `.build/ui_release`;
4. compile `familybook_app.py` into one-file binary with Nuitka;
5. place sidecar binary in `desktop/src-tauri/binaries/` with target triple naming;
6. run `tauri build` and produce installer artifacts.
   - On macOS, script builds `.app` with Tauri and creates `.dmg` via `hdiutil` (avoids flaky `create-dmg` path).

## Build Sidecar Only
```bash
python3 scripts/build_sidecar.py
```

## Optional Cython Hardening
```bash
python3 -m pip install -r requirements-build.txt
python3 scripts/build_native_extensions.py
```

At runtime, the app tries `*_native` modules first and falls back to pure Python automatically.

## Output Artifacts
- Sidecar binary: `desktop/src-tauri/binaries/familybook-backend-<target-triple>[.exe]`
- Bundles/installers: `desktop/src-tauri/target/release/bundle/`
  - macOS DMG: `desktop/src-tauri/target/release/bundle/dmg/Familybook.dmg`

## Config in Release
- App defaults to `127.0.0.1:53682`.
- Runtime config file for source/dev runs: `familybook.app.json`.
- Release can still use env vars if needed (`FAMILYBOOK_HOST`, `FAMILYBOOK_PORT`, etc).

## Hardening Notes (Local-only)
- Nuitka one-file removes direct `.py` distribution.
- UI bundle is minified before embedding.
- Tauri release window disables devtools (`devtools: false`).
- For stronger protection, migrate sensitive modules to Cython/Rust progressively.

## Signing
- Use `scripts/sign_release.sh` as checklist/examples.
- Real certificates/secrets are required in CI or local secure keychain.

## Limitations
- No local desktop app can be made 100% tamper-proof.
- This setup raises the bar significantly for casual inspection/modification.
