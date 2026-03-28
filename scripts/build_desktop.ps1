Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$remainingArgs = @()
$withNative = $false
foreach ($arg in $args) {
  if ($arg -eq "--with-native") {
    $withNative = $true
  } else {
    $remainingArgs += $arg
  }
}

if (-not (Test-Path ".venv-build")) {
  python -m venv .venv-build
}

$pyBin = Join-Path $root ".venv-build\Scripts\python.exe"
& $pyBin -m pip install --upgrade pip
& $pyBin -m pip install -r requirements-build.txt
if ($withNative) {
  & $pyBin scripts/build_native_extensions.py
}
& $pyBin scripts/build_sidecar.py @remainingArgs

Set-Location (Join-Path $root "desktop")
npm install
npm run tauri:build
