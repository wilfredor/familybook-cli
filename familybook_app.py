from __future__ import annotations

import argparse
import csv
import io
import json
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import zipfile
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from urllib.request import Request, urlopen

import familybook

try:
    import familybook_db_native as familybook_db  # type: ignore[import-not-found]
except ImportError:
    import familybook_db

try:
    import familybook_gedcom_native as familybook_gedcom  # type: ignore[import-not-found]
except ImportError:
    import familybook_gedcom

try:
    import familybook_mirror_native as familybook_mirror  # type: ignore[import-not-found]
except ImportError:
    import familybook_mirror

try:
    import py7zr
    HAS_PY7ZR = True
except ImportError:
    HAS_PY7ZR = False

PANDOC_PATH = "/opt/homebrew/bin/pandoc"


def resolve_pandoc_path() -> str | None:
    path = shutil.which("pandoc")
    if path:
        return path
    if os.path.isfile(PANDOC_PATH) and os.access(PANDOC_PATH, os.X_OK):
        return PANDOC_PATH
    return None


def pandoc_capabilities() -> tuple[bool, str | None]:
    pandoc_path = resolve_pandoc_path()
    return (pandoc_path is not None, pandoc_path)


def resolve_pandoc_pdf_engine() -> str | None:
    for engine in ("tectonic", "xelatex", "lualatex", "pdflatex", "wkhtmltopdf", "weasyprint", "prince"):
        if shutil.which(engine):
            return engine
    return None


def _resolve_ui_dir() -> Path:
    explicit = os.getenv("FAMILYBOOK_UI_DIR", "").strip()
    if explicit:
        candidate = Path(explicit).expanduser()
        if candidate.exists():
            return candidate.resolve()
    return (Path(__file__).resolve().parent / "ui").resolve()


UI_DIR = _resolve_ui_dir()
DEFAULT_DB_PATH = "output/familybook.sqlite"
DEFAULT_ASSETS_ROOT = "output/familybook_assets"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 53682
DEFAULT_APP_CONFIG_PATH = "familybook.app.json"


def _load_app_config(path: str | None) -> dict:
    if not path:
        return {}
    target = Path(path).expanduser()
    if not target.exists():
        return {}
    try:
        raw = target.read_text(encoding="utf-8")
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


class AppState:
    def __init__(self, db_path: str, assets_root: str) -> None:
        self.db_path = os.path.abspath(db_path)
        self.assets_root = os.path.abspath(assets_root)
        self._lock = threading.Lock()
        self._sync_process: subprocess.Popen[str] | None = None
        self._oauth_pending: dict | None = None
        self._oauth_last_error: str | None = None
        self._oauth_last_event_at: str | None = None
        self._auto_recovery_attempted = False
        self._auto_recovery_result: dict | None = None

    def sync_status(self) -> dict:
        with self._lock:
            proc = self._sync_process
            if not proc:
                return {"running": False, "pid": None, "returncode": None}
            return {
                "running": proc.poll() is None,
                "pid": proc.pid,
                "returncode": proc.poll(),
            }

    def _oauth_settings(self, request_host: str | None = None) -> dict:
        explicit_redirect = os.getenv("FS_OAUTH_REDIRECT_URI", "").strip()
        redirect_uri = explicit_redirect or familybook.DEFAULT_OAUTH_REDIRECT_URI
        timeout_raw = os.getenv("FS_OAUTH_TIMEOUT_SECONDS", str(familybook.DEFAULT_OAUTH_TIMEOUT_SECONDS))
        try:
            timeout_seconds = int(timeout_raw)
        except ValueError:
            timeout_seconds = familybook.DEFAULT_OAUTH_TIMEOUT_SECONDS
        return {
            "client_id": os.getenv("FS_OAUTH_CLIENT_ID", "").strip(),
            "ident_base_url": os.getenv("FS_IDENT_BASE_URL", familybook.DEFAULT_IDENT_BASE_URL),
            "scope": os.getenv("FS_OAUTH_SCOPE", familybook.DEFAULT_OAUTH_SCOPE),
            "redirect_uri": redirect_uri,
            "timeout_seconds": timeout_seconds,
            "cache_path": os.path.expanduser(os.getenv("FS_TOKEN_CACHE_PATH", familybook.DEFAULT_TOKEN_CACHE_PATH)),
        }

    def _ensure_familysearch_oauth(self, request_host: str | None = None) -> dict:
        settings = self._oauth_settings(request_host)
        token = os.getenv("FS_ACCESS_TOKEN", "").strip()
        client_id = settings["client_id"]
        ident_base_url = settings["ident_base_url"]
        cache_path = settings["cache_path"]

        if token:
            return {"ok": True, "source": "env"}

        cached = None
        try:
            cached = familybook.load_cached_tokens(cache_path)
        except (OSError, ValueError, json.JSONDecodeError):
            cached = None

        if cached:
            if cached.is_expired():
                if cached.refresh_token and client_id:
                    try:
                        refreshed = familybook.refresh_access_token(
                            ident_base_url=ident_base_url,
                            client_id=client_id,
                            refresh_token=cached.refresh_token,
                        )
                        familybook.save_cached_tokens(cache_path, refreshed)
                        os.environ["FS_ACCESS_TOKEN"] = refreshed.access_token
                        self._oauth_last_error = None
                        self._oauth_last_event_at = familybook_db.utc_now_iso()
                        return {"ok": True, "source": "refresh"}
                    except Exception as exc:
                        return {"ok": False, "reason": "oauth_refresh_failed", "error": str(exc)}
                return {"ok": False, "reason": "oauth_token_expired", "error": "Token expirado sin refresh_token válido."}
            os.environ["FS_ACCESS_TOKEN"] = cached.access_token
            return {"ok": True, "source": "cache"}

        if not client_id:
            return {
                "ok": False,
                "reason": "missing_oauth_client_id",
                "error": "Falta FS_OAUTH_CLIENT_ID (AppKey).",
            }

        return {
            "ok": False,
            "reason": "oauth_login_required",
            "error": "OAuth login requerido.",
            "auth_start_path": "/api/auth/familysearch/start",
        }

    def oauth_status(self, request_host: str | None = None) -> dict:
        settings = self._oauth_settings(request_host)
        ensure = self._ensure_familysearch_oauth(request_host)
        with self._lock:
            pending = dict(self._oauth_pending) if self._oauth_pending else None
            if pending and "code_verifier" in pending:
                pending.pop("code_verifier", None)
        return {
            "authenticated": bool(ensure.get("ok")),
            "reason": ensure.get("reason"),
            "error": ensure.get("error"),
            "client_id_set": bool(settings["client_id"]),
            "redirect_uri": settings["redirect_uri"],
            "ident_base_url": settings["ident_base_url"],
            "pending": pending,
            "last_error": self._oauth_last_error,
            "last_event_at": self._oauth_last_event_at,
        }

    def oauth_start(self, request_host: str | None = None) -> dict:
        settings = self._oauth_settings(request_host)
        ensure = self._ensure_familysearch_oauth(request_host)
        if ensure.get("ok"):
            return {"started": False, "reason": "already_authenticated"}
        client_id = settings["client_id"]
        if not client_id:
            return {"started": False, "reason": "missing_oauth_client_id", "error": "Falta FS_OAUTH_CLIENT_ID (AppKey)."}

        state = secrets.token_urlsafe(24)
        code_verifier, code_challenge = familybook.build_pkce_pair()
        auth_url = (
            f"{settings['ident_base_url'].rstrip('/')}/cis-web/oauth2/v3/authorization?"
            + urlencode(
                {
                    "response_type": "code",
                    "client_id": client_id,
                    "redirect_uri": settings["redirect_uri"],
                    "scope": settings["scope"],
                    "state": state,
                    "code_challenge": code_challenge,
                    "code_challenge_method": "S256",
                }
            )
        )
        with self._lock:
            self._oauth_pending = {
                "state": state,
                "code_verifier": code_verifier,
                "redirect_uri": settings["redirect_uri"],
                "ident_base_url": settings["ident_base_url"],
                "client_id": client_id,
                "created_at": familybook_db.utc_now_iso(),
            }
            self._oauth_last_error = None
        return {"started": True, "auth_url": auth_url, "state": state, "redirect_uri": settings["redirect_uri"]}

    def oauth_handle_callback(self, query: dict[str, list[str]]) -> tuple[bool, str]:
        code = ((query.get("code") or [None])[0] or "").strip()
        returned_state = ((query.get("state") or [None])[0] or "").strip()
        oauth_error = ((query.get("error") or [None])[0] or "").strip()
        oauth_error_desc = ((query.get("error_description") or [None])[0] or "").strip()

        with self._lock:
            pending = dict(self._oauth_pending) if self._oauth_pending else None

        if not pending:
            self._oauth_last_error = "No hay una autenticación OAuth pendiente."
            return False, "No hay una autenticación OAuth pendiente."

        if oauth_error:
            message = oauth_error_desc or oauth_error
            with self._lock:
                self._oauth_pending = None
                self._oauth_last_error = message
                self._oauth_last_event_at = familybook_db.utc_now_iso()
            return False, f"Proveedor OAuth devolvió error: {message}"

        if returned_state != str(pending.get("state") or ""):
            with self._lock:
                self._oauth_pending = None
                self._oauth_last_error = "State inválido."
                self._oauth_last_event_at = familybook_db.utc_now_iso()
            return False, "State OAuth inválido."
        if not code:
            with self._lock:
                self._oauth_pending = None
                self._oauth_last_error = "Callback sin code."
                self._oauth_last_event_at = familybook_db.utc_now_iso()
            return False, "No se recibió code en el callback."

        try:
            tokens = familybook.exchange_authorization_code(
                ident_base_url=str(pending["ident_base_url"]),
                client_id=str(pending["client_id"]),
                redirect_uri=str(pending["redirect_uri"]),
                code=code,
                code_verifier=str(pending["code_verifier"]),
            )
            settings = self._oauth_settings()
            familybook.save_cached_tokens(str(settings["cache_path"]), tokens)
            os.environ["FS_ACCESS_TOKEN"] = tokens.access_token
            with self._lock:
                self._oauth_pending = None
                self._oauth_last_error = None
                self._oauth_last_event_at = familybook_db.utc_now_iso()
            return True, "Autenticación FamilySearch completada."
        except Exception as exc:
            with self._lock:
                self._oauth_pending = None
                self._oauth_last_error = str(exc)
                self._oauth_last_event_at = familybook_db.utc_now_iso()
            return False, f"No se pudo completar OAuth: {exc}"

    def oauth_disconnect(self) -> None:
        settings = self._oauth_settings()
        os.environ.pop("FS_ACCESS_TOKEN", None)
        cache_path = str(settings["cache_path"])
        try:
            if os.path.isfile(cache_path):
                os.remove(cache_path)
        except OSError:
            pass
        with self._lock:
            self._oauth_pending = None
            self._oauth_last_error = None
            self._oauth_last_event_at = familybook_db.utc_now_iso()

    def stop_sync(self) -> dict:
        with self._lock:
            proc = self._sync_process
            if not proc or proc.poll() is not None:
                self._sync_process = None
                return {"stopped": False, "reason": "sync_not_running"}
            pid = proc.pid
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass
        returncode = proc.poll()
        with self._lock:
            if self._sync_process is proc and returncode is not None:
                self._sync_process = None
        return {"stopped": True, "pid": pid, "returncode": returncode}

    @staticmethod
    def _timestamp_slug() -> str:
        return (
            familybook_db.utc_now_iso()
            .replace("-", "")
            .replace(":", "")
            .replace("T", "_")
            .replace("Z", "")
        )

    def _list_recovery_candidates(self, limit: int = 20) -> list[Path]:
        db_file = Path(self.db_path)
        parent = db_file.parent
        patterns = [
            f"{db_file.name}.corrupt_*.bak",
            f"{db_file.name}.pre_recreate_*.bak",
            f"{db_file.name}.invalid_pre_recover_*.bak",
            f"{db_file.name}.import_tmp*",
            f"{db_file.name}.bak",
        ]
        seen: set[Path] = set()
        candidates: list[Path] = []
        for pattern in patterns:
            for item in parent.glob(pattern):
                resolved = item.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                if item.is_file():
                    candidates.append(item)
        candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
        return candidates[: max(1, limit)]

    def _clear_db_sidecars(self) -> None:
        for suffix in ("-wal", "-shm"):
            sidecar = Path(self.db_path + suffix)
            if sidecar.exists():
                try:
                    sidecar.unlink()
                except OSError:
                    pass

    def inspect_database(self) -> dict:
        db_file = Path(self.db_path)
        exists = db_file.exists() and db_file.is_file()
        valid = False
        error: str | None = None
        persons = 0

        if exists:
            valid, error = _validate_backup_database(db_file)
            if valid:
                conn: sqlite3.Connection | None = None
                try:
                    conn = sqlite3.connect(self.db_path)
                    row = conn.execute("SELECT COUNT(*) FROM persons").fetchone()
                    persons = int(row[0]) if row and row[0] is not None else 0
                except sqlite3.Error as exc:
                    valid = False
                    error = str(exc)
                finally:
                    if conn is not None:
                        conn.close()
        else:
            error = "missing_database_file"

        return {
            "path": self.db_path,
            "exists": exists,
            "valid": valid,
            "error": error,
            "persons": persons,
        }

    def recover_database(self, *, allow_repair: bool = True) -> dict:
        if self.sync_status().get("running"):
            return {"ok": False, "reason": "sync_running"}

        current = self.inspect_database()
        if current.get("valid"):
            return {"ok": False, "reason": "db_already_valid"}

        candidates = self._list_recovery_candidates()
        if not candidates:
            return {"ok": False, "reason": "no_backup_candidates", "attempted": []}

        db_file = Path(self.db_path)
        timestamp = self._timestamp_slug()
        attempted: list[dict] = []

        for idx, candidate in enumerate(candidates, start=1):
            direct_ok, direct_error = _validate_backup_database(candidate)
            if direct_ok:
                tmp_copy = db_file.with_name(f"{db_file.name}.recover_tmp_{timestamp}_{idx}")
                try:
                    shutil.copy2(str(candidate), str(tmp_copy))
                    tmp_valid, tmp_error = _validate_backup_database(tmp_copy)
                    if not tmp_valid:
                        attempted.append({
                            "candidate": str(candidate),
                            "method": "direct_copy",
                            "error": tmp_error or "tmp_validation_failed",
                        })
                    else:
                        preserved_invalid: str | None = None
                        self._clear_db_sidecars()
                        if db_file.exists():
                            invalid_bak = db_file.with_name(f"{db_file.name}.invalid_pre_recover_{timestamp}.bak")
                            os.replace(str(db_file), str(invalid_bak))
                            preserved_invalid = str(invalid_bak)
                        os.replace(str(tmp_copy), str(db_file))
                        return {
                            "ok": True,
                            "method": "direct_copy",
                            "source": str(candidate),
                            "preserved_invalid_db": preserved_invalid,
                            "status": self.inspect_database(),
                        }
                except OSError as exc:
                    attempted.append({
                        "candidate": str(candidate),
                        "method": "direct_copy",
                        "error": str(exc),
                    })
                finally:
                    if tmp_copy.exists():
                        try:
                            tmp_copy.unlink()
                        except OSError:
                            pass
                continue

            attempted.append({
                "candidate": str(candidate),
                "method": "direct_copy",
                "error": direct_error or "invalid_candidate",
            })
            if not allow_repair:
                continue

            repair_tmp = db_file.with_name(f"{db_file.name}.repair_tmp_{timestamp}_{idx}")
            repaired_ok, repair_error = _repair_sqlite_backup(candidate, repair_tmp)
            if not repaired_ok:
                attempted.append({
                    "candidate": str(candidate),
                    "method": "repair",
                    "error": repair_error or "repair_failed",
                })
                continue
            repaired_valid, repaired_validation_error = _validate_backup_database(repair_tmp)
            if not repaired_valid:
                attempted.append({
                    "candidate": str(candidate),
                    "method": "repair",
                    "error": repaired_validation_error or "repaired_db_invalid",
                })
                if repair_tmp.exists():
                    try:
                        repair_tmp.unlink()
                    except OSError:
                        pass
                continue
            preserved_invalid = None
            self._clear_db_sidecars()
            if db_file.exists():
                invalid_bak = db_file.with_name(f"{db_file.name}.invalid_pre_recover_{timestamp}.bak")
                os.replace(str(db_file), str(invalid_bak))
                preserved_invalid = str(invalid_bak)
            os.replace(str(repair_tmp), str(db_file))
            return {
                "ok": True,
                "method": "repair",
                "source": str(candidate),
                "preserved_invalid_db": preserved_invalid,
                "status": self.inspect_database(),
            }

        return {"ok": False, "reason": "recovery_failed", "attempted": attempted}

    def bootstrap_status(self, request_host: str | None = None) -> dict:
        db = self.inspect_database()
        auto_recovery: dict | None = None
        if not db.get("valid"):
            with self._lock:
                should_attempt = not self._auto_recovery_attempted
            if should_attempt:
                auto_recovery = self.recover_database(allow_repair=True)
                with self._lock:
                    self._auto_recovery_attempted = True
                    self._auto_recovery_result = auto_recovery
                db = self.inspect_database()
            else:
                with self._lock:
                    auto_recovery = self._auto_recovery_result
        else:
            with self._lock:
                auto_recovery = self._auto_recovery_result
        oauth = self.oauth_status(request_host=request_host)
        sync = self.sync_status()
        latest_run = None
        last_sync_at = None

        if db.get("valid"):
            conn: sqlite3.Connection | None = None
            try:
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                latest_row = conn.execute(
                    """
                    SELECT id, started_at, finished_at, status, jobs_done, jobs_failed,
                           persons_count, relationships_count, media_count, last_error
                    FROM sync_runs
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
                if latest_row:
                    latest_run = dict(latest_row)
                meta_row = conn.execute(
                    "SELECT value FROM metadata WHERE key = 'last_sync_at'"
                ).fetchone()
                if meta_row and meta_row["value"] is not None:
                    last_sync_at = str(meta_row["value"])
            except sqlite3.Error:
                pass
            finally:
                if conn is not None:
                    conn.close()

        has_people = bool(db.get("valid")) and int(db.get("persons") or 0) > 0
        return {
            "db": db,
            "oauth": oauth,
            "sync_process": sync,
            "latest_run": latest_run,
            "last_sync_at": last_sync_at,
            "assets_root": self.assets_root,
            "assets_exists": Path(self.assets_root).is_dir(),
            "auto_recovery_attempted": self._auto_recovery_attempted,
            "auto_recovery": auto_recovery,
            "ready_for_app": bool(db.get("valid")),
            "has_people": has_people,
            "ready": has_people,
        }

    def recreate_database(self, force: bool = False) -> dict:
        if self.sync_status().get("running"):
            return {"ok": False, "reason": "sync_running"}

        current = self.inspect_database()
        current_people = int(current.get("persons") or 0)
        if current.get("valid") and current_people > 0 and not force:
            return {"ok": False, "reason": "db_has_data", "persons": current_people}

        db_file = Path(self.db_path)
        timestamp = self._timestamp_slug()
        backups: list[str] = []

        try:
            if db_file.exists():
                bak_path = db_file.with_name(f"{db_file.name}.pre_recreate_{timestamp}.bak")
                os.replace(str(db_file), str(bak_path))
                backups.append(str(bak_path))

            for suffix in ("-wal", "-shm"):
                sidecar = Path(self.db_path + suffix)
                if sidecar.exists():
                    sidecar_bak = sidecar.with_name(f"{sidecar.name}.pre_recreate_{timestamp}.bak")
                    os.replace(str(sidecar), str(sidecar_bak))
                    backups.append(str(sidecar_bak))

            conn = familybook_db.connect(self.db_path)
            conn.close()
            os.makedirs(self.assets_root, exist_ok=True)
        except (OSError, sqlite3.Error) as exc:
            return {"ok": False, "reason": "recreate_failed", "error": str(exc), "backups": backups}

        return {"ok": True, "backups": backups, "status": self.bootstrap_status()}

    def start_sync(
        self,
        job_limit: int | None = None,
        force: bool = False,
        stubs_mode: bool = False,
        root_person_id: str | None = None,
        generations: int = 4,
        collateral_depth: int | None = None,
        request_host: str | None = None,
    ) -> dict:
        with self._lock:
            if self._sync_process and self._sync_process.poll() is None:
                return {"started": False, "reason": "sync_already_running", "pid": self._sync_process.pid}

            oauth = self._ensure_familysearch_oauth(request_host)
            if not oauth.get("ok"):
                return {"started": False, "reason": oauth.get("reason", "oauth_unavailable"), "error": oauth.get("error")}

            cmd = [
                sys.executable,
                "familybook.py",
                "--sync-local-db",
                "--no-pdf",
                "--book-only",
                "--local-db-path",
                self.db_path,
            ]
            if stubs_mode:
                cmd.append("--current-person")
            elif root_person_id:
                cmd.extend(["--person-id", root_person_id.strip()])
            else:
                cmd.append("--current-person")
            parsed_generations = max(1, min(int(generations), 12))
            if collateral_depth is None:
                parsed_collateral_depth = parsed_generations
            else:
                parsed_collateral_depth = max(0, min(int(collateral_depth), 12))
            cmd.extend(["--generations", str(parsed_generations)])
            cmd.extend(["--collateral-depth", str(parsed_collateral_depth)])
            if job_limit is not None:
                cmd.extend(["--sync-job-limit", str(job_limit)])
            if force:
                cmd.append("--sync-force")
            if stubs_mode:
                cmd.append("--sync-db-stubs")

            env = os.environ.copy()
            proc = subprocess.Popen(
                cmd,
                cwd=str(Path(__file__).resolve().parent),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            self._sync_process = proc
            return {"started": True, "pid": proc.pid}


def json_response(handler: "FamilybookHandler", payload: dict | list, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


WIKIPEDIA_API_BASE = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_PAGE_BASE = "https://en.wikipedia.org/wiki/"
WIKIPEDIA_UA = "familybook-local-app/0.1 (historical-events-sync)"


def _slugify_key(text: str, max_len: int = 64) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (text or "").strip().lower()).strip("-")
    if not slug:
        return "event"
    return slug[: max_len].strip("-") or "event"


def _http_get_json(url: str, timeout: int = 20) -> dict | None:
    req = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": WIKIPEDIA_UA,
        },
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _wiki_get_year_wikitext(page_title: str) -> str | None:
    url = (
        f"{WIKIPEDIA_API_BASE}?action=parse&format=json&formatversion=2"
        f"&prop=wikitext&page={quote(page_title)}"
    )
    payload = _http_get_json(url, timeout=25)
    if not payload:
        return None
    parse = payload.get("parse") if isinstance(payload, dict) else None
    if not isinstance(parse, dict):
        return None
    wikitext = parse.get("wikitext")
    return str(wikitext) if isinstance(wikitext, str) else None


def _clean_wiki_markup(text: str) -> str:
    out = text
    out = re.sub(r"<ref[^>]*>.*?</ref>", "", out, flags=re.IGNORECASE | re.DOTALL)
    out = re.sub(r"<[^>]+>", "", out)
    # Remove simple templates repeatedly.
    for _ in range(5):
        next_out = re.sub(r"\{\{[^{}]*\}\}", "", out)
        if next_out == out:
            break
        out = next_out
    out = re.sub(r"\[\[(?:[^|\]]+\|)?([^\]]+)\]\]", r"\1", out)
    out = out.replace("'''", "").replace("''", "")
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _extract_events_from_year_wikitext(wikitext: str) -> list[str]:
    lines = wikitext.splitlines()
    in_events = False
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not in_events and re.match(r"^==+\s*Events\s*==+\s*$", stripped, flags=re.IGNORECASE):
            in_events = True
            continue
        if in_events and re.match(r"^==[^=].*==\s*$", stripped):
            break
        if not in_events or not stripped.startswith("*"):
            continue
        cleaned = _clean_wiki_markup(stripped.lstrip("*").strip())
        if len(cleaned) < 18:
            continue
        out.append(cleaned)
    return out


def _event_title_from_line(line: str, fallback: str) -> str:
    candidate = line
    if " – " in candidate:
        candidate = candidate.split(" – ", 1)[1].strip()
    elif " - " in candidate:
        candidate = candidate.split(" - ", 1)[1].strip()
    candidate = candidate.split(". ", 1)[0].strip(" .")
    return (candidate[:140] if candidate else fallback)


def _event_relevance_score(line: str, *, country: str | None = None) -> float:
    lowered = line.lower()
    score = 0.0

    major_tokens = [
        "war",
        "revolution",
        "independence",
        "constitution",
        "treaty",
        "founded",
        "foundation",
        "independencia",
        "constitución",
        "tratado",
        "pandemic",
        "epidemic",
        "earthquake",
        "hurricane",
        "election",
        "coup",
        "peace",
        "united nations",
        "un ",
    ]
    for token in major_tokens:
        if token in lowered:
            score += 2.4

    if country:
        country_l = country.strip().lower()
        if country_l and country_l in lowered:
            score += 4.0

    soft_tokens = [
        "wins",
        "win ",
        "chart",
        "album",
        "film",
        "movie",
        "series",
        "sports",
        "football",
        "baseball",
    ]
    for token in soft_tokens:
        if token in lowered:
            score -= 1.1

    length = len(line)
    if length < 40:
        score -= 1.5
    elif length > 85:
        score += 0.6

    if re.search(r"\b\d{2,4}\b", line):
        score += 0.25
    if " - " in line or " – " in line:
        score += 0.2
    return score


def _pick_best_event_line(events: list[str], *, country: str | None = None, require_country: bool = False) -> str | None:
    if not events:
        return None
    country_l = (country or "").strip().lower()
    if require_country and country_l:
        filtered = [line for line in events if country_l in line.lower()]
    else:
        filtered = events
    if not filtered:
        return None
    return max(filtered, key=lambda line: _event_relevance_score(line, country=country))


def _fetch_year_event(year: int, *, country: str | None = None) -> dict | None:
    page = f"{year} in {country}" if country else str(year)
    best = None
    wikitext = _wiki_get_year_wikitext(page)
    if wikitext:
        events = _extract_events_from_year_wikitext(wikitext)
        best = _pick_best_event_line(events, country=country, require_country=False)

    # Fallback: if local yearly page doesn't exist, try global year and only keep local mentions.
    if country and not best:
        global_page = str(year)
        global_wikitext = _wiki_get_year_wikitext(global_page)
        if global_wikitext:
            global_events = _extract_events_from_year_wikitext(global_wikitext)
            best = _pick_best_event_line(global_events, country=country, require_country=True)
            if best:
                page = global_page

    if not best:
        return None
    return {
        "title": _event_title_from_line(best, f"{year}"),
        "description": best[:800],
        "source_url": WIKIPEDIA_PAGE_BASE + quote(page.replace(" ", "_")),
    }


def sync_historical_events_from_service(
    db_path: str,
    *,
    year_from: int,
    year_to: int,
    local_country: str,
) -> dict:
    safe_from = max(1500, min(int(year_from), int(year_to)))
    safe_to = min(2100, max(int(year_from), int(year_to)))
    if safe_to - safe_from > 80:
        safe_to = safe_from + 80
    country = (local_country or "Venezuela").strip() or "Venezuela"
    country_slug = re.sub(r"[^a-z0-9]+", "-", country.lower()).strip("-") or "local"
    stored_global = 0
    stored_local = 0
    attempted_years = 0
    for year in range(safe_from, safe_to + 1):
        attempted_years += 1
        global_event = _fetch_year_event(year, country=None)
        if global_event:
            familybook_db.upsert_historical_event(
                db_path,
                event_key=f"svc:global:{year}",
                scope="global",
                title=global_event["title"],
                description=global_event["description"],
                start_year=year,
                end_year=year,
                source_url=global_event.get("source_url"),
                match_terms=[],
            )
            stored_global += 1
        local_event = _fetch_year_event(year, country=country)
        if local_event:
            familybook_db.upsert_historical_event(
                db_path,
                event_key=f"svc:local:{country_slug}:{year}",
                scope="local",
                title=local_event["title"],
                description=local_event["description"],
                start_year=year,
                end_year=year,
                source_url=local_event.get("source_url"),
                match_terms=[country.lower()],
            )
            stored_local += 1
    return {
        "year_from": safe_from,
        "year_to": safe_to,
        "country": country,
        "years_processed": attempted_years,
        "stored_global": stored_global,
        "stored_local": stored_local,
    }


def _csv_headers_map(fieldnames: list[str] | None) -> dict[str, str]:
    return {str(name).strip().lower(): str(name) for name in (fieldnames or []) if str(name).strip()}


def _parse_float(value: object) -> float | None:
    raw = str(value or "").strip().replace(",", "")
    if not raw:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _parse_int(value: object) -> int | None:
    raw = str(value or "").strip().replace(",", "")
    if not raw:
        return None
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _parse_json_payload(text: str) -> dict | list | None:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, (dict, list)) else None


def _parse_dna_csv(csv_text: str) -> tuple[str, list]:
    """Auto-detect DNA CSV format and return (source, segments_list)."""
    reader = csv.DictReader(io.StringIO(csv_text))
    headers = [h.strip().lower() for h in (reader.fieldnames or [])]
    header_map = _csv_headers_map(reader.fieldnames)

    # Detect provider
    if "start_point" in headers and "end_point" in headers:
        source = "23andme"
        chr_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("chromosome", "chromosomeid")), "chromosome")
        start_col = next((h for h in reader.fieldnames or [] if h.strip().lower() == "start_point"), "start_point")
        end_col = next((h for h in reader.fieldnames or [] if h.strip().lower() == "end_point"), "end_point")
        ancestry_col = next((h for h in reader.fieldnames or [] if h.strip().lower() == "ancestry"), None)
        cm_col = None
    elif "start pos" in headers or "start_pos" in headers:
        source = "ancestrydna"
        chr_col = next((h for h in reader.fieldnames or [] if h.strip().lower() == "chromosome"), "Chromosome")
        start_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("start pos", "start_pos")), "Start Pos")
        end_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("end pos", "end_pos")), "End Pos")
        ancestry_col = next((h for h in reader.fieldnames or [] if "ethnicity" in h.strip().lower()), None)
        cm_col = None
    elif "start location" in headers or "centimorgans" in headers:
        source = "ftdna"
        chr_col = next((h for h in reader.fieldnames or [] if h.strip().lower() == "chromosome"), "Chromosome")
        start_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("start location", "start_location")), "Start Location")
        end_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("end location", "end_location")), "End Location")
        cm_col = next((h for h in reader.fieldnames or [] if "centimorgan" in h.strip().lower()), None)
        ancestry_col = None
    else:
        source = "generic"
        chr_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("chr", "chromosome", "chrom")), "chromosome")
        start_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("start", "start_pos", "startpos")), "start")
        end_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("end", "end_pos", "endpos")), "end")
        cm_col = next((h for h in reader.fieldnames or [] if "centimorgan" in h.strip().lower() or h.strip().lower() == "cm"), None)
        ancestry_col = next((h for h in reader.fieldnames or [] if h.strip().lower() in ("ancestry", "category", "label")), None)
    match_col = header_map.get("match_name") or header_map.get("match") or header_map.get("relative")
    branch_side_col = header_map.get("branch_side") or header_map.get("side") or header_map.get("parent_side")
    branch_label_col = header_map.get("branch_label") or header_map.get("branch") or header_map.get("ancestor")
    ancestor_person_col = header_map.get("ancestor_person_id")
    kind_col = header_map.get("segment_kind") or header_map.get("kind")

    segments = []
    for row in reader:
        try:
            chrom = str(row.get(chr_col, "")).strip()
            if not chrom:
                continue
            start_pos = _parse_int(row.get(start_col, "0")) or 0
            end_pos = _parse_int(row.get(end_col, "0")) or 0
            cm = _parse_float(row.get(cm_col, "")) if cm_col else None
            ancestry = str(row.get(ancestry_col, "")).strip() if ancestry_col else None
            segments.append({
                "chromosome": chrom,
                "start_pos": start_pos,
                "end_pos": end_pos,
                "centimorgans": cm,
                "ancestry": ancestry or None,
                "match_name": str(row.get(match_col, "")).strip() or None if match_col else None,
                "branch_side": str(row.get(branch_side_col, "")).strip() or None if branch_side_col else None,
                "branch_label": str(row.get(branch_label_col, "")).strip() or None if branch_label_col else None,
                "ancestor_person_id": str(row.get(ancestor_person_col, "")).strip() or None if ancestor_person_col else None,
                "segment_kind": str(row.get(kind_col, "")).strip() or None if kind_col else None,
            })
        except (ValueError, TypeError):
            continue
    return source, segments


def _parse_raw_snps_text(text: str) -> list[dict]:
    payload = _parse_json_payload(text)
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("items"), list):
        rows = payload["items"]
    else:
        rows = None
    if isinstance(rows, list):
        snps = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            rsid = str(item.get("rsid", "")).strip()
            chromosome = str(item.get("chromosome", "")).strip()
            position = _parse_int(item.get("position"))
            genotype = str(item.get("genotype", "")).strip().upper()
            if rsid and chromosome and position and genotype:
                snps.append({
                    "rsid": rsid,
                    "chromosome": chromosome,
                    "position": position,
                    "genotype": genotype,
                })
        return snps

    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not lines:
        return []
    delimiter = "\t" if "\t" in lines[0] else ","
    if lines[0].lower().startswith("rsid") or "chromosome" in lines[0].lower():
        reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter=delimiter)
        header_map = _csv_headers_map(reader.fieldnames)
        rsid_col = header_map.get("rsid", "rsid")
        chr_col = header_map.get("chromosome") or header_map.get("chrom") or "chromosome"
        pos_col = header_map.get("position") or header_map.get("pos") or "position"
        genotype_col = header_map.get("genotype") or "genotype"
        snps = []
        for row in reader:
            rsid = str(row.get(rsid_col, "")).strip()
            chromosome = str(row.get(chr_col, "")).strip()
            position = _parse_int(row.get(pos_col))
            genotype = str(row.get(genotype_col, "")).strip().upper()
            if rsid and chromosome and position and genotype:
                snps.append({
                    "rsid": rsid,
                    "chromosome": chromosome,
                    "position": position,
                    "genotype": genotype,
                })
        return snps

    snps = []
    for line in lines:
        parts = re.split(r"[\t, ]+", line)
        if len(parts) < 4:
            continue
        rsid, chromosome, position_raw, genotype = parts[:4]
        position = _parse_int(position_raw)
        if rsid and chromosome and position and genotype:
            snps.append({
                "rsid": rsid.strip(),
                "chromosome": chromosome.strip(),
                "position": position,
                "genotype": genotype.strip().upper(),
            })
    return snps


def _parse_ethnicity_payload(text: str) -> list[dict]:
    payload = _parse_json_payload(text)
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        payload = payload["items"]
    if isinstance(payload, list):
        items = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            region = str(row.get("region", "")).strip()
            percentage = _parse_float(row.get("percentage"))
            if region and percentage is not None:
                items.append({
                    "region": region,
                    "percentage": percentage,
                    "reference_panel": row.get("reference_panel"),
                    "generation_estimate": row.get("generation_estimate") or row.get("generation"),
                    "side": row.get("side"),
                    "color_hint": row.get("color_hint") or row.get("color"),
                })
        return items

    reader = csv.DictReader(io.StringIO(text))
    header_map = _csv_headers_map(reader.fieldnames)
    region_col = header_map.get("region") or header_map.get("population") or "region"
    pct_col = header_map.get("percentage") or header_map.get("percent") or "percentage"
    ref_col = header_map.get("reference_panel") or header_map.get("panel")
    gen_col = header_map.get("generation_estimate") or header_map.get("generation")
    side_col = header_map.get("side")
    color_col = header_map.get("color_hint") or header_map.get("color")
    items = []
    for row in reader:
        region = str(row.get(region_col, "")).strip()
        percentage = _parse_float(row.get(pct_col))
        if region and percentage is not None:
            items.append({
                "region": region,
                "percentage": percentage,
                "reference_panel": row.get(ref_col) if ref_col else None,
                "generation_estimate": row.get(gen_col) if gen_col else None,
                "side": row.get(side_col) if side_col else None,
                "color_hint": row.get(color_col) if color_col else None,
            })
    return items


def _parse_haplogroups_payload(text: str) -> dict:
    payload = _parse_json_payload(text)
    if isinstance(payload, dict):
        return payload
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if rows:
        row = rows[0]
        header_map = _csv_headers_map(reader.fieldnames)
        return {
            "y_haplogroup": row.get(header_map.get("y_haplogroup") or "y_haplogroup"),
            "mt_haplogroup": row.get(header_map.get("mt_haplogroup") or "mt_haplogroup"),
        }
    parsed: dict[str, object] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _parse_matches_payload(text: str) -> list[dict]:
    payload = _parse_json_payload(text)
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        payload = payload["items"]
    if isinstance(payload, list):
        matches = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            match_name = str(row.get("match_name", "")).strip()
            if not match_name:
                continue
            matches.append({
                "match_name": match_name,
                "total_cm": _parse_float(row.get("total_cm")),
                "segments_count": _parse_int(row.get("segments_count")),
                "predicted_relationship": row.get("predicted_relationship"),
                "side": row.get("side"),
                "notes": row.get("notes") if isinstance(row.get("notes"), dict) else {},
                "segments": row.get("segments") if isinstance(row.get("segments"), list) else [],
            })
        return matches

    reader = csv.DictReader(io.StringIO(text))
    header_map = _csv_headers_map(reader.fieldnames)
    match_col = header_map.get("match_name") or header_map.get("match") or header_map.get("relative") or "match_name"
    total_col = header_map.get("total_cm") or header_map.get("shared_cm")
    count_col = header_map.get("segments_count")
    rel_col = header_map.get("predicted_relationship") or header_map.get("relationship")
    side_col = header_map.get("side")
    chr_col = header_map.get("chromosome") or header_map.get("chr")
    start_col = header_map.get("start") or header_map.get("start_pos")
    end_col = header_map.get("end") or header_map.get("end_pos")
    cm_col = header_map.get("centimorgans") or header_map.get("cm")
    ancestry_col = header_map.get("ancestry")
    grouped: dict[str, dict] = {}
    for row in reader:
        match_name = str(row.get(match_col, "")).strip()
        if not match_name:
            continue
        item = grouped.setdefault(
            match_name,
            {
                "match_name": match_name,
                "total_cm": _parse_float(row.get(total_col)) if total_col else None,
                "segments_count": _parse_int(row.get(count_col)) if count_col else None,
                "predicted_relationship": row.get(rel_col) if rel_col else None,
                "side": row.get(side_col) if side_col else None,
                "notes": {},
                "segments": [],
            },
        )
        if chr_col and start_col and end_col:
            chromosome = str(row.get(chr_col, "")).strip()
            start_pos = _parse_int(row.get(start_col))
            end_pos = _parse_int(row.get(end_col))
            if chromosome and start_pos is not None and end_pos is not None:
                item["segments"].append({
                    "chromosome": chromosome,
                    "start_pos": start_pos,
                    "end_pos": end_pos,
                    "centimorgans": _parse_float(row.get(cm_col)) if cm_col else None,
                    "ancestry": str(row.get(ancestry_col, "")).strip() or None if ancestry_col else None,
                })
    return list(grouped.values())


def _validate_backup_database(path: Path) -> tuple[bool, str | None]:
    required_tables = {"persons", "relationships", "metadata"}
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(path))
        row = conn.execute("PRAGMA integrity_check").fetchone()
        integrity = str(row[0]).strip().lower() if row and row[0] is not None else ""
        if integrity != "ok":
            return False, f"integrity_check_failed:{integrity or 'unknown'}"
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        table_names = {str(item[0]) for item in rows if item and item[0]}
        missing = sorted(required_tables - table_names)
        if missing:
            return False, f"missing_tables:{','.join(missing)}"
        conn.execute("SELECT COUNT(*) FROM persons").fetchone()
        return True, None
    except sqlite3.Error as exc:
        return False, str(exc)
    finally:
        if conn is not None:
            conn.close()


def _repair_sqlite_backup(source: Path, target: Path) -> tuple[bool, str | None]:
    if target.exists():
        try:
            target.unlink()
        except OSError:
            pass

    cli_error: str | None = None
    sqlite_cli = shutil.which("sqlite3")
    if sqlite_cli:
        try:
            recovered_sql = subprocess.run(
                [sqlite_cli, str(source), ".recover"],
                capture_output=True,
                text=True,
                timeout=90,
                check=False,
            )
            sql_text = (recovered_sql.stdout or "").strip()
            if recovered_sql.returncode == 0 and sql_text:
                conn = sqlite3.connect(str(target))
                try:
                    conn.executescript(sql_text)
                    conn.commit()
                finally:
                    conn.close()
                return True, None
            cli_error = (recovered_sql.stderr or "").strip() or f"sqlite3_recover_exit_{recovered_sql.returncode}"
        except (OSError, subprocess.SubprocessError) as exc:
            cli_error = str(exc)
    else:
        cli_error = "sqlite3_cli_unavailable"

    # Fallback path: dump readable objects and rebuild a fresh DB.
    src: sqlite3.Connection | None = None
    dst: sqlite3.Connection | None = None
    try:
        src = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
        dump_sql = "\n".join(src.iterdump()).strip()
        if not dump_sql:
            return False, f"empty_dump_after_repair ({cli_error})"
        dst = sqlite3.connect(str(target))
        dst.executescript(dump_sql)
        dst.commit()
        return True, None
    except sqlite3.Error as exc:
        return False, f"{cli_error}; {exc}"
    finally:
        if src is not None:
            src.close()
        if dst is not None:
            dst.close()


class FamilybookHandler(SimpleHTTPRequestHandler):
    server_version = "FamilybookApp/0.1"

    def log_message(self, fmt: str, *args: object) -> None:
        return

    @staticmethod
    def _coerce_int(value: object, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            parsed = default
        if minimum is not None:
            parsed = max(minimum, parsed)
        if maximum is not None:
            parsed = min(maximum, parsed)
        return parsed

    @staticmethod
    def _safe_child_path(root: Path, relative_path: str) -> Path | None:
        root_resolved = root.resolve()
        candidate = (root_resolved / relative_path).resolve()
        if candidate == root_resolved or root_resolved in candidate.parents:
            return candidate
        return None

    def _cors_allowed_origin(self) -> str | None:
        origin = (self.headers.get("Origin") or "").strip()
        if not origin:
            return None
        parsed = urlparse(origin)
        if parsed.scheme == "tauri":
            return origin
        if parsed.scheme in {"http", "https"} and parsed.hostname in {"127.0.0.1", "localhost"}:
            return origin
        return None

    def end_headers(self) -> None:
        allowed_origin = self._cors_allowed_origin()
        if allowed_origin:
            self.send_header("Access-Control-Allow-Origin", allowed_origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        super().end_headers()

    @property
    def app_state(self) -> AppState:
        return self.server.app_state  # type: ignore[attr-defined]

    def _request_host(self) -> str | None:
        host = (self.headers.get("Host") or "").strip()
        if not host:
            return None
        return host.split("/", 1)[0]

    def _is_oauth_callback_path(self, path: str) -> bool:
        settings = self.app_state._oauth_settings(self._request_host())
        configured = urlparse(str(settings.get("redirect_uri") or "")).path or ""
        if configured and path == configured:
            return True
        # Backward compatibility with previous default route.
        return path == "/auth/familysearch/callback"

    def _sync_start_http_status(self, result: dict) -> int:
        if result.get("started"):
            return HTTPStatus.ACCEPTED
        reason = str(result.get("reason") or "")
        if reason in {"missing_oauth_client_id", "oauth_login_required", "oauth_token_expired", "oauth_refresh_failed"}:
            return HTTPStatus.UNAUTHORIZED
        if reason == "sync_already_running":
            return HTTPStatus.CONFLICT
        return HTTPStatus.BAD_REQUEST

    def _oauth_callback_html(self, ok: bool, message: str) -> str:
        safe_message = (
            str(message or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        title = "FamilySearch conectado" if ok else "Error de autenticación"
        status = "success" if ok else "error"
        return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 2rem; color: #1f2937; }}
    .box {{ max-width: 36rem; padding: 1rem 1.25rem; border: 1px solid #d1d5db; border-radius: .5rem; }}
    .status {{ font-weight: 700; margin-bottom: .4rem; }}
    .success .status {{ color: #065f46; }}
    .error .status {{ color: #991b1b; }}
    .muted {{ color: #6b7280; font-size: .92rem; }}
  </style>
</head>
<body>
  <div class="box {status}">
    <div class="status">{title}</div>
    <div>{safe_message}</div>
    <p class="muted">Puedes cerrar esta ventana y volver a Familybook.</p>
  </div>
  <script>
    try {{
      if (window.opener) {{
        window.opener.postMessage({{
          source: "familybook-oauth",
          ok: {str(ok).lower()},
          message: {json.dumps(message, ensure_ascii=False)}
        }}, "*");
      }}
    }} catch (_) {{}}
    setTimeout(function () {{ window.close(); }}, 300);
  </script>
</body>
</html>"""

    def _serve_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        content_type, _ = mimetypes.guess_type(str(path))
        raw = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _decorate_media(self, items: list[dict]) -> list[dict]:
        out = []
        for item in items:
            decorated = dict(item)
            decorated["asset_url"] = self._asset_url_for_local_path(decorated.get("local_path"))
            out.append(decorated)
        return out

    def _asset_url_for_local_path(self, local_path: str | None) -> str | None:
        if not local_path:
            return None
        assets_root = Path(self.app_state.assets_root)
        project_root = Path(__file__).resolve().parent
        candidate = Path(local_path).resolve()
        if not candidate.exists() or not candidate.is_file():
            return None
        try:
            rel_path = candidate.relative_to(assets_root.resolve())
            return "/assets/" + "/".join(rel_path.parts)
        except ValueError:
            try:
                rel_path = candidate.relative_to(project_root)
                return "/files/" + quote("/".join(rel_path.parts))
            except ValueError:
                return None

    def _portrait_fallback_url(self, person_id: str | None) -> str | None:
        if not person_id:
            return None
        portraits_dir = Path(self.app_state.assets_root) / "portraits"
        if not portraits_dir.is_dir():
            return None
        matches = sorted(portraits_dir.glob(f"*{person_id}*"))
        if not matches:
            return None
        best = next((m for m in matches if m.is_file()), None)
        if not best:
            return None
        return "/assets/portraits/" + quote(best.name)

    def _decorate_people(self, items: list[dict]) -> list[dict]:
        out = []
        for item in items:
            decorated = dict(item)
            decorated["portrait_url"] = (
                self._asset_url_for_local_path(decorated.get("portrait_local_path"))
                or self._portrait_fallback_url(decorated.get("person_id"))
            )
            out.append(decorated)
        return out

    def _decorate_person_detail(self, detail: dict) -> dict:
        decorated = dict(detail)
        person = dict(detail.get("person") or {})
        person["portrait_url"] = (
            self._asset_url_for_local_path(person.get("portrait_local_path"))
            or self._portrait_fallback_url(person.get("person_id"))
        )
        decorated["person"] = person
        related_people = {}
        for person_id, rel_person in (detail.get("related_people") or {}).items():
            rel_copy = dict(rel_person)
            rel_copy["portrait_url"] = (
                self._asset_url_for_local_path(rel_copy.get("portrait_local_path"))
                or self._portrait_fallback_url(rel_copy.get("person_id"))
            )
            related_people[person_id] = rel_copy
        decorated["related_people"] = related_people
        grouped = {}
        for rel_type, items in (detail.get("grouped_relationships") or {}).items():
            grouped_items = []
            for item in items:
                item_copy = dict(item)
                rel_person = item_copy.get("related_person")
                if rel_person:
                    rel_copy = dict(rel_person)
                    rel_copy["portrait_url"] = (
                        self._asset_url_for_local_path(rel_copy.get("portrait_local_path"))
                        or self._portrait_fallback_url(rel_copy.get("person_id"))
                    )
                    item_copy["related_person"] = rel_copy
                grouped_items.append(item_copy)
            grouped[rel_type] = grouped_items
        decorated["grouped_relationships"] = grouped
        return decorated

    def _send_file_download(self, path: Path, filename: str, content_type: str) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        raw = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(raw)

    def _handle_api_exception(self, exc: Exception) -> None:
        if isinstance(exc, sqlite3.DatabaseError):
            json_response(
                self,
                {
                    "error": "database_invalid",
                    "detail": str(exc),
                    "hint": "Reimporta un backup valido o recrea output/familybook.sqlite.",
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        json_response(self, {"error": "internal_error", "detail": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if self._is_oauth_callback_path(parsed.path):
            query = parse_qs(parsed.query)
            ok, message = self.app_state.oauth_handle_callback(query)
            raw = self._oauth_callback_html(ok, message).encode("utf-8")
            self.send_response(HTTPStatus.OK if ok else HTTPStatus.BAD_REQUEST)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return
        if parsed.path.startswith("/api/"):
            try:
                self._handle_api_get(parsed)
            except Exception as exc:
                self._handle_api_exception(exc)
            return
        if parsed.path.startswith("/assets/"):
            rel = parsed.path[len("/assets/") :].lstrip("/")
            target = self._safe_child_path(Path(self.app_state.assets_root), unquote(rel))
            if not target:
                self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
                return
            self._serve_file(target)
            return
        if parsed.path.startswith("/files/"):
            rel = parsed.path[len("/files/") :].lstrip("/")
            project_root = Path(__file__).resolve().parent
            target = (project_root / unquote(rel)).resolve()
            if project_root not in target.parents and target != project_root:
                self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
                return
            self._serve_file(target)
            return
        if parsed.path == "/" or parsed.path == "/index.html":
            self._serve_file(UI_DIR / "index.html")
            return
        target = self._safe_child_path(UI_DIR, parsed.path.lstrip("/"))
        if not target:
            self.send_error(HTTPStatus.FORBIDDEN, "Forbidden")
            return
        self._serve_file(target)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_DELETE(self) -> None:  # noqa: N802
        try:
            self._do_delete()
        except Exception as exc:
            self._handle_api_exception(exc)

    def _do_delete(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) == 4 and parts[1] == "historical" and parts[2] == "events":
            json_response(self, {"error": "historical_events_are_read_only"}, status=HTTPStatus.METHOD_NOT_ALLOWED)
            return
        if len(parts) == 4 and parts[1] == "people" and parts[3] == "dna":
            person_id = parts[2]
            source = query.get("source", [""])[0]
            if not source:
                json_response(self, {"error": "missing_source"}, status=400)
                return
            familybook_db.delete_dna_segments(self.app_state.db_path, person_id, source)
            json_response(self, {"deleted": True, "source": source})
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._do_post()
        except Exception as exc:
            self._handle_api_exception(exc)

    def _do_post(self) -> None:
        parsed = urlparse(self.path)
        db_path = self.app_state.db_path
        request_host = self._request_host()

        if parsed.path == "/api/bootstrap/recreate-db":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {}
            confirm = str(payload.get("confirm") or "").strip().upper()
            if confirm != "RECREATE":
                json_response(self, {"ok": False, "error": "confirm_required"}, status=400)
                return
            force = bool(payload.get("force"))
            result = self.app_state.recreate_database(force=force)
            if result.get("ok"):
                json_response(self, result, status=200)
                return
            reason = str(result.get("reason") or "unknown")
            if reason in {"sync_running", "db_has_data"}:
                json_response(self, result, status=409)
                return
            json_response(self, result, status=500)
            return

        if parsed.path == "/api/bootstrap/recover-db":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {}
            allow_repair = bool(payload.get("allow_repair", True))
            result = self.app_state.recover_database(allow_repair=allow_repair)
            with self.app_state._lock:
                self.app_state._auto_recovery_attempted = True
                self.app_state._auto_recovery_result = result
            if result.get("ok"):
                json_response(self, result, status=200)
                return
            reason = str(result.get("reason") or "unknown")
            if reason in {"sync_running", "db_already_valid", "no_backup_candidates"}:
                json_response(self, result, status=409)
                return
            json_response(self, result, status=500)
            return

        if parsed.path == "/api/auth/familysearch/start":
            result = self.app_state.oauth_start(request_host=request_host)
            status = HTTPStatus.OK if result.get("started") or result.get("reason") == "already_authenticated" else HTTPStatus.BAD_REQUEST
            json_response(self, result, status=status)
            return

        if parsed.path == "/api/auth/familysearch/disconnect":
            self.app_state.oauth_disconnect()
            json_response(self, {"ok": True})
            return

        if parsed.path == "/api/dedupe/ignore":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {}
            person_a = str(payload.get("person_id_a") or "").strip()
            person_b = str(payload.get("person_id_b") or "").strip()
            reason = str(payload.get("reason") or "").strip() or None
            if not person_a or not person_b or person_a == person_b:
                json_response(self, {"error": "invalid_pair"}, status=400)
                return
            familybook_db.ignore_duplicate_pair(db_path, person_a, person_b, reason)
            json_response(self, {"ignored": True, "person_id_a": person_a, "person_id_b": person_b})
            return

        if parsed.path == "/api/sync/stop":
            result = self.app_state.stop_sync()
            status = HTTPStatus.OK if result.get("stopped") else HTTPStatus.CONFLICT
            json_response(self, result, status=status)
            return

        if parsed.path == "/api/historical/events":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {}
            scope = str(payload.get("scope") or "global").strip().lower()
            if scope not in {"global", "local"}:
                json_response(self, {"error": "invalid_scope"}, status=400)
                return
            title = str(payload.get("title") or "").strip()
            if not title:
                json_response(self, {"error": "missing_title"}, status=400)
                return
            try:
                start_year = int(payload.get("start_year"))
            except (TypeError, ValueError):
                json_response(self, {"error": "invalid_start_year"}, status=400)
                return
            raw_end_year = payload.get("end_year")
            try:
                end_year = int(raw_end_year) if raw_end_year not in (None, "") else start_year
            except (TypeError, ValueError):
                json_response(self, {"error": "invalid_end_year"}, status=400)
                return
            if start_year > end_year:
                start_year, end_year = end_year, start_year
            if start_year < 1000 or end_year > 2100:
                json_response(self, {"error": "invalid_year_range"}, status=400)
                return

            source_url = str(payload.get("source_url") or "").strip() or None
            if source_url and not re.match(r"^https?://", source_url, flags=re.IGNORECASE):
                json_response(self, {"error": "invalid_source_url"}, status=400)
                return

            raw_terms = payload.get("match_terms")
            terms: list[str] = []
            if isinstance(raw_terms, str):
                terms = [part.strip().lower() for part in re.split(r"[,\n;]+", raw_terms) if part.strip()]
            elif isinstance(raw_terms, list):
                terms = [str(part).strip().lower() for part in raw_terms if str(part).strip()]
            terms = sorted(set(terms))

            event_key = str(payload.get("event_key") or "").strip()
            if not event_key:
                event_key = f"manual:{scope}:{start_year}:{_slugify_key(title, max_len=52)}"
            description = str(payload.get("description") or "").strip()
            familybook_db.upsert_historical_event(
                db_path,
                event_key=event_key,
                scope=scope,
                title=title,
                description=description,
                start_year=start_year,
                end_year=end_year,
                source_url=source_url,
                match_terms=terms,
            )
            json_response(
                self,
                {
                    "saved": True,
                    "item": {
                        "event_key": event_key,
                        "scope": scope,
                        "title": title,
                        "description": description,
                        "start_year": start_year,
                        "end_year": end_year,
                        "source_url": source_url,
                        "match_terms": terms,
                    },
                },
                status=200,
            )
            return

        if parsed.path == "/api/historical/sync":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {}
            year_from = payload.get("year_from", 1900)
            year_to = payload.get("year_to", 2025)
            local_country = str(payload.get("local_country") or "Venezuela")
            try:
                result = sync_historical_events_from_service(
                    db_path,
                    year_from=int(year_from),
                    year_to=int(year_to),
                    local_country=local_country,
                )
            except Exception as exc:
                json_response(self, {"error": str(exc)}, status=500)
                return
            json_response(self, result, status=200)
            return

        if parsed.path == "/api/import/backup":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            if content_length > 4_294_967_296:
                json_response(self, {"error": "file_too_large"}, status=413)
                return
            if self.app_state.sync_status()["running"]:
                json_response(self, {"error": "sync_running"}, status=409)
                return
            body = self.rfile.read(content_length) if content_length else b""
            if not body:
                json_response(self, {"error": "empty_body"}, status=400)
                return
            # Detect format
            content_type = self.headers.get("Content-Type", "")
            is_7z = body[:4] == b"7z\xbc\xaf" or "7z-compressed" in content_type
            is_zip = body[:4] == b"PK\x03\x04" or "zip" in content_type

            if not is_7z and not is_zip:
                json_response(self, {"error": "unknown_format"}, status=400)
                return

            tmp_dir = tempfile.mkdtemp()
            try:
                tmp_archive = os.path.join(tmp_dir, "backup.archive")
                with open(tmp_archive, "wb") as f:
                    f.write(body)

                extract_dir = os.path.join(tmp_dir, "extracted")
                os.makedirs(extract_dir, exist_ok=True)

                if is_7z:
                    if not HAS_PY7ZR:
                        json_response(self, {"error": "py7zr_not_installed"}, status=503)
                        return
                    with py7zr.SevenZipFile(tmp_archive, "r") as zf:
                        zf.extractall(path=extract_dir)
                else:
                    with zipfile.ZipFile(tmp_archive, "r") as zf:
                        zf.extractall(extract_dir)

                # Validate: must contain familybook.sqlite
                db_candidates = list(Path(extract_dir).glob("**/familybook.sqlite"))
                if not db_candidates:
                    json_response(self, {"error": "no_db_in_archive"}, status=400)
                    return
                new_db = db_candidates[0]

                # Validate backup DB before replacing the current database.
                import_tmp = Path(db_path + ".import_tmp")
                shutil.copy2(str(new_db), str(import_tmp))
                valid_db, validation_error = _validate_backup_database(import_tmp)
                if not valid_db:
                    try:
                        import_tmp.unlink()
                    except OSError:
                        pass
                    json_response(
                        self,
                        {"error": "invalid_backup_db", "detail": validation_error or "unknown_error"},
                        status=400,
                    )
                    return
                os.replace(str(import_tmp), db_path)

                # Replace assets dir if present
                assets_root = self.app_state.assets_root
                new_assets = Path(extract_dir) / "assets"
                if new_assets.is_dir():
                    old_bak = assets_root + ".bak"
                    if os.path.isdir(assets_root):
                        if os.path.isdir(old_bak):
                            shutil.rmtree(old_bak)
                        os.rename(assets_root, old_bak)
                    shutil.copytree(str(new_assets), assets_root)
                    if os.path.isdir(old_bak):
                        shutil.rmtree(old_bak)

                # Count persons in new DB
                conn = sqlite3.connect(db_path)
                persons_row = conn.execute("SELECT COUNT(*) FROM persons").fetchone()
                persons_count = int(persons_row[0]) if persons_row and persons_row[0] is not None else 0
                conn.close()
                json_response(self, {"success": True, "persons": persons_count})
            except Exception as exc:
                json_response(self, {"error": str(exc)}, status=500)
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            return

        if parsed.path == "/api/import/gedcom":
            query = parse_qs(parsed.query)
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length else b""
            if not body:
                json_response(self, {"error": "empty_body"}, status=400)
                return
            try:
                root_name = (query.get("root_name", [""])[0] or "").strip() or None
                stats = familybook_gedcom.import_gedcom_to_db(db_path, body, root_person_name=root_name)
                json_response(self, stats)
            except Exception as exc:
                json_response(self, {"error": str(exc)}, status=500)
            return

        if parsed.path == "/api/import/familysearch":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = {}
            root_person_id = str(payload.get("person_id") or "").strip().upper()
            if not root_person_id:
                json_response(self, {"error": "missing_person_id"}, status=400)
                return
            generations = payload.get("generations", 4)
            try:
                generations = int(generations)
            except (TypeError, ValueError):
                generations = 4
            generations = max(1, min(generations, 12))
            collateral_depth_raw = payload.get("collateral_depth", generations)
            try:
                collateral_depth = int(collateral_depth_raw)
            except (TypeError, ValueError):
                collateral_depth = generations
            collateral_depth = max(0, min(collateral_depth, 12))
            force = bool(payload.get("force"))
            job_limit = payload.get("job_limit")
            if isinstance(job_limit, int) and job_limit > 0:
                parsed_limit = int(job_limit)
            else:
                parsed_limit = None
            result = self.app_state.start_sync(
                job_limit=parsed_limit,
                force=force,
                stubs_mode=False,
                root_person_id=root_person_id,
                generations=generations,
                collateral_depth=collateral_depth,
                request_host=request_host,
            )
            if result.get("started"):
                familybook_db.set_metadata(db_path, "default_root_person_id", root_person_id)
            result["root_person_id"] = root_person_id
            result["generations"] = generations
            result["collateral_depth"] = collateral_depth
            status = self._sync_start_http_status(result)
            json_response(self, result, status=status)
            return

        # DNA upload
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 4 and parts[1] == "people" and parts[3] == "dna":
            person_id = parts[2]
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length else b""
            if not body:
                json_response(self, {"error": "empty_body"}, status=400)
                return
            try:
                payload_text = body.decode("utf-8", errors="replace")
                if len(parts) == 4:
                    source, segments = _parse_dna_csv(payload_text)
                    stored = familybook_db.upsert_dna_segments(db_path, person_id, source, segments)
                    json_response(self, {"stored": stored, "source": source})
                    return
                target = parts[4]
                if target == "raw":
                    snps = _parse_raw_snps_text(payload_text)
                    stored = familybook_db.replace_dna_raw_snps(db_path, person_id, snps)
                    json_response(self, {"stored": stored, "kind": "raw_snps"})
                    return
                if target == "ethnicity":
                    items = _parse_ethnicity_payload(payload_text)
                    stored = familybook_db.replace_dna_ethnicity(db_path, person_id, items)
                    json_response(self, {"stored": stored, "kind": "ethnicity"})
                    return
                if target == "haplogroups":
                    payload = _parse_haplogroups_payload(payload_text)
                    result = familybook_db.upsert_dna_haplogroups(
                        db_path,
                        person_id,
                        payload.get("y_haplogroup"),
                        payload.get("mt_haplogroup"),
                        payload.get("y_timeline") if isinstance(payload.get("y_timeline"), list) else None,
                        payload.get("mt_timeline") if isinstance(payload.get("mt_timeline"), list) else None,
                        payload.get("notes") if isinstance(payload.get("notes"), dict) else None,
                    )
                    json_response(self, result)
                    return
                if target == "matches":
                    matches = _parse_matches_payload(payload_text)
                    stored = familybook_db.replace_dna_matches(db_path, person_id, matches)
                    json_response(self, {"stored": stored, "kind": "matches"})
                    return
                json_response(self, {"error": "unsupported_dna_target"}, status=400)
            except Exception as exc:
                json_response(self, {"error": str(exc)}, status=500)
            return

        if parsed.path != "/api/sync":
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(content_length) if content_length else b"{}"
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = {}
        job_limit = payload.get("job_limit")
        force = bool(payload.get("force"))
        stubs = bool(payload.get("stubs"))
        root_person_id = str(payload.get("root_person_id") or "").strip().upper() or None
        generations = payload.get("generations", 4)
        try:
            generations = int(generations)
        except (TypeError, ValueError):
            generations = 4
        generations = max(1, min(generations, 12))
        collateral_depth_raw = payload.get("collateral_depth", generations)
        try:
            collateral_depth = int(collateral_depth_raw)
        except (TypeError, ValueError):
            collateral_depth = generations
        collateral_depth = max(0, min(collateral_depth, 12))
        result = self.app_state.start_sync(
            job_limit=int(job_limit) if isinstance(job_limit, int) and job_limit > 0 else None,
            force=force,
            stubs_mode=stubs,
            root_person_id=root_person_id,
            generations=generations,
            collateral_depth=collateral_depth,
            request_host=request_host,
        )
        if result.get("started") and root_person_id and not stubs:
            familybook_db.set_metadata(db_path, "default_root_person_id", root_person_id)
        result["generations"] = generations
        result["collateral_depth"] = collateral_depth
        status = self._sync_start_http_status(result)
        json_response(self, result, status=status)

    def _handle_api_get(self, parsed) -> None:
        query = parse_qs(parsed.query)
        db_path = self.app_state.db_path

        if parsed.path == "/api/bootstrap/status":
            json_response(self, self.app_state.bootstrap_status(request_host=self._request_host()))
            return

        if parsed.path == "/api/auth/familysearch/status":
            json_response(self, self.app_state.oauth_status(request_host=self._request_host()))
            return

        if parsed.path == "/api/auth/familysearch/start":
            result = self.app_state.oauth_start(request_host=self._request_host())
            if query.get("redirect", ["0"])[0].lower() in {"1", "true", "yes"} and result.get("started") and result.get("auth_url"):
                self.send_response(HTTPStatus.FOUND)
                self.send_header("Location", str(result["auth_url"]))
                self.end_headers()
                return
            status = HTTPStatus.OK if result.get("started") or result.get("reason") == "already_authenticated" else HTTPStatus.BAD_REQUEST
            json_response(self, result, status=status)
            return

        if parsed.path == "/api/dedupe/candidates":
            raw_limit = query.get("limit", ["120"])[0]
            raw_offset = query.get("offset", ["0"])[0]
            raw_min = query.get("min_score", ["55"])[0]
            try:
                limit = max(1, min(500, int(raw_limit)))
            except (TypeError, ValueError):
                limit = 120
            try:
                offset = max(0, int(raw_offset))
            except (TypeError, ValueError):
                offset = 0
            try:
                min_score = max(1, min(100, int(raw_min)))
            except (TypeError, ValueError):
                min_score = 55
            items, total = familybook_db.list_duplicate_candidates(db_path, limit=limit, min_score=min_score, offset=offset)
            json_response(self, {"items": items, "limit": limit, "offset": offset, "total": total, "min_score": min_score})
            return

        if parsed.path == "/api/historical/events":
            raw_limit = query.get("limit", ["120"])[0]
            raw_offset = query.get("offset", ["0"])[0]
            raw_scope = (query.get("scope", [""])[0] or "").strip().lower() or None
            raw_place = (query.get("place", [""])[0] or "").strip() or None
            raw_from = query.get("year_from", [""])[0]
            raw_to = query.get("year_to", [""])[0]
            try:
                limit = max(1, min(1000, int(raw_limit)))
            except (TypeError, ValueError):
                limit = 120
            try:
                offset = max(0, int(raw_offset))
            except (TypeError, ValueError):
                offset = 0
            try:
                year_from = int(raw_from) if raw_from else None
            except (TypeError, ValueError):
                year_from = None
            try:
                year_to = int(raw_to) if raw_to else None
            except (TypeError, ValueError):
                year_to = None
            items, total = familybook_db.list_historical_events(
                db_path,
                start_year=year_from,
                end_year=year_to,
                scope=raw_scope,
                place_query=raw_place,
                offset=offset,
                limit=limit,
            )
            json_response(self, {"items": items, "limit": limit, "offset": offset, "total": total, "scope": raw_scope, "place": raw_place})
            return

        if parsed.path == "/api/status":
            payload = familybook_mirror.get_status(db_path)
            payload["sync_process"] = self.app_state.sync_status()
            payload["oauth"] = self.app_state.oauth_status(request_host=self._request_host())
            json_response(self, payload)
            return

        if parsed.path == "/api/people":
            raw_limit = query.get("limit", ["200"])[0]
            raw_offset = query.get("offset", ["0"])[0]
            include_total = query.get("include_total", ["0"])[0].lower() in {"1", "true", "yes"}
            try:
                limit = max(1, int(raw_limit))
            except ValueError:
                limit = 200
            try:
                offset = max(0, int(raw_offset))
            except ValueError:
                offset = 0
            people = familybook_mirror.list_people(
                db_path,
                query=query.get("q", [""])[0],
                limit=limit,
                offset=offset,
            )
            decorated = self._decorate_people(people)
            if include_total:
                total = familybook_mirror.count_people(db_path, query=query.get("q", [""])[0])
                json_response(self, {"items": decorated, "total": total, "limit": limit, "offset": offset})
            else:
                json_response(self, decorated)
            return

        if parsed.path == "/api/sync/status":
            json_response(self, self.app_state.sync_status())
            return

        if parsed.path == "/api/runs":
            limit = self._coerce_int(query.get("limit", ["12"])[0], 12, minimum=1, maximum=200)
            json_response(self, familybook_mirror.list_runs(db_path, limit=limit))
            return

        if parsed.path == "/api/tree":
            root_person_id = query.get("root", [""])[0]
            mode = query.get("mode", ["family"])[0]
            depth = self._coerce_int(query.get("depth", ["3"])[0], 3, minimum=1, maximum=12)
            tree = familybook_mirror.get_tree_view(db_path, root_person_id or None, mode=mode, depth=depth)
            if not tree:
                json_response(self, {"error": "person_not_found"}, status=404)
                return
            tree["root"]["portrait_url"] = (
                self._asset_url_for_local_path(tree["root"].get("portrait_local_path"))
                or self._portrait_fallback_url(tree["root"].get("person_id"))
            )
            for level in tree.get("levels", []):
                for node in level.get("nodes", []):
                    node["portrait_url"] = (
                        self._asset_url_for_local_path(node.get("portrait_local_path"))
                        or self._portrait_fallback_url(node.get("person_id"))
                    )
            json_response(self, tree)
            return

        if parsed.path == "/api/tree/pedigree":
            root_person_id = query.get("root", [""])[0] or None
            generations = self._coerce_int(query.get("generations", ["4"])[0], 4, minimum=1, maximum=12)
            tree = familybook_mirror.get_pedigree_view(db_path, root_person_id, generations=generations)
            if not tree:
                json_response(self, {"error": "person_not_found"}, status=404)
                return
            tree["root"]["portrait_url"] = (
                self._asset_url_for_local_path(tree["root"].get("portrait_local_path"))
                or self._portrait_fallback_url(tree["root"].get("person_id"))
            )
            for level in tree.get("levels", []):
                for node in level.get("nodes", []):
                    person = node.get("person")
                    if person:
                        person["portrait_url"] = (
                            self._asset_url_for_local_path(person.get("portrait_local_path"))
                            or self._portrait_fallback_url(person.get("person_id"))
                        )
            json_response(self, tree)
            return

        if parsed.path == "/api/connection":
            source_person_id = query.get("source", [""])[0]
            target_person_id = query.get("target", [""])[0]
            if not source_person_id or not target_person_id:
                json_response(self, {"error": "missing_people"}, status=400)
                return
            max_depth = self._coerce_int(query.get("max_depth", ["15"])[0], 15, minimum=1, maximum=30)
            max_paths = self._coerce_int(query.get("max_paths", ["3"])[0], 3, minimum=1, maximum=5)
            connection = familybook_mirror.get_connection_view(
                db_path, source_person_id, target_person_id, max_depth=max_depth, max_paths=max_paths
            )
            if not connection:
                json_response(self, {"error": "person_not_found"}, status=404)
                return
            def _decorate_person(p):
                if p:
                    p["portrait_url"] = (
                        self._asset_url_for_local_path(p.get("portrait_local_path"))
                        or self._portrait_fallback_url(p.get("person_id"))
                    )
            for key in ("source", "target"):
                _decorate_person(connection.get(key))
            for step in connection.get("path", []):
                _decorate_person(step.get("person"))
            for path_obj in connection.get("paths", []):
                for step in path_obj.get("steps", []):
                    _decorate_person(step.get("person"))
            json_response(self, connection)
            return

        if parsed.path == "/api/connections":
            people_param = query.get("people", [""])[0]
            person_ids = [p.strip() for p in people_param.split(",") if p.strip()]
            if len(person_ids) < 2:
                json_response(self, {"error": "need_at_least_2"}, status=400)
                return
            max_depth = self._coerce_int(query.get("max_depth", ["15"])[0], 15, minimum=1, maximum=30)
            max_paths = self._coerce_int(query.get("max_paths", ["2"])[0], 2, minimum=1, maximum=5)
            results = familybook_mirror.get_multi_connection_view(db_path, person_ids, max_depth=max_depth, max_paths=max_paths)
            def _decorate_person(p):
                if p:
                    p["portrait_url"] = (
                        self._asset_url_for_local_path(p.get("portrait_local_path"))
                        or self._portrait_fallback_url(p.get("person_id"))
                    )
            for pair in results:
                _decorate_person(pair.get("source"))
                _decorate_person(pair.get("target"))
                for path_obj in pair.get("paths", []):
                    for step in path_obj.get("steps", []):
                        _decorate_person(step.get("person"))
            json_response(self, {"pairs": results})
            return

        if parsed.path == "/api/stub-count":
            stub_ids = familybook_db.list_stub_person_ids(db_path)
            json_response(self, {"stub_count": len(stub_ids)})
            return

        if parsed.path == "/api/backup/db":
            db_file = Path(db_path)
            self._send_file_download(db_file, db_file.name, "application/x-sqlite3")
            return

        if parsed.path == "/api/backup/full":
            db_file = Path(db_path)
            assets_dir = Path(self.app_state.assets_root)
            tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
            try:
                tmp.close()
                with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
                    if db_file.exists():
                        zf.write(db_file, db_file.name)
                    if assets_dir.is_dir():
                        for asset in assets_dir.rglob("*"):
                            if asset.is_file():
                                zf.write(asset, "assets/" + str(asset.relative_to(assets_dir)))
                self._send_file_download(Path(tmp.name), "familybook-backup.zip", "application/zip")
            finally:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass
            return

        if parsed.path == "/api/backup/full7z":
            if not HAS_PY7ZR:
                json_response(self, {"error": "py7zr_not_installed"}, status=503)
                return
            db_file = Path(db_path)
            assets_dir = Path(self.app_state.assets_root)
            tmp = tempfile.NamedTemporaryFile(suffix=".7z", delete=False)
            try:
                tmp.close()
                with py7zr.SevenZipFile(tmp.name, "w") as zf:
                    if db_file.exists():
                        zf.write(db_file, db_file.name)
                    if assets_dir.is_dir():
                        for asset in assets_dir.rglob("*"):
                            if asset.is_file():
                                zf.write(asset, "assets/" + str(asset.relative_to(assets_dir)))
                self._send_file_download(Path(tmp.name), "familybook-backup.7z", "application/x-7z-compressed")
            finally:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass
            return

        if parsed.path == "/api/backup/capabilities":
            has_pandoc, pandoc_path = pandoc_capabilities()
            pdf_engine = resolve_pandoc_pdf_engine()
            json_response(self, {
                "py7zr": HAS_PY7ZR,
                "pandoc": has_pandoc,
                "pandoc_path": pandoc_path,
                "pandoc_pdf_engine": pdf_engine,
            })
            return

        if parsed.path == "/api/book/markdown":
            root_person_id = str(query.get("person_id", [""])[0] or "").strip().upper() or None
            lang = str(query.get("lang", ["es"])[0] or "es").strip().lower()[:2]
            md = familybook_mirror.generate_book_markdown(db_path, root_person_id=root_person_id, lang=lang)
            body = md.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/markdown; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Disposition", 'attachment; filename="familybook.md"')
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/api/book/pdf":
            has_pandoc, pandoc_path = pandoc_capabilities()
            if not has_pandoc or not pandoc_path:
                json_response(self, {"error": "pandoc_unavailable"}, status=503)
                return
            pdf_engine = resolve_pandoc_pdf_engine()
            if not pdf_engine:
                json_response(self, {"error": "pdf_engine_unavailable"}, status=503)
                return
            root_person_id = str(query.get("person_id", [""])[0] or "").strip().upper() or None
            lang = str(query.get("lang", ["es"])[0] or "es").strip().lower()[:2]
            md = familybook_mirror.generate_book_markdown(db_path, root_person_id=root_person_id, lang=lang)
            tmp_md = tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode="w", encoding="utf-8")
            tmp_pdf_path = tmp_md.name.replace(".md", ".pdf")
            try:
                tmp_md.write(md)
                tmp_md.close()
                result = subprocess.run(
                    [pandoc_path, tmp_md.name, "-o", tmp_pdf_path, "--pdf-engine", pdf_engine],
                    capture_output=True,
                    timeout=120,
                )
                if result.returncode != 0 or not os.path.isfile(tmp_pdf_path):
                    json_response(self, {"error": "pandoc_unavailable", "detail": result.stderr.decode("utf-8", errors="replace")}, status=503)
                    return
                self._send_file_download(Path(tmp_pdf_path), "familybook.pdf", "application/pdf")
            finally:
                try:
                    os.unlink(tmp_md.name)
                except OSError:
                    pass
                try:
                    os.unlink(tmp_pdf_path)
                except OSError:
                    pass
            return

        if parsed.path == "/api/export/gedcom":
            gedcom_raw = familybook_gedcom.export_gedcom_from_db(db_path)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(gedcom_raw)))
            self.send_header("Content-Disposition", 'attachment; filename="familybook-export.ged"')
            self.end_headers()
            self.wfile.write(gedcom_raw)
            return

        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 3 and parts[1] == "people":
            person_id = parts[2]
            if len(parts) == 3:
                detail = familybook_mirror.get_person_detail(db_path, person_id)
                if not detail:
                    json_response(self, {"error": "person_not_found"}, status=404)
                    return
                json_response(self, self._decorate_person_detail(detail))
                return
            if len(parts) == 4 and parts[3] == "media":
                json_response(self, self._decorate_media(familybook_mirror.list_person_media(db_path, person_id)))
                return
            if len(parts) == 4 and parts[3] == "sources":
                json_response(self, familybook_mirror.list_person_sources(db_path, person_id))
                return
            if len(parts) == 4 and parts[3] == "notes":
                json_response(self, familybook_mirror.list_person_notes(db_path, person_id))
                return
            if len(parts) == 4 and parts[3] == "memories":
                json_response(self, familybook_mirror.list_person_memories(db_path, person_id))
                return
            if len(parts) == 4 and parts[3] == "timeline":
                json_response(self, familybook_mirror.get_person_timeline(db_path, person_id))
                return
            if len(parts) == 4 and parts[3] == "dna":
                json_response(self, familybook_db.get_dna_overview(db_path, person_id))
                return
            if len(parts) == 5 and parts[3] == "dna" and parts[4] == "painter":
                json_response(self, familybook_db.get_dna_painter_data(db_path, person_id))
                return
            if len(parts) == 5 and parts[3] == "dna" and parts[4] == "traits":
                json_response(self, familybook_db.get_dna_traits(db_path, person_id))
                return
            if len(parts) == 5 and parts[3] == "dna" and parts[4] == "ethnicity":
                json_response(self, familybook_db.get_dna_ethnicity(db_path, person_id))
                return
            if len(parts) == 5 and parts[3] == "dna" and parts[4] == "haplogroups":
                json_response(self, familybook_db.get_dna_haplogroups(db_path, person_id))
                return
            if len(parts) == 5 and parts[3] == "dna" and parts[4] == "matches":
                json_response(self, familybook_db.get_dna_matches(db_path, person_id))
                return
            if len(parts) == 5 and parts[3] == "media" and parts[4] == "download":
                media_items = familybook_mirror.list_person_media(db_path, person_id)
                local_items = [m for m in media_items if m.get("local_path")]
                person_detail = familybook_mirror.get_person_detail(db_path, person_id)
                safe_name = "person"
                if person_detail and person_detail.get("person"):
                    raw_name = person_detail["person"].get("name") or person_id
                    safe_name = "".join(c if c.isalnum() or c in "-_ " else "_" for c in raw_name).strip() or "person"
                tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
                try:
                    tmp.close()
                    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
                        for item in local_items:
                            lp = Path(item["local_path"])
                            if lp.exists() and lp.is_file():
                                role = item.get("media_role") or "media"
                                arcname = f"{role}/{lp.name}"
                                zf.write(lp, arcname)
                    self._send_file_download(Path(tmp.name), f"{safe_name}-media.zip", "application/zip")
                finally:
                    try:
                        os.unlink(tmp.name)
                    except OSError:
                        pass
                return

        json_response(self, {"error": "not_found"}, status=404)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Familybook local web app")
    parser.add_argument(
        "--config",
        default=os.getenv("FAMILYBOOK_APP_CONFIG", DEFAULT_APP_CONFIG_PATH),
        help=f"Optional app config JSON path (default: {DEFAULT_APP_CONFIG_PATH})",
    )
    parser.add_argument("--db-path", default=None, help=f"SQLite path (default: {DEFAULT_DB_PATH})")
    parser.add_argument(
        "--assets-root",
        default=None,
        help=f"Mirror assets root (default: {DEFAULT_ASSETS_ROOT})",
    )
    parser.add_argument("--host", default=None, help=f"Host to bind (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=None, help=f"Port to bind (default: {DEFAULT_PORT})")
    args = parser.parse_args()
    config = _load_app_config(args.config)

    args.db_path = (
        args.db_path
        or os.getenv("FAMILYBOOK_DB_PATH", "").strip()
        or str(config.get("db_path") or "").strip()
        or DEFAULT_DB_PATH
    )
    args.assets_root = (
        args.assets_root
        or os.getenv("FAMILYBOOK_ASSETS_ROOT", "").strip()
        or str(config.get("assets_root") or "").strip()
        or DEFAULT_ASSETS_ROOT
    )
    args.host = (
        args.host
        or os.getenv("FAMILYBOOK_HOST", "").strip()
        or str(config.get("host") or "").strip()
        or DEFAULT_HOST
    )

    if args.port is None:
        raw_port = (
            os.getenv("FAMILYBOOK_PORT", "").strip()
            or str(config.get("port") or "").strip()
            or str(DEFAULT_PORT)
        )
        try:
            args.port = int(raw_port)
        except ValueError:
            args.port = DEFAULT_PORT
    args.port = max(1, min(int(args.port), 65535))
    return args


def main() -> None:
    args = parse_args()
    app_state = AppState(args.db_path, args.assets_root)
    server = ThreadingHTTPServer((args.host, args.port), FamilybookHandler)
    server.app_state = app_state  # type: ignore[attr-defined]
    print(f"Familybook app running on http://{args.host}:{args.port}")
    print(f"DB: {app_state.db_path}")
    print(f"Assets: {app_state.assets_root}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
