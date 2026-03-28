"""
Script para generar un libro genealógico a partir de un ID de FamilySearch.

Requisitos:
- Python 3.10+
- requests (pip install requests)

Autenticación:
- Necesitas un token OAuth de FamilySearch con acceso a la API de árbol.
- Exporta la variable de entorno FS_ACCESS_TOKEN con tu token.
- Opcional: exporta FS_BASE_URL si quieres usar el sandbox (por defecto apunta a producción).

Uso:
python familybook.py --person-id KWZC-RQ1 --generations 4 --output book.md
python familybook.py --current-person --generations 4 --output book.md
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import math
import os
import tempfile
import re
import secrets
import shutil
import subprocess
import getpass
import threading
import unicodedata
import webbrowser
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote_plus, urlencode, urljoin, urlparse

import requests

try:
    import familybook_db_native as familybook_db  # type: ignore[import-not-found]
except ImportError:
    import familybook_db


# SYNC_ASSETS_DIR: persistent mirror store (written by --sync-local-db, never overwritten by book generation)
# BOOK_ASSETS_DIR: ephemeral book render store (overwritten on each book generation)
SYNC_ASSETS_DIR = "output/familybook_assets"
BOOK_ASSETS_DIR = "output/family_book_assets"

DEFAULT_BASE_URL = os.getenv("FS_BASE_URL", "https://api.familysearch.org")
DEFAULT_IDENT_BASE_URL = os.getenv("FS_IDENT_BASE_URL", "https://ident.familysearch.org")
DEFAULT_ACCEPT = "application/x-gedcomx-v1+json"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WORLD_BANK_API_BASE_URL = "https://api.worldbank.org/v2"
DEFAULT_AI_PROMPT_PATH = "prompts/familybook_context.md"
DEFAULT_OAUTH_SCOPE = os.getenv("FS_OAUTH_SCOPE", "openid offline_access")
DEFAULT_PERSON_ID = os.getenv("FS_PERSON_ID")
DEFAULT_OAUTH_REDIRECT_URI = os.getenv("FS_OAUTH_REDIRECT_URI", "http://127.0.0.1:53682/callback")
DEFAULT_OAUTH_TIMEOUT_SECONDS = int(os.getenv("FS_OAUTH_TIMEOUT_SECONDS", "180"))
DEFAULT_TOKEN_CACHE_PATH = os.path.expanduser(
    os.getenv("FS_TOKEN_CACHE_PATH", "~/.familybook/oauth_token.json")
)
DEFAULT_SYNC_CONFIG = familybook_db.DEFAULT_SYNC_CONFIG


@dataclass
class LifeEvent:
    label: str
    date: Optional[str]
    place: Optional[str]
    source: str = "familysearch"


@dataclass
class ContextEvent:
    label: str
    date: str
    place: Optional[str]
    source: str = "wikidata"


@dataclass
class OAuthTokens:
    access_token: str
    refresh_token: Optional[str] = None
    expires_in: Optional[int] = None
    obtained_at: int = field(default_factory=lambda: int(dt.datetime.now(dt.timezone.utc).timestamp()))

    @property
    def expires_at(self) -> Optional[int]:
        if self.expires_in is None:
            return None
        return self.obtained_at + self.expires_in

    def is_expired(self, leeway_seconds: int = 60) -> bool:
        expires_at = self.expires_at
        if expires_at is None:
            return False
        now = int(dt.datetime.now(dt.timezone.utc).timestamp())
        return now >= (expires_at - leeway_seconds)


def iso_year(date_str: str | None) -> Optional[int]:
    if not date_str:
        return None
    match = re.search(r"(\d{4})", date_str)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def build_session(token: str, base_url: str = DEFAULT_BASE_URL) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": DEFAULT_ACCEPT,
            "Authorization": f"Bearer {token}",
            "User-Agent": "familybook-cli/0.1",
        }
    )
    session.base_url = base_url.rstrip("/")  # type: ignore[attr-defined]
    return session


def prompt_for_token() -> str:
    token = getpass.getpass("Token OAuth de FamilySearch (no se mostrara): ").strip()
    if not token:
        raise SystemExit("Token vacio. Aborta.")
    return token


def build_pkce_pair() -> Tuple[str, str]:
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).decode("ascii").rstrip("=")
    challenge_bytes = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(challenge_bytes).decode("ascii").rstrip("=")
    return verifier, challenge


def parse_oauth_callback_url(callback_url: str) -> Dict[str, Optional[str]]:
    parsed = urlparse(callback_url.strip())
    query = parse_qs(parsed.query)
    return {
        "code": (query.get("code") or [None])[0],
        "state": (query.get("state") or [None])[0],
        "error": (query.get("error") or [None])[0],
        "error_description": (query.get("error_description") or [None])[0],
    }


def wait_for_oauth_code(
    redirect_uri: str, expected_state: str, timeout_seconds: int
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    parsed = urlparse(redirect_uri)
    if parsed.scheme != "http" or not parsed.hostname:
        raise ValueError("Para callback local automático, redirect_uri debe usar http://host:puerto/ruta")

    listen_host = parsed.hostname
    listen_port = parsed.port or 80
    callback_path = parsed.path or "/"
    result: Dict[str, Optional[str]] = {"code": None, "state": None, "error": None, "error_description": None}
    done = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802
            incoming = urlparse(self.path)
            if incoming.path != callback_path:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not found")
                return

            query = parse_qs(incoming.query)
            result["code"] = (query.get("code") or [None])[0]
            result["state"] = (query.get("state") or [None])[0]
            result["error"] = (query.get("error") or [None])[0]
            result["error_description"] = (query.get("error_description") or [None])[0]

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h3>Autenticacion completada.</h3>"
                b"<p>Ya puedes volver a la terminal.</p></body></html>"
            )
            done.set()

    try:
        server = ThreadingHTTPServer((listen_host, listen_port), CallbackHandler)
    except OSError as exc:
        raise RuntimeError(
            f"No se pudo abrir callback local en {listen_host}:{listen_port}. "
            "Revisa tu redirect URI o usa un puerto libre."
        ) from exc

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    done.wait(timeout_seconds)
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
    return result["code"], result["state"], result["error"]


def exchange_authorization_code(
    *,
    ident_base_url: str,
    client_id: str,
    redirect_uri: str,
    code: str,
    code_verifier: str,
) -> OAuthTokens:
    token_url = f"{ident_base_url.rstrip('/')}/cis-web/oauth2/v3/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code": code,
        "code_verifier": code_verifier,
    }
    headers = {"Accept": "application/json"}
    resp = requests.post(token_url, data=data, headers=headers, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Error obteniendo token ({resp.status_code}): {resp.text}")

    payload = resp.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("La respuesta OAuth no contiene access_token.")
    expires_in_raw = payload.get("expires_in")
    expires_in = int(expires_in_raw) if isinstance(expires_in_raw, int) or str(expires_in_raw).isdigit() else None
    return OAuthTokens(
        access_token=access_token,
        refresh_token=payload.get("refresh_token"),
        expires_in=expires_in,
    )


def refresh_access_token(*, ident_base_url: str, client_id: str, refresh_token: str) -> OAuthTokens:
    token_url = f"{ident_base_url.rstrip('/')}/cis-web/oauth2/v3/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    }
    headers = {"Accept": "application/json"}
    resp = requests.post(token_url, data=data, headers=headers, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Error renovando token ({resp.status_code}): {resp.text}")
    payload = resp.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("La respuesta de refresh no contiene access_token.")
    new_refresh_token = payload.get("refresh_token") or refresh_token
    expires_in_raw = payload.get("expires_in")
    expires_in = int(expires_in_raw) if isinstance(expires_in_raw, int) or str(expires_in_raw).isdigit() else None
    return OAuthTokens(
        access_token=access_token,
        refresh_token=new_refresh_token,
        expires_in=expires_in,
    )


def load_cached_tokens(cache_path: str) -> Optional[OAuthTokens]:
    if not os.path.exists(cache_path):
        return None
    with open(cache_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    access_token = payload.get("access_token")
    if not access_token:
        return None
    expires_in_raw = payload.get("expires_in")
    expires_in = int(expires_in_raw) if isinstance(expires_in_raw, int) or str(expires_in_raw).isdigit() else None
    obtained_at_raw = payload.get("obtained_at")
    obtained_at = (
        int(obtained_at_raw)
        if isinstance(obtained_at_raw, int) or str(obtained_at_raw).isdigit()
        else int(dt.datetime.now(dt.timezone.utc).timestamp())
    )
    return OAuthTokens(
        access_token=access_token,
        refresh_token=payload.get("refresh_token"),
        expires_in=expires_in,
        obtained_at=obtained_at,
    )


def save_cached_tokens(cache_path: str, tokens: OAuthTokens) -> None:
    folder = os.path.dirname(cache_path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    payload = {
        "access_token": tokens.access_token,
        "refresh_token": tokens.refresh_token,
        "expires_in": tokens.expires_in,
        "obtained_at": tokens.obtained_at,
    }
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)
    os.chmod(cache_path, 0o600)


def oauth_login(
    *,
    ident_base_url: str,
    client_id: str,
    redirect_uri: str,
    scope: str,
    timeout_seconds: int,
    open_browser: bool,
) -> OAuthTokens:
    state = secrets.token_urlsafe(24)
    code_verifier, code_challenge = build_pkce_pair()
    auth_url = (
        f"{ident_base_url.rstrip('/')}/cis-web/oauth2/v3/authorization?"
        + urlencode(
            {
                "response_type": "code",
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "scope": scope,
                "state": state,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
        )
    )

    if open_browser:
        print("Abriendo navegador para autenticación FamilySearch.")
        print("En esa pantalla puedes elegir entrar con Google.")
        print(f"Si no abre automáticamente, usa esta URL:\n{auth_url}\n")
        webbrowser.open(auth_url, new=1)
    else:
        print("Abre esta URL en tu navegador para autenticación FamilySearch:")
        print("En esa pantalla puedes elegir entrar con Google.")
        print(f"{auth_url}\n")

    code: Optional[str] = None
    returned_state: Optional[str] = None
    oauth_error: Optional[str] = None
    try:
        code, returned_state, oauth_error = wait_for_oauth_code(redirect_uri, state, timeout_seconds)
    except Exception as exc:
        print(f"Callback automático no disponible: {exc}")

    if oauth_error:
        raise RuntimeError(f"El proveedor OAuth devolvió un error: {oauth_error}")

    if not code:
        print("No se recibió callback automático. Pega la URL final de redirección:")
        callback_url = input("> ").strip()
        parsed = parse_oauth_callback_url(callback_url)
        oauth_error = parsed.get("error")
        if oauth_error:
            raise RuntimeError(f"El proveedor OAuth devolvió un error: {oauth_error}")
        code = parsed.get("code")
        returned_state = parsed.get("state")

    if returned_state != state:
        raise RuntimeError("Estado OAuth inválido (state mismatch).")
    if not code:
        raise RuntimeError("No se recibió código de autorización.")

    return exchange_authorization_code(
        ident_base_url=ident_base_url,
        client_id=client_id,
        redirect_uri=redirect_uri,
        code=code,
        code_verifier=code_verifier,
    )


def extract_person_id_from_location(location: str | None) -> Optional[str]:
    if not location:
        return None
    path = location.split("?", 1)[0].rstrip("/")
    if not path:
        return None
    return path.split("/")[-1] or None


def fetch_current_tree_person(session: requests.Session) -> Tuple[str, Dict]:
    """
    Obtiene la persona del árbol que representa al usuario actual usando
    /platform/tree/current-person, y devuelve (person_id, person).
    """
    url = f"{session.base_url}/platform/tree/current-person"  # type: ignore[attr-defined]
    resp = session.get(url, timeout=20, allow_redirects=False)

    if resp.status_code in {301, 302, 303, 307, 308}:
        person_id = extract_person_id_from_location(resp.headers.get("Location"))
        if person_id:
            return person_id, fetch_person(session, person_id)

    if resp.status_code == 200:
        person_id = extract_person_id_from_location(resp.headers.get("Location"))
        if person_id:
            return person_id, fetch_person(session, person_id)
        try:
            data = resp.json()
        except ValueError:
            data = {}
        persons = data.get("persons", []) if isinstance(data, dict) else []
        if persons:
            person = persons[0]
            person_id = person.get("id")
            if person_id:
                return person_id, person

    resp.raise_for_status()
    raise ValueError("No se pudo obtener la persona actual del árbol.")


def fetch_person(session: requests.Session, person_id: str) -> Dict:
    url = f"{session.base_url}/platform/tree/persons/{person_id}"  # type: ignore[attr-defined]
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    persons = data.get("persons", [])
    if not persons:
        raise ValueError(f"No se encontró persona para ID {person_id}")
    return persons[0]


def fetch_optional_json(
    session: requests.Session,
    url: str,
    *,
    timeout: int = 20,
    params: Optional[Dict[str, object]] = None,
) -> Optional[Dict]:
    try:
        resp = session.get(url, timeout=timeout, params=params)
    except requests.RequestException:
        return None
    if resp.status_code >= 400:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def clean_text_block(text: str) -> str:
    compact = text.replace("\r", "\n")
    compact = re.sub(r"\n{3,}", "\n\n", compact)
    return compact.strip()


def maybe_add_text(chunks: List[str], value: str | None, min_len: int = 8) -> None:
    if not isinstance(value, str):
        return
    cleaned = clean_text_block(value)
    if len(cleaned) >= min_len:
        chunks.append(cleaned)


def fetch_person_notes_payload(session: requests.Session, person_id: str) -> Optional[Dict]:
    url = f"{session.base_url}/platform/tree/persons/{person_id}/notes"  # type: ignore[attr-defined]
    return fetch_optional_json(session, url, timeout=25)


def fetch_person_notes_texts_from_payload(payload: Optional[Dict]) -> List[str]:
    if not payload:
        return []
    notes = list(payload.get("notes", []) or [])
    for person in payload.get("persons", []) or []:
        notes.extend(person.get("notes", []) or [])
    out: List[str] = []
    for note in notes:
        subject = note.get("subject")
        text = note.get("text") or note.get("value")
        if isinstance(subject, str) and isinstance(text, str):
            maybe_add_text(out, f"{subject}\n\n{text}")
            continue
        maybe_add_text(out, text)
        maybe_add_text(out, subject)
    return out


def fetch_person_notes_texts(session: requests.Session, person_id: str) -> List[str]:
    return fetch_person_notes_texts_from_payload(fetch_person_notes_payload(session, person_id))


def fetch_person_sources_payload(session: requests.Session, person_id: str) -> Optional[Dict]:
    url = f"{session.base_url}/platform/tree/persons/{person_id}/sources"  # type: ignore[attr-defined]
    return fetch_optional_json(session, url, timeout=25)


def fetch_person_sources_texts_from_payload(payload: Optional[Dict]) -> List[str]:
    if not payload:
        return []
    out: List[str] = []
    for source in payload.get("sources", []):
        for title in source.get("titles", []):
            maybe_add_text(out, title.get("value"), min_len=4)
        for citation in source.get("citations", []):
            maybe_add_text(out, citation.get("value"))
    for desc in payload.get("sourceDescriptions", []):
        for title in desc.get("titles", []):
            maybe_add_text(out, title.get("value"), min_len=4)
        for note in desc.get("notes", []):
            maybe_add_text(out, note.get("text") or note.get("value"))
    return out


def fetch_person_sources_texts(session: requests.Session, person_id: str) -> List[str]:
    return fetch_person_sources_texts_from_payload(fetch_person_sources_payload(session, person_id))


def extract_memories_download_links(payload: Dict) -> List[str]:
    links: List[str] = []

    def walk(node: object) -> None:
        if isinstance(node, dict):
            if "links" in node and isinstance(node["links"], dict):
                for rel, data in node["links"].items():
                    if rel.lower() in {"artifact", "download", "file", "document", "text"} and isinstance(data, dict):
                        href = data.get("href")
                        if isinstance(href, str):
                            links.append(href)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    unique: List[str] = []
    seen: set[str] = set()
    for href in links:
        if href in seen:
            continue
        seen.add(href)
        unique.append(href)
    return unique


def extract_memory_media_entries(payloads: List[Dict]) -> List[Dict[str, str]]:
    preferred_rel_order = ["artifact", "image", "document", "download", "file", "text", "image-thumbnail", "image-icon"]

    def pick_links(links: Dict[str, Dict[str, str]]) -> List[tuple[str, str]]:
        available: Dict[str, str] = {}
        for rel, data in links.items():
            href = data.get("href") if isinstance(data, dict) else None
            if isinstance(href, str):
                available[rel.lower()] = href
        for rel in preferred_rel_order:
            href = available.get(rel)
            if href:
                # Si existe una variante fuerte (artifact/image/document), no bajar iconos/thumbnails.
                return [(rel, href)]
        return []

    entries: List[Dict[str, str]] = []
    seen: set[str] = set()
    for payload_index, payload in enumerate(payloads):
        for idx, desc in enumerate(payload.get("sourceDescriptions", []) or []):
            item_id = str(desc.get("id") or f"{payload_index}:sourceDescription:{idx}")
            title = None
            for title_obj in desc.get("titles", []) or []:
                if title_obj.get("value"):
                    title = title_obj.get("value")
                    break
            title = title or desc.get("about") or "Memory attachment"
            candidate_links: List[tuple[str, str]] = []
            links = desc.get("links", {}) if isinstance(desc, dict) else {}
            if isinstance(links, dict):
                candidate_links.extend(pick_links(links))
            for rel, href in candidate_links:
                dedupe_key = f"{item_id}|{href}"
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                entries.append(
                    {
                        "memory_key": item_id,
                        "title": title,
                        "remote_url": href,
                        "media_role": f"memory_{rel}",
                    }
                )
        for key in ("memories", "stories", "documents"):
            for idx, item in enumerate(payload.get(key, []) or []):
                item_id = str(item.get("id") or item.get("links", {}).get("self", {}).get("href") or f"{payload_index}:{key}:{idx}")
                title = item.get("title") or item.get("description") or key[:-1].capitalize()
                candidate_links: List[tuple[str, str]] = []
                links = item.get("links", {}) if isinstance(item, dict) else {}
                if isinstance(links, dict):
                    candidate_links.extend(pick_links(links))
                if not candidate_links:
                    for href in extract_memories_download_links(item if isinstance(item, dict) else {}):
                        candidate_links.append(("attachment", href))
                for rel, href in candidate_links:
                    dedupe_key = f"{item_id}|{href}"
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    entries.append(
                        {
                            "memory_key": item_id,
                            "title": title,
                            "remote_url": href,
                            "media_role": f"memory_{rel}",
                        }
                    )
    return entries


def read_text_like_attachment(session: requests.Session, url: str) -> Optional[str]:
    try:
        resp = session.get(url, timeout=30)
    except requests.RequestException:
        return None
    if resp.status_code >= 400:
        return None
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "text/" in ctype or "json" in ctype or "xml" in ctype or url.lower().endswith((".txt", ".md", ".json")):
        text = resp.text
        return clean_text_block(text)[:12000] if text else None
    if "pdf" in ctype and shutil.which("pdftotext"):
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
            tmp_pdf.write(resp.content)
            tmp_pdf_path = tmp_pdf.name
        try:
            result = subprocess.run(
                ["pdftotext", "-layout", tmp_pdf_path, "-"],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and result.stdout.strip():
                return clean_text_block(result.stdout)[:12000]
        finally:
            try:
                os.remove(tmp_pdf_path)
            except OSError:
                pass
    return None


def fetch_person_memories_payloads(session: requests.Session, person_id: str) -> List[Dict]:
    endpoints = [
        f"{session.base_url}/platform/tree/persons/{person_id}/memories",  # type: ignore[attr-defined]
        f"{session.base_url}/platform/tree/persons/{person_id}/stories",  # type: ignore[attr-defined]
    ]
    payloads: List[Dict] = []
    for endpoint in endpoints:
        payload = fetch_optional_json(session, endpoint, timeout=30)
        if payload:
            payloads.append(payload)
    return payloads


def fetch_person_memories_texts_from_payloads(session: requests.Session, payloads: List[Dict]) -> List[str]:
    out: List[str] = []
    for payload in payloads:
        for desc in payload.get("sourceDescriptions", []) or []:
            maybe_add_text(out, desc.get("about"), min_len=4)
            for title in desc.get("titles", []) or []:
                maybe_add_text(out, title.get("value"), min_len=4)
        # Captura textos directos de memories/stories.
        for key in ("memories", "stories", "documents"):
            for item in payload.get(key, []):
                maybe_add_text(out, item.get("title"), min_len=4)
                maybe_add_text(out, item.get("description"))
                maybe_add_text(out, item.get("text") or item.get("value"))

        # Descarga adjuntos textuales si hay links útiles.
        for href in extract_memories_download_links(payload):
            attachment_text = read_text_like_attachment(session, href)
            if attachment_text:
                maybe_add_text(out, attachment_text)
    return out


def fetch_person_memories_texts(session: requests.Session, person_id: str) -> List[str]:
    payloads = fetch_person_memories_payloads(session, person_id)
    return fetch_person_memories_texts_from_payloads(session, payloads)


def dedupe_text_blocks(values: List[str], max_items: int = 12) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        key = normalize_lookup_key(value[:300])
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
        if len(out) >= max_items:
            break
    return out


def compose_biography_text(person: Dict, narrative_blocks: List[str]) -> Optional[str]:
    base = extract_brief_life_history(person)
    blocks: List[str] = []
    if base:
        blocks.append(base)
    blocks.extend(narrative_blocks)
    merged = dedupe_text_blocks(blocks)
    if not merged:
        return None
    return "\n\n".join(merged)


def fetch_person_narrative_material(session: requests.Session, person_id: str) -> List[str]:
    notes_payload = fetch_person_notes_payload(session, person_id)
    sources_payload = fetch_person_sources_payload(session, person_id)
    memories_payloads = fetch_person_memories_payloads(session, person_id)
    chunks: List[str] = []
    chunks.extend(fetch_person_notes_texts_from_payload(notes_payload))
    chunks.extend(fetch_person_sources_texts_from_payload(sources_payload))
    chunks.extend(fetch_person_memories_texts_from_payloads(session, memories_payloads))
    return dedupe_text_blocks(chunks)


def _portraits_markdown_path_for_assets(assets_root: str) -> str:
    """Return a markdown output path such that download_portraits stores files in assets_root/portraits/.

    download_portraits derives portraits_dir as: dirname(md_path)/{stem}_assets/portraits/
    So we want: dirname=parent(assets_root), stem=basename(assets_root).removesuffix("_assets")
    """
    abs_root = os.path.abspath(assets_root)
    parent = os.path.dirname(abs_root)
    base = os.path.basename(abs_root)
    stem = base.removesuffix("_assets") if base.endswith("_assets") else base
    return os.path.join(parent, f"{stem}.md")


def download_portraits_for_storage(
    session: requests.Session,
    persons: Dict[str, Dict],
    assets_root: str,
) -> Dict[str, Dict[str, str]]:
    portrait_output = _portraits_markdown_path_for_assets(assets_root)
    downloaded = download_portraits(session, persons, portrait_output)
    media: Dict[str, Dict[str, str]] = {}
    for pid, local_path in downloaded.items():
        media[pid] = {
            "title": f"Portrait of {extract_name(persons.get(pid, {}))}",
            "remote_url": resolve_portrait_url(session, pid, persons.get(pid, {})) or "",
            "local_path": os.path.abspath(local_path),
            "mime_type": "",
            "status": "downloaded",
        }
    return media


def download_single_portrait_for_storage(
    session: requests.Session,
    person_id: str,
    person: Dict,
    assets_root: str,
) -> Optional[Dict[str, str]]:
    portrait_output = _portraits_markdown_path_for_assets(assets_root)
    downloaded = download_portraits(session, {person_id: person}, portrait_output)
    local_path = downloaded.get(person_id)
    if not local_path:
        return None
    return {
        "title": f"Portrait of {extract_name(person)}",
        "remote_url": resolve_portrait_url(session, person_id, person) or "",
        "local_path": os.path.abspath(local_path),
        "mime_type": "",
        "status": "downloaded",
    }


def enqueue_sync_jobs(
    *,
    db_path: str,
    run_id: int,
    person_ids: List[str],
    stale_hours_by_phase: Dict[str, int],
    force: bool,
) -> None:
    conn = familybook_db.connect(db_path)
    with conn:
        for pid in person_ids:
            state = familybook_db.get_person_sync_state_with_conn(conn, pid)
            if force or familybook_db.is_phase_stale_from_state(state, "last_fetched_at", stale_hours_by_phase["person"]):
                familybook_db.enqueue_job(conn, run_id=run_id, job_type="fetch_person", person_id=pid, priority=10)
                familybook_db.enqueue_job(conn, run_id=run_id, job_type="fetch_relationships", person_id=pid, priority=20)
            if force or familybook_db.is_phase_stale_from_state(state, "last_notes_at", stale_hours_by_phase["notes"]):
                familybook_db.enqueue_job(conn, run_id=run_id, job_type="fetch_notes", person_id=pid, priority=30)
            if force or familybook_db.is_phase_stale_from_state(state, "last_sources_at", stale_hours_by_phase["sources"]):
                familybook_db.enqueue_job(conn, run_id=run_id, job_type="fetch_sources", person_id=pid, priority=40)
            if force or familybook_db.is_phase_stale_from_state(state, "last_memories_at", stale_hours_by_phase["memories"]):
                familybook_db.enqueue_job(conn, run_id=run_id, job_type="fetch_memories", person_id=pid, priority=50)
            if force or familybook_db.is_phase_stale_from_state(state, "last_portrait_at", stale_hours_by_phase["portraits"]):
                familybook_db.enqueue_job(conn, run_id=run_id, job_type="download_portrait", person_id=pid, priority=60)
    conn.close()


def _cleanup_nested_portraits_dir(assets_root: str) -> None:
    """Move any portrait files from the old buggy nested path to the correct flat location.

    Old bug: portraits were stored at assets_root/portraits/portraits_assets/portraits/
    Correct: assets_root/portraits/
    """
    nested_dir = os.path.join(assets_root, "portraits", "portraits_assets", "portraits")
    flat_dir = os.path.join(assets_root, "portraits")
    if not os.path.isdir(nested_dir):
        return
    os.makedirs(flat_dir, exist_ok=True)
    for fname in os.listdir(nested_dir):
        src = os.path.join(nested_dir, fname)
        dst = os.path.join(flat_dir, fname)
        if os.path.isfile(src) and not os.path.exists(dst):
            shutil.move(src, dst)
    # Remove empty nested directories
    for subdir in [nested_dir, os.path.join(assets_root, "portraits", "portraits_assets")]:
        try:
            os.rmdir(subdir)
        except OSError:
            pass


def run_incremental_sync(
    *,
    session: requests.Session,
    db_path: str,
    root_person_id: str,
    generations: int,
    person_ids: List[str],
    stale_hours_by_phase: Dict[str, int],
    max_retries: int,
    retry_delay_minutes: int,
    force: bool,
    job_limit: Optional[int] = None,
) -> int:
    base_url = session.base_url  # type: ignore[attr-defined]
    run_id = familybook_db.start_or_resume_run(
        db_path=db_path,
        root_person_id=root_person_id,
        generations=generations,
        base_url=base_url,
    )
    enqueue_sync_jobs(
        db_path=db_path,
        run_id=run_id,
        person_ids=person_ids,
        stale_hours_by_phase=stale_hours_by_phase,
        force=force,
    )
    assets_root = os.path.join(
        os.path.dirname(os.path.abspath(db_path)) or os.getcwd(),
        f"{os.path.splitext(os.path.basename(db_path))[0]}_assets",
    )
    os.makedirs(assets_root, exist_ok=True)
    _cleanup_nested_portraits_dir(assets_root)

    try:
        jobs_processed = 0
        while True:
            if job_limit is not None and jobs_processed >= job_limit:
                break
            job = familybook_db.claim_next_job(db_path, run_id)
            if not job:
                break
            job_id = int(job["id"])
            person_id = job.get("person_id")
            try:
                if job["job_type"] == "fetch_person":
                    person = fetch_person(session, person_id)
                    familybook_db.persist_person(db_path, person_id, person, run_id)
                elif job["job_type"] == "fetch_relationships":
                    payload = fetch_relationships(session, person_id)
                    rel = parse_relationships(payload).get(person_id, {"father": None, "mother": None, "spouses": [], "children": []})
                    familybook_db.persist_relationships(db_path, person_id, rel, run_id, raw_payload=payload)
                elif job["job_type"] == "fetch_notes":
                    payload = fetch_person_notes_payload(session, person_id) or {"notes": []}
                    familybook_db.persist_notes(db_path, person_id, payload, run_id)
                elif job["job_type"] == "fetch_sources":
                    payload = fetch_person_sources_payload(session, person_id) or {"sources": [], "sourceDescriptions": []}
                    familybook_db.persist_sources(db_path, person_id, payload, run_id)
                elif job["job_type"] == "fetch_memories":
                    payloads = fetch_person_memories_payloads(session, person_id)
                    familybook_db.persist_memories(db_path, person_id, payloads, run_id)
                    conn = familybook_db.connect(db_path)
                    with conn:
                        for entry in extract_memory_media_entries(payloads):
                            familybook_db.enqueue_job(
                                conn,
                                run_id=run_id,
                                job_type="download_memory_media",
                                person_id=person_id,
                                remote_url=entry["remote_url"],
                                payload=entry,
                                priority=55,
                            )
                    conn.close()
                elif job["job_type"] == "download_portrait":
                    person = fetch_person(session, person_id)
                    media = download_single_portrait_for_storage(session, person_id, person, assets_root)
                    if media:
                        familybook_db.persist_media_item(
                            db_path,
                            person_id=person_id,
                            media_key=f"portrait:{person_id}",
                            media_role="portrait",
                            media=media,
                            run_id=run_id,
                        )
                elif job["job_type"] == "download_memory_media":
                    descriptor = job.get("payload") or {}
                    media = download_memory_media_for_storage(session, person_id, descriptor, assets_root)
                    if media:
                        memory_key = descriptor.get("memory_key") or hashlib.sha1(
                            (job.get("remote_url") or "").encode("utf-8")
                        ).hexdigest()
                        media_key = f"memory:{person_id}:{memory_key}:{hashlib.sha1((job.get('remote_url') or '').encode('utf-8')).hexdigest()[:12]}"
                        familybook_db.persist_media_item(
                            db_path,
                            person_id=person_id,
                            media_key=media_key,
                            media_role=media.get("media_role") or "memory_attachment",
                            media=media,
                            run_id=run_id,
                            memory_key=memory_key,
                        )
                familybook_db.complete_job(db_path, job_id)
                jobs_processed += 1
            except requests.RequestException as exc:
                familybook_db.fail_job(
                    db_path,
                    job_id,
                    str(exc),
                    retryable=True,
                    max_retries=max_retries,
                    retry_delay_minutes=retry_delay_minutes,
                )
            except Exception as exc:
                familybook_db.fail_job(
                    db_path,
                    job_id,
                    str(exc),
                    retryable=False,
                    max_retries=max_retries,
                    retry_delay_minutes=retry_delay_minutes,
                )
        familybook_db.refresh_run_counts(db_path, run_id)
        stats = familybook_db.run_queue_stats(db_path, run_id)
        if stats.get("failed", 0) > 0:
            familybook_db.finalize_run(db_path, run_id, "failed", "Hay jobs fallidos; la sync puede reanudarse.")
        elif stats.get("pending", 0) > 0 or stats.get("in_progress", 0) > 0:
            familybook_db.finalize_run(db_path, run_id, "running", "Sync parcial pendiente de reanudación.")
        else:
            familybook_db.finalize_run(db_path, run_id, "completed")
        return run_id
    except Exception as exc:
        familybook_db.refresh_run_counts(db_path, run_id)
        familybook_db.finalize_run(db_path, run_id, "aborted", str(exc))
        raise


def resolve_sync_config_from_args(args: argparse.Namespace) -> Dict[str, int]:
    stored = familybook_db.get_sync_config(args.local_db_path)
    resolved = dict(stored)
    overrides = {
        "sync_stale_person_hours": args.sync_stale_person_hours,
        "sync_stale_notes_hours": args.sync_stale_notes_hours,
        "sync_stale_sources_hours": args.sync_stale_sources_hours,
        "sync_stale_memories_hours": args.sync_stale_memories_hours,
        "sync_stale_portraits_hours": args.sync_stale_portraits_hours,
        "sync_max_retries": args.sync_max_retries,
        "sync_retry_delay_minutes": args.sync_retry_delay_minutes,
    }
    changed = False
    for key, value in overrides.items():
        if value is not None:
            resolved[key] = value
            changed = True
    if changed:
        familybook_db.save_sync_config(args.local_db_path, resolved)
    return resolved


def fetch_relationships(session: requests.Session, person_id: str) -> Dict:
    url = f"{session.base_url}/platform/tree/persons/{person_id}/relationships"  # type: ignore[attr-defined]
    resp = session.get(url, timeout=20)
    if resp.status_code == 404:
        # En algunos entornos (p. ej. beta) este recurso puede no estar habilitado.
        fallback_url = f"{session.base_url}/platform/tree/persons/{person_id}/families"  # type: ignore[attr-defined]
        fallback = session.get(fallback_url, timeout=20)
        fallback.raise_for_status()
        return fallback.json()
    resp.raise_for_status()
    return resp.json()


def fetch_persons_batch(session: requests.Session, person_ids: List[str], chunk_size: int = 50) -> Dict[str, Dict]:
    """
    Lee personas completas (facts, names, etc.) en lotes usando /platform/tree/persons?pids=...
    """
    out: Dict[str, Dict] = {}
    unique_ids = [pid for pid in dict.fromkeys(person_ids) if pid]
    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i : i + chunk_size]
        url = f"{session.base_url}/platform/tree/persons"  # type: ignore[attr-defined]
        resp = session.get(url, params={"pids": ",".join(chunk)}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for person in data.get("persons", []):
            pid = person.get("id")
            if pid:
                out[pid] = person
    return out


def fetch_ancestry(session: requests.Session, person_id: str, generations: int = 4) -> Dict[str, Dict]:
    persons, _ = fetch_ancestry_bundle(session, person_id, generations=generations)
    return persons


ANCESTRY_API_MAX_GENERATIONS = 8
DESCENDANCY_API_MAX_GENERATIONS = 4
COLLATERAL_MAX_PEOPLE = max(200, int(os.getenv("FS_COLLATERAL_MAX_PEOPLE", "3000")))


def _fetch_ancestry_bundle_chunk(
    session: requests.Session,
    person_id: str,
    generations: int,
) -> Tuple[Dict[str, Dict], Dict[str, Tuple[Optional[str], Optional[str]]], Dict[str, int]]:
    url = f"{session.base_url}/platform/tree/ancestry"  # type: ignore[attr-defined]
    params = {"person": person_id, "generations": generations}
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    persons = {p["id"]: p for p in data.get("persons", [])}
    parent_map: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    depth_map: Dict[str, int] = {}

    # Ancestry suele incluir display.ascendancyNumber (Ahnentafel):
    # 1=root, 2=padre, 3=madre, 4=padre del padre, etc.
    number_to_id: Dict[int, str] = {}
    for p in data.get("persons", []):
        pid = p.get("id")
        asc = (p.get("display", {}) or {}).get("ascendancyNumber")
        if not pid or not asc or not isinstance(asc, str):
            continue
        if not asc.isdigit():
            # p.ej. "1-S" para spouse en algunos payloads
            continue
        num = int(asc)
        number_to_id[num] = pid
        depth_map[pid] = max(0, num.bit_length() - 1)

    for child_num, child_id in number_to_id.items():
        father = number_to_id.get(child_num * 2)
        mother = number_to_id.get(child_num * 2 + 1)
        if father or mother:
            parent_map[child_id] = (father, mother)
    return persons, parent_map, depth_map


def fetch_ancestry_bundle(
    session: requests.Session, person_id: str, generations: int = 4
) -> Tuple[Dict[str, Dict], Dict[str, Tuple[Optional[str], Optional[str]]]]:
    """
    Devuelve:
    - personas de ancestry
    - índice child -> (father_id, mother_id) usando childAndParentsRelationships
    """
    requested = max(1, int(generations))
    if requested <= ANCESTRY_API_MAX_GENERATIONS:
        persons, parent_map, _ = _fetch_ancestry_bundle_chunk(session, person_id, requested)
        return persons, parent_map

    # FamilySearch /ancestry rejects values > 8 in beta.
    # Expand in layers from deepest returned ancestors until requested depth.
    all_persons: Dict[str, Dict] = {}
    all_parent_map: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    pending: List[Tuple[str, int]] = [(person_id, requested)]
    best_remaining: Dict[str, int] = {}

    while pending:
        target_pid, remaining = pending.pop(0)
        if remaining <= 0:
            continue
        if best_remaining.get(target_pid, 0) >= remaining:
            continue
        best_remaining[target_pid] = remaining

        chunk_generations = min(remaining, ANCESTRY_API_MAX_GENERATIONS)
        persons, parent_map, depth_map = _fetch_ancestry_bundle_chunk(
            session, target_pid, chunk_generations
        )
        all_persons.update(persons)
        all_parent_map.update(parent_map)

        if remaining <= ANCESTRY_API_MAX_GENERATIONS:
            continue
        deepest = max(depth_map.values(), default=0)
        target_depth = min(chunk_generations - 1, deepest)
        if target_depth <= 0:
            continue
        next_remaining = remaining - target_depth
        for next_pid, depth in depth_map.items():
            if depth == target_depth:
                pending.append((next_pid, next_remaining))

    return all_persons, all_parent_map


def fetch_descendancy(session: requests.Session, person_id: str, generations: int = 3) -> Dict[str, Dict]:
    requested = max(1, int(generations))

    def parse_desc_depth(raw: object) -> Optional[int]:
        if not isinstance(raw, str) or not raw.strip():
            return None
        # Descendancy numbers may include spouse markers like:
        # 1-S1, 1.3-S2.1.1, etc. Remove spouse markers and keep numeric path.
        cleaned = re.sub(r"-S\d+", "", raw).strip(".")
        if not cleaned:
            return None
        parts = cleaned.split(".")
        if not all(part.isdigit() for part in parts):
            return None
        return max(0, len(parts) - 1)

    def fetch_desc_chunk(target_pid: str, gens: int) -> Tuple[Dict[str, Dict], Dict[str, int]]:
        url = f"{session.base_url}/platform/tree/descendancy"  # type: ignore[attr-defined]
        params = {"person": target_pid, "generations": gens}
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        people = {p["id"]: p for p in data.get("persons", [])}
        depths: Dict[str, int] = {}
        for p in data.get("persons", []):
            pid = p.get("id")
            if not pid:
                continue
            desc_num = (p.get("display", {}) or {}).get("descendancyNumber")
            depth = parse_desc_depth(desc_num)
            if depth is not None:
                depths[pid] = depth
        return people, depths

    if requested <= DESCENDANCY_API_MAX_GENERATIONS:
        people, _ = fetch_desc_chunk(person_id, requested)
        return people

    # FamilySearch /descendancy rejects values > 4 in beta.
    # Expand in layers from deepest descendants until requested depth.
    all_people: Dict[str, Dict] = {}
    pending: List[Tuple[str, int]] = [(person_id, requested)]
    best_remaining: Dict[str, int] = {}

    while pending:
        target_pid, remaining = pending.pop(0)
        if remaining <= 0:
            continue
        if best_remaining.get(target_pid, 0) >= remaining:
            continue
        best_remaining[target_pid] = remaining

        chunk_generations = min(remaining, DESCENDANCY_API_MAX_GENERATIONS)
        people, depth_map = fetch_desc_chunk(target_pid, chunk_generations)
        all_people.update(people)

        if remaining <= DESCENDANCY_API_MAX_GENERATIONS:
            continue
        deepest = max(depth_map.values(), default=0)
        target_depth = min(chunk_generations - 1, deepest)
        if target_depth <= 0:
            continue
        next_remaining = remaining - target_depth
        for next_pid, depth in depth_map.items():
            if depth == target_depth:
                pending.append((next_pid, next_remaining))

    return all_people


def _relationship_neighbor_ids(rel_index: Dict[str, Dict], person_id: str) -> List[str]:
    rel = rel_index.get(person_id) or {}
    out: List[str] = []
    father = rel.get("father")
    mother = rel.get("mother")
    if father:
        out.append(str(father))
    if mother:
        out.append(str(mother))
    for spouse_id in rel.get("spouses", []) or []:
        if spouse_id:
            out.append(str(spouse_id))
    for child_id in rel.get("children", []) or []:
        if child_id:
            out.append(str(child_id))
    return out


def fetch_collateral_ids(
    session: requests.Session,
    root_person_id: str,
    *,
    max_depth: int,
    max_people: int = COLLATERAL_MAX_PEOPLE,
) -> set[str]:
    """
    BFS on direct family edges (parents/spouses/children) from root.
    This captures collateral branches such as siblings, uncles/aunts and cousins
    within the provided depth limit.
    """
    target_depth = max(1, int(max_depth))
    visited: set[str] = {root_person_id}
    queue: List[Tuple[str, int]] = [(root_person_id, 0)]
    cursor = 0

    while cursor < len(queue):
        person_id, depth = queue[cursor]
        cursor += 1
        if depth >= target_depth:
            continue
        try:
            payload = fetch_relationships(session, person_id)
        except requests.RequestException:
            continue
        rel_index = parse_relationships(payload)
        for relative_id in _relationship_neighbor_ids(rel_index, person_id):
            if not relative_id or relative_id in visited:
                continue
            visited.add(relative_id)
            if len(visited) >= max_people:
                return visited
            queue.append((relative_id, depth + 1))
    return visited


def extract_name(person: Dict) -> str:
    display = person.get("display", {})
    return display.get("name") or person.get("titles", [{}])[0].get("value", "Nombre no disponible")


def extract_lifespan(person: Dict) -> str:
    display = person.get("display", {})
    lifespan = display.get("lifespan")
    if lifespan:
        return lifespan
    birth = display.get("birthDate", "")
    death = display.get("deathDate", "")
    if birth or death:
        return f"{birth} – {death}"
    return "Sin fechas"


def extract_portrait(person: Dict) -> Optional[str]:
    links = person.get("links", {})
    portrait = links.get("portrait")
    if portrait:
        return portrait.get("href")
    return None


def slugify_filename(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._")
    return slug or "portrait"


def infer_image_extension(url: str, content_type: str | None) -> str:
    if content_type:
        lower = content_type.lower()
        if "jpeg" in lower or "jpg" in lower:
            return ".jpg"
        if "png" in lower:
            return ".png"
        if "gif" in lower:
            return ".gif"
        if "webp" in lower:
            return ".webp"
    path = urlparse(url).path.lower()
    for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
        if path.endswith(ext):
            return ext
    return ".jpg"


def infer_media_extension(url: str, content_type: str | None) -> str:
    if content_type:
        lower = content_type.lower()
        if "jpeg" in lower or "jpg" in lower:
            return ".jpg"
        if "png" in lower:
            return ".png"
        if "gif" in lower:
            return ".gif"
        if "webp" in lower:
            return ".webp"
        if "pdf" in lower:
            return ".pdf"
        if "plain" in lower:
            return ".txt"
        if "json" in lower:
            return ".json"
        if "xml" in lower:
            return ".xml"
        if "mp4" in lower:
            return ".mp4"
        if "mpeg" in lower or "mp3" in lower:
            return ".mp3"
    path = urlparse(url).path.lower()
    for ext in (
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".pdf",
        ".txt",
        ".json",
        ".xml",
        ".mp4",
        ".mp3",
    ):
        if path.endswith(ext):
            return ext
    return ".bin"


def download_memory_media_for_storage(
    session: requests.Session,
    person_id: str,
    descriptor: Dict[str, str],
    assets_root: str,
) -> Optional[Dict[str, str]]:
    remote_url = descriptor.get("remote_url")
    if not remote_url:
        return None
    try:
        resp = session.get(remote_url, timeout=60, stream=True)
    except requests.RequestException:
        return None
    if resp.status_code >= 400:
        return None
    media_dir = os.path.join(assets_root, "memories", slugify_filename(person_id))
    os.makedirs(media_dir, exist_ok=True)
    ext = infer_media_extension(remote_url, resp.headers.get("Content-Type"))
    base_name = slugify_filename(descriptor.get("title") or descriptor.get("memory_key") or "memory")
    short_hash = hashlib.sha1(remote_url.encode("utf-8")).hexdigest()[:12]
    final_path = os.path.join(media_dir, f"{base_name}_{short_hash}{ext}")
    tmp_path = f"{final_path}.tmp"
    try:
        with open(tmp_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 64):
                if chunk:
                    fh.write(chunk)
        os.replace(tmp_path, final_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
    return {
        "title": descriptor.get("title") or descriptor.get("memory_key") or "Memory attachment",
        "remote_url": remote_url,
        "local_path": os.path.abspath(final_path),
        "mime_type": resp.headers.get("Content-Type") or "",
        "status": "downloaded",
        "memory_key": descriptor.get("memory_key") or "",
        "media_role": descriptor.get("media_role") or "memory_attachment",
    }


def resolve_portrait_url(session: requests.Session, person_id: str, person: Dict) -> Optional[str]:
    """
    Resuelve la URL de retrato para una persona.
    1) Usa links.portrait si viene en el payload de persona.
    2) Fallback a /platform/tree/persons/{pid}/portrait (beta suele devolver URL en Location).
    """
    portrait_url = extract_portrait(person)
    if portrait_url:
        return portrait_url

    endpoint = f"{session.base_url}/platform/tree/persons/{person_id}/portrait"  # type: ignore[attr-defined]
    resp = None
    for _ in range(2):
        try:
            resp = session.get(endpoint, timeout=20, allow_redirects=False)
            break
        except requests.RequestException:
            resp = None
    if resp is None:
        return None

    if resp.status_code >= 400:
        return None

    location = resp.headers.get("Location")
    if not location:
        return None
    return urljoin(endpoint, location)


def download_portraits(
    session: requests.Session,
    persons: Dict[str, Dict],
    markdown_output_path: str,
) -> Dict[str, str]:
    output_dir = os.path.dirname(os.path.abspath(markdown_output_path)) or os.getcwd()
    base_name = os.path.splitext(os.path.basename(markdown_output_path))[0]
    portraits_dir = os.path.join(output_dir, f"{base_name}_assets", "portraits")
    local_paths: Dict[str, str] = {}

    for pid, person in persons.items():
        portrait_url = resolve_portrait_url(session, pid, person)
        if not portrait_url:
            continue

        resp = None
        last_exc: Optional[Exception] = None
        for _ in range(2):
            try:
                candidate = session.get(portrait_url, timeout=30)
                candidate.raise_for_status()
                resp = candidate
                last_exc = None
                break
            except requests.RequestException as exc:
                last_exc = exc
                resp = None
        if resp is None:
            if last_exc:
                print(f"No se pudo descargar retrato para {pid}: {last_exc}")
            continue

        os.makedirs(portraits_dir, exist_ok=True)
        ext = infer_image_extension(portrait_url, resp.headers.get("Content-Type"))
        filename = f"{slugify_filename(extract_name(person))}_{pid}{ext}"
        local_path = os.path.join(portraits_dir, filename)
        with open(local_path, "wb") as f:
            f.write(resp.content)
        local_paths[pid] = os.path.relpath(local_path, start=output_dir).replace(os.sep, "/")

    return local_paths


def render_markdown_list(lines: List[str], title: str, values: List[str]) -> None:
    if values:
        lines.append(f"- {title}:")
        for value in values:
            lines.append(f"  - {value}")


def xml_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def wrap_text(text: str, max_chars: int = 26, max_lines: Optional[int] = None) -> List[str]:
    words = text.split()
    if not words:
        return []
    lines: List[str] = []
    current = words[0]
    truncated = False
    for word in words[1:]:
        if len(current) + 1 + len(word) <= max_chars:
            current += f" {word}"
        else:
            lines.append(current)
            current = word
            if max_lines and len(lines) >= max_lines - 1:
                truncated = True
                break
    lines.append(current)
    if max_lines and len(lines) > max_lines:
        return lines[:max_lines]
    if truncated and lines:
        if len(lines[-1]) >= max_chars:
            lines[-1] = lines[-1][: max(0, max_chars - 1)] + "…"
        else:
            lines[-1] = lines[-1] + "…"
    return lines


def build_placeholder_avatar_svg(cx: float, cy: float, r: int, gender_type: str) -> List[str]:
    is_female = gender_type.endswith("female")
    fill = "#f2d8e6" if is_female else "#d7e3f7"
    return [f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r-3}" fill="{fill}"/>']


def build_pedigree_svg(
    *,
    root_id: str,
    persons: Dict[str, Dict],
    parent_map: Dict[str, Tuple[Optional[str], Optional[str]]],
    portrait_paths: Dict[str, str],
    svg_path: str,
    generations: int,
) -> None:
    generations = max(2, generations)
    max_nodes = 2**generations
    ahn: Dict[int, Optional[str]] = {1: root_id}
    for a in range(1, max_nodes):
        pid = ahn.get(a)
        if not pid:
            continue
        g = int(math.floor(math.log2(a)))
        if g >= generations - 1:
            continue
        father, mother = parent_map.get(pid, (None, None))
        ahn[2 * a] = father
        ahn[2 * a + 1] = mother

    left_pad = 70
    top_pad = 60
    bottom_pad = 100
    min_slot = 210
    level_gap = 290
    leaves = 2 ** (generations - 1)
    width = left_pad * 2 + leaves * min_slot

    def node_radius(gen: int) -> int:
        if gen == 0:
            return 78
        if gen == 1:
            return 62
        if gen == 2:
            return 54
        return 48

    node_text: Dict[int, Dict[str, object]] = {}
    max_label_height_by_gen: Dict[int, float] = {}
    for a in sorted(ahn.keys()):
        pid = ahn.get(a)
        if not pid:
            continue
        person = persons.get(pid, {"display": {"name": "Desconocido"}})
        g = int(math.floor(math.log2(a)))
        name = extract_name(person)
        lifespan = extract_lifespan(person)
        name_size = 18 if g == 0 else (15 if g <= 2 else 13)
        max_chars = 18 if g == 0 else (16 if g <= 2 else 14)
        max_lines = 4 if g == 0 else (3 if g <= 2 else 2)
        wrapped = wrap_text(name, max_chars=max_chars, max_lines=max_lines)
        label_h = len(wrapped) * (name_size + 2) + 20 + 12
        prev_h = max_label_height_by_gen.get(g, 0.0)
        if label_h > prev_h:
            max_label_height_by_gen[g] = float(label_h)
        node_text[a] = {
            "name": name,
            "lifespan": lifespan,
            "name_size": name_size,
            "wrapped": wrapped,
            "gender_type": (((person.get("gender") or {}).get("type")) or "").lower(),
        }

    # Margen inferior dinámico para que el nodo raíz + etiquetas siempre queden dentro del lienzo.
    root_label_h = max_label_height_by_gen.get(0, 90.0)
    bottom_pad = int(max(bottom_pad, node_radius(0) + 24 + root_label_h + 22))
    height = top_pad + bottom_pad + generations * level_gap

    lines: List[str] = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" '
        f'width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
    )
    lines.append("<defs>")
    lines.append(
        '<linearGradient id="skyGrad" x1="0%" y1="0%" x2="0%" y2="100%">'
        '<stop offset="0%" stop-color="#f7ecd0"/>'
        '<stop offset="58%" stop-color="#e4d8b8"/>'
        '<stop offset="100%" stop-color="#c7b489"/>'
        "</linearGradient>"
    )
    lines.append(
        '<linearGradient id="groundGrad" x1="0%" y1="0%" x2="0%" y2="100%">'
        '<stop offset="0%" stop-color="#6d8b4f"/>'
        '<stop offset="100%" stop-color="#445f33"/>'
        "</linearGradient>"
    )
    lines.append(
        '<linearGradient id="branchGrad" x1="0%" y1="0%" x2="100%" y2="100%">'
        '<stop offset="0%" stop-color="#8d5a34"/>'
        '<stop offset="100%" stop-color="#5f3d24"/>'
        "</linearGradient>"
    )
    lines.append(
        '<radialGradient id="fruitGrad" cx="40%" cy="35%" r="70%">'
        '<stop offset="0%" stop-color="#fff9ef"/>'
        '<stop offset="100%" stop-color="#d6b680"/>'
        "</radialGradient>"
    )
    lines.append(
        '<filter id="softShadow" x="-50%" y="-50%" width="200%" height="200%">'
        '<feDropShadow dx="0" dy="3" stdDeviation="3" flood-color="#2d2018" flood-opacity="0.35"/>'
        "</filter>"
    )
    lines.append("</defs>")
    lines.append('<rect width="100%" height="100%" fill="url(#skyGrad)"/>')
    lines.append(f'<ellipse cx="{width/2:.1f}" cy="{height+130:.1f}" rx="{width*0.68:.1f}" ry="260" fill="url(#groundGrad)" opacity="0.9"/>')
    lines.append(f'<ellipse cx="{width*0.24:.1f}" cy="{height+80:.1f}" rx="{width*0.45:.1f}" ry="210" fill="#5b7742" opacity="0.45"/>')
    lines.append(f'<ellipse cx="{width*0.76:.1f}" cy="{height+80:.1f}" rx="{width*0.45:.1f}" ry="210" fill="#5b7742" opacity="0.45"/>')
    lines.append(
        '<style>'
        '.edge{stroke:url(#branchGrad);stroke-linecap:round;fill:none;opacity:0.94;}'
        '.ring{fill:url(#fruitGrad);stroke:#9a6e38;stroke-width:5;filter:url(#softShadow);}'
        '.fruit-back{fill:#67884b;opacity:0.22;}'
        '.name{font:700 16px Merriweather,Georgia,serif;fill:#2f2218;}'
        '.meta{font:500 13px Lora,Georgia,serif;fill:#443226;}'
        '.male{fill:#2f6fad;}'
        '.female{fill:#c14b7d;}'
        '</style>'
    )

    centers: Dict[int, Tuple[float, float]] = {}
    for a in sorted(ahn.keys()):
        pid = ahn.get(a)
        if not pid:
            continue
        g = int(math.floor(math.log2(a)))
        index = a - (2**g)
        n_in_gen = 2**g
        slot = (width - 2 * left_pad) / n_in_gen
        x = left_pad + (index + 0.5) * slot
        y = height - bottom_pad - g * level_gap
        cx = x
        cy = y
        centers[a] = (cx, cy)

    root_center = centers.get(1, (width / 2.0, height - bottom_pad))
    root_trunk_start_y = root_center[1] + node_radius(0) - 10
    trunk_left = root_center[0] - min(140.0, width * 0.07)
    trunk_right = root_center[0] + min(140.0, width * 0.07)
    lines.append(
        f'<path d="M{root_center[0]:.1f},{root_trunk_start_y:.1f} '
        f'C{trunk_left:.1f},{height-220:.1f} {trunk_left-34:.1f},{height-120:.1f} {trunk_left+10:.1f},{height+8:.1f} '
        f'L{trunk_right-10:.1f},{height+8:.1f} '
        f'C{trunk_right+34:.1f},{height-120:.1f} {trunk_right:.1f},{height-220:.1f} {root_center[0]:.1f},{root_trunk_start_y:.1f} Z" '
        'fill="url(#branchGrad)" opacity="0.93"/>'
    )
    lines.append(f'<ellipse cx="{root_center[0]:.1f}" cy="{max(120.0, top_pad+40):.1f}" rx="{width*0.26:.1f}" ry="{height*0.11:.1f}" fill="#6b9250" opacity="0.30"/>')
    lines.append(f'<ellipse cx="{root_center[0]-width*0.16:.1f}" cy="{max(160.0, top_pad+86):.1f}" rx="{width*0.16:.1f}" ry="{height*0.09:.1f}" fill="#5e8347" opacity="0.28"/>')
    lines.append(f'<ellipse cx="{root_center[0]+width*0.16:.1f}" cy="{max(160.0, top_pad+86):.1f}" rx="{width*0.16:.1f}" ry="{height*0.09:.1f}" fill="#5e8347" opacity="0.28"/>')

    # Ramas de conexión (detrás de los nodos/frutos)
    for a, pid in ahn.items():
        if not pid or a not in centers:
            continue
        g = int(math.floor(math.log2(a)))
        r_child = node_radius(g)
        child_c = centers[a]
        for pa in (2 * a, 2 * a + 1):
            if pa in centers and ahn.get(pa):
                gp = int(math.floor(math.log2(pa)))
                r_parent = node_radius(gp)
                par_c = centers[pa]
                x1 = child_c[0]
                y1 = child_c[1] - r_child - 3
                x2 = par_c[0]
                y2 = par_c[1] + r_parent + 3
                hub_y = y2 + 12
                if hub_y >= y1:
                    hub_y = y2 + (y1 - y2) * 0.35
                branch_width = max(3.0, 11.0 - (g * 1.7))
                lines.append(
                    f'<path class="edge" style="stroke-width:{branch_width:.1f}" d="M{x1:.1f},{y1:.1f} '
                    f'C{x1:.1f},{hub_y:.1f} {x2:.1f},{hub_y:.1f} {x2:.1f},{y2:.1f}"/>'
                )
                leaf_x = (x1 + x2) / 2.0
                leaf_y = hub_y - 8
                lines.append(f'<ellipse cx="{leaf_x:.1f}" cy="{leaf_y:.1f}" rx="8" ry="4" fill="#719652" opacity="0.55"/>')
                lines.append(f'<ellipse cx="{leaf_x+7:.1f}" cy="{leaf_y+4:.1f}" rx="7" ry="3.5" fill="#608345" opacity="0.45"/>')

    for a in sorted(ahn.keys()):
        pid = ahn.get(a)
        if not pid or a not in centers:
            continue
        person = persons.get(pid, {"display": {"name": "Desconocido"}})
        text_info = node_text.get(a, {})
        name = str(text_info.get("name", extract_name(person)))
        lifespan = str(text_info.get("lifespan", extract_lifespan(person)))
        g = int(math.floor(math.log2(a)))
        r = node_radius(g)
        cx, cy = centers[a]
        portrait = portrait_paths.get(pid)
        lines.append(f'<g><title>{xml_escape(name)} ({xml_escape(lifespan)}) [{xml_escape(pid)}]</title>')
        lines.append(f'<circle class="fruit-back" cx="{cx:.1f}" cy="{cy:.1f}" r="{r+16}"/>')
        lines.append(f'<circle class="ring" cx="{cx:.1f}" cy="{cy:.1f}" r="{r}"/>')
        if portrait:
            clip_id = f"clip_{a}"
            lines.append(f'<clipPath id="{clip_id}"><circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r-3}"/></clipPath>')
            lines.append(
                f'<image x="{(cx-r+3):.1f}" y="{(cy-r+3):.1f}" width="{2*(r-3):.1f}" height="{2*(r-3):.1f}" '
                f'preserveAspectRatio="xMidYMid slice" clip-path="url(#{clip_id})" href="{xml_escape(portrait)}"/>'
            )
        else:
            gender_type = str(text_info.get("gender_type", ""))
            lines.extend(build_placeholder_avatar_svg(cx, cy, r, gender_type))

        wrapped = list(text_info.get("wrapped", wrap_text(name, max_chars=16, max_lines=None)))
        text_y = cy + r + 24
        name_size = int(text_info.get("name_size", 14))
        lines.append(f'<g transform="translate(0,0)" style="font-size:{name_size}px">')
        for i, line in enumerate(wrapped):
            lines.append(f'<text class="name" text-anchor="middle" x="{cx:.1f}" y="{text_y + i * (name_size + 2):.1f}">{xml_escape(line)}</text>')
        lines.append(
            f'<text class="meta" text-anchor="middle" x="{cx:.1f}" '
            f'y="{text_y + len(wrapped) * (name_size + 2) + 20:.1f}">{xml_escape(lifespan)}</text>'
        )

        gender_type = str(text_info.get("gender_type", (((person.get("gender") or {}).get("type")) or "").lower()))
        marker_x = cx - min(62, r + 18)
        marker_y = text_y + len(wrapped) * (name_size + 2) + 11
        if gender_type.endswith("female"):
            lines.append(f'<circle class="female" cx="{marker_x:.1f}" cy="{marker_y:.1f}" r="5"/>')
        else:
            lines.append(f'<rect class="male" x="{marker_x-5:.1f}" y="{marker_y-5:.1f}" width="10" height="10"/>')
        lines.append("</g>")
        lines.append("</g>")

    lines.append("</svg>")
    with open(svg_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def extract_life_events(person: Dict) -> List[LifeEvent]:
    events: List[LifeEvent] = []
    for fact in person.get("facts", []):
        fact_type = fact.get("type", "")
        lower_type = fact_type.lower()
        if lower_type.endswith("lifesketch"):
            # Se renderiza aparte en "Brief Life History".
            continue
        if lower_type.startswith("data:,"):
            # Filtra metadatos técnicos (kits DNA, flags internas, etc.).
            continue
        raw_label = fact_type.split("/")[-1] or fact_type or "Evento"
        label = unquote_plus(raw_label).strip()
        label = re.sub(r"(?<!^)(?=[A-Z])", " ", label)
        if label.lower() == "birth":
            label = "Nacimiento"
        elif label.lower() == "death":
            label = "Fallecimiento"
        elif label.lower() == "christening":
            label = "Bautizo"
        elif label.lower() == "residence":
            label = "Residencia"
        elif label.lower() == "occupation":
            label = "Ocupación"
        elif label.lower() == "religion":
            label = "Religión"
        elif label.lower() == "affiliation":
            label = "Afiliación"
        elif label.lower() == "burial":
            label = "Sepelio"
        date = None
        if fact.get("date"):
            date = fact["date"].get("original") or fact["date"].get("normalized", [{}])[0].get("value")
        place = None
        if fact.get("place"):
            place = fact["place"].get("original") or fact["place"].get("normalized", [{}])[0].get("value")
        value = (fact.get("value") or "").strip()
        if value and value.lower() != label.lower():
            label = f"{label}: {value}"
        events.append(LifeEvent(label=label, date=date, place=place))
    events.sort(key=lambda e: (iso_year(e.date) is None, iso_year(e.date) or 9999, e.date or ""))
    return events


def extract_brief_life_history(person: Dict) -> Optional[str]:
    for fact in person.get("facts", []):
        fact_type = (fact.get("type") or "").lower()
        if not fact_type.endswith("lifesketch"):
            continue
        value = fact.get("value")
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                return cleaned
    return None


def extract_gender_label(person: Dict) -> str:
    display_gender = ((person.get("display") or {}).get("gender") or "").strip().lower()
    if display_gender in {"male", "masculino"}:
        return "Masculino"
    if display_gender in {"female", "femenino"}:
        return "Femenino"
    gender_type = (((person.get("gender") or {}).get("type") or "").split("/")[-1]).strip().lower()
    if gender_type == "male":
        return "Masculino"
    if gender_type == "female":
        return "Femenino"
    return "No especificado"


def normalize_lookup_key(value: str) -> str:
    lowered = value.strip().lower()
    normalized = unicodedata.normalize("NFD", lowered)
    no_diacritics = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return re.sub(r"[^a-z0-9]+", " ", no_diacritics).strip()


def extract_country_from_place(place: str | None) -> Optional[str]:
    if not place:
        return None
    parts = [p.strip() for p in place.split(",") if p.strip()]
    if not parts:
        return None
    return parts[-1]


def fetch_world_bank_country_index() -> Dict[str, str]:
    per_page = 400
    payload = request_json_with_retries(
        f"{WORLD_BANK_API_BASE_URL}/country",
        params={"format": "json", "per_page": per_page},
        timeout=25,
        attempts=2,
    )
    rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
    out: Dict[str, str] = {}
    for row in rows:
        iso2 = row.get("iso2Code")
        name = row.get("name")
        region = (row.get("region") or {}).get("value", "")
        if not iso2 or iso2 == "NA" or not name or str(region).lower() == "aggregates":
            continue
        key = normalize_lookup_key(name)
        if key:
            out[key] = iso2
    aliases = {
        "usa": "US",
        "united states": "US",
        "united states of america": "US",
        "venezuela": "VE",
        "venezuela bolivarian republic of": "VE",
        "brasil": "BR",
        "brazil": "BR",
        "espana": "ES",
        "spain": "ES",
        "reino unido": "GB",
        "united kingdom": "GB",
    }
    out.update(aliases)
    return out


def query_world_bank_indicator(country_iso2: str, indicator: str, start_year: int, end_year: int) -> Optional[Tuple[int, float]]:
    payload = request_json_with_retries(
        f"{WORLD_BANK_API_BASE_URL}/country/{country_iso2}/indicator/{indicator}",
        params={"format": "json", "date": f"{start_year}:{end_year}", "per_page": 200},
        timeout=25,
        attempts=2,
    )
    rows = payload[1] if isinstance(payload, list) and len(payload) > 1 else []
    for row in rows:
        value = row.get("value")
        date = row.get("date")
        if value is None or date is None:
            continue
        try:
            return int(date), float(value)
        except (TypeError, ValueError):
            continue
    return None


def query_world_bank_context(
    country_name: str,
    start_year: int,
    end_year: int,
    country_index: Dict[str, str],
) -> List[ContextEvent]:
    key = normalize_lookup_key(country_name)
    iso2 = country_index.get(key)
    if not iso2:
        return []

    safe_end_year = min(end_year, dt.datetime.now(dt.timezone.utc).year)
    if safe_end_year < start_year:
        safe_end_year = start_year

    indicators = [
        ("SP.POP.TOTL", "Población total"),
        ("SP.DYN.LE00.IN", "Esperanza de vida al nacer"),
        ("NY.GDP.PCAP.CD", "PIB per cápita (USD actuales)"),
    ]
    events: List[ContextEvent] = []
    for code, label in indicators:
        result = query_world_bank_indicator(iso2, code, start_year, safe_end_year)
        if not result:
            continue
        year, value = result
        if code == "SP.POP.TOTL":
            value_txt = f"{int(round(value)):,}".replace(",", ".")
        elif code == "SP.DYN.LE00.IN":
            value_txt = f"{value:.1f} años"
        else:
            value_txt = f"{value:,.0f}".replace(",", ".")
        events.append(
            ContextEvent(
                label=f"{label}: {value_txt}",
                date=str(year),
                place=country_name,
                source="worldbank",
            )
        )
    return events


def request_json_with_retries(
    url: str,
    *,
    params: Optional[Dict[str, object]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 20,
    attempts: int = 2,
) -> Any:
    last_exc: Optional[Exception] = None
    for _ in range(max(1, attempts)):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
    if last_exc:
        raise last_exc
    return {}


def query_wikidata_events(place: str, start_year: int, end_year: int, limit: int = 8) -> List[ContextEvent]:
    place_escaped = place.replace('"', '\\"')
    query = f"""
SELECT ?event ?eventLabel ?date ?locationLabel WHERE {{
  ?event wdt:P31/wdt:P279* wd:Q1190554.
  ?event wdt:P585 ?date.
  OPTIONAL {{ ?event wdt:P131 ?location. }}
  OPTIONAL {{ ?event wdt:P17 ?country. }}
  BIND(COALESCE(?location, ?country) AS ?loc)
  ?loc rdfs:label ?locationLabel.
  FILTER(LANG(?locationLabel) = "es" || LANG(?locationLabel) = "en")
  FILTER(CONTAINS(LCASE(?locationLabel), LCASE("{place_escaped}")))
  FILTER(?date >= "{start_year}-01-01T00:00:00Z"^^xsd:dateTime && ?date <= "{end_year}-12-31T23:59:59Z"^^xsd:dateTime)
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "es,en,pt,en". }}
}}
ORDER BY ?date
LIMIT {limit}
"""
    headers = {"Accept": "application/sparql-results+json", "User-Agent": "familybook-cli/0.1"}
    wrapped = request_json_with_retries(
        WIKIDATA_SPARQL_URL,
        params={"query": query},
        headers=headers,
        timeout=20,
        attempts=2,
    )
    data = wrapped
    events: List[ContextEvent] = []
    for row in data.get("results", {}).get("bindings", []):
        label = row.get("eventLabel", {}).get("value", "Evento")
        date = row.get("date", {}).get("value", "")
        place_label = row.get("locationLabel", {}).get("value")
        events.append(ContextEvent(label=label, date=date, place=place_label))
    return events


def build_markdown(
    root_id: str,
    persons: Dict[str, Dict],
    relationships: Dict[str, Dict],
    context_events: Dict[str, List[ContextEvent]],
    portrait_paths: Dict[str, str],
    biography_texts: Optional[Dict[str, str]] = None,
) -> str:
    lines: List[str] = []
    lines.append(f"# Libro genealógico de {extract_name(persons[root_id])}")
    lines.append("")
    ordered_ids = [root_id] + sorted(
        [pid for pid in persons.keys() if pid != root_id],
        key=lambda pid: extract_name(persons.get(pid, {})).lower(),
    )
    for pid in ordered_ids:
        person = persons[pid]
        name = extract_name(person)
        lines.append(f"## {name}")
        lines.append(f"_ID FamilySearch: `{pid}`_")
        lines.append("")
        portrait = portrait_paths.get(pid)
        if portrait:
            lines.append(f"![Foto de {name}]({portrait})")
        lines.append("")

        display = person.get("display", {})
        birth = display.get("birthPlace")
        death = display.get("deathPlace")
        lines.append("### Ficha")
        lines.append("| Campo | Valor |")
        lines.append("| --- | --- |")
        lines.append(f"| Nombre | {name} |")
        lines.append(f"| Vida | {extract_lifespan(person)} |")
        lines.append(f"| Sexo | {extract_gender_label(person)} |")
        lines.append(f"| Nacimiento | {display.get('birthDate', 'N/D')} en {birth or 'N/D'} |")
        lines.append(f"| Fallecimiento | {display.get('deathDate', 'N/D')} en {death or 'N/D'} |")
        lines.append("")

        lines.append(f"### Brief Life History of {name}")
        brief_life = (biography_texts or {}).get(pid) or extract_brief_life_history(person)
        if not brief_life:
            lines.append("- No disponible")
        else:
            for paragraph in [p.strip() for p in re.split(r"\n\s*\n", brief_life) if p.strip()]:
                single_line_paragraph = re.sub(r"\s*\n\s*", " ", paragraph)
                lines.append(f"> {single_line_paragraph}")
                lines.append(">")
        lines.append("")

        life_events = extract_life_events(person)
        lines.append("### Línea de vida")
        if not life_events:
            lines.append("- Sin eventos registrados")
        else:
            for ev in life_events:
                when = ev.date or "N/D"
                where = f" en {ev.place}" if ev.place else ""
                lines.append(f"- {when}: {ev.label}{where}")
        lines.append("")

        ctx_list = context_events.get(pid, [])
        lines.append("### Contexto histórico local/país")
        if not ctx_list:
            lines.append("- No se encontraron eventos de contexto para este período")
        else:
            for ev in ctx_list:
                where = f" en {ev.place}" if ev.place else ""
                date_short = ev.date.split("T")[0] if "T" in ev.date else ev.date
                lines.append(f"- {date_short}: {ev.label}{where} [{ev.source}]")
        lines.append("")

        rel = relationships.get(pid, {})
        lines.append("### Familia inmediata")
        father = rel.get("father")
        mother = rel.get("mother")
        spouses = rel.get("spouses", [])
        children = rel.get("children", [])
        if father:
            lines.append(f"- Padre: {extract_name(persons.get(father, {'display': {'name': 'Desconocido'}}))} ({father})")
        if mother:
            lines.append(f"- Madre: {extract_name(persons.get(mother, {'display': {'name': 'Desconocido'}}))} ({mother})")
        render_markdown_list(
            lines,
            "Cónyuges",
            [f"{extract_name(persons.get(sid, {'display': {'name': 'Desconocido'}}))} ({sid})" for sid in spouses],
        )
        render_markdown_list(
            lines,
            "Hijos",
            [f"{extract_name(persons.get(cid, {'display': {'name': 'Desconocido'}}))} ({cid})" for cid in children],
        )
        if not (father or mother or spouses or children):
            lines.append("- Sin relaciones inmediatas registradas")
        lines.append("")
        lines.append("---")
        lines.append("")
    return "\n".join(lines)


def build_ai_context_payload(
    *,
    root_id: str,
    persons: Dict[str, Dict],
    relationships: Dict[str, Dict],
    context_events: Dict[str, List[ContextEvent]],
    portrait_paths: Dict[str, str],
    biography_texts: Optional[Dict[str, str]] = None,
) -> Dict:
    people: List[Dict] = []
    ordered_ids = [root_id] + sorted(
        [pid for pid in persons.keys() if pid != root_id],
        key=lambda pid: extract_name(persons.get(pid, {})).lower(),
    )
    for pid in ordered_ids:
        person = persons[pid]
        display = person.get("display", {})
        rel = relationships.get(pid, {})
        life_events = extract_life_events(person)
        brief_life = (biography_texts or {}).get(pid) or extract_brief_life_history(person)
        person_payload = {
            "id": pid,
            "name": extract_name(person),
            "lifespan": extract_lifespan(person),
            "gender": extract_gender_label(person),
            "birth_date": display.get("birthDate"),
            "birth_place": display.get("birthPlace"),
            "death_date": display.get("deathDate"),
            "death_place": display.get("deathPlace"),
            "portrait_path": portrait_paths.get(pid),
            "brief_life_history": brief_life,
            "life_events": [
                {"label": ev.label, "date": ev.date, "place": ev.place, "source": ev.source} for ev in life_events
            ],
            "historical_context": [
                {"label": ev.label, "date": ev.date, "place": ev.place, "source": ev.source}
                for ev in context_events.get(pid, [])
            ],
            "family": {
                "father": rel.get("father"),
                "mother": rel.get("mother"),
                "spouses": rel.get("spouses", []),
                "children": rel.get("children", []),
            },
            "family_names": {
                "father": extract_name(persons.get(rel.get("father"), {"display": {"name": "Desconocido"}}))
                if rel.get("father")
                else None,
                "mother": extract_name(persons.get(rel.get("mother"), {"display": {"name": "Desconocido"}}))
                if rel.get("mother")
                else None,
                "spouses": [extract_name(persons.get(sid, {"display": {"name": "Desconocido"}})) for sid in rel.get("spouses", [])],
                "children": [extract_name(persons.get(cid, {"display": {"name": "Desconocido"}})) for cid in rel.get("children", [])],
            },
        }
        people.append(person_payload)

    return {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "root_person_id": root_id,
        "root_person_name": extract_name(persons[root_id]),
        "people_count": len(people),
        "people": people,
    }


def load_prompt_template(path: str) -> str:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    return (
        "Eres un historiador familiar. Usa los datos estructurados para escribir un libro narrativo.\n"
        "No inventes hechos. Si un dato falta, indícalo claramente.\n"
        "Prioriza precisión en nombres, fechas, lugares e IDs de FamilySearch."
    )


def build_ai_draft_markdown(prompt_text: str, payload: Dict) -> str:
    lines: List[str] = []
    lines.append("# FamilyBook AI Draft Input")
    lines.append("")
    lines.append("Este archivo está diseñado para usarse como entrada de un modelo de IA.")
    lines.append("Incluye prompt versionado + contexto estructurado para redactar el libro narrativo.")
    lines.append("")
    lines.append("## Prompt")
    lines.append("")
    lines.append(prompt_text)
    lines.append("")
    lines.append("## Context JSON")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(payload, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## Instrucciones sugeridas al modelo")
    lines.append("")
    lines.append("1. Escribe una narrativa por persona usando primero `brief_life_history` y luego `life_events`.")
    lines.append("2. Inserta contexto histórico solo desde `historical_context` sin inventar.")
    lines.append("3. Mantén tono humano, claro y respetuoso.")
    lines.append("4. Conserva referencias de parentesco según `family` y `family_names`.")
    lines.append("5. Si hay ambigüedad o ausencia de datos, decláralo explícitamente.")
    lines.append("")
    return "\n".join(lines)


def build_pdf(markdown_path: str, pdf_path: str) -> None:
    pandoc_path = shutil.which("pandoc")
    if not pandoc_path:
        raise RuntimeError("No se encontró pandoc en PATH. Instálalo para generar PDF.")
    markdown_dir = os.path.dirname(os.path.abspath(markdown_path)) or "."
    cmd = [pandoc_path, markdown_path, "-o", pdf_path, "--resource-path", markdown_dir]
    if shutil.which("weasyprint"):
        cmd.extend(["--pdf-engine", "weasyprint"])
    elif shutil.which("tectonic"):
        cmd.extend(["--pdf-engine", "tectonic"])
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Falló la generación de PDF: {exc}") from exc


def parse_relationships(raw: Dict) -> Dict[str, Dict]:
    """
    Devuelve un índice simple {person_id: {father, mother, spouses, children}}
    a partir de la estructura de relationships.
    """
    rel_index: Dict[str, Dict] = {}
    for rel in raw.get("childAndParentsRelationships", []):
        child = rel.get("child", {}).get("resourceId")
        father = rel.get("father", {}).get("resourceId") or rel.get("parent1", {}).get("resourceId")
        mother = rel.get("mother", {}).get("resourceId") or rel.get("parent2", {}).get("resourceId")
        if child:
            entry = rel_index.setdefault(child, {"spouses": [], "children": []})
            if father:
                entry["father"] = father
            if mother:
                entry["mother"] = mother
    for rel in raw.get("coupleRelationships", []):
        person1 = rel.get("person1", {}).get("resourceId")
        person2 = rel.get("person2", {}).get("resourceId")
        if person1 and person2:
            rel_index.setdefault(person1, {"spouses": [], "children": []})["spouses"].append(person2)
            rel_index.setdefault(person2, {"spouses": [], "children": []})["spouses"].append(person1)
    for rel in raw.get("parentChildRelationships", []):
        parent = rel.get("parent", {}).get("resourceId")
        child = rel.get("child", {}).get("resourceId")
        if parent and child:
            rel_index.setdefault(parent, {"spouses": [], "children": []})["children"].append(child)

    # Variante común en /families: lista genérica de relationships (couple + parentChild).
    for rel in raw.get("relationships", []):
        person1 = rel.get("person1", {}).get("resourceId")
        person2 = rel.get("person2", {}).get("resourceId")
        rel_type = (rel.get("type") or "").lower()
        if not (person1 and person2):
            continue
        if rel_type.endswith("parentchild"):
            parent, child = person1, person2
            rel_index.setdefault(parent, {"spouses": [], "children": []})["children"].append(child)
            continue
        # Si no viene tipo explícito, tratarlo como relación de pareja.
        rel_index.setdefault(person1, {"spouses": [], "children": []})["spouses"].append(person2)
        rel_index.setdefault(person2, {"spouses": [], "children": []})["spouses"].append(person1)
    return rel_index


def collect_ancestor_subtree_ids(
    start_id: Optional[str],
    parent_map: Dict[str, Tuple[Optional[str], Optional[str]]],
) -> set[str]:
    if not start_id:
        return set()
    out: set[str] = set()
    stack: List[str] = [start_id]
    while stack:
        pid = stack.pop()
        if not pid or pid in out:
            continue
        out.add(pid)
        father, mother = parent_map.get(pid, (None, None))
        if father:
            stack.append(father)
        if mother:
            stack.append(mother)
    return out


def filter_branch_data(
    *,
    allowed_ids: set[str],
    persons: Dict[str, Dict],
    relationships: Dict[str, Dict],
    context_events: Dict[str, List[ContextEvent]],
    portrait_paths: Dict[str, str],
    biography_texts: Dict[str, str],
) -> Tuple[Dict[str, Dict], Dict[str, Dict], Dict[str, List[ContextEvent]], Dict[str, str], Dict[str, str]]:
    people = {pid: person for pid, person in persons.items() if pid in allowed_ids}
    rel_out: Dict[str, Dict] = {}
    for pid in allowed_ids:
        rel = relationships.get(pid, {})
        father = rel.get("father") if rel.get("father") in allowed_ids else None
        mother = rel.get("mother") if rel.get("mother") in allowed_ids else None
        spouses = [sid for sid in rel.get("spouses", []) if sid in allowed_ids]
        children = [cid for cid in rel.get("children", []) if cid in allowed_ids]
        if father or mother or spouses or children:
            rel_out[pid] = {
                "father": father,
                "mother": mother,
                "spouses": spouses,
                "children": children,
            }
    ctx = {pid: events for pid, events in context_events.items() if pid in allowed_ids}
    portraits = {pid: path for pid, path in portrait_paths.items() if pid in allowed_ids}
    bios = {pid: text for pid, text in biography_texts.items() if pid in allowed_ids}
    return people, rel_out, ctx, portraits, bios


def derive_branch_output_path(base_output: str, branch_suffix: str) -> str:
    base_abs = os.path.abspath(base_output)
    root, ext = os.path.splitext(base_abs)
    final_ext = ext or ".md"
    return f"{root}_{branch_suffix}{final_ext}"


def write_book(
    *,
    output_path: str,
    root_id: str,
    persons: Dict[str, Dict],
    relationships: Dict[str, Dict],
    context_events: Dict[str, List[ContextEvent]],
    portrait_paths: Dict[str, str],
    biography_texts: Dict[str, str],
    generate_pdf: bool,
) -> None:
    md = build_markdown(
        root_id,
        persons,
        relationships,
        context_events,
        portrait_paths,
        biography_texts=biography_texts,
    )
    output_dir = os.path.dirname(os.path.abspath(output_path)) or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"Libro generado en {output_path}")
    if generate_pdf:
        pdf_path = os.path.splitext(output_path)[0] + ".pdf"
        build_pdf(output_path, pdf_path)
        print(f"PDF generado en {pdf_path}")


def clean_output_directory(output_path: str) -> None:
    output_dir = os.path.dirname(os.path.abspath(output_path)) or os.getcwd()
    # Protección básica: solo limpiar carpetas llamadas "output".
    if os.path.basename(output_dir.rstrip(os.sep)) != "output":
        print(f"Se omite limpieza automática: '{output_dir}' no es carpeta 'output'.")
        return
    if not os.path.isdir(output_dir):
        return
    for entry in os.scandir(output_dir):
        target = entry.path
        try:
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(target)
            else:
                os.remove(target)
        except OSError as exc:
            print(f"No se pudo eliminar '{target}': {exc}")
    print(f"Carpeta limpiada: {output_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera un libro genealógico desde FamilySearch")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--person-id",
        default=DEFAULT_PERSON_ID,
        help="ID de FamilySearch de la persona raíz (o FS_PERSON_ID)",
    )
    group.add_argument(
        "--current-person",
        action="store_true",
        help="Usar la persona del árbol que representa al usuario autenticado",
    )
    parser.add_argument("--generations", type=int, default=4, help="Número de generaciones a explorar")
    parser.add_argument(
        "--collateral-depth",
        type=int,
        default=None,
        help="Profundidad de expansión colateral (hermanos/tíos/primos) para sync local; 0 desactiva",
    )
    parser.add_argument(
        "--output",
        default="output/family_book.md",
        help="Ruta de salida del libro (Markdown, default: output/family_book.md)",
    )
    parser.add_argument(
        "--pdf",
        action="store_true",
        help="Genera también un PDF con pandoc usando el mismo nombre base del Markdown",
    )
    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="No genera PDF; solo Markdown",
    )
    parser.add_argument(
        "--tree-svg",
        action="store_true",
        help="Genera también un árbol de ancestros en SVG para impresión",
    )
    parser.add_argument(
        "--tree-svg-path",
        default="output/family_tree.svg",
        help="Ruta de salida del árbol SVG (default: output/family_tree.svg)",
    )
    parser.add_argument(
        "--tree-generations",
        type=int,
        default=None,
        help="Generaciones para el árbol SVG (default: usa --generations)",
    )
    parser.add_argument(
        "--context",
        action="store_true",
        help="Agregar contexto histórico usando Wikidata + World Bank",
    )
    parser.add_argument(
        "--ai-hybrid",
        action="store_true",
        help="Genera artefactos para flujo híbrido IA (prompt + contexto estructurado + draft)",
    )
    parser.add_argument(
        "--ai-prompt-path",
        default=DEFAULT_AI_PROMPT_PATH,
        help=f"Ruta del prompt versionado para IA (default: {DEFAULT_AI_PROMPT_PATH})",
    )
    parser.add_argument(
        "--ai-context-output",
        default="output/ai_context.json",
        help="Ruta de salida del contexto estructurado para IA (JSON)",
    )
    parser.add_argument(
        "--ai-draft-output",
        default="output/family_book_ai.md",
        help="Ruta de salida del archivo de entrada para IA (Markdown con prompt + JSON)",
    )
    parser.add_argument(
        "--enrich-biography",
        action="store_true",
        help="Enriquece biografías con notas, memorias y fuentes textuales por persona",
    )
    parser.add_argument(
        "--sync-local-db",
        action="store_true",
        help="Sincroniza personas, relaciones, notas, fuentes, memorias y retratos a una base SQLite local",
    )
    parser.add_argument(
        "--local-db-path",
        default="output/familybook.sqlite",
        help="Ruta de la base SQLite local (default: output/familybook.sqlite)",
    )
    parser.add_argument(
        "--sync-stale-person-hours",
        type=int,
        default=None,
        help="Horas para refrescar datos base de persona y relaciones (default persistido: 24)",
    )
    parser.add_argument(
        "--sync-stale-notes-hours",
        type=int,
        default=None,
        help="Horas para refrescar notas (default persistido: 72)",
    )
    parser.add_argument(
        "--sync-stale-sources-hours",
        type=int,
        default=None,
        help="Horas para refrescar fuentes (default persistido: 72)",
    )
    parser.add_argument(
        "--sync-stale-memories-hours",
        type=int,
        default=None,
        help="Horas para refrescar memorias y documentos (default persistido: 168)",
    )
    parser.add_argument(
        "--sync-stale-portraits-hours",
        type=int,
        default=None,
        help="Horas para refrescar retratos (default persistido: 720)",
    )
    parser.add_argument(
        "--sync-max-retries",
        type=int,
        default=None,
        help="Máximo de reintentos retryable por job antes de fallar definitivo (default persistido: 3)",
    )
    parser.add_argument(
        "--sync-retry-delay-minutes",
        type=int,
        default=None,
        help="Minutos de espera antes de reintentar jobs retryable (default persistido: 10)",
    )
    parser.add_argument(
        "--sync-job-limit",
        type=int,
        default=None,
        help="Procesa como máximo N jobs en esta ejecución y deja el resto pendiente para reanudar",
    )
    parser.add_argument(
        "--sync-force",
        action="store_true",
        help="Ignora timestamps locales y reencola todas las fases de sync",
    )
    parser.add_argument(
        "--sync-db-stubs",
        action="store_true",
        help="Enqueue full sync jobs for all stubs in the local DB (persons with no fetched profile)",
    )
    parser.add_argument(
        "--split-branches",
        action="store_true",
        help="Genera dos libros separados: rama paterna y rama materna",
    )
    parser.add_argument(
        "--branches-only",
        action="store_true",
        help="Con --split-branches, omite el libro general y deja solo paterna/materna",
    )
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Limpia la carpeta output/ antes de generar archivos",
    )
    parser.add_argument("--prompt-token", action="store_true", help="Pedir token por consola (input oculto)")
    parser.add_argument(
        "--oauth-login",
        action="store_true",
        help="Iniciar sesión OAuth en navegador (permite usar Google desde FamilySearch)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Con --oauth-login, no intenta abrir el navegador; solo imprime la URL",
    )
    parser.add_argument(
        "--oauth-client-id",
        default=os.getenv("FS_OAUTH_CLIENT_ID"),
        help="Client ID (App Key) de FamilySearch (o FS_OAUTH_CLIENT_ID)",
    )
    parser.add_argument(
        "--oauth-redirect-uri",
        default=DEFAULT_OAUTH_REDIRECT_URI,
        help=f"Redirect URI para OAuth (default: {DEFAULT_OAUTH_REDIRECT_URI})",
    )
    parser.add_argument(
        "--oauth-scope",
        default=DEFAULT_OAUTH_SCOPE,
        help=f"Scopes OAuth separados por espacio (default: {DEFAULT_OAUTH_SCOPE})",
    )
    parser.add_argument(
        "--ident-base-url",
        default=DEFAULT_IDENT_BASE_URL,
        help=f"Base URL de identidad OAuth (default: {DEFAULT_IDENT_BASE_URL})",
    )
    parser.add_argument(
        "--oauth-timeout-seconds",
        type=int,
        default=DEFAULT_OAUTH_TIMEOUT_SECONDS,
        help=f"Tiempo de espera para callback OAuth local en segundos (default: {DEFAULT_OAUTH_TIMEOUT_SECONDS})",
    )
    parser.add_argument(
        "--token-cache-path",
        default=DEFAULT_TOKEN_CACHE_PATH,
        help=f"Ruta para cache local de token OAuth (default: {DEFAULT_TOKEN_CACHE_PATH})",
    )
    parser.add_argument(
        "--no-token-cache",
        action="store_true",
        help="Desactiva lectura y guardado de cache de token OAuth",
    )
    parser.add_argument(
        "--mirror-status",
        action="store_true",
        help="Imprime un reporte de salud del mirror local (SQLite) sin llamar a FamilySearch y sale",
    )
    mode_group = parser.add_mutually_exclusive_group(required=False)
    mode_group.add_argument(
        "--book-only",
        action="store_true",
        help="Genera solo libro (Markdown/PDF), sin árbol SVG",
    )
    mode_group.add_argument(
        "--tree-only",
        action="store_true",
        help="Genera solo árbol SVG, sin libro Markdown/PDF",
    )
    args = parser.parse_args()

    if args.mirror_status:
        familybook_db.print_mirror_status(args.local_db_path)
        return

    if not args.person_id and not args.current_person:
        parser.error("Debes indicar --person-id (o FS_PERSON_ID) o usar --current-person.")

    use_token_cache = not args.no_token_cache
    token = os.getenv("FS_ACCESS_TOKEN")
    oauth_tokens: Optional[OAuthTokens] = None

    if use_token_cache and not token and not args.prompt_token and not args.oauth_login:
        try:
            oauth_tokens = load_cached_tokens(args.token_cache_path)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"No se pudo leer cache de token ({args.token_cache_path}): {exc}")
            oauth_tokens = None

        if oauth_tokens:
            if oauth_tokens.is_expired():
                if oauth_tokens.refresh_token and args.oauth_client_id:
                    try:
                        oauth_tokens = refresh_access_token(
                            ident_base_url=args.ident_base_url,
                            client_id=args.oauth_client_id,
                            refresh_token=oauth_tokens.refresh_token,
                        )
                        save_cached_tokens(args.token_cache_path, oauth_tokens)
                        print("Token OAuth renovado desde cache local.")
                        token = oauth_tokens.access_token
                    except Exception as exc:
                        print(f"No se pudo renovar token cached: {exc}")
                        oauth_tokens = None
                else:
                    oauth_tokens = None
            else:
                token = oauth_tokens.access_token
                print("Usando token OAuth desde cache local.")

    if args.oauth_login:
        if not args.oauth_client_id:
            raise SystemExit(
                "Falta --oauth-client-id (App Key) o FS_OAUTH_CLIENT_ID para iniciar sesión OAuth."
            )
        oauth_tokens = oauth_login(
            ident_base_url=args.ident_base_url,
            client_id=args.oauth_client_id,
            redirect_uri=args.oauth_redirect_uri,
            scope=args.oauth_scope,
            timeout_seconds=args.oauth_timeout_seconds,
            open_browser=not args.no_browser,
        )
        token = oauth_tokens.access_token
        if use_token_cache:
            try:
                save_cached_tokens(args.token_cache_path, oauth_tokens)
                print(f"Token OAuth guardado en cache: {args.token_cache_path}")
            except OSError as exc:
                print(f"No se pudo guardar cache de token: {exc}")
    elif args.prompt_token:
        token = prompt_for_token()
    elif not token:
        print("FS_ACCESS_TOKEN no esta definido. Se solicitara el token por consola.")
        token = prompt_for_token()

    session = build_session(token)

    if args.clean_output:
        clean_output_directory(args.output)

    # Reunir personas y relaciones
    if args.current_person:
        root_id, root_person = fetch_current_tree_person(session)
    else:
        root_id = args.person_id
        root_person = fetch_person(session, root_id)

    ancestry_people, ancestry_parent_map = fetch_ancestry_bundle(session, root_id, generations=args.generations)
    descendancy_people = fetch_descendancy(session, root_id, generations=args.generations)
    all_ids = [root_id, *ancestry_people.keys(), *descendancy_people.keys()]
    if args.sync_local_db:
        if args.collateral_depth is None:
            effective_collateral_depth = args.generations
        else:
            effective_collateral_depth = max(0, int(args.collateral_depth))
        if effective_collateral_depth > 0:
            collateral_ids = fetch_collateral_ids(session, root_id, max_depth=effective_collateral_depth)
            collateral_extra = len(set(collateral_ids) - set(all_ids))
            all_ids = [*all_ids, *collateral_ids]
            all_ids = list(dict.fromkeys(all_ids))
            if collateral_extra > 0:
                print(
                    "Expansión colateral "
                    f"(profundidad {effective_collateral_depth}): +{collateral_extra} personas "
                    f"(límite {COLLATERAL_MAX_PEOPLE})."
                )
    persons = fetch_persons_batch(session, all_ids)
    if root_id not in persons:
        persons[root_id] = root_person

    relationships_raw = fetch_relationships(session, root_id)
    relationships = parse_relationships(relationships_raw)

    if args.ai_hybrid and not args.context:
        print("Activando --context automáticamente por --ai-hybrid.")
        args.context = True
    if args.ai_hybrid and not args.enrich_biography:
        print("Activando --enrich-biography automáticamente por --ai-hybrid.")
        args.enrich_biography = True
    if args.branches_only and not args.split_branches:
        print("Ignorando --branches-only porque falta --split-branches.")

    # Eventos de contexto por persona
    context_events: Dict[str, List[ContextEvent]] = {}
    if args.context:
        wikidata_cache: Dict[Tuple[str, int, int], List[ContextEvent]] = {}
        worldbank_cache: Dict[Tuple[str, int, int], List[ContextEvent]] = {}
        worldbank_index: Dict[str, str] = {}
        try:
            worldbank_index = fetch_world_bank_country_index()
        except requests.RequestException as exc:
            print(f"No se pudo inicializar catálogo de países World Bank: {exc}")
            worldbank_index = {}

        for pid, person in persons.items():
            display = person.get("display", {})
            birth_place = display.get("birthPlace")
            birth_year = iso_year(display.get("birthDate"))
            death_year = iso_year(display.get("deathDate")) or (birth_year + 90 if birth_year else None)
            if not (birth_place and birth_year and death_year):
                continue
            combined: List[ContextEvent] = []
            wikidata_key = (birth_place, birth_year, death_year)
            try:
                if wikidata_key not in wikidata_cache:
                    wikidata_cache[wikidata_key] = query_wikidata_events(birth_place, birth_year, death_year)
                combined.extend(wikidata_cache[wikidata_key])
            except requests.RequestException:
                # No bloquea el libro si falla Wikidata
                pass

            country = extract_country_from_place(birth_place)
            if country and worldbank_index:
                wb_key = (country, birth_year, death_year)
                try:
                    if wb_key not in worldbank_cache:
                        worldbank_cache[wb_key] = query_world_bank_context(
                            country_name=country,
                            start_year=birth_year,
                            end_year=death_year,
                            country_index=worldbank_index,
                        )
                    combined.extend(worldbank_cache[wb_key])
                except requests.RequestException:
                    pass

            if combined:
                context_events[pid] = sorted(
                    combined,
                    key=lambda e: (iso_year(e.date) is None, iso_year(e.date) or 9999, e.date),
                )

    should_generate_tree = args.tree_svg or args.tree_only
    tree_generations = args.tree_generations or args.generations
    tree_parent_map = ancestry_parent_map
    if should_generate_tree and tree_generations > args.generations:
        extra_ancestry, tree_parent_map = fetch_ancestry_bundle(session, root_id, generations=tree_generations)
        missing = [pid for pid in extra_ancestry.keys() if pid not in persons]
        if missing:
            persons.update(fetch_persons_batch(session, missing))

    output_dir = os.path.dirname(os.path.abspath(args.output)) or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)
    portrait_paths = download_portraits(session, persons, args.output)
    biography_texts: Dict[str, str] = {}
    notes_payloads: Dict[str, Dict] = {}
    sources_payloads: Dict[str, Dict] = {}
    memories_payloads: Dict[str, List[Dict]] = {}
    if args.enrich_biography:
        for pid, person in persons.items():
            try:
                notes_payload = fetch_person_notes_payload(session, pid)
                sources_payload = fetch_person_sources_payload(session, pid)
                memories_payload = fetch_person_memories_payloads(session, pid)
            except requests.RequestException:
                notes_payload = None
                sources_payload = None
                memories_payload = []
            if notes_payload:
                notes_payloads[pid] = notes_payload
            if sources_payload:
                sources_payloads[pid] = sources_payload
            if memories_payload:
                memories_payloads[pid] = memories_payload
            materials = []
            materials.extend(fetch_person_notes_texts_from_payload(notes_payload))
            materials.extend(fetch_person_sources_texts_from_payload(sources_payload))
            materials.extend(fetch_person_memories_texts_from_payloads(session, memories_payload))
            merged = compose_biography_text(person, materials)
            if merged:
                biography_texts[pid] = merged

    if args.sync_local_db:
        sync_config = resolve_sync_config_from_args(args)
        stale_hours_by_phase = {
            "person": sync_config["sync_stale_person_hours"],
            "notes": sync_config["sync_stale_notes_hours"],
            "sources": sync_config["sync_stale_sources_hours"],
            "memories": sync_config["sync_stale_memories_hours"],
            "portraits": sync_config["sync_stale_portraits_hours"],
        }
        run_id = run_incremental_sync(
            session=session,
            db_path=args.local_db_path,
            root_person_id=root_id,
            generations=args.generations,
            person_ids=list(persons.keys()),
            stale_hours_by_phase=stale_hours_by_phase,
            max_retries=sync_config["sync_max_retries"],
            retry_delay_minutes=sync_config["sync_retry_delay_minutes"],
            force=args.sync_force,
            job_limit=args.sync_job_limit,
        )
        print(f"Base local sincronizada incrementalmente en {args.local_db_path} (run {run_id})")

    if args.sync_db_stubs:
        stub_ids = familybook_db.list_stub_person_ids(args.local_db_path)
        if stub_ids:
            print(f"Sincronizando {len(stub_ids)} personas stub...")
            stub_run_id = run_incremental_sync(
                session=session,
                db_path=args.local_db_path,
                root_person_id=stub_ids[0],
                generations=0,
                person_ids=stub_ids,
                stale_hours_by_phase={"person": 0, "notes": 0, "sources": 0, "memories": 0, "portraits": 0},
                max_retries=3,
                retry_delay_minutes=10,
                force=True,
                job_limit=args.sync_job_limit,
            )
            print(f"Stubs sincronizados (run {stub_run_id})")
        else:
            print("No hay stubs en la base local.")

    should_generate_pdf = args.pdf or not args.no_pdf
    if not args.tree_only and not (args.split_branches and args.branches_only):
        write_book(
            output_path=args.output,
            root_id=root_id,
            persons=persons,
            relationships=relationships,
            context_events=context_events,
            portrait_paths=portrait_paths,
            biography_texts=biography_texts,
            generate_pdf=should_generate_pdf,
        )

    if args.split_branches and not args.tree_only:
        root_rel = relationships.get(root_id, {})
        father_id = ancestry_parent_map.get(root_id, (None, None))[0] or root_rel.get("father")
        mother_id = ancestry_parent_map.get(root_id, (None, None))[1] or root_rel.get("mother")

        branch_specs = [
            ("paterna", father_id),
            ("materna", mother_id),
        ]
        for branch_name, branch_start in branch_specs:
            if not branch_start:
                print(f"No se encontró ancestro raíz para rama {branch_name}; se omite.")
                continue
            branch_ids = collect_ancestor_subtree_ids(branch_start, ancestry_parent_map)
            branch_ids.add(root_id)
            branch_persons, branch_rels, branch_ctx, branch_portraits, branch_bios = filter_branch_data(
                allowed_ids=branch_ids,
                persons=persons,
                relationships=relationships,
                context_events=context_events,
                portrait_paths=portrait_paths,
                biography_texts=biography_texts,
            )
            if root_id not in branch_persons:
                branch_persons[root_id] = persons[root_id]
            branch_output = derive_branch_output_path(args.output, branch_name)
            write_book(
                output_path=branch_output,
                root_id=root_id,
                persons=branch_persons,
                relationships=branch_rels,
                context_events=branch_ctx,
                portrait_paths=branch_portraits,
                biography_texts=branch_bios,
                generate_pdf=should_generate_pdf,
            )

    if args.ai_hybrid:
        ai_context_dir = os.path.dirname(os.path.abspath(args.ai_context_output)) or os.getcwd()
        ai_draft_dir = os.path.dirname(os.path.abspath(args.ai_draft_output)) or os.getcwd()
        os.makedirs(ai_context_dir, exist_ok=True)
        os.makedirs(ai_draft_dir, exist_ok=True)

        ai_payload = build_ai_context_payload(
            root_id=root_id,
            persons=persons,
            relationships=relationships,
            context_events=context_events,
            portrait_paths=portrait_paths,
            biography_texts=biography_texts,
        )
        with open(args.ai_context_output, "w", encoding="utf-8") as f:
            json.dump(ai_payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        print(f"Contexto IA generado en {args.ai_context_output}")

        prompt_text = load_prompt_template(args.ai_prompt_path)
        draft_markdown = build_ai_draft_markdown(prompt_text, ai_payload)
        with open(args.ai_draft_output, "w", encoding="utf-8") as f:
            f.write(draft_markdown)
        print(f"Draft IA generado en {args.ai_draft_output}")

    if should_generate_tree and not args.book_only:
        tree_output_dir = os.path.dirname(os.path.abspath(args.tree_svg_path)) or os.getcwd()
        os.makedirs(tree_output_dir, exist_ok=True)
        build_pedigree_svg(
            root_id=root_id,
            persons=persons,
            parent_map=tree_parent_map,
            portrait_paths=portrait_paths,
            svg_path=args.tree_svg_path,
            generations=tree_generations,
        )
        print(f"Árbol SVG generado en {args.tree_svg_path}")


if __name__ == "__main__":
    main()
