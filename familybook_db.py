from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import unicodedata
from itertools import combinations
from typing import Any, Dict, List, Optional


SCHEMA_VERSION = 9
JOB_STALE_MINUTES = 15
RETRY_DELAY_MINUTES = 10
FSID_RE = re.compile(r"^[A-Z0-9]{4}-[A-Z0-9]{3,4}$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)
DEFAULT_SYNC_CONFIG = {
    "sync_stale_person_hours": 24,
    "sync_stale_notes_hours": 72,
    "sync_stale_sources_hours": 72,
    "sync_stale_memories_hours": 168,
    "sync_stale_portraits_hours": 720,
    "sync_max_retries": 3,
    "sync_retry_delay_minutes": 10,
}

HISTORICAL_EVENTS_SEED = [
    {
        "event_key": "global_world_war_i",
        "scope": "global",
        "title": "Primera Guerra Mundial",
        "description": "Conflicto global entre potencias europeas y sus aliados.",
        "start_year": 1914,
        "end_year": 1918,
        "source_url": "https://www.britannica.com/event/World-War-I",
        "match_terms": [],
    },
    {
        "event_key": "global_spanish_flu",
        "scope": "global",
        "title": "Pandemia de gripe de 1918",
        "description": "Pandemia de influenza que afectó a gran parte del mundo.",
        "start_year": 1918,
        "end_year": 1920,
        "source_url": "https://www.britannica.com/event/influenza-pandemic-of-1918-1919",
        "match_terms": [],
    },
    {
        "event_key": "global_world_war_ii",
        "scope": "global",
        "title": "Segunda Guerra Mundial",
        "description": "Conflicto militar global con impacto demográfico y social masivo.",
        "start_year": 1939,
        "end_year": 1945,
        "source_url": "https://www.britannica.com/event/World-War-II",
        "match_terms": [],
    },
    {
        "event_key": "global_un_founded",
        "scope": "global",
        "title": "Fundación de la ONU",
        "description": "Creación de la Organización de las Naciones Unidas.",
        "start_year": 1945,
        "end_year": 1945,
        "source_url": "https://www.un.org/en/about-us/history-of-the-un",
        "match_terms": [],
    },
    {
        "event_key": "global_cold_war",
        "scope": "global",
        "title": "Guerra Fría",
        "description": "Periodo de tensión geopolítica entre EE.UU. y la URSS.",
        "start_year": 1947,
        "end_year": 1991,
        "source_url": "https://www.britannica.com/event/Cold-War",
        "match_terms": [],
    },
    {
        "event_key": "global_moon_landing",
        "scope": "global",
        "title": "Llegada del ser humano a la Luna",
        "description": "La misión Apollo 11 coloca astronautas en la Luna.",
        "start_year": 1969,
        "end_year": 1969,
        "source_url": "https://www.nasa.gov/history/apollo-11-mission-overview/",
        "match_terms": [],
    },
    {
        "event_key": "global_berlin_wall_fall",
        "scope": "global",
        "title": "Caída del Muro de Berlín",
        "description": "Evento clave del fin de la Guerra Fría.",
        "start_year": 1989,
        "end_year": 1989,
        "source_url": "https://www.britannica.com/event/Berlin-Wall",
        "match_terms": [],
    },
    {
        "event_key": "global_covid19",
        "scope": "global",
        "title": "Pandemia de COVID-19",
        "description": "Pandemia global con alto impacto sanitario, social y económico.",
        "start_year": 2020,
        "end_year": 2023,
        "source_url": "https://www.who.int/emergencies/diseases/novel-coronavirus-2019",
        "match_terms": [],
    },
    {
        "event_key": "ve_independence_1811",
        "scope": "local",
        "title": "Acta de Independencia de Venezuela",
        "description": "Declaración de independencia de Venezuela.",
        "start_year": 1811,
        "end_year": 1811,
        "source_url": "https://www.britannica.com/place/Venezuela/Independence-movement",
        "match_terms": ["venezuela", "caracas", "falcón", "miranda", "coro", "punto fijo"],
    },
    {
        "event_key": "ve_carabobo_1821",
        "scope": "local",
        "title": "Batalla de Carabobo",
        "description": "Batalla decisiva para la independencia de Venezuela.",
        "start_year": 1821,
        "end_year": 1821,
        "source_url": "https://www.britannica.com/event/Battle-of-Carabobo",
        "match_terms": ["venezuela", "carabobo", "valencia", "falcón", "miranda"],
    },
    {
        "event_key": "ve_federal_war",
        "scope": "local",
        "title": "Guerra Federal en Venezuela",
        "description": "Guerra civil entre fuerzas federalistas y centralistas.",
        "start_year": 1859,
        "end_year": 1863,
        "source_url": "https://www.britannica.com/place/Venezuela/The-19th-century",
        "match_terms": ["venezuela", "falcón", "miranda", "coro", "caracas"],
    },
    {
        "event_key": "ve_barroso_ii_1922",
        "scope": "local",
        "title": "Reventón del pozo Barroso II",
        "description": "Evento emblemático del inicio de la era petrolera venezolana.",
        "start_year": 1922,
        "end_year": 1922,
        "source_url": "https://www.britannica.com/place/Venezuela/The-20th-century",
        "match_terms": ["venezuela", "zulia", "maracaibo", "falcón"],
    },
    {
        "event_key": "ve_democracy_1958",
        "scope": "local",
        "title": "Retorno a la democracia en Venezuela",
        "description": "Caída de la dictadura de Marcos Pérez Jiménez.",
        "start_year": 1958,
        "end_year": 1958,
        "source_url": "https://www.britannica.com/biography/Marcos-Perez-Jimenez",
        "match_terms": ["venezuela", "caracas", "falcón", "miranda"],
    },
    {
        "event_key": "ve_caracazo_1989",
        "scope": "local",
        "title": "El Caracazo",
        "description": "Protestas y disturbios de gran impacto social en Venezuela.",
        "start_year": 1989,
        "end_year": 1989,
        "source_url": "https://www.britannica.com/place/Venezuela/The-21st-century",
        "match_terms": ["venezuela", "caracas", "miranda", "falcón"],
    },
    {
        "event_key": "ve_constitution_1999",
        "scope": "local",
        "title": "Nueva Constitución de Venezuela",
        "description": "Aprobación de la Constitución de la República Bolivariana de Venezuela.",
        "start_year": 1999,
        "end_year": 1999,
        "source_url": "https://www.britannica.com/place/Venezuela/The-21st-century",
        "match_terms": ["venezuela", "caracas", "falcón", "miranda"],
    },
    {
        "event_key": "ve_vargas_tragedy_1999",
        "scope": "local",
        "title": "Tragedia de Vargas",
        "description": "Desastre natural por deslizamientos e inundaciones en la costa central.",
        "start_year": 1999,
        "end_year": 1999,
        "source_url": "https://www.britannica.com/place/Venezuela/The-21st-century",
        "match_terms": ["venezuela", "vargas", "la guaira", "miranda", "caracas"],
    },
]

DNA_TRAITS_SEED = [
    {
        "rsid": "rs12913832",
        "category": "physical",
        "trait_name": "Color de ojos claros",
        "allele": "G",
        "effect": "Mayor probabilidad de ojos claros o verdes.",
        "confidence": "medium",
        "source_url": "https://www.snpedia.com/index.php/Rs12913832",
    },
    {
        "rsid": "rs1805007",
        "category": "physical",
        "trait_name": "Cabello rojizo / baja eumelanina",
        "allele": "T",
        "effect": "Asociado a cabello rojizo y piel más sensible al sol.",
        "confidence": "medium",
        "source_url": "https://www.snpedia.com/index.php/Rs1805007",
    },
    {
        "rsid": "rs4988235",
        "category": "health",
        "trait_name": "Persistencia de lactasa",
        "allele": "T",
        "effect": "Mayor probabilidad de tolerancia a la lactosa en la adultez.",
        "confidence": "high",
        "source_url": "https://www.snpedia.com/index.php/Rs4988235",
    },
    {
        "rsid": "rs7903146",
        "category": "health",
        "trait_name": "Predisposición glucémica",
        "allele": "T",
        "effect": "Asociación documentada con mayor riesgo estadístico de diabetes tipo 2.",
        "confidence": "medium",
        "source_url": "https://www.snpedia.com/index.php/Rs7903146",
    },
    {
        "rsid": "rs9939609",
        "category": "health",
        "trait_name": "Predisposición al peso corporal",
        "allele": "A",
        "effect": "Asociación probabilística con mayor IMC en estudios poblacionales.",
        "confidence": "medium",
        "source_url": "https://www.snpedia.com/index.php/Rs9939609",
    },
    {
        "rsid": "rs1815739",
        "category": "physical",
        "trait_name": "Perfil muscular ACTN3",
        "allele": "C",
        "effect": "Asociado a potencia muscular y respuesta de fibras rápidas.",
        "confidence": "medium",
        "source_url": "https://www.snpedia.com/index.php/Rs1815739",
    },
    {
        "rsid": "rs4680",
        "category": "behavior",
        "trait_name": "Procesamiento dopaminérgico COMT",
        "allele": "A",
        "effect": "Asociación probabilística con diferencias en respuesta al estrés y función ejecutiva.",
        "confidence": "low",
        "source_url": "https://www.snpedia.com/index.php/Rs4680",
    },
]

DEFAULT_HAPLOGROUP_TIMELINES = {
    "y": {
        "R": [
            {"label": "Asia Central", "period": "hace ~25 mil años", "lat": 48.0, "lon": 67.0},
            {"label": "Europa Oriental", "period": "hace ~8 mil años", "lat": 50.0, "lon": 30.0},
            {"label": "Europa Occidental", "period": "hace ~4 mil años", "lat": 47.0, "lon": 2.0},
        ],
        "I": [
            {"label": "Europa Sudoriental", "period": "hace ~20 mil años", "lat": 43.0, "lon": 20.0},
            {"label": "Balcanes", "period": "hace ~10 mil años", "lat": 44.0, "lon": 18.0},
            {"label": "Europa Central", "period": "hace ~4 mil años", "lat": 50.0, "lon": 12.0},
        ],
        "J": [
            {"label": "Creciente Fértil", "period": "hace ~18 mil años", "lat": 33.0, "lon": 44.0},
            {"label": "Mediterráneo Oriental", "period": "hace ~9 mil años", "lat": 36.0, "lon": 35.0},
            {"label": "Mediterráneo", "period": "hace ~4 mil años", "lat": 41.0, "lon": 15.0},
        ],
        "E": [
            {"label": "África Oriental", "period": "hace ~25 mil años", "lat": 8.0, "lon": 39.0},
            {"label": "Norte de África", "period": "hace ~10 mil años", "lat": 27.0, "lon": 17.0},
            {"label": "Mediterráneo", "period": "hace ~4 mil años", "lat": 37.0, "lon": 12.0},
        ],
        "Q": [
            {"label": "Asia Central", "period": "hace ~20 mil años", "lat": 46.0, "lon": 76.0},
            {"label": "Siberia", "period": "hace ~12 mil años", "lat": 62.0, "lon": 105.0},
            {"label": "Américas", "period": "hace ~8 mil años", "lat": 19.0, "lon": -99.0},
        ],
    },
    "mt": {
        "H": [
            {"label": "Asia Occidental", "period": "hace ~25 mil años", "lat": 35.0, "lon": 44.0},
            {"label": "Europa", "period": "hace ~10 mil años", "lat": 48.0, "lon": 14.0},
            {"label": "Atlántico europeo", "period": "hace ~4 mil años", "lat": 43.0, "lon": -3.0},
        ],
        "U": [
            {"label": "Asia Occidental", "period": "hace ~35 mil años", "lat": 37.0, "lon": 46.0},
            {"label": "Europa Oriental", "period": "hace ~15 mil años", "lat": 50.0, "lon": 30.0},
            {"label": "Europa", "period": "hace ~5 mil años", "lat": 46.0, "lon": 12.0},
        ],
        "J": [
            {"label": "Levante", "period": "hace ~25 mil años", "lat": 32.0, "lon": 35.0},
            {"label": "Anatolia", "period": "hace ~10 mil años", "lat": 39.0, "lon": 35.0},
            {"label": "Europa", "period": "hace ~4 mil años", "lat": 47.0, "lon": 9.0},
        ],
        "L": [
            {"label": "África Oriental", "period": "hace ~70 mil años", "lat": 5.0, "lon": 38.0},
            {"label": "Cuerno de África", "period": "hace ~30 mil años", "lat": 11.0, "lon": 42.0},
            {"label": "Diáspora global", "period": "era histórica", "lat": 13.0, "lon": -16.0},
        ],
        "A": [
            {"label": "Asia Oriental", "period": "hace ~30 mil años", "lat": 45.0, "lon": 128.0},
            {"label": "Beringia", "period": "hace ~15 mil años", "lat": 65.0, "lon": -168.0},
            {"label": "Américas", "period": "hace ~8 mil años", "lat": 20.0, "lon": -99.0},
        ],
    },
}


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _json_dump(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _hash_file(path: str) -> Optional[str]:
    if not path or not os.path.isfile(path):
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_text_for_identity(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFD", str(value))
    without_marks = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    compact = re.sub(r"\s+", " ", without_marks.replace("–", "-").replace("—", "-")).strip().lower()
    return compact


def _normalize_place_for_identity(value: str | None) -> str:
    return _normalize_text_for_identity(value)


def _is_probable_fsid(person_id: str | None) -> bool:
    return bool(person_id and FSID_RE.match(str(person_id).strip().upper()))


def _is_probable_uuid(person_id: str | None) -> bool:
    return bool(person_id and UUID_RE.match(str(person_id).strip()))


def _merge_prefer_text(*values: Any) -> Optional[str]:
    cleaned = [str(value).strip() for value in values if str(value or "").strip()]
    if not cleaned:
        return None
    return max(cleaned, key=lambda item: (len(item), item))


def _max_iso_text(*values: Any) -> Optional[str]:
    cleaned = [str(value).strip() for value in values if str(value or "").strip()]
    return max(cleaned) if cleaned else None


def _resolve_person_alias_with_conn(conn: sqlite3.Connection, person_id: str | None) -> str | None:
    current = str(person_id or "").strip()
    if not current:
        return None
    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        row = conn.execute(
            "SELECT canonical_person_id FROM person_aliases WHERE alias_person_id = ?",
            (current,),
        ).fetchone()
        if not row or not row["canonical_person_id"]:
            return current
        current = str(row["canonical_person_id"]).strip()
    return current or None


def _register_person_alias_with_conn(
    conn: sqlite3.Connection,
    alias_person_id: str | None,
    canonical_person_id: str | None,
    reason: str | None = None,
) -> None:
    alias_person_id = str(alias_person_id or "").strip()
    canonical_person_id = str(canonical_person_id or "").strip()
    if not alias_person_id or not canonical_person_id or alias_person_id == canonical_person_id:
        return
    canonical_person_id = _resolve_person_alias_with_conn(conn, canonical_person_id) or canonical_person_id
    conn.execute(
        """
        INSERT INTO person_aliases(alias_person_id, canonical_person_id, reason, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(alias_person_id) DO UPDATE SET
            canonical_person_id=excluded.canonical_person_id,
            reason=COALESCE(excluded.reason, person_aliases.reason),
            created_at=excluded.created_at
        """,
        (alias_person_id, canonical_person_id, (reason or "").strip() or None, utc_now_iso()),
    )


def _load_identity_snapshots_with_conn(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    people_rows = conn.execute(
        """
        SELECT p.person_id, p.name, p.gender, p.birth_date, p.birth_place, p.death_date, p.death_place,
               p.lifespan, p.raw_json,
               CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END AS is_stub
        FROM persons p
        LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
        """
    ).fetchall()
    rel_rows = conn.execute(
        """
        SELECT person_id, relation_type, related_person_id
        FROM relationships
        ORDER BY person_id, relation_type, related_person_id
        """
    ).fetchall()
    by_person: Dict[str, Dict[str, Any]] = {}
    for row in people_rows:
        person_id = str(row["person_id"])
        raw_json = str(row["raw_json"] or "").strip()
        by_person[person_id] = {
            "person_id": person_id,
            "name": row["name"],
            "name_norm": _normalize_text_for_identity(row["name"]),
            "gender": row["gender"],
            "birth_date": row["birth_date"],
            "birth_year": _extract_year_from_text(row["birth_date"]),
            "birth_place": row["birth_place"],
            "birth_place_norm": _normalize_place_for_identity(row["birth_place"]),
            "death_date": row["death_date"],
            "death_year": _extract_year_from_text(row["death_date"]),
            "death_place": row["death_place"],
            "lifespan": row["lifespan"],
            "is_stub": bool(row["is_stub"]),
            "is_fsid": _is_probable_fsid(person_id),
            "is_uuid": _is_probable_uuid(person_id),
            "raw_json_size": len(raw_json),
            "parents": set(),
            "spouses": set(),
            "children": set(),
        }
    for row in rel_rows:
        person_id = str(row["person_id"] or "").strip()
        related = str(row["related_person_id"] or "").strip()
        if person_id not in by_person or not related:
            continue
        rel_type = str(row["relation_type"] or "").strip().lower()
        if rel_type in ("father", "mother"):
            by_person[person_id]["parents"].add(related)
        elif rel_type == "spouses":
            by_person[person_id]["spouses"].add(related)
        elif rel_type == "children":
            by_person[person_id]["children"].add(related)
    for item in by_person.values():
        item["detail_score"] = (
            (40 if item["is_fsid"] else 0)
            + (25 if not item["is_uuid"] else 0)
            + (20 if not item["is_stub"] else 0)
            + (8 if item["birth_year"] is not None else 0)
            + (6 if item["death_year"] is not None else 0)
            + (5 if item["birth_place_norm"] else 0)
            + min(15, len(item["parents"]) * 4 + len(item["spouses"]) * 2 + len(item["children"]) * 2)
            + min(10, item["raw_json_size"] // 300)
        )
    return by_person


def _prefer_canonical_snapshot(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    left_rank = (
        int(left.get("detail_score") or 0),
        1 if left.get("is_fsid") else 0,
        1 if not left.get("is_stub") else 0,
        1 if not left.get("is_uuid") else 0,
        -len(str(left.get("person_id") or "")),
        str(left.get("person_id") or ""),
    )
    right_rank = (
        int(right.get("detail_score") or 0),
        1 if right.get("is_fsid") else 0,
        1 if not right.get("is_stub") else 0,
        1 if not right.get("is_uuid") else 0,
        -len(str(right.get("person_id") or "")),
        str(right.get("person_id") or ""),
    )
    return left if left_rank >= right_rank else right


def _name_token_set(name_norm: str | None) -> set[str]:
    return {token for token in re.split(r"[^a-z0-9]+", str(name_norm or "")) if len(token) >= 3}


def _names_look_compatible(left_name_norm: str | None, right_name_norm: str | None) -> bool:
    if not left_name_norm or not right_name_norm:
        return False
    if left_name_norm == right_name_norm:
        return True
    left_tokens = _name_token_set(left_name_norm)
    right_tokens = _name_token_set(right_name_norm)
    if not left_tokens or not right_tokens:
        return False
    smaller, larger = (left_tokens, right_tokens) if len(left_tokens) <= len(right_tokens) else (right_tokens, left_tokens)
    return len(smaller) >= 2 and smaller.issubset(larger)


def _is_strong_duplicate_snapshot(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    if not _names_look_compatible(left.get("name_norm"), right.get("name_norm")):
        return False
    left_birth = left.get("birth_year")
    right_birth = right.get("birth_year")
    left_death = left.get("death_year")
    right_death = right.get("death_year")
    birth_exact = left_birth is not None and right_birth is not None and left_birth == right_birth
    death_exact = left_death is not None and right_death is not None and left_death == right_death
    birth_missing_compatible = birth_exact or left_birth is None or right_birth is None
    death_missing_compatible = death_exact or left_death is None or right_death is None
    if not birth_missing_compatible or not death_missing_compatible:
        return False
    same_birth_place = bool(left.get("birth_place_norm")) and left.get("birth_place_norm") == right.get("birth_place_norm")
    same_parents = bool(left.get("parents")) and left.get("parents") == right.get("parents")
    spouse_overlap = bool(set(left.get("spouses") or set()) & set(right.get("spouses") or set()))
    child_overlap = bool(set(left.get("children") or set()) & set(right.get("children") or set()))
    family_signal = same_parents or (spouse_overlap and child_overlap) or child_overlap
    same_spouses = bool(left.get("spouses")) and left.get("spouses") == right.get("spouses")
    same_children = bool(left.get("children")) and left.get("children") == right.get("children")
    one_is_weak_alias = bool(left.get("is_uuid") or right.get("is_uuid") or left.get("is_stub") or right.get("is_stub"))
    if birth_exact and death_exact and (family_signal or same_birth_place or one_is_weak_alias):
        return True
    if birth_exact and death_exact and same_parents and same_spouses and same_children:
        return True
    if birth_exact and same_parents:
        return True
    if birth_exact and spouse_overlap and child_overlap:
        return True
    if death_exact and spouse_overlap and child_overlap:
        return True
    return False


def _find_profile_duplicate_with_conn(
    conn: sqlite3.Connection,
    incoming_person_id: str,
    *,
    name: str | None,
    birth_date: str | None,
    birth_place: str | None,
    death_date: str | None,
    is_stub: bool,
) -> Optional[str]:
    incoming_name_norm = _normalize_text_for_identity(name)
    if not incoming_name_norm:
        return None
    incoming_birth = _extract_year_from_text(birth_date)
    incoming_death = _extract_year_from_text(death_date)
    incoming_place = _normalize_place_for_identity(birth_place)
    rows = conn.execute(
        """
        SELECT p.person_id, p.name, p.birth_date, p.birth_place, p.death_date,
               CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END AS is_stub
        FROM persons p
        LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
        WHERE p.name IS NOT NULL AND trim(p.name) <> ''
        """
    ).fetchall()
    candidates: List[Dict[str, Any]] = []
    for row in rows:
        person_id = str(row["person_id"] or "").strip()
        if not person_id or person_id == incoming_person_id:
            continue
        resolved = _resolve_person_alias_with_conn(conn, person_id)
        if resolved and resolved != person_id:
            continue
        row_name_norm = _normalize_text_for_identity(row["name"])
        if row_name_norm != incoming_name_norm:
            continue
        row_birth = _extract_year_from_text(row["birth_date"])
        row_death = _extract_year_from_text(row["death_date"])
        same_birth = incoming_birth is not None and row_birth is not None and incoming_birth == row_birth
        same_death = incoming_death is not None and row_death is not None and incoming_death == row_death
        same_place = bool(incoming_place) and incoming_place == _normalize_place_for_identity(row["birth_place"])
        if not ((same_birth and same_death) or (same_birth and same_place)):
            continue
        candidates.append(
            {
                "person_id": person_id,
                "is_stub": bool(row["is_stub"]),
                "is_fsid": _is_probable_fsid(person_id),
                "is_uuid": _is_probable_uuid(person_id),
                "detail_score": (30 if _is_probable_fsid(person_id) else 0) + (15 if not bool(row["is_stub"]) else 0),
            }
        )
    if not candidates:
        return None
    incoming_snapshot = {
        "person_id": incoming_person_id,
        "is_stub": is_stub,
        "is_fsid": _is_probable_fsid(incoming_person_id),
        "is_uuid": _is_probable_uuid(incoming_person_id),
        "detail_score": (30 if _is_probable_fsid(incoming_person_id) else 0) + (15 if not is_stub else 0),
    }
    preferred = _prefer_canonical_snapshot(
        incoming_snapshot,
        max(candidates, key=lambda item: (item["detail_score"], item["person_id"])),
    )
    return None if preferred["person_id"] == incoming_person_id else str(preferred["person_id"])


def _merge_person_facts_with_conn(conn: sqlite3.Connection, canonical_person_id: str, alias_person_id: str) -> None:
    existing_rows = conn.execute(
        """
        SELECT fact_type, date_original, place_original, value_json
        FROM person_facts
        WHERE person_id = ?
        """,
        (canonical_person_id,),
    ).fetchall()
    existing_keys = {
        (
            str(row["fact_type"] or ""),
            str(row["date_original"] or ""),
            str(row["place_original"] or ""),
            str(row["value_json"] or ""),
        )
        for row in existing_rows
    }
    next_seq_row = conn.execute(
        "SELECT COALESCE(MAX(seq), -1) AS max_seq FROM person_facts WHERE person_id = ?",
        (canonical_person_id,),
    ).fetchone()
    next_seq = int(next_seq_row["max_seq"] or -1) + 1
    alias_rows = conn.execute(
        """
        SELECT fact_type, date_original, place_original, value_json, updated_at
        FROM person_facts
        WHERE person_id = ?
        ORDER BY seq
        """,
        (alias_person_id,),
    ).fetchall()
    for row in alias_rows:
        fact_key = (
            str(row["fact_type"] or ""),
            str(row["date_original"] or ""),
            str(row["place_original"] or ""),
            str(row["value_json"] or ""),
        )
        if fact_key in existing_keys:
            continue
        conn.execute(
            """
            INSERT INTO person_facts(person_id, seq, fact_type, date_original, place_original, value_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                canonical_person_id,
                next_seq,
                row["fact_type"],
                row["date_original"],
                row["place_original"],
                row["value_json"],
                row["updated_at"] or utc_now_iso(),
            ),
        )
        existing_keys.add(fact_key)
        next_seq += 1
    conn.execute("DELETE FROM person_facts WHERE person_id = ?", (alias_person_id,))


def _merge_simple_person_table_with_conn(
    conn: sqlite3.Connection,
    table: str,
    key_columns: List[str],
    canonical_person_id: str,
    alias_person_id: str,
) -> None:
    columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    payload_columns = [col for col in columns if col != "person_id"]
    select_cols = ", ".join(payload_columns)
    if not payload_columns:
        return
    rows = conn.execute(
        f"SELECT {select_cols} FROM {table} WHERE person_id = ?",
        (alias_person_id,),
    ).fetchall()
    if not rows:
        return
    insert_cols = ", ".join(["person_id"] + payload_columns)
    placeholders = ", ".join("?" for _ in range(len(payload_columns) + 1))
    for row in rows:
        values = [canonical_person_id] + [row[col] for col in payload_columns]
        conn.execute(f"INSERT OR IGNORE INTO {table}({insert_cols}) VALUES ({placeholders})", values)
    conn.execute(f"DELETE FROM {table} WHERE person_id = ?", (alias_person_id,))


def _merge_person_sync_state_with_conn(conn: sqlite3.Connection, canonical_person_id: str, alias_person_id: str) -> None:
    canonical = conn.execute("SELECT * FROM person_sync_state WHERE person_id = ?", (canonical_person_id,)).fetchone()
    alias = conn.execute("SELECT * FROM person_sync_state WHERE person_id = ?", (alias_person_id,)).fetchone()
    if not alias:
        return
    merged = {
        "last_fetched_at": _max_iso_text(canonical["last_fetched_at"] if canonical else None, alias["last_fetched_at"]),
        "last_notes_at": _max_iso_text(canonical["last_notes_at"] if canonical else None, alias["last_notes_at"]),
        "last_sources_at": _max_iso_text(canonical["last_sources_at"] if canonical else None, alias["last_sources_at"]),
        "last_memories_at": _max_iso_text(canonical["last_memories_at"] if canonical else None, alias["last_memories_at"]),
        "last_portrait_at": _max_iso_text(canonical["last_portrait_at"] if canonical else None, alias["last_portrait_at"]),
        "last_relationships_at": _max_iso_text(canonical["last_relationships_at"] if canonical else None, alias["last_relationships_at"]),
        "last_run_id": alias["last_run_id"] or (canonical["last_run_id"] if canonical else None),
    }
    conn.execute(
        """
        INSERT INTO person_sync_state(
            person_id, last_fetched_at, last_notes_at, last_sources_at, last_memories_at,
            last_portrait_at, last_run_id, last_relationships_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(person_id) DO UPDATE SET
            last_fetched_at=excluded.last_fetched_at,
            last_notes_at=excluded.last_notes_at,
            last_sources_at=excluded.last_sources_at,
            last_memories_at=excluded.last_memories_at,
            last_portrait_at=excluded.last_portrait_at,
            last_run_id=excluded.last_run_id,
            last_relationships_at=excluded.last_relationships_at
        """,
        (
            canonical_person_id,
            merged["last_fetched_at"],
            merged["last_notes_at"],
            merged["last_sources_at"],
            merged["last_memories_at"],
            merged["last_portrait_at"],
            merged["last_run_id"],
            merged["last_relationships_at"],
        ),
    )
    conn.execute("DELETE FROM person_sync_state WHERE person_id = ?", (alias_person_id,))


def _merge_relationships_with_conn(conn: sqlite3.Connection, canonical_person_id: str, alias_person_id: str) -> None:
    rows = conn.execute(
        """
        SELECT person_id, relation_type, related_person_id, value_json, fs_relationship_id, updated_at
        FROM relationships
        WHERE person_id = ? OR related_person_id = ?
        """,
        (alias_person_id, alias_person_id),
    ).fetchall()
    if not rows:
        return
    conn.execute("DELETE FROM relationships WHERE person_id = ? OR related_person_id = ?", (alias_person_id, alias_person_id))
    for row in rows:
        person_id = canonical_person_id if row["person_id"] == alias_person_id else str(row["person_id"])
        related_person_id = canonical_person_id if row["related_person_id"] == alias_person_id else str(row["related_person_id"])
        if person_id == related_person_id:
            continue
        conn.execute(
            """
            INSERT INTO relationships(person_id, relation_type, related_person_id, value_json, fs_relationship_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(person_id, relation_type, related_person_id) DO UPDATE SET
                fs_relationship_id=COALESCE(relationships.fs_relationship_id, excluded.fs_relationship_id),
                updated_at=CASE
                    WHEN relationships.updated_at >= excluded.updated_at THEN relationships.updated_at
                    ELSE excluded.updated_at
                END
            """,
            (
                person_id,
                row["relation_type"],
                related_person_id,
                row["value_json"],
                row["fs_relationship_id"],
                row["updated_at"] or utc_now_iso(),
            ),
        )


def _merge_duplicate_pair_ignored_with_conn(conn: sqlite3.Connection, canonical_person_id: str, alias_person_id: str) -> None:
    rows = conn.execute(
        """
        SELECT person_id_a, person_id_b, reason, created_at
        FROM duplicate_pair_ignored
        WHERE person_id_a = ? OR person_id_b = ?
        """,
        (alias_person_id, alias_person_id),
    ).fetchall()
    if not rows:
        return
    conn.execute("DELETE FROM duplicate_pair_ignored WHERE person_id_a = ? OR person_id_b = ?", (alias_person_id, alias_person_id))
    for row in rows:
        person_id_a = canonical_person_id if row["person_id_a"] == alias_person_id else str(row["person_id_a"])
        person_id_b = canonical_person_id if row["person_id_b"] == alias_person_id else str(row["person_id_b"])
        if not person_id_a or not person_id_b or person_id_a == person_id_b:
            continue
        left, right = _pair_order(person_id_a, person_id_b)
        conn.execute(
            """
            INSERT INTO duplicate_pair_ignored(person_id_a, person_id_b, reason, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(person_id_a, person_id_b) DO UPDATE SET
                reason=COALESCE(duplicate_pair_ignored.reason, excluded.reason),
                created_at=CASE
                    WHEN duplicate_pair_ignored.created_at >= excluded.created_at THEN duplicate_pair_ignored.created_at
                    ELSE excluded.created_at
                END
            """,
            (left, right, row["reason"], row["created_at"] or utc_now_iso()),
        )


def _merge_dna_haplogroups_with_conn(conn: sqlite3.Connection, canonical_person_id: str, alias_person_id: str) -> None:
    canonical = conn.execute("SELECT * FROM dna_haplogroups WHERE person_id = ?", (canonical_person_id,)).fetchone()
    alias = conn.execute("SELECT * FROM dna_haplogroups WHERE person_id = ?", (alias_person_id,)).fetchone()
    if not alias:
        return
    conn.execute(
        """
        INSERT INTO dna_haplogroups(person_id, y_haplogroup, mt_haplogroup, y_timeline_json, mt_timeline_json, notes_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(person_id) DO UPDATE SET
            y_haplogroup=excluded.y_haplogroup,
            mt_haplogroup=excluded.mt_haplogroup,
            y_timeline_json=excluded.y_timeline_json,
            mt_timeline_json=excluded.mt_timeline_json,
            notes_json=excluded.notes_json,
            updated_at=excluded.updated_at
        """,
        (
            canonical_person_id,
            _merge_prefer_text(canonical["y_haplogroup"] if canonical else None, alias["y_haplogroup"]),
            _merge_prefer_text(canonical["mt_haplogroup"] if canonical else None, alias["mt_haplogroup"]),
            alias["y_timeline_json"] if alias["y_timeline_json"] not in (None, "[]") else (canonical["y_timeline_json"] if canonical else "[]"),
            alias["mt_timeline_json"] if alias["mt_timeline_json"] not in (None, "[]") else (canonical["mt_timeline_json"] if canonical else "[]"),
            alias["notes_json"] if alias["notes_json"] not in (None, "{}") else (canonical["notes_json"] if canonical else "{}"),
            _max_iso_text(canonical["updated_at"] if canonical else None, alias["updated_at"]) or utc_now_iso(),
        ),
    )
    conn.execute("DELETE FROM dna_haplogroups WHERE person_id = ?", (alias_person_id,))


def _merge_person_rows_with_conn(conn: sqlite3.Connection, canonical_person_id: str, alias_person_id: str) -> None:
    canonical = conn.execute("SELECT * FROM persons WHERE person_id = ?", (canonical_person_id,)).fetchone()
    alias = conn.execute("SELECT * FROM persons WHERE person_id = ?", (alias_person_id,)).fetchone()
    if not canonical or not alias:
        return
    conn.execute(
        """
        UPDATE persons
        SET name = ?,
            gender = ?,
            lifespan = ?,
            birth_date = ?,
            birth_place = ?,
            death_date = ?,
            death_place = ?,
            raw_json = ?,
            updated_at = ?
        WHERE person_id = ?
        """,
        (
            _merge_prefer_text(canonical["name"], alias["name"]),
            _merge_prefer_text(canonical["gender"], alias["gender"]),
            _merge_prefer_text(canonical["lifespan"], alias["lifespan"]),
            _merge_prefer_text(canonical["birth_date"], alias["birth_date"]),
            _merge_prefer_text(canonical["birth_place"], alias["birth_place"]),
            _merge_prefer_text(canonical["death_date"], alias["death_date"]),
            _merge_prefer_text(canonical["death_place"], alias["death_place"]),
            canonical["raw_json"] if len(str(canonical["raw_json"] or "")) >= len(str(alias["raw_json"] or "")) else alias["raw_json"],
            _max_iso_text(canonical["updated_at"], alias["updated_at"]) or utc_now_iso(),
            canonical_person_id,
        ),
    )


def _canonicalize_metadata_with_conn(conn: sqlite3.Connection, canonical_person_id: str, alias_person_id: str) -> None:
    metadata_rows = conn.execute("SELECT key, value FROM metadata").fetchall()
    for row in metadata_rows:
        value = str(row["value"] or "").strip()
        key = str(row["key"] or "").strip()
        if value == alias_person_id:
            conn.execute("UPDATE metadata SET value = ? WHERE key = ?", (canonical_person_id, key))
        if key == f"fsid:{alias_person_id}":
            conn.execute("DELETE FROM metadata WHERE key = ?", (key,))


def _merge_person_records_with_conn(
    conn: sqlite3.Connection,
    canonical_person_id: str,
    alias_person_ids: List[str],
    *,
    reason: str | None = None,
) -> int:
    merged = 0
    canonical_person_id = _resolve_person_alias_with_conn(conn, canonical_person_id) or canonical_person_id
    for alias_person_id in alias_person_ids:
        alias_person_id = _resolve_person_alias_with_conn(conn, alias_person_id) or alias_person_id
        if not alias_person_id or alias_person_id == canonical_person_id:
            continue
        canonical_exists = conn.execute("SELECT 1 FROM persons WHERE person_id = ?", (canonical_person_id,)).fetchone()
        alias_exists = conn.execute("SELECT 1 FROM persons WHERE person_id = ?", (alias_person_id,)).fetchone()
        if not canonical_exists or not alias_exists:
            continue
        _register_person_alias_with_conn(conn, alias_person_id, canonical_person_id, reason or "auto_merge")
        _merge_person_rows_with_conn(conn, canonical_person_id, alias_person_id)
        _merge_person_facts_with_conn(conn, canonical_person_id, alias_person_id)
        _merge_simple_person_table_with_conn(conn, "person_notes", ["note_key"], canonical_person_id, alias_person_id)
        _merge_simple_person_table_with_conn(conn, "person_sources", ["source_key"], canonical_person_id, alias_person_id)
        _merge_simple_person_table_with_conn(conn, "person_memories", ["memory_key"], canonical_person_id, alias_person_id)
        conn.execute("UPDATE media_items SET person_id = ? WHERE person_id = ?", (canonical_person_id, alias_person_id))
        conn.execute("UPDATE sync_queue SET person_id = ? WHERE person_id = ?", (canonical_person_id, alias_person_id))
        conn.execute("UPDATE sync_runs SET root_person_id = ? WHERE root_person_id = ?", (canonical_person_id, alias_person_id))
        conn.execute("UPDATE dna_raw_snps SET person_id = ? WHERE person_id = ?", (canonical_person_id, alias_person_id))
        conn.execute("UPDATE dna_ethnicity SET person_id = ? WHERE person_id = ?", (canonical_person_id, alias_person_id))
        conn.execute("UPDATE dna_segments SET person_id = ? WHERE person_id = ?", (canonical_person_id, alias_person_id))
        conn.execute("UPDATE dna_segments SET ancestor_person_id = ? WHERE ancestor_person_id = ?", (canonical_person_id, alias_person_id))
        conn.execute(
            """
            INSERT OR IGNORE INTO dna_matches(person_id, match_name, total_cm, segments_count, predicted_relationship, side, notes_json, updated_at)
            SELECT ?, match_name, total_cm, segments_count, predicted_relationship, side, notes_json, updated_at
            FROM dna_matches
            WHERE person_id = ?
            """,
            (canonical_person_id, alias_person_id),
        )
        conn.execute("DELETE FROM dna_matches WHERE person_id = ?", (alias_person_id,))
        _merge_dna_haplogroups_with_conn(conn, canonical_person_id, alias_person_id)
        _merge_person_sync_state_with_conn(conn, canonical_person_id, alias_person_id)
        _merge_relationships_with_conn(conn, canonical_person_id, alias_person_id)
        _merge_duplicate_pair_ignored_with_conn(conn, canonical_person_id, alias_person_id)
        _canonicalize_metadata_with_conn(conn, canonical_person_id, alias_person_id)
        conn.execute("DELETE FROM persons WHERE person_id = ?", (alias_person_id,))
        merged += 1
    return merged


def _auto_merge_duplicate_people_with_conn(conn: sqlite3.Connection) -> Dict[str, Any]:
    merged_pairs: List[Dict[str, str]] = []
    snapshots = _load_identity_snapshots_with_conn(conn)
    candidate_ids = sorted(snapshots)
    for left_id, right_id in combinations(candidate_ids, 2):
        left_id = _resolve_person_alias_with_conn(conn, left_id)
        right_id = _resolve_person_alias_with_conn(conn, right_id)
        if not left_id or not right_id or left_id == right_id:
            continue
        left_snapshot = snapshots.get(left_id)
        right_snapshot = snapshots.get(right_id)
        if not left_snapshot or not right_snapshot:
            continue
        if not _is_strong_duplicate_snapshot(left_snapshot, right_snapshot):
            continue
        canonical = _prefer_canonical_snapshot(left_snapshot, right_snapshot)
        alias = right_snapshot if canonical["person_id"] == left_snapshot["person_id"] else left_snapshot
        if _merge_person_records_with_conn(
            conn,
            canonical["person_id"],
            [alias["person_id"]],
            reason="auto_merge_strong_identity",
        ):
            merged_pairs.append({"canonical": canonical["person_id"], "alias": alias["person_id"]})
            snapshots = _load_identity_snapshots_with_conn(conn)
    return {"merged": len(merged_pairs), "pairs": merged_pairs}


def auto_merge_duplicate_people(db_path: str) -> Dict[str, Any]:
    conn = connect(db_path)
    try:
        conn.isolation_level = None  # autocommit mode for explicit transaction control
        conn.execute("BEGIN EXCLUSIVE")
        try:
            result = _auto_merge_duplicate_people_with_conn(conn)
            conn.execute("COMMIT")
            return result
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def _apply_migration_v5(conn: sqlite3.Connection) -> None:
    """Idempotent migration from schema v4 to v5. Adds dna_segments table."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dna_segments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            source TEXT NOT NULL,
            chromosome TEXT NOT NULL,
            start_pos INTEGER NOT NULL,
            end_pos INTEGER NOT NULL,
            centimorgans REAL,
            ancestry TEXT,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dna_segments_person_id ON dna_segments(person_id)")


def _apply_migration_v6(conn: sqlite3.Connection) -> None:
    """Idempotent migration from schema v5 to v6. Adds duplicate review ignore table."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS duplicate_pair_ignored (
            person_id_a TEXT NOT NULL,
            person_id_b TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (person_id_a, person_id_b),
            FOREIGN KEY (person_id_a) REFERENCES persons(person_id) ON DELETE CASCADE,
            FOREIGN KEY (person_id_b) REFERENCES persons(person_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_duplicate_pair_ignored_a ON duplicate_pair_ignored(person_id_a)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_duplicate_pair_ignored_b ON duplicate_pair_ignored(person_id_b)")


def _apply_migration_v7(conn: sqlite3.Connection) -> None:
    """Idempotent migration from schema v6 to v7. Adds historical events catalog."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS historical_events (
            event_key TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            start_year INTEGER NOT NULL,
            end_year INTEGER NOT NULL,
            source_url TEXT,
            match_terms_json TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_events_scope ON historical_events(scope)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_historical_events_years ON historical_events(start_year, end_year)")


def _apply_migration_v8(conn: sqlite3.Connection) -> None:
    """Idempotent migration from schema v7 to v8. Adds ADN module tables."""
    def _cols(table: str) -> set:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dna_raw_snps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            rsid TEXT NOT NULL,
            chromosome TEXT NOT NULL,
            position INTEGER NOT NULL,
            genotype TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dna_traits (
            rsid TEXT NOT NULL,
            category TEXT NOT NULL,
            trait_name TEXT NOT NULL,
            allele TEXT NOT NULL,
            effect TEXT NOT NULL,
            confidence TEXT,
            source_url TEXT,
            PRIMARY KEY (rsid, category, trait_name, allele)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dna_ethnicity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            region TEXT NOT NULL,
            percentage REAL NOT NULL,
            reference_panel TEXT,
            generation_estimate TEXT,
            side TEXT,
            color_hint TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dna_haplogroups (
            person_id TEXT PRIMARY KEY,
            y_haplogroup TEXT,
            mt_haplogroup TEXT,
            y_timeline_json TEXT NOT NULL DEFAULT '[]',
            mt_timeline_json TEXT NOT NULL DEFAULT '[]',
            notes_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dna_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            match_name TEXT NOT NULL,
            total_cm REAL,
            segments_count INTEGER,
            predicted_relationship TEXT,
            side TEXT,
            notes_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            UNIQUE(person_id, match_name),
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        )
        """
    )

    segment_cols = _cols("dna_segments")
    if "match_name" not in segment_cols:
        conn.execute("ALTER TABLE dna_segments ADD COLUMN match_name TEXT")
    if "branch_side" not in segment_cols:
        conn.execute("ALTER TABLE dna_segments ADD COLUMN branch_side TEXT")
    if "branch_label" not in segment_cols:
        conn.execute("ALTER TABLE dna_segments ADD COLUMN branch_label TEXT")
    if "ancestor_person_id" not in segment_cols:
        conn.execute("ALTER TABLE dna_segments ADD COLUMN ancestor_person_id TEXT")
    if "segment_kind" not in segment_cols:
        conn.execute("ALTER TABLE dna_segments ADD COLUMN segment_kind TEXT NOT NULL DEFAULT 'painter'")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_dna_raw_snps_person_id ON dna_raw_snps(person_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dna_raw_snps_rsid ON dna_raw_snps(rsid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dna_ethnicity_person_id ON dna_ethnicity(person_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dna_matches_person_id ON dna_matches(person_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dna_segments_match_name ON dna_segments(person_id, match_name)")


def _apply_migration_v9(conn: sqlite3.Connection) -> None:
    """Idempotent migration from schema v8 to v9. Adds person alias registry for canonical merges."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS person_aliases (
            alias_person_id TEXT PRIMARY KEY,
            canonical_person_id TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (canonical_person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_person_aliases_canonical ON person_aliases(canonical_person_id)")


def _seed_historical_events(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(*) AS qty FROM historical_events").fetchone()
    if row and int(row["qty"] or 0) > 0:
        return
    now = utc_now_iso()
    for item in HISTORICAL_EVENTS_SEED:
        conn.execute(
            """
            INSERT INTO historical_events(
                event_key, scope, title, description, start_year, end_year, source_url, match_terms_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                scope=excluded.scope,
                title=excluded.title,
                description=excluded.description,
                start_year=excluded.start_year,
                end_year=excluded.end_year,
                source_url=excluded.source_url,
                match_terms_json=excluded.match_terms_json,
                updated_at=excluded.updated_at
            """,
            (
                str(item["event_key"]),
                str(item["scope"]),
                str(item["title"]),
                str(item.get("description") or ""),
                int(item["start_year"]),
                int(item["end_year"]),
                str(item.get("source_url") or ""),
                _json_dump(item.get("match_terms") or []),
                now,
            ),
        )


def _seed_dna_traits(conn: sqlite3.Connection) -> None:
    now_catalog = conn.execute("SELECT COUNT(*) AS qty FROM dna_traits").fetchone()
    if now_catalog and int(now_catalog["qty"] or 0) > 0:
        return
    for item in DNA_TRAITS_SEED:
        conn.execute(
            """
            INSERT INTO dna_traits(rsid, category, trait_name, allele, effect, confidence, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rsid, category, trait_name, allele) DO UPDATE SET
                effect=excluded.effect,
                confidence=excluded.confidence,
                source_url=excluded.source_url
            """,
            (
                str(item["rsid"]),
                str(item["category"]),
                str(item["trait_name"]),
                str(item["allele"]),
                str(item["effect"]),
                str(item.get("confidence") or ""),
                str(item.get("source_url") or ""),
            ),
        )


def _apply_migration_v4(conn: sqlite3.Connection) -> None:
    """Idempotent migration from schema v3 to v4. Safe to run on existing databases."""
    def _cols(table: str) -> set:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    media_cols = _cols("media_items")
    if "memory_key" not in media_cols:
        conn.execute("ALTER TABLE media_items ADD COLUMN memory_key TEXT")
    if "source_key" not in media_cols:
        conn.execute("ALTER TABLE media_items ADD COLUMN source_key TEXT")

    pss_cols = _cols("person_sync_state")
    if "last_relationships_at" not in pss_cols:
        conn.execute("ALTER TABLE person_sync_state ADD COLUMN last_relationships_at TEXT")

    rel_cols = _cols("relationships")
    if "fs_relationship_id" not in rel_cols:
        conn.execute("ALTER TABLE relationships ADD COLUMN fs_relationship_id TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_person_facts_person_id ON person_facts(person_id)")


def _conn_row_factory(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    return conn


def connect(db_path: str) -> sqlite3.Connection:
    db_dir = os.path.dirname(os.path.abspath(db_path)) or os.getcwd()
    os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn = _conn_row_factory(conn)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            root_person_id TEXT NOT NULL,
            generations INTEGER NOT NULL,
            base_url TEXT NOT NULL,
            last_error TEXT,
            persons_count INTEGER NOT NULL DEFAULT 0,
            relationships_count INTEGER NOT NULL DEFAULT 0,
            notes_count INTEGER NOT NULL DEFAULT 0,
            sources_count INTEGER NOT NULL DEFAULT 0,
            memories_count INTEGER NOT NULL DEFAULT 0,
            media_count INTEGER NOT NULL DEFAULT 0,
            jobs_done INTEGER NOT NULL DEFAULT 0,
            jobs_failed INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS sync_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            job_type TEXT NOT NULL,
            person_id TEXT,
            remote_url TEXT,
            person_key TEXT NOT NULL DEFAULT '',
            remote_key TEXT NOT NULL DEFAULT '',
            payload_json TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            priority INTEGER NOT NULL DEFAULT 100,
            attempts INTEGER NOT NULL DEFAULT 0,
            available_at TEXT NOT NULL,
            claimed_at TEXT,
            completed_at TEXT,
            last_error TEXT,
            UNIQUE(run_id, job_type, person_key, remote_key),
            FOREIGN KEY (run_id) REFERENCES sync_runs(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS person_sync_state (
            person_id TEXT PRIMARY KEY,
            last_fetched_at TEXT,
            last_notes_at TEXT,
            last_sources_at TEXT,
            last_memories_at TEXT,
            last_portrait_at TEXT,
            last_relationships_at TEXT,
            last_run_id INTEGER,
            FOREIGN KEY (last_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS persons (
            person_id TEXT PRIMARY KEY,
            name TEXT,
            gender TEXT,
            lifespan TEXT,
            birth_date TEXT,
            birth_place TEXT,
            death_date TEXT,
            death_place TEXT,
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS person_facts (
            person_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            fact_type TEXT,
            date_original TEXT,
            place_original TEXT,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (person_id, seq),
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS relationships (
            person_id TEXT NOT NULL,
            relation_type TEXT NOT NULL,
            related_person_id TEXT NOT NULL,
            value_json TEXT NOT NULL,
            fs_relationship_id TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (person_id, relation_type, related_person_id),
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS person_notes (
            person_id TEXT NOT NULL,
            note_key TEXT NOT NULL,
            subject TEXT,
            text_value TEXT,
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (person_id, note_key),
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS person_sources (
            person_id TEXT NOT NULL,
            source_key TEXT NOT NULL,
            title TEXT,
            citation TEXT,
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (person_id, source_key),
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS person_memories (
            person_id TEXT NOT NULL,
            memory_key TEXT NOT NULL,
            memory_type TEXT,
            title TEXT,
            description TEXT,
            text_value TEXT,
            raw_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (person_id, memory_key),
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS media_items (
            media_key TEXT PRIMARY KEY,
            person_id TEXT NOT NULL,
            media_role TEXT NOT NULL,
            title TEXT,
            remote_url TEXT,
            local_path TEXT,
            mime_type TEXT,
            bytes_size INTEGER,
            sha256 TEXT,
            status TEXT NOT NULL,
            raw_json TEXT,
            memory_key TEXT,
            source_key TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS dna_segments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            source TEXT NOT NULL,
            chromosome TEXT NOT NULL,
            start_pos INTEGER NOT NULL,
            end_pos INTEGER NOT NULL,
            centimorgans REAL,
            ancestry TEXT,
            match_name TEXT,
            branch_side TEXT,
            branch_label TEXT,
            ancestor_person_id TEXT,
            segment_kind TEXT NOT NULL DEFAULT 'painter',
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id)
        );

        CREATE TABLE IF NOT EXISTS dna_raw_snps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            rsid TEXT NOT NULL,
            chromosome TEXT NOT NULL,
            position INTEGER NOT NULL,
            genotype TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS dna_traits (
            rsid TEXT NOT NULL,
            category TEXT NOT NULL,
            trait_name TEXT NOT NULL,
            allele TEXT NOT NULL,
            effect TEXT NOT NULL,
            confidence TEXT,
            source_url TEXT,
            PRIMARY KEY (rsid, category, trait_name, allele)
        );

        CREATE TABLE IF NOT EXISTS dna_ethnicity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            region TEXT NOT NULL,
            percentage REAL NOT NULL,
            reference_panel TEXT,
            generation_estimate TEXT,
            side TEXT,
            color_hint TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS dna_haplogroups (
            person_id TEXT PRIMARY KEY,
            y_haplogroup TEXT,
            mt_haplogroup TEXT,
            y_timeline_json TEXT NOT NULL DEFAULT '[]',
            mt_timeline_json TEXT NOT NULL DEFAULT '[]',
            notes_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS dna_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            match_name TEXT NOT NULL,
            total_cm REAL,
            segments_count INTEGER,
            predicted_relationship TEXT,
            side TEXT,
            notes_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL,
            UNIQUE(person_id, match_name),
            FOREIGN KEY (person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS duplicate_pair_ignored (
            person_id_a TEXT NOT NULL,
            person_id_b TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (person_id_a, person_id_b),
            FOREIGN KEY (person_id_a) REFERENCES persons(person_id) ON DELETE CASCADE,
            FOREIGN KEY (person_id_b) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS person_aliases (
            alias_person_id TEXT PRIMARY KEY,
            canonical_person_id TEXT NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (canonical_person_id) REFERENCES persons(person_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS historical_events (
            event_key TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            start_year INTEGER NOT NULL,
            end_year INTEGER NOT NULL,
            source_url TEXT,
            match_terms_json TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_relationships_person_id ON relationships(person_id);
        CREATE INDEX IF NOT EXISTS idx_media_items_person_id ON media_items(person_id);
        CREATE INDEX IF NOT EXISTS idx_sync_queue_run_status_priority ON sync_queue(run_id, status, priority, id);
        CREATE INDEX IF NOT EXISTS idx_sync_queue_available_at ON sync_queue(available_at);
        CREATE INDEX IF NOT EXISTS idx_person_facts_person_id ON person_facts(person_id);
        CREATE INDEX IF NOT EXISTS idx_dna_segments_person_id ON dna_segments(person_id);
        CREATE INDEX IF NOT EXISTS idx_dna_raw_snps_person_id ON dna_raw_snps(person_id);
        CREATE INDEX IF NOT EXISTS idx_dna_raw_snps_rsid ON dna_raw_snps(rsid);
        CREATE INDEX IF NOT EXISTS idx_dna_ethnicity_person_id ON dna_ethnicity(person_id);
        CREATE INDEX IF NOT EXISTS idx_dna_matches_person_id ON dna_matches(person_id);
        CREATE INDEX IF NOT EXISTS idx_duplicate_pair_ignored_a ON duplicate_pair_ignored(person_id_a);
        CREATE INDEX IF NOT EXISTS idx_duplicate_pair_ignored_b ON duplicate_pair_ignored(person_id_b);
        CREATE INDEX IF NOT EXISTS idx_person_aliases_canonical ON person_aliases(canonical_person_id);
        CREATE INDEX IF NOT EXISTS idx_historical_events_scope ON historical_events(scope);
        CREATE INDEX IF NOT EXISTS idx_historical_events_years ON historical_events(start_year, end_year);
        """
    )
    # Migrate existing databases that are below current schema version
    _schema_row = conn.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
    if _schema_row is not None:
        _current_version = int(_schema_row[0]) if _schema_row[0] else 0
        if _current_version < 4:
            _apply_migration_v4(conn)
        if _current_version < 5:
            _apply_migration_v5(conn)
        if _current_version < 6:
            _apply_migration_v6(conn)
        if _current_version < 7:
            _apply_migration_v7(conn)
        if _current_version < 8:
            _apply_migration_v8(conn)
        if _current_version < 9:
            _apply_migration_v9(conn)
    conn.execute(
        """
        INSERT INTO metadata(key, value) VALUES('schema_version', ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (str(SCHEMA_VERSION),),
    )
    for key, value in DEFAULT_SYNC_CONFIG.items():
        conn.execute(
            """
            INSERT INTO metadata(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO NOTHING
            """,
            (key, str(value)),
        )
    segment_cols = {row[1] for row in conn.execute("PRAGMA table_info(dna_segments)").fetchall()}
    if "match_name" in segment_cols:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dna_segments_match_name ON dna_segments(person_id, match_name)")
    _seed_historical_events(conn)
    _seed_dna_traits(conn)
    conn.commit()


def get_metadata(db_path: str, key: str) -> Optional[str]:
    conn = connect(db_path)
    row = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    conn.close()
    return str(row["value"]) if row else None


def set_metadata(db_path: str, key: str, value: str) -> None:
    conn = connect(db_path)
    with conn:
        conn.execute(
            """
            INSERT INTO metadata(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
    conn.close()


def get_sync_config(db_path: str) -> Dict[str, int]:
    conn = connect(db_path)
    rows = conn.execute(
        """
        SELECT key, value
        FROM metadata
        WHERE key IN (?, ?, ?, ?, ?, ?, ?)
        """,
        tuple(DEFAULT_SYNC_CONFIG.keys()),
    ).fetchall()
    conn.close()
    out = dict(DEFAULT_SYNC_CONFIG)
    for row in rows:
        try:
            out[str(row["key"])] = int(row["value"])
        except (TypeError, ValueError):
            continue
    return out


def save_sync_config(db_path: str, config: Dict[str, int]) -> None:
    conn = connect(db_path)
    with conn:
        for key, value in config.items():
            conn.execute(
                """
                INSERT INTO metadata(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (key, str(value)),
            )
    conn.close()


def start_or_resume_run(*, db_path: str, root_person_id: str, generations: int, base_url: str) -> int:
    conn = connect(db_path)
    now = utc_now_iso()
    row = conn.execute(
        """
        SELECT id FROM sync_runs
        WHERE root_person_id = ? AND generations = ? AND base_url = ? AND status = 'running'
        ORDER BY id DESC
        LIMIT 1
        """,
        (root_person_id, generations, base_url),
    ).fetchone()
    if row:
        run_id = int(row["id"])
        _requeue_stale_jobs(conn, run_id)
        conn.close()
        return run_id
    with conn:
        run_id = conn.execute(
            """
            INSERT INTO sync_runs(started_at, root_person_id, generations, base_url, status)
            VALUES (?, ?, ?, ?, 'running')
            """,
            (now, root_person_id, generations, base_url),
        ).lastrowid
    conn.close()
    return int(run_id)


def get_person_sync_state(db_path: str, person_id: str) -> Dict[str, Optional[str]]:
    conn = connect(db_path)
    state = get_person_sync_state_with_conn(conn, person_id)
    conn.close()
    return state


def get_person_sync_state_with_conn(conn: sqlite3.Connection, person_id: str) -> Dict[str, Optional[str]]:
    row = conn.execute(
        """
        SELECT *
        FROM person_sync_state
        WHERE person_id = ?
        """,
        (person_id,),
    ).fetchone()
    if not row:
        return {}
    return dict(row)


def _parse_iso(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def is_person_phase_stale(
    db_path: str,
    person_id: str,
    phase_field: str,
    stale_hours: int,
) -> bool:
    conn = connect(db_path)
    state = get_person_sync_state_with_conn(conn, person_id)
    conn.close()
    return is_phase_stale_from_state(state, phase_field, stale_hours)


def is_phase_stale_from_state(
    state: Dict[str, Optional[str]],
    phase_field: str,
    stale_hours: int,
) -> bool:
    last_value = _parse_iso(state.get(phase_field))
    if not last_value:
        return True
    now = dt.datetime.now(dt.timezone.utc)
    if last_value.tzinfo is None:
        last_value = last_value.replace(tzinfo=dt.timezone.utc)
    return (now - last_value) >= dt.timedelta(hours=stale_hours)


def _requeue_stale_jobs(conn: sqlite3.Connection, run_id: int) -> None:
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=JOB_STALE_MINUTES)).replace(microsecond=0).isoformat()
    with conn:
        conn.execute(
            """
            UPDATE sync_queue
            SET status = 'pending', claimed_at = NULL
            WHERE run_id = ? AND status = 'in_progress' AND claimed_at IS NOT NULL AND claimed_at < ?
            """,
            (run_id, cutoff),
        )


def enqueue_job(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    job_type: str,
    person_id: Optional[str] = None,
    remote_url: Optional[str] = None,
    payload: Optional[Dict] = None,
    priority: int = 100,
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO sync_queue(
            run_id, job_type, person_id, remote_url, person_key, remote_key, payload_json, priority, available_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, job_type, person_key, remote_key) DO NOTHING
        """,
        (
            run_id,
            job_type,
            person_id,
            remote_url,
            person_id or "",
            remote_url or "",
            _json_dump(payload) if payload else None,
            priority,
            now,
        ),
    )


def claim_next_job(db_path: str, run_id: int) -> Optional[Dict]:
    conn = connect(db_path)
    _requeue_stale_jobs(conn, run_id)
    now = utc_now_iso()
    row = None
    with conn:
        row = conn.execute(
            """
            SELECT * FROM sync_queue
            WHERE run_id = ? AND status = 'pending' AND available_at <= ?
            ORDER BY priority ASC, id ASC
            LIMIT 1
            """,
            (run_id, now),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE sync_queue
                SET status = 'in_progress', claimed_at = ?
                WHERE id = ?
                """,
                (now, int(row["id"])),
            )
    if not row:
        conn.close()
        return None
    job = dict(row)
    if job.get("payload_json"):
        job["payload"] = json.loads(job["payload_json"])
    else:
        job["payload"] = None
    conn.close()
    return job


def complete_job(db_path: str, job_id: int) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    with conn:
        conn.execute(
            """
            UPDATE sync_queue
            SET status = 'done', completed_at = ?, last_error = NULL
            WHERE id = ?
            """,
            (now, job_id),
        )
    conn.close()


def fail_job(
    db_path: str,
    job_id: int,
    error: str,
    *,
    retryable: bool = True,
    max_retries: int = 3,
    retry_delay_minutes: int = RETRY_DELAY_MINUTES,
) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    row = conn.execute("SELECT attempts FROM sync_queue WHERE id = ?", (job_id,)).fetchone()
    attempts = int(row["attempts"]) if row else 0
    should_retry = retryable and attempts < max_retries
    next_time = (
        (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=retry_delay_minutes)).replace(microsecond=0).isoformat()
        if should_retry
        else now
    )
    new_status = "pending" if should_retry else "failed"
    with conn:
        conn.execute(
            """
            UPDATE sync_queue
            SET status = ?, attempts = attempts + 1, last_error = ?, available_at = ?, claimed_at = NULL
            WHERE id = ?
            """,
            (new_status, error[:2000], next_time, job_id),
        )
    conn.close()


def run_queue_stats(db_path: str, run_id: int) -> Dict[str, int]:
    conn = connect(db_path)
    rows = conn.execute(
        """
        SELECT status, COUNT(*) AS qty
        FROM sync_queue
        WHERE run_id = ?
        GROUP BY status
        """,
        (run_id,),
    ).fetchall()
    conn.close()
    out = {"pending": 0, "in_progress": 0, "done": 0, "failed": 0}
    for row in rows:
        out[str(row["status"])] = int(row["qty"])
    return out


def finalize_run(db_path: str, run_id: int, status: str, last_error: Optional[str] = None) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    stats = run_queue_stats(db_path, run_id)
    finished_at = None if status == "running" else now
    with conn:
        conn.execute(
            """
            UPDATE sync_runs
            SET finished_at = ?, status = ?, last_error = ?, jobs_done = ?, jobs_failed = ?
            WHERE id = ?
            """,
            (
                finished_at,
                status,
                last_error[:2000] if last_error else None,
                stats.get("done", 0),
                stats.get("failed", 0),
                run_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO metadata(key, value) VALUES('last_sync_run_id', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(run_id),),
        )
        conn.execute(
            """
            INSERT INTO metadata(key, value) VALUES('last_sync_at', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (now,),
        )
    conn.close()


def persist_person(db_path: str, person_id: str, person: Dict, run_id: int) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    display = person.get("display", {}) or {}
    with conn:
        resolved_person_id = _resolve_person_alias_with_conn(conn, person_id) or person_id
        duplicate_person_id = _find_profile_duplicate_with_conn(
            conn,
            resolved_person_id,
            name=display.get("name"),
            birth_date=display.get("birthDate"),
            birth_place=display.get("birthPlace"),
            death_date=display.get("deathDate"),
            is_stub=False,
        )
        merge_existing_alias: str | None = None
        if duplicate_person_id and duplicate_person_id != resolved_person_id:
            incoming_snapshot = {
                "person_id": resolved_person_id,
                "is_stub": False,
                "is_fsid": _is_probable_fsid(resolved_person_id),
                "is_uuid": _is_probable_uuid(resolved_person_id),
                "detail_score": 45 if _is_probable_fsid(resolved_person_id) else 20,
            }
            duplicate_snapshot = {
                "person_id": duplicate_person_id,
                "is_stub": False,
                "is_fsid": _is_probable_fsid(duplicate_person_id),
                "is_uuid": _is_probable_uuid(duplicate_person_id),
                "detail_score": 30 if _is_probable_fsid(duplicate_person_id) else 10,
            }
            preferred = _prefer_canonical_snapshot(incoming_snapshot, duplicate_snapshot)
            if preferred["person_id"] == resolved_person_id:
                merge_existing_alias = duplicate_person_id
            else:
                _register_person_alias_with_conn(conn, resolved_person_id, duplicate_person_id, "auto_profile_match")
                resolved_person_id = duplicate_person_id
        conn.execute(
            """
            INSERT INTO persons(
                person_id, name, gender, lifespan, birth_date, birth_place,
                death_date, death_place, raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                name=excluded.name,
                gender=excluded.gender,
                lifespan=excluded.lifespan,
                birth_date=excluded.birth_date,
                birth_place=excluded.birth_place,
                death_date=excluded.death_date,
                death_place=excluded.death_place,
                raw_json=excluded.raw_json,
                updated_at=excluded.updated_at
            """,
            (
                resolved_person_id,
                display.get("name"),
                display.get("gender"),
                display.get("lifespan"),
                display.get("birthDate"),
                display.get("birthPlace"),
                display.get("deathDate"),
                display.get("deathPlace"),
                _json_dump(person),
                now,
            ),
        )
        conn.execute("DELETE FROM person_facts WHERE person_id = ?", (resolved_person_id,))
        for seq, fact in enumerate(person.get("facts", []) or []):
            date_value = ((fact.get("date") or {}).get("original")) if isinstance(fact, dict) else None
            place_value = ((fact.get("place") or {}).get("original")) if isinstance(fact, dict) else None
            conn.execute(
                """
                INSERT INTO person_facts(
                    person_id, seq, fact_type, date_original, place_original, value_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    resolved_person_id,
                    seq,
                    fact.get("type") if isinstance(fact, dict) else None,
                    date_value,
                    place_value,
                    _json_dump(fact),
                    now,
                ),
            )
        conn.execute(
            """
            INSERT INTO person_sync_state(person_id, last_fetched_at, last_run_id)
            VALUES (?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                last_fetched_at=excluded.last_fetched_at,
                last_run_id=excluded.last_run_id
            """,
            (resolved_person_id, now, run_id),
        )
        if merge_existing_alias:
            _merge_person_records_with_conn(conn, resolved_person_id, [merge_existing_alias], reason="auto_profile_match")
    conn.close()


def _persist_person_stubs_with_conn(conn: sqlite3.Connection, persons: List[Dict]) -> None:
    now = utc_now_iso()
    for person in persons:
        if not isinstance(person, dict):
            continue
        person_id = str(person.get("id") or "").strip()
        if not person_id:
            continue
        display = person.get("display", {}) or {}
        resolved_person_id = _resolve_person_alias_with_conn(conn, person_id) or person_id
        duplicate_person_id = _find_profile_duplicate_with_conn(
            conn,
            resolved_person_id,
            name=display.get("name"),
            birth_date=display.get("birthDate"),
            birth_place=display.get("birthPlace"),
            death_date=display.get("deathDate"),
            is_stub=True,
        )
        merge_existing_alias: str | None = None
        if duplicate_person_id and duplicate_person_id != resolved_person_id:
            incoming_snapshot = {
                "person_id": resolved_person_id,
                "is_stub": True,
                "is_fsid": _is_probable_fsid(resolved_person_id),
                "is_uuid": _is_probable_uuid(resolved_person_id),
                "detail_score": 25 if _is_probable_fsid(resolved_person_id) else 5,
            }
            duplicate_snapshot = {
                "person_id": duplicate_person_id,
                "is_stub": False,
                "is_fsid": _is_probable_fsid(duplicate_person_id),
                "is_uuid": _is_probable_uuid(duplicate_person_id),
                "detail_score": 30 if _is_probable_fsid(duplicate_person_id) else 10,
            }
            preferred = _prefer_canonical_snapshot(incoming_snapshot, duplicate_snapshot)
            if preferred["person_id"] == resolved_person_id:
                merge_existing_alias = duplicate_person_id
            else:
                _register_person_alias_with_conn(conn, resolved_person_id, duplicate_person_id, "auto_stub_match")
                resolved_person_id = duplicate_person_id
        conn.execute(
            """
            INSERT INTO persons(
                person_id, name, gender, lifespan, birth_date, birth_place,
                death_date, death_place, raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                name=COALESCE(persons.name, excluded.name),
                gender=COALESCE(persons.gender, excluded.gender),
                lifespan=COALESCE(persons.lifespan, excluded.lifespan),
                birth_date=COALESCE(persons.birth_date, excluded.birth_date),
                birth_place=COALESCE(persons.birth_place, excluded.birth_place),
                death_date=COALESCE(persons.death_date, excluded.death_date),
                death_place=COALESCE(persons.death_place, excluded.death_place),
                raw_json=CASE
                    WHEN persons.raw_json IS NULL OR persons.raw_json = '' THEN excluded.raw_json
                    ELSE persons.raw_json
                END,
                updated_at=persons.updated_at
            """,
            (
                resolved_person_id,
                display.get("name"),
                display.get("gender"),
                display.get("lifespan"),
                display.get("birthDate"),
                display.get("birthPlace"),
                display.get("deathDate"),
                display.get("deathPlace"),
                _json_dump(person),
                now,
            ),
        )
        if merge_existing_alias:
            _merge_person_records_with_conn(conn, resolved_person_id, [merge_existing_alias], reason="auto_stub_match")


def _build_fs_rel_id_lookup(person_id: str, raw_payload: Optional[Dict]) -> Dict:
    """Build {(related_person_id, relation_type) → fs_relationship_id} from raw API payload."""
    lookup: Dict = {}
    if not raw_payload:
        return lookup
    for rel in raw_payload.get("childAndParentsRelationships", []):
        rel_id = rel.get("id")
        if not rel_id:
            continue
        child_id = (rel.get("child") or {}).get("resourceId")
        father_id = (rel.get("father") or rel.get("parent1") or {}).get("resourceId")
        mother_id = (rel.get("mother") or rel.get("parent2") or {}).get("resourceId")
        if child_id == person_id:
            if father_id:
                lookup[(father_id, "father")] = rel_id
            if mother_id:
                lookup[(mother_id, "mother")] = rel_id
        if father_id == person_id and child_id:
            lookup[(child_id, "children")] = rel_id
        if mother_id == person_id and child_id:
            lookup[(child_id, "children")] = rel_id
    for rel in raw_payload.get("coupleRelationships", []):
        rel_id = rel.get("id")
        if not rel_id:
            continue
        p1 = (rel.get("person1") or {}).get("resourceId")
        p2 = (rel.get("person2") or {}).get("resourceId")
        if p1 == person_id and p2:
            lookup[(p2, "spouses")] = rel_id
        if p2 == person_id and p1:
            lookup[(p1, "spouses")] = rel_id
    for rel in raw_payload.get("relationships", []):
        rel_id = rel.get("id")
        if not rel_id:
            continue
        p1 = (rel.get("person1") or {}).get("resourceId")
        p2 = (rel.get("person2") or {}).get("resourceId")
        rel_type = (rel.get("type") or "").lower()
        if rel_type.endswith("parentchild"):
            if p1 == person_id and p2:
                lookup[(p2, "children")] = rel_id
        else:
            if p1 == person_id and p2:
                lookup[(p2, "spouses")] = rel_id
            if p2 == person_id and p1:
                lookup[(p1, "spouses")] = rel_id
    return lookup


def persist_relationships(db_path: str, person_id: str, relationships: Dict, run_id: int, *, raw_payload: Optional[Dict] = None) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    resolved_person_id = person_id
    with conn:
        resolved_person_id = _resolve_person_alias_with_conn(conn, person_id) or person_id
        fs_id_lookup = _build_fs_rel_id_lookup(person_id, raw_payload)
        if raw_payload:
            _persist_person_stubs_with_conn(conn, list(raw_payload.get("persons", []) or []))
        conn.execute("DELETE FROM relationships WHERE person_id = ?", (resolved_person_id,))
        for relation_type, related in relationships.items():
            if isinstance(related, list):
                related_ids = [value for value in related if isinstance(value, str) and value]
            elif isinstance(related, str):
                related_ids = [related]
            else:
                related_ids = []
            seen_related: set[str] = set()
            for related_person_id in related_ids:
                resolved_related_id = _resolve_person_alias_with_conn(conn, related_person_id) or related_person_id
                if resolved_related_id in seen_related or resolved_related_id == resolved_person_id:
                    continue
                seen_related.add(resolved_related_id)
                fs_rel_id = fs_id_lookup.get((related_person_id, relation_type))
                conn.execute(
                    """
                    INSERT INTO relationships(
                        person_id, relation_type, related_person_id, value_json, fs_relationship_id, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (resolved_person_id, relation_type, resolved_related_id, _json_dump(relationships), fs_rel_id, now),
                )
        conn.execute(
            """
            INSERT INTO person_sync_state(person_id, last_relationships_at, last_run_id)
            VALUES (?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                last_relationships_at=excluded.last_relationships_at,
                last_run_id=excluded.last_run_id
            """,
            (resolved_person_id, now, run_id),
        )
        if raw_payload:
            _auto_merge_duplicate_people_with_conn(conn)
    conn.close()


def persist_notes(db_path: str, person_id: str, payload: Dict, run_id: int) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    notes = list(payload.get("notes", []) or [])
    for person in payload.get("persons", []) or []:
        notes.extend(person.get("notes", []) or [])
    with conn:
        conn.execute("DELETE FROM person_notes WHERE person_id = ?", (person_id,))
        for idx, note in enumerate(notes):
            note_key = str(note.get("id") or note.get("links", {}).get("self", {}).get("href") or idx)
            conn.execute(
                """
                INSERT INTO person_notes(person_id, note_key, subject, text_value, raw_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (person_id, note_key, note.get("subject"), note.get("text") or note.get("value"), _json_dump(note), now),
            )
        conn.execute(
            """
            INSERT INTO person_sync_state(person_id, last_notes_at, last_run_id)
            VALUES (?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                last_notes_at=excluded.last_notes_at,
                last_run_id=excluded.last_run_id
            """,
            (person_id, now, run_id),
        )
    conn.close()


def persist_sources(db_path: str, person_id: str, payload: Dict, run_id: int) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    with conn:
        conn.execute("DELETE FROM person_sources WHERE person_id = ?", (person_id,))
        for idx, source in enumerate(payload.get("sources", []) or []):
            title = source.get("titles", [{}])[0].get("value") if source.get("titles") else None
            citation = source.get("citations", [{}])[0].get("value") if source.get("citations") else None
            source_key = str(source.get("id") or source.get("descriptionId") or idx)
            conn.execute(
                """
                INSERT INTO person_sources(person_id, source_key, title, citation, raw_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (person_id, source_key, title, citation, _json_dump(source), now),
            )
        for idx, desc in enumerate(payload.get("sourceDescriptions", []) or []):
            title = desc.get("titles", [{}])[0].get("value") if desc.get("titles") else None
            source_key = f"desc:{desc.get('id') or idx}"
            conn.execute(
                """
                INSERT INTO person_sources(person_id, source_key, title, citation, raw_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (person_id, source_key, title, None, _json_dump(desc), now),
            )
        conn.execute(
            """
            INSERT INTO person_sync_state(person_id, last_sources_at, last_run_id)
            VALUES (?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                last_sources_at=excluded.last_sources_at,
                last_run_id=excluded.last_run_id
            """,
            (person_id, now, run_id),
        )
    conn.close()


def persist_memories(db_path: str, person_id: str, payloads: List[Dict], run_id: int) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    with conn:
        conn.execute("DELETE FROM person_memories WHERE person_id = ?", (person_id,))
        for payload_index, payload in enumerate(payloads):
            for idx, desc in enumerate(payload.get("sourceDescriptions", []) or []):
                memory_key = str(desc.get("id") or f"{payload_index}:sourceDescription:{idx}")
                title = None
                for title_obj in desc.get("titles", []) or []:
                    if title_obj.get("value"):
                        title = title_obj.get("value")
                        break
                conn.execute(
                    """
                    INSERT INTO person_memories(
                        person_id, memory_key, memory_type, title, description,
                        text_value, raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        person_id,
                        memory_key,
                        "sourceDescription",
                        title,
                        desc.get("about"),
                        None,
                        _json_dump(desc),
                        now,
                    ),
                )
            for key in ("memories", "stories", "documents"):
                for idx, item in enumerate(payload.get(key, []) or []):
                    memory_key = str(item.get("id") or item.get("links", {}).get("self", {}).get("href") or f"{payload_index}:{key}:{idx}")
                    conn.execute(
                        """
                        INSERT INTO person_memories(
                            person_id, memory_key, memory_type, title, description,
                            text_value, raw_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            person_id,
                            memory_key,
                            key[:-1] if key.endswith("s") else key,
                            item.get("title"),
                            item.get("description"),
                            item.get("text") or item.get("value"),
                            _json_dump(item),
                            now,
                        ),
                    )
        conn.execute(
            """
            INSERT INTO person_sync_state(person_id, last_memories_at, last_run_id)
            VALUES (?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                last_memories_at=excluded.last_memories_at,
                last_run_id=excluded.last_run_id
            """,
            (person_id, now, run_id),
        )
    conn.close()


def persist_media_item(
    db_path: str,
    person_id: str,
    media_key: str,
    media_role: str,
    media: Dict,
    run_id: int,
    *,
    memory_key: Optional[str] = None,
    source_key: Optional[str] = None,
) -> None:
    conn = connect(db_path)
    now = utc_now_iso()
    local_path = media.get("local_path")
    bytes_size = os.path.getsize(local_path) if local_path and os.path.isfile(local_path) else None
    with conn:
        conn.execute(
            """
            INSERT INTO media_items(
                media_key, person_id, media_role, title, remote_url, local_path,
                mime_type, bytes_size, sha256, status, raw_json, memory_key, source_key, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(media_key) DO UPDATE SET
                remote_url=excluded.remote_url,
                local_path=excluded.local_path,
                mime_type=excluded.mime_type,
                bytes_size=excluded.bytes_size,
                sha256=excluded.sha256,
                status=excluded.status,
                raw_json=excluded.raw_json,
                memory_key=excluded.memory_key,
                source_key=excluded.source_key,
                updated_at=excluded.updated_at
            """,
            (
                media_key,
                person_id,
                media_role,
                media.get("title"),
                media.get("remote_url"),
                local_path,
                media.get("mime_type"),
                bytes_size,
                _hash_file(local_path),
                media.get("status") or "downloaded",
                _json_dump(media),
                memory_key,
                source_key,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO person_sync_state(person_id, last_portrait_at, last_run_id)
            VALUES (?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                last_portrait_at=COALESCE(excluded.last_portrait_at, person_sync_state.last_portrait_at),
                last_run_id=excluded.last_run_id
            """,
            (person_id, now if media_role == "portrait" else None, run_id),
        )
    conn.close()


def refresh_run_counts(db_path: str, run_id: int) -> None:
    conn = connect(db_path)
    def _scalar_int(query: str, params: tuple[object, ...] = ()) -> int:
        row = conn.execute(query, params).fetchone()
        if not row:
            return 0
        value = row[0]
        return int(value) if value is not None else 0

    counts = {
        "persons_count": _scalar_int("SELECT COUNT(*) FROM persons"),
        "relationships_count": _scalar_int("SELECT COUNT(*) FROM relationships"),
        "notes_count": _scalar_int("SELECT COUNT(*) FROM person_notes"),
        "sources_count": _scalar_int("SELECT COUNT(*) FROM person_sources"),
        "memories_count": _scalar_int("SELECT COUNT(*) FROM person_memories"),
        "media_count": _scalar_int("SELECT COUNT(*) FROM media_items"),
    }
    stats = run_queue_stats(db_path, run_id)
    with conn:
        conn.execute(
            """
            UPDATE sync_runs
            SET persons_count = ?, relationships_count = ?, notes_count = ?, sources_count = ?,
                memories_count = ?, media_count = ?, jobs_done = ?, jobs_failed = ?
            WHERE id = ?
            """,
            (
                counts["persons_count"],
                counts["relationships_count"],
                counts["notes_count"],
                counts["sources_count"],
                counts["memories_count"],
                counts["media_count"],
                stats.get("done", 0),
                stats.get("failed", 0),
                run_id,
            ),
        )
    conn.close()


def print_mirror_status(db_path: str) -> None:
    """Print a health report of the local mirror without making any API calls."""
    if not os.path.isfile(db_path):
        print(f"DB not found: {db_path}")
        return

    db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
    conn = connect(db_path)
    def _scalar_int(query: str, params: tuple[object, ...] = ()) -> int:
        row = conn.execute(query, params).fetchone()
        if not row:
            return 0
        value = row[0]
        return int(value) if value is not None else 0

    schema_v = (conn.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone() or [None])[0] or "?"
    last_run_id_row = conn.execute("SELECT value FROM metadata WHERE key='last_sync_run_id'").fetchone()
    last_run_id = int(last_run_id_row[0]) if last_run_id_row else None

    last_run = None
    if last_run_id:
        last_run = conn.execute("SELECT * FROM sync_runs WHERE id = ?", (last_run_id,)).fetchone()

    total_persons = _scalar_int("SELECT COUNT(*) FROM persons")
    with_portrait = _scalar_int(
        "SELECT COUNT(DISTINCT person_id) FROM media_items WHERE media_role='portrait' AND status='downloaded'"
    )
    with_sources = _scalar_int("SELECT COUNT(DISTINCT person_id) FROM person_sources")
    with_notes = _scalar_int("SELECT COUNT(DISTINCT person_id) FROM person_notes")
    with_memories = _scalar_int("SELECT COUNT(DISTINCT person_id) FROM person_memories")
    with_facts = _scalar_int("SELECT COUNT(DISTINCT person_id) FROM person_facts")
    never_synced = _scalar_int(
        "SELECT COUNT(*) FROM persons p WHERE NOT EXISTS "
        "(SELECT 1 FROM person_sync_state s WHERE s.person_id = p.person_id)"
    )

    total_media = _scalar_int("SELECT COUNT(*) FROM media_items")
    portraits_count = _scalar_int("SELECT COUNT(*) FROM media_items WHERE media_role='portrait'")
    portraits_bytes = _scalar_int("SELECT COALESCE(SUM(bytes_size),0) FROM media_items WHERE media_role='portrait'")
    memories_count = _scalar_int("SELECT COUNT(*) FROM media_items WHERE media_role!='portrait'")
    memories_bytes = _scalar_int("SELECT COALESCE(SUM(bytes_size),0) FROM media_items WHERE media_role!='portrait'")

    without_sha = _scalar_int("SELECT COUNT(*) FROM media_items WHERE sha256 IS NULL AND local_path IS NOT NULL")
    local_paths = conn.execute("SELECT local_path FROM media_items WHERE local_path IS NOT NULL").fetchall()
    missing_on_disk = sum(1 for row in local_paths if row[0] and not os.path.isfile(row[0]))

    recent_runs = conn.execute("SELECT * FROM sync_runs ORDER BY id DESC LIMIT 5").fetchall()

    failed_jobs = _scalar_int("SELECT COUNT(*) FROM sync_queue WHERE status='failed'")
    cutoff_24h = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24)).replace(microsecond=0).isoformat()
    stale_persons = _scalar_int(
        "SELECT COUNT(*) FROM person_sync_state WHERE last_fetched_at IS NULL OR last_fetched_at < ?",
        (cutoff_24h,),
    )

    conn.close()

    print("=== Mirror Status ===")
    print(f"DB: {db_path} ({db_size_mb:.1f} MB)")
    print(f"Schema version: {schema_v}")
    if last_run:
        r = dict(last_run)
        print(f"Last sync: {r.get('finished_at') or r.get('started_at')} (run #{r['id']}, {r['status']})")
    else:
        print("Last sync: none")

    print()
    print(f"--- Personas ({total_persons} total) ---")
    print(f"  Con retrato descargado:       {with_portrait} / {total_persons}")
    print(f"  Con fuentes:                  {with_sources} / {total_persons}")
    print(f"  Con notas:                    {with_notes} / {total_persons}")
    print(f"  Con memorias:                 {with_memories} / {total_persons}")
    print(f"  Con hechos (facts):           {with_facts} / {total_persons}")
    print(f"  Fases nunca sincronizadas:     {never_synced}")

    print()
    print(f"--- Media ({total_media} items) ---")
    print(f"  Retratos:          {portraits_count}  ({portraits_bytes / (1024 * 1024):.1f} MB)")
    print(f"  Memorias/adjuntos: {memories_count} ({memories_bytes / (1024 * 1024):.1f} MB)")
    print(f"  Archivos faltantes en disco:   {missing_on_disk}  <- detecta rot")
    print(f"  Archivos sin SHA256:           {without_sha}")

    print()
    print("--- Sync runs (últimos 5) ---")
    for run in recent_runs:
        r = dict(run)
        date_str = (r.get("started_at") or "")[:10]
        print(f"  #{r['id']}  {date_str}  {r['status']:<12}  "
              f"{r.get('persons_count', 0)}p  "
              f"{r.get('relationships_count', 0)}r  "
              f"{r.get('media_count', 0)}m")

    print()
    print("--- Deuda pendiente ---")
    print(f"  Jobs fallidos sin reintentar:  {failed_jobs}")
    print(f"  Personas sin fetch reciente (>24h): {stale_persons}")


def upsert_dna_segments(db_path: str, person_id: str, source: str, segments: List[Dict]) -> int:
    """Delete previous segments for person+source and insert new batch. Returns stored count."""
    conn = connect(db_path)
    now = utc_now_iso()
    with conn:
        conn.execute(
            "DELETE FROM dna_segments WHERE person_id = ? AND source = ?",
            (person_id, source),
        )
        for seg in segments:
            conn.execute(
                """
                INSERT INTO dna_segments(
                    person_id, source, chromosome, start_pos, end_pos, centimorgans, ancestry,
                    match_name, branch_side, branch_label, ancestor_person_id, segment_kind, uploaded_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    person_id,
                    source,
                    str(seg.get("chromosome", "")),
                    int(seg.get("start_pos", 0)),
                    int(seg.get("end_pos", 0)),
                    float(seg["centimorgans"]) if seg.get("centimorgans") is not None else None,
                    seg.get("ancestry"),
                    seg.get("match_name"),
                    seg.get("branch_side"),
                    seg.get("branch_label"),
                    seg.get("ancestor_person_id"),
                    str(seg.get("segment_kind") or "painter"),
                    now,
                ),
            )
    conn.close()
    return len(segments)


def replace_dna_raw_snps(db_path: str, person_id: str, snps: List[Dict[str, Any]]) -> int:
    conn = connect(db_path)
    now = utc_now_iso()
    with conn:
        conn.execute("DELETE FROM dna_raw_snps WHERE person_id = ?", (person_id,))
        for snp in snps:
            conn.execute(
                """
                INSERT INTO dna_raw_snps(person_id, rsid, chromosome, position, genotype, uploaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    person_id,
                    str(snp.get("rsid", "")).strip(),
                    str(snp.get("chromosome", "")).strip(),
                    int(snp.get("position", 0) or 0),
                    str(snp.get("genotype", "")).strip().upper(),
                    now,
                ),
            )
    conn.close()
    return len(snps)


def replace_dna_ethnicity(db_path: str, person_id: str, rows: List[Dict[str, Any]]) -> int:
    conn = connect(db_path)
    now = utc_now_iso()
    with conn:
        conn.execute("DELETE FROM dna_ethnicity WHERE person_id = ?", (person_id,))
        for item in rows:
            conn.execute(
                """
                INSERT INTO dna_ethnicity(
                    person_id, region, percentage, reference_panel, generation_estimate, side, color_hint, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    person_id,
                    str(item.get("region", "")).strip(),
                    float(item.get("percentage", 0) or 0),
                    str(item.get("reference_panel") or "").strip() or None,
                    str(item.get("generation_estimate") or "").strip() or None,
                    str(item.get("side") or "").strip() or None,
                    str(item.get("color_hint") or "").strip() or None,
                    now,
                ),
            )
    conn.close()
    return len(rows)


def upsert_dna_haplogroups(
    db_path: str,
    person_id: str,
    y_haplogroup: str | None,
    mt_haplogroup: str | None,
    y_timeline: List[Dict[str, Any]] | None = None,
    mt_timeline: List[Dict[str, Any]] | None = None,
    notes: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    conn = connect(db_path)
    now = utc_now_iso()
    y_value = str(y_haplogroup or "").strip() or None
    mt_value = str(mt_haplogroup or "").strip() or None
    y_timeline_resolved = y_timeline or _default_haplogroup_timeline("y", y_value)
    mt_timeline_resolved = mt_timeline or _default_haplogroup_timeline("mt", mt_value)
    with conn:
        conn.execute(
            """
            INSERT INTO dna_haplogroups(person_id, y_haplogroup, mt_haplogroup, y_timeline_json, mt_timeline_json, notes_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(person_id) DO UPDATE SET
                y_haplogroup=excluded.y_haplogroup,
                mt_haplogroup=excluded.mt_haplogroup,
                y_timeline_json=excluded.y_timeline_json,
                mt_timeline_json=excluded.mt_timeline_json,
                notes_json=excluded.notes_json,
                updated_at=excluded.updated_at
            """,
            (
                person_id,
                y_value,
                mt_value,
                _json_dump(y_timeline_resolved),
                _json_dump(mt_timeline_resolved),
                _json_dump(notes or {}),
                now,
            ),
        )
    conn.close()
    return get_dna_haplogroups(db_path, person_id)


def replace_dna_matches(db_path: str, person_id: str, matches: List[Dict[str, Any]]) -> int:
    conn = connect(db_path)
    now = utc_now_iso()
    with conn:
        conn.execute("DELETE FROM dna_matches WHERE person_id = ?", (person_id,))
        conn.execute("DELETE FROM dna_segments WHERE person_id = ? AND segment_kind = 'match'", (person_id,))
        for item in matches:
            match_name = str(item.get("match_name", "")).strip()
            if not match_name:
                continue
            segments = item.get("segments") if isinstance(item.get("segments"), list) else []
            conn.execute(
                """
                INSERT INTO dna_matches(person_id, match_name, total_cm, segments_count, predicted_relationship, side, notes_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(person_id, match_name) DO UPDATE SET
                    total_cm=excluded.total_cm,
                    segments_count=excluded.segments_count,
                    predicted_relationship=excluded.predicted_relationship,
                    side=excluded.side,
                    notes_json=excluded.notes_json,
                    updated_at=excluded.updated_at
                """,
                (
                    person_id,
                    match_name,
                    float(item["total_cm"]) if item.get("total_cm") is not None else None,
                    int(item["segments_count"]) if item.get("segments_count") is not None else len(segments),
                    str(item.get("predicted_relationship") or "").strip() or None,
                    str(item.get("side") or "").strip() or None,
                    _json_dump(item.get("notes") or {}),
                    now,
                ),
            )
            for seg in segments:
                conn.execute(
                    """
                    INSERT INTO dna_segments(
                        person_id, source, chromosome, start_pos, end_pos, centimorgans, ancestry,
                        match_name, branch_side, branch_label, ancestor_person_id, segment_kind, uploaded_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'match', ?)
                    """,
                    (
                        person_id,
                        str(seg.get("source") or "matches"),
                        str(seg.get("chromosome", "")).strip(),
                        int(seg.get("start_pos", 0) or 0),
                        int(seg.get("end_pos", 0) or 0),
                        float(seg["centimorgans"]) if seg.get("centimorgans") is not None else None,
                        str(seg.get("ancestry") or "").strip() or None,
                        match_name,
                        str(seg.get("branch_side") or item.get("side") or "").strip() or None,
                        str(seg.get("branch_label") or "").strip() or None,
                        str(seg.get("ancestor_person_id") or "").strip() or None,
                        now,
                    ),
                )
    conn.close()
    return len(matches)


def list_dna_segments(db_path: str, person_id: str) -> List[Dict]:
    """Return all DNA segments for a person, grouped by source."""
    conn = connect(db_path)
    rows = conn.execute(
        """
        SELECT source, chromosome, start_pos, end_pos, centimorgans, ancestry,
               match_name, branch_side, branch_label, ancestor_person_id, segment_kind, uploaded_at
        FROM dna_segments
        WHERE person_id = ?
        ORDER BY segment_kind, source, chromosome, start_pos
        """,
        (person_id,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def _parse_json_column(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _normalize_match_text(value: str | None) -> str:
    if not value:
        return ""
    lowered = str(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", " ", lowered).strip()


def _fetch_branch_candidates(conn: sqlite3.Connection, person_id: str) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    parent_rows = conn.execute(
        """
        SELECT relation_type, related_person_id
        FROM relationships
        WHERE person_id = ? AND relation_type IN ('father', 'mother')
        ORDER BY relation_type
        """,
        (person_id,),
    ).fetchall()
    for parent in parent_rows:
        rel_type = str(parent["relation_type"])
        parent_id = str(parent["related_person_id"])
        parent_person = conn.execute(
            "SELECT person_id, name FROM persons WHERE person_id = ?",
            (parent_id,),
        ).fetchone()
        side = "paternal" if rel_type == "father" else "maternal"
        if parent_person and parent_person["name"]:
            candidates.append(
                {
                    "person_id": parent_person["person_id"],
                    "name": str(parent_person["name"]),
                    "side": side,
                    "branch_key": side,
                    "branch_label": f"{'Línea paterna' if side == 'paternal' else 'Línea materna'} · {parent_person['name']}",
                }
            )
        grand_rows = conn.execute(
            """
            SELECT relation_type, related_person_id
            FROM relationships
            WHERE person_id = ? AND relation_type IN ('father', 'mother')
            ORDER BY relation_type
            """,
            (parent_id,),
        ).fetchall()
        for grand in grand_rows:
            grand_person = conn.execute(
                "SELECT person_id, name FROM persons WHERE person_id = ?",
                (grand["related_person_id"],),
            ).fetchone()
            if grand_person and grand_person["name"]:
                branch_key = f"{side}:{grand_person['person_id']}"
                candidates.append(
                    {
                        "person_id": grand_person["person_id"],
                        "name": str(grand_person["name"]),
                        "side": side,
                        "branch_key": branch_key,
                        "branch_label": str(grand_person["name"]),
                    }
                )
    return candidates


def _infer_segment_branch(seg: Dict[str, Any], candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    explicit_side = str(seg.get("branch_side") or "").strip().lower()
    branch_label = str(seg.get("branch_label") or "").strip()
    probable_ancestor = ""
    if seg.get("ancestor_person_id"):
        for item in candidates:
            if str(item.get("person_id")) == str(seg.get("ancestor_person_id")):
                probable_ancestor = str(item.get("name") or "")
                branch_label = branch_label or str(item.get("branch_label") or probable_ancestor)
                explicit_side = explicit_side or str(item.get("side") or "")
                return {
                    "branch_key": str(item.get("branch_key") or explicit_side or "unknown"),
                    "branch_label": branch_label or "Origen no asignado",
                    "branch_side": explicit_side or None,
                    "probable_ancestor": probable_ancestor or None,
                }

    blob = _normalize_match_text(
        " ".join(
            part for part in [
                str(seg.get("ancestry") or ""),
                str(seg.get("match_name") or ""),
                branch_label,
            ]
            if part
        )
    )
    matched_candidate = None
    for item in candidates:
        normalized_name = _normalize_match_text(str(item.get("name") or ""))
        if normalized_name and normalized_name in blob:
            matched_candidate = item
            break
    if matched_candidate:
        return {
            "branch_key": str(matched_candidate.get("branch_key") or "unknown"),
            "branch_label": branch_label or str(matched_candidate.get("branch_label") or matched_candidate.get("name") or "Origen probable"),
            "branch_side": explicit_side or str(matched_candidate.get("side") or "") or None,
            "probable_ancestor": str(matched_candidate.get("name") or "") or None,
        }
    if not explicit_side:
        if any(token in blob for token in ("maternal", "materna", "madre")):
            explicit_side = "maternal"
        elif any(token in blob for token in ("paternal", "paterna", "padre")):
            explicit_side = "paternal"
    if explicit_side == "maternal":
        return {
            "branch_key": "maternal",
            "branch_label": branch_label or "Línea materna",
            "branch_side": "maternal",
            "probable_ancestor": None,
        }
    if explicit_side == "paternal":
        return {
            "branch_key": "paternal",
            "branch_label": branch_label or "Línea paterna",
            "branch_side": "paternal",
            "probable_ancestor": None,
        }
    return {
        "branch_key": branch_label or str(seg.get("ancestry") or "").strip() or "unassigned",
        "branch_label": branch_label or str(seg.get("ancestry") or "").strip() or "Origen no asignado",
        "branch_side": None,
        "probable_ancestor": None,
    }


def _allele_match_count(genotype: str | None, allele: str | None) -> int:
    genotype_value = str(genotype or "").strip().upper()
    allele_value = str(allele or "").strip().upper()
    if not genotype_value or not allele_value or "-" in genotype_value:
        return 0
    if len(allele_value) == 1:
        return genotype_value.count(allele_value)
    normalized_genotype = "".join(sorted(genotype_value))
    normalized_allele = "".join(sorted(allele_value))
    return 2 if normalized_genotype == normalized_allele else 0


def _list_dna_trait_items_conn(conn: sqlite3.Connection, person_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT s.rsid, s.chromosome, s.position, s.genotype,
               t.category, t.trait_name, t.allele, t.effect, t.confidence, t.source_url
        FROM dna_raw_snps s
        JOIN dna_traits t ON t.rsid = s.rsid
        WHERE s.person_id = ?
        ORDER BY t.category, t.trait_name, s.position
        """,
        (person_id,),
    ).fetchall()
    items: List[Dict[str, Any]] = []
    for row in rows:
        copies = _allele_match_count(row["genotype"], row["allele"])
        if copies <= 0:
            continue
        item = dict(row)
        item["copies"] = copies
        item["probabilistic"] = str(item.get("category") or "") in ("behavior", "health")
        items.append(item)
    return items


def get_dna_traits(db_path: str, person_id: str) -> Dict[str, Any]:
    conn = connect(db_path)
    try:
        items = _list_dna_trait_items_conn(conn, person_id)
        categories: Dict[str, List[Dict[str, Any]]] = {"health": [], "physical": [], "behavior": []}
        for item in items:
            categories.setdefault(str(item["category"]), []).append(
                {
                    "rsid": item["rsid"],
                    "chromosome": item["chromosome"],
                    "position": item["position"],
                    "genotype": item["genotype"],
                    "trait_name": item["trait_name"],
                    "allele": item["allele"],
                    "effect": item["effect"],
                    "confidence": item.get("confidence"),
                    "source_url": item.get("source_url"),
                    "copies": item["copies"],
                }
            )
        return {
            "categories": categories,
            "summary": {key: len(value) for key, value in categories.items()},
            "disclaimers": {
                "health": "Información orientativa, no diagnóstico médico.",
                "behavior": "Los rasgos cognitivos o conductuales son probabilísticos y no deterministas.",
            },
        }
    finally:
        conn.close()


def get_dna_ethnicity(db_path: str, person_id: str) -> Dict[str, Any]:
    conn = connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT region, percentage, reference_panel, generation_estimate, side, color_hint, updated_at
            FROM dna_ethnicity
            WHERE person_id = ?
            ORDER BY percentage DESC, region
            """,
            (person_id,),
        ).fetchall()
        items = [dict(row) for row in rows]
        total = round(sum(float(item.get("percentage") or 0) for item in items), 2)
        by_generation: Dict[str, float] = {}
        for item in items:
            generation = str(item.get("generation_estimate") or "").strip()
            if generation:
                by_generation[generation] = round(by_generation.get(generation, 0.0) + float(item.get("percentage") or 0), 2)
        return {
            "items": items,
            "total_percentage": total,
            "generation_breakdown": [{"generation": key, "percentage": value} for key, value in by_generation.items()],
        }
    finally:
        conn.close()


def _default_haplogroup_timeline(kind: str, haplogroup: str | None) -> List[Dict[str, Any]]:
    value = str(haplogroup or "").strip().upper()
    if not value:
        return []
    prefix = value[0]
    return list(DEFAULT_HAPLOGROUP_TIMELINES.get(kind, {}).get(prefix, []))


def get_dna_haplogroups(db_path: str, person_id: str) -> Dict[str, Any]:
    conn = connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT person_id, y_haplogroup, mt_haplogroup, y_timeline_json, mt_timeline_json, notes_json, updated_at
            FROM dna_haplogroups
            WHERE person_id = ?
            """,
            (person_id,),
        ).fetchone()
        if not row:
            return {
                "person_id": person_id,
                "y_haplogroup": None,
                "mt_haplogroup": None,
                "y_timeline": [],
                "mt_timeline": [],
                "notes": {},
            }
        payload = dict(row)
        return {
            "person_id": payload["person_id"],
            "y_haplogroup": payload.get("y_haplogroup"),
            "mt_haplogroup": payload.get("mt_haplogroup"),
            "y_timeline": _parse_json_column(payload.get("y_timeline_json"), []),
            "mt_timeline": _parse_json_column(payload.get("mt_timeline_json"), []),
            "notes": _parse_json_column(payload.get("notes_json"), {}),
            "updated_at": payload.get("updated_at"),
        }
    finally:
        conn.close()


def get_dna_matches(db_path: str, person_id: str) -> Dict[str, Any]:
    conn = connect(db_path)
    try:
        matches = [
            {
                **dict(row),
                "notes": _parse_json_column(row["notes_json"], {}),
                "segments": [],
            }
            for row in conn.execute(
                """
                SELECT match_name, total_cm, segments_count, predicted_relationship, side, notes_json, updated_at
                FROM dna_matches
                WHERE person_id = ?
                ORDER BY COALESCE(total_cm, 0) DESC, match_name
                """,
                (person_id,),
            ).fetchall()
        ]
        segments = conn.execute(
            """
            SELECT match_name, chromosome, start_pos, end_pos, centimorgans, ancestry, branch_side, branch_label, uploaded_at
            FROM dna_segments
            WHERE person_id = ? AND segment_kind = 'match'
            ORDER BY match_name, chromosome, start_pos
            """,
            (person_id,),
        ).fetchall()
        by_match = {item["match_name"]: item for item in matches}
        for row in segments:
            item = by_match.get(row["match_name"])
            if item is not None:
                item["segments"].append(dict(row))
        return {"items": matches}
    finally:
        conn.close()


def get_dna_painter_data(db_path: str, person_id: str) -> Dict[str, Any]:
    conn = connect(db_path)
    try:
        segments = [
            dict(row)
            for row in conn.execute(
                """
                SELECT source, chromosome, start_pos, end_pos, centimorgans, ancestry,
                       match_name, branch_side, branch_label, ancestor_person_id, segment_kind, uploaded_at
                FROM dna_segments
                WHERE person_id = ? AND segment_kind != 'match'
                ORDER BY chromosome, start_pos
                """,
                (person_id,),
            ).fetchall()
        ]
        branch_candidates = _fetch_branch_candidates(conn, person_id)
        matched_traits = _list_dna_trait_items_conn(conn, person_id)
        traits_by_chr: Dict[str, List[Dict[str, Any]]] = {}
        for trait in matched_traits:
            traits_by_chr.setdefault(str(trait["chromosome"]), []).append(trait)
        enriched: List[Dict[str, Any]] = []
        legend: Dict[str, Dict[str, Any]] = {}
        for seg in segments:
            branch = _infer_segment_branch(seg, branch_candidates)
            segment_traits = [
                {
                    "trait_name": trait["trait_name"],
                    "category": trait["category"],
                    "rsid": trait["rsid"],
                    "position": trait["position"],
                }
                for trait in traits_by_chr.get(str(seg["chromosome"]), [])
                if int(seg["start_pos"]) <= int(trait["position"]) <= int(seg["end_pos"])
            ]
            payload = {
                **seg,
                **branch,
                "genomic_range": f"{seg['chromosome']}:{seg['start_pos']}-{seg['end_pos']}",
                "traits": segment_traits[:6],
                "trait_count": len(segment_traits),
            }
            legend[str(branch["branch_key"])] = {
                "branch_key": str(branch["branch_key"]),
                "branch_label": str(branch["branch_label"]),
                "branch_side": branch.get("branch_side"),
            }
            enriched.append(payload)
        return {
            "person_id": person_id,
            "segments": enriched,
            "legend": list(legend.values()),
            "summary": {
                "segments": len(enriched),
                "sources": sorted({str(item["source"]) for item in enriched}),
                "phased": any(item.get("branch_side") for item in enriched),
            },
        }
    finally:
        conn.close()


def get_dna_overview(db_path: str, person_id: str) -> Dict[str, Any]:
    segments = list_dna_segments(db_path, person_id)
    sources: Dict[str, List[Dict[str, Any]]] = {}
    for seg in segments:
        sources.setdefault(str(seg["source"]), []).append(seg)
    raw_stats = {"snps": 0}
    conn = connect(db_path)
    try:
        raw_count = conn.execute("SELECT COUNT(*) AS qty FROM dna_raw_snps WHERE person_id = ?", (person_id,)).fetchone()
        raw_stats["snps"] = int(raw_count["qty"] if raw_count else 0)
    finally:
        conn.close()
    return {
        "sources": sources,
        "summary": {
            "segments": len(segments),
            "raw_snps": raw_stats["snps"],
            "ethnicity_regions": len(get_dna_ethnicity(db_path, person_id)["items"]),
            "matches": len(get_dna_matches(db_path, person_id)["items"]),
        },
    }


def delete_dna_segments(db_path: str, person_id: str, source: str) -> None:
    """Delete all segments for person+source."""
    conn = connect(db_path)
    with conn:
        conn.execute(
            "DELETE FROM dna_segments WHERE person_id = ? AND source = ?",
            (person_id, source),
        )
    conn.close()


def list_stub_person_ids(db_path: str) -> List[str]:
    """Return person_ids that exist in persons but have no fetched profile (is_stub=1)."""
    conn = connect(db_path)
    rows = conn.execute(
        """
        SELECT p.person_id
        FROM persons p
        LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
        WHERE pss.last_fetched_at IS NULL
        ORDER BY p.person_id
        """
    ).fetchall()
    conn.close()
    return [row[0] for row in rows]


def _normalize_name_for_dedupe(name: str | None) -> str:
    return _normalize_text_for_identity(name)


def _extract_year_from_text(value: str | None) -> Optional[int]:
    if not value:
        return None
    match = re.search(r"\b(\d{4})\b", str(value))
    if not match:
        return None
    try:
        year = int(match.group(1))
    except ValueError:
        return None
    return year if 1000 <= year <= 2100 else None


def _pair_order(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a <= b else (b, a)


def _ignored_pairs(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    rows = conn.execute(
        "SELECT person_id_a, person_id_b FROM duplicate_pair_ignored"
    ).fetchall()
    return {_pair_order(str(row["person_id_a"]), str(row["person_id_b"])) for row in rows}


def list_duplicate_candidates(
    db_path: str,
    limit: int = 120,
    min_score: int = 55,
    offset: int = 0,
) -> tuple[List[Dict], int]:
    """Return likely duplicate person pairs for human review."""
    conn = connect(db_path)
    rows = conn.execute(
        """
        SELECT person_id, name, gender, birth_date, death_date, lifespan
        FROM persons
        WHERE name IS NOT NULL AND trim(name) <> ''
        ORDER BY name COLLATE NOCASE, person_id
        """
    ).fetchall()
    ignored = _ignored_pairs(conn)
    conn.close()

    grouped: Dict[str, List[Dict]] = {}
    for row in rows:
        item = dict(row)
        key = _normalize_name_for_dedupe(item.get("name"))
        if not key:
            continue
        grouped.setdefault(key, []).append(item)

    candidates: List[Dict] = []
    for key, people in grouped.items():
        if len(people) < 2:
            continue
        for left, right in combinations(people, 2):
            pair = _pair_order(str(left["person_id"]), str(right["person_id"]))
            if pair in ignored:
                continue
            score = 45
            reasons: List[str] = ["same_name"]

            left_birth = _extract_year_from_text(left.get("birth_date"))
            right_birth = _extract_year_from_text(right.get("birth_date"))
            if left_birth is not None and right_birth is not None:
                if left_birth == right_birth:
                    score += 28
                    reasons.append("birth_year_exact")
                elif abs(left_birth - right_birth) <= 1:
                    score += 16
                    reasons.append("birth_year_close")
                elif abs(left_birth - right_birth) >= 10:
                    score -= 35
            elif left_birth is not None or right_birth is not None:
                score += 4

            left_death = _extract_year_from_text(left.get("death_date"))
            right_death = _extract_year_from_text(right.get("death_date"))
            if left_death is not None and right_death is not None:
                if left_death == right_death:
                    score += 20
                    reasons.append("death_year_exact")
                elif abs(left_death - right_death) <= 1:
                    score += 10
                    reasons.append("death_year_close")
                elif abs(left_death - right_death) >= 10:
                    score -= 20
            elif left_death is not None or right_death is not None:
                score += 2

            left_gender = str(left.get("gender") or "").strip().upper()
            right_gender = str(right.get("gender") or "").strip().upper()
            if left_gender and right_gender:
                if left_gender == right_gender:
                    score += 5
                    reasons.append("gender_match")
                else:
                    score -= 22

            if score < min_score:
                continue

            candidates.append(
                {
                    "score": max(0, min(100, score)),
                    "reasons": reasons,
                    "left": left,
                    "right": right,
                }
            )

    candidates.sort(
        key=lambda item: (
            -int(item["score"]),
            _normalize_name_for_dedupe(item["left"].get("name")),
            str(item["left"].get("person_id")),
            str(item["right"].get("person_id")),
        )
    )
    total = len(candidates)
    start = max(0, int(offset))
    end = start + max(1, int(limit))
    return candidates[start:end], total


def ignore_duplicate_pair(db_path: str, person_id_a: str, person_id_b: str, reason: str | None = None) -> None:
    a, b = _pair_order(person_id_a.strip(), person_id_b.strip())
    if not a or not b or a == b:
        return
    conn = connect(db_path)
    with conn:
        conn.execute(
            """
            INSERT INTO duplicate_pair_ignored(person_id_a, person_id_b, reason, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(person_id_a, person_id_b) DO UPDATE SET
                reason = COALESCE(excluded.reason, duplicate_pair_ignored.reason),
                created_at = excluded.created_at
            """,
            (a, b, (reason or "").strip() or None, utc_now_iso()),
        )
    conn.close()


def list_historical_events(
    db_path: str,
    *,
    start_year: int | None = None,
    end_year: int | None = None,
    scope: str | None = None,
    place_query: str | None = None,
    offset: int = 0,
    limit: int = 500,
) -> tuple[List[Dict], int]:
    conn = connect(db_path)
    params: List[object] = []
    where: List[str] = []
    if start_year is not None:
        where.append("end_year >= ?")
        params.append(int(start_year))
    if end_year is not None:
        where.append("start_year <= ?")
        params.append(int(end_year))
    if scope:
        scope_value = scope.strip().lower()
        if scope_value not in {"global", "local"}:
            conn.close()
            return [], 0
        where.append("scope = ?")
        params.append(scope_value)
    if place_query:
        like_value = f"%{place_query.strip().lower()}%"
        where.append(
            "(lower(title) LIKE ? OR lower(description) LIKE ? OR lower(COALESCE(match_terms_json, '')) LIKE ?)"
        )
        params.extend([like_value, like_value, like_value])
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    total_row = conn.execute(
        f"SELECT COUNT(*) AS qty FROM historical_events {where_clause}",
        params,
    ).fetchone()
    total = int(total_row["qty"]) if total_row else 0
    params = list(params)
    params.extend([max(1, int(limit)), max(0, int(offset))])
    rows = conn.execute(
        f"""
        SELECT event_key, scope, title, description, start_year, end_year, source_url, match_terms_json
        FROM historical_events
        {where_clause}
        ORDER BY start_year, end_year, title
        LIMIT ?
        OFFSET ?
        """,
        params,
    ).fetchall()
    conn.close()
    out: List[Dict] = []
    for row in rows:
        item = dict(row)
        try:
            item["match_terms"] = json.loads(item.get("match_terms_json") or "[]")
        except Exception:
            item["match_terms"] = []
        item.pop("match_terms_json", None)
        out.append(item)
    return out, total


def upsert_historical_event(
    db_path: str,
    *,
    event_key: str,
    scope: str,
    title: str,
    description: str = "",
    start_year: int,
    end_year: int | None = None,
    source_url: str | None = None,
    match_terms: List[str] | None = None,
) -> None:
    conn = connect(db_path)
    with conn:
        conn.execute(
            """
            INSERT INTO historical_events(
                event_key, scope, title, description, start_year, end_year, source_url, match_terms_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                scope=excluded.scope,
                title=excluded.title,
                description=excluded.description,
                start_year=excluded.start_year,
                end_year=excluded.end_year,
                source_url=excluded.source_url,
                match_terms_json=excluded.match_terms_json,
                updated_at=excluded.updated_at
            """,
            (
                event_key.strip(),
                scope.strip().lower(),
                title.strip(),
                (description or "").strip(),
                int(start_year),
                int(end_year if end_year is not None else start_year),
                (source_url or "").strip() or None,
                _json_dump(match_terms or []),
                utc_now_iso(),
            ),
        )
    conn.close()
