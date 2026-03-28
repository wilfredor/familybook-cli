from __future__ import annotations

import json
import os
import re
import sqlite3
import unicodedata
from collections import deque
from datetime import datetime
from itertools import groupby
from typing import Any, Dict, List, Optional
from urllib.parse import unquote_plus, urlparse

import familybook_db


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows]


FACT_TYPE_LABELS = {
    "Birth": "Birth",
    "Death": "Death",
    "Burial": "Burial",
    "Christening": "Christening",
    "Occupation": "Occupation",
    "Residence": "Residence",
    "Immigration": "Immigration",
    "Ethnicity": "Ethnicity",
    "Religion": "Religion",
    "NationalId": "National ID",
    "LifeSketch": "Life Sketch",
    "Affiliation": "Affiliation",
}


def _clean_text_block(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = re.sub(r"\r\n?", "\n", value).strip()
    if not text:
        return None
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _normalize_fact(row: sqlite3.Row) -> Optional[Dict[str, Any]]:
    raw_type = row["fact_type"] or ""
    label = raw_type.strip()
    fact_group = "other"

    if raw_type.startswith("http://") or raw_type.startswith("https://"):
        parsed = urlparse(raw_type)
        tail = parsed.path.rstrip("/").split("/")[-1]
        label = FACT_TYPE_LABELS.get(tail, tail or "Event")
        fact_group = "standard"
    elif raw_type.startswith("data:,"):
        label = unquote_plus(raw_type[len("data:,") :]).strip() or "Custom data"
        fact_group = "custom"

    try:
        value_payload = json.loads(row["value_json"] or "{}")
    except json.JSONDecodeError:
        value_payload = {}

    date_original = (row["date_original"] or "").strip() or None
    place_original = (row["place_original"] or "").strip() or None
    raw_value = value_payload.get("value")
    fact_value = None
    fact_url = None
    if isinstance(raw_value, str):
        fact_value = raw_value.strip() or None
        if fact_value and (fact_value.startswith("http://") or fact_value.startswith("https://")):
            fact_url = fact_value
    elif isinstance(raw_value, dict):
        candidate = raw_value.get("text") or raw_value.get("value")
        if isinstance(candidate, str):
            fact_value = candidate.strip() or None
    has_visible_value = bool(date_original or place_original or fact_value)

    if not has_visible_value:
        return None

    if not label:
        label = "Event"

    return {
        "seq": row["seq"],
        "fact_type": raw_type,
        "fact_label": label,
        "fact_group": fact_group,
        "date_original": date_original,
        "place_original": place_original,
        "fact_value": fact_value,
        "fact_url": fact_url,
        "has_visible_value": has_visible_value,
    }


def _extract_biography(
    facts: List[Dict[str, Any]],
    note_rows: List[sqlite3.Row],
) -> Optional[Dict[str, Any]]:
    for fact in facts:
        raw_type = str(fact.get("fact_type") or "")
        label = str(fact.get("fact_label") or "")
        if "LifeSketch" not in raw_type and label != "Life Sketch":
            continue
        text = _clean_text_block(fact.get("fact_value"))
        if text:
            return {
                "text": text,
                "source": "life_sketch",
                "label": label or "Life Sketch",
            }

    note_candidates: List[Dict[str, Any]] = []
    for row in note_rows:
        subject = str(row["subject"] or "").strip()
        text = _clean_text_block(row["text_value"])
        if not text:
            continue
        normalized_subject = _normalize_match_text(subject)
        score = 0
        if len(text) >= 280:
            score += 1
        if any(term in normalized_subject for term in ("biografia", "biography", "historia", "life sketch", "semblanza")):
            score += 3
        if score:
            note_candidates.append(
                {
                    "score": score,
                    "text": text,
                    "source": "note",
                    "label": subject or "Note",
                }
            )
    if not note_candidates:
        return None
    note_candidates.sort(key=lambda item: (-int(item["score"]), -len(item["text"])))
    best = note_candidates[0]
    best.pop("score", None)
    return best


def _extract_year(value: str | None) -> Optional[int]:
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


def _normalize_match_text(value: str | None) -> str:
    if not value:
        return ""
    lowered = str(value).strip().lower()
    folded = unicodedata.normalize("NFKD", lowered)
    ascii_only = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", ascii_only).strip()


def get_status(db_path: str) -> Dict[str, Any]:
    conn = _connect(db_path)
    try:
        counts = {}
        for table in (
            "persons",
            "relationships",
            "person_notes",
            "person_sources",
            "person_memories",
            "media_items",
            "sync_runs",
            "sync_queue",
        ):
            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        latest_run = conn.execute(
            """
            SELECT id, started_at, finished_at, status, jobs_done, jobs_failed,
                   persons_count, relationships_count, notes_count, sources_count,
                   memories_count, media_count, last_error, root_person_id, generations
            FROM sync_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        active_run = conn.execute(
            """
            SELECT id, started_at, finished_at, status, jobs_done, jobs_failed,
                   persons_count, relationships_count, notes_count, sources_count,
                   memories_count, media_count, last_error, root_person_id, generations
            FROM sync_runs
            WHERE status = 'running'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        queue_by_status = _rows_to_dicts(
            conn.execute(
                """
                SELECT status, COUNT(*) AS qty
                FROM sync_queue
                GROUP BY status
                ORDER BY status
                """
            ).fetchall()
        )
        active_payload = None
        if active_run:
            run_id = int(active_run["id"])
            run_queue_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS qty
                FROM sync_queue
                WHERE run_id = ?
                GROUP BY status
                ORDER BY status
                """,
                (run_id,),
            ).fetchall()
            run_queue = {"pending": 0, "in_progress": 0, "done": 0, "failed": 0}
            for row in run_queue_rows:
                run_queue[str(row["status"])] = int(row["qty"])
            total_jobs = int(sum(run_queue.values()))
            completed_jobs = int(run_queue.get("done", 0) + run_queue.get("failed", 0))
            progress_percent = (completed_jobs * 100.0 / total_jobs) if total_jobs > 0 else 0.0
            active_payload = {
                **dict(active_run),
                "queue": run_queue,
                "total_jobs": total_jobs,
                "completed_jobs": completed_jobs,
                "progress_percent": round(progress_percent, 1),
            }
        metadata_rows = conn.execute(
            """
            SELECT key, value
            FROM metadata
            WHERE key IN (
                'last_sync_at',
                'sync_stale_person_hours',
                'sync_stale_notes_hours',
                'sync_stale_sources_hours',
                'sync_stale_memories_hours',
                'sync_stale_portraits_hours',
                'sync_max_retries',
                'sync_retry_delay_minutes'
            )
            ORDER BY key
            """
        ).fetchall()
        metadata = {row["key"]: row["value"] for row in metadata_rows}
        return {
            "db_path": os.path.abspath(db_path),
            "counts": counts,
            "latest_run": dict(latest_run) if latest_run else None,
            "active_run": active_payload,
            "queue_by_status": queue_by_status,
            "metadata": metadata,
        }
    finally:
        conn.close()


def list_runs(db_path: str, limit: int = 12) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT id, started_at, finished_at, status, jobs_done, jobs_failed,
                   persons_count, relationships_count, notes_count, sources_count,
                   memories_count, media_count, last_error
            FROM sync_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def count_people(db_path: str, query: str = "") -> int:
    conn = _connect(db_path)
    try:
        if query.strip():
            pattern = f"%{query.strip().lower()}%"
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM persons p
                WHERE lower(coalesce(p.name, '')) LIKE ?
                   OR lower(p.person_id) LIKE ?
                """,
                (pattern, pattern),
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) FROM persons").fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def list_people(db_path: str, query: str = "", limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        safe_limit = max(1, int(limit))
        safe_offset = max(0, int(offset))
        if query.strip():
            pattern = f"%{query.strip().lower()}%"
            rows = conn.execute(
                """
                SELECT p.person_id, p.name, p.gender, p.lifespan, p.birth_date, p.birth_place, p.death_date, p.death_place,
                       pss.last_fetched_at,
                       CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END AS is_stub,
                       (
                           SELECT mi.local_path
                           FROM media_items mi
                           WHERE mi.person_id = p.person_id AND mi.media_role = 'portrait' AND mi.local_path IS NOT NULL
                           ORDER BY mi.updated_at DESC
                           LIMIT 1
                       ) AS portrait_local_path
                FROM persons p
                LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
                WHERE lower(coalesce(p.name, '')) LIKE ?
                   OR lower(p.person_id) LIKE ?
                ORDER BY p.name COLLATE NOCASE, p.person_id
                LIMIT ?
                OFFSET ?
                """,
                (pattern, pattern, safe_limit, safe_offset),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT p.person_id, p.name, p.gender, p.lifespan, p.birth_date, p.birth_place, p.death_date, p.death_place,
                       pss.last_fetched_at,
                       CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END AS is_stub,
                       (
                           SELECT mi.local_path
                           FROM media_items mi
                           WHERE mi.person_id = p.person_id AND mi.media_role = 'portrait' AND mi.local_path IS NOT NULL
                           ORDER BY mi.updated_at DESC
                           LIMIT 1
                       ) AS portrait_local_path
                FROM persons p
                LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
                ORDER BY p.name COLLATE NOCASE, p.person_id
                LIMIT ?
                OFFSET ?
                """,
                (safe_limit, safe_offset),
            ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def get_person_detail(db_path: str, person_id: str) -> Optional[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        person = conn.execute(
            """
            SELECT p.person_id, p.name, p.gender, p.lifespan, p.birth_date, p.birth_place, p.death_date, p.death_place,
                   p.updated_at, pss.last_fetched_at,
                   (
                       SELECT mi.local_path
                       FROM media_items mi
                       WHERE mi.person_id = p.person_id AND mi.media_role = 'portrait' AND mi.local_path IS NOT NULL
                       ORDER BY mi.updated_at DESC
                       LIMIT 1
                   ) AS portrait_local_path,
                   CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END AS is_stub
            FROM persons p
            LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
            WHERE p.person_id = ?
            """,
            (person_id,),
        ).fetchone()
        if not person:
            return None
        raw_facts = conn.execute(
                """
                SELECT seq, fact_type, date_original, place_original, value_json
                FROM person_facts
                WHERE person_id = ?
                ORDER BY seq
                """,
                (person_id,),
            ).fetchall()
        facts = [fact for row in raw_facts if (fact := _normalize_fact(row)) is not None]
        note_rows = conn.execute(
            """
            SELECT subject, text_value
            FROM person_notes
            WHERE person_id = ?
            ORDER BY updated_at DESC, note_key
            LIMIT 8
            """,
            (person_id,),
        ).fetchall()
        biography = _extract_biography(facts, note_rows)
        relationships = _rows_to_dicts(
            conn.execute(
                """
                SELECT relation_type, related_person_id, fs_relationship_id
                FROM relationships
                WHERE person_id = ?
                ORDER BY relation_type, related_person_id
                """,
                (person_id,),
            ).fetchall()
        )
        related_ids = [row["related_person_id"] for row in relationships]
        related_people = {}
        if related_ids:
            placeholders = ",".join("?" for _ in related_ids)
            rel_rows = conn.execute(
                f"""
                SELECT p.person_id, p.name, p.lifespan, p.birth_date, p.death_date, pss.last_fetched_at,
                       CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END AS is_stub,
                       (
                           SELECT mi.local_path
                           FROM media_items mi
                           WHERE mi.person_id = p.person_id AND mi.media_role = 'portrait' AND mi.local_path IS NOT NULL
                           ORDER BY mi.updated_at DESC
                           LIMIT 1
                       ) AS portrait_local_path
                FROM persons p
                LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
                WHERE p.person_id IN ({placeholders})
                """,
                related_ids,
            ).fetchall()
            related_people = {row["person_id"]: dict(row) for row in rel_rows}
        grouped_relationships: Dict[str, List[Dict[str, Any]]] = {}
        for item in relationships:
            rel_type = item["relation_type"]
            grouped_relationships.setdefault(rel_type, []).append(
                {
                    **item,
                    "related_person": related_people.get(item["related_person_id"]),
                }
            )

        stats = {
            "facts": len(facts),
            "relationships": len(relationships),
            "parents": len(grouped_relationships.get("father", [])) + len(grouped_relationships.get("mother", [])),
            "spouses": len(grouped_relationships.get("spouses", [])),
            "children": len(grouped_relationships.get("children", [])),
        }

        return {
            "person": dict(person),
            "facts": facts,
            "relationships": relationships,
            "grouped_relationships": grouped_relationships,
            "related_people": related_people,
            "stats": stats,
            "biography": biography,
        }
    finally:
        conn.close()


def list_person_media(db_path: str, person_id: str) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT media_key, media_role, title, remote_url, local_path, mime_type,
                   bytes_size, status, memory_key, source_key, updated_at
            FROM media_items
            WHERE person_id = ?
            ORDER BY media_role, title, media_key
            """,
            (person_id,),
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def list_person_sources(db_path: str, person_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT source_key, title, citation, raw_json, updated_at
            FROM person_sources
            WHERE person_id = ?
            ORDER BY title, source_key
            LIMIT ?
            """,
            (person_id, limit),
        ).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            try:
                raw = json.loads(item.pop("raw_json") or "{}")
            except json.JSONDecodeError:
                raw = {}
            links = raw.get("links") or {}
            item["source_url"] = (
                ((links.get("description") or {}).get("href"))
                or raw.get("about")
                or None
            )
            out.append(item)
        return out
    finally:
        conn.close()


def list_person_notes(db_path: str, person_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT note_key, subject, text_value, updated_at
            FROM person_notes
            WHERE person_id = ?
            ORDER BY updated_at DESC, note_key
            LIMIT ?
            """,
            (person_id, limit),
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def list_person_memories(db_path: str, person_id: str, limit: int = 300) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT memory_key, memory_type, title, description, text_value, updated_at
            FROM person_memories
            WHERE person_id = ?
            ORDER BY updated_at DESC, memory_key
            LIMIT ?
            """,
            (person_id, limit),
        ).fetchall()
        return _rows_to_dicts(rows)
    finally:
        conn.close()


def get_person_timeline(db_path: str, person_id: str, limit: int = 140) -> List[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        person = conn.execute(
            """
            SELECT person_id, name, birth_date, birth_place, death_date, death_place
            FROM persons
            WHERE person_id = ?
            """,
            (person_id,),
        ).fetchone()
        if not person:
            return []
        fact_rows = conn.execute(
            """
            SELECT seq, fact_type, date_original, place_original, value_json
            FROM person_facts
            WHERE person_id = ?
            ORDER BY seq
            """,
            (person_id,),
        ).fetchall()
        relationship_rows = conn.execute(
            """
            SELECT r.relation_type, p.person_id, p.name, p.birth_date, p.birth_place, p.death_date, p.death_place
            FROM relationships r
            JOIN persons p ON p.person_id = r.related_person_id
            WHERE r.person_id = ?
              AND r.relation_type IN ('father', 'mother', 'spouses', 'children')
            ORDER BY r.relation_type, p.name
            """,
            (person_id,),
        ).fetchall()
    finally:
        conn.close()

    timeline: List[Dict[str, Any]] = []
    known_places: List[str] = []
    known_years: List[int] = []
    seen: set[tuple] = set()

    def add_entry(entry: Dict[str, Any]) -> None:
        year = entry.get("year")
        if not isinstance(year, int):
            return
        key = (
            str(entry.get("entry_type") or ""),
            year,
            str(entry.get("title") or ""),
            str(entry.get("place") or ""),
        )
        if key in seen:
            return
        seen.add(key)
        timeline.append(entry)

    birth_year = _extract_year(person["birth_date"])
    death_year = _extract_year(person["death_date"])
    if birth_year is not None:
        known_years.append(birth_year)
        add_entry(
            {
                "entry_type": "person_life",
                "year": birth_year,
                "year_label": str(birth_year),
                "title": "Birth",
                "description": person["birth_date"] or "",
                "place": person["birth_place"] or "",
                "source_url": None,
            }
        )
    if death_year is not None:
        known_years.append(death_year)
        add_entry(
            {
                "entry_type": "person_life",
                "year": death_year,
                "year_label": str(death_year),
                "title": "Death",
                "description": person["death_date"] or "",
                "place": person["death_place"] or "",
                "source_url": None,
            }
        )
    if person["birth_place"]:
        known_places.append(str(person["birth_place"]))
    if person["death_place"]:
        known_places.append(str(person["death_place"]))

    for row in fact_rows:
        fact = _normalize_fact(row)
        if not fact:
            continue
        if "LifeSketch" in str(fact.get("fact_type") or "") or fact.get("fact_label") == "Life Sketch":
            continue
        year = _extract_year(fact.get("date_original")) or _extract_year(fact.get("fact_value"))
        if year is None:
            continue
        known_years.append(year)
        place = str(fact.get("place_original") or "")
        if place:
            known_places.append(place)
        detail_chunks = [chunk for chunk in [fact.get("date_original"), fact.get("fact_value")] if chunk]
        add_entry(
            {
                "entry_type": "person_fact",
                "year": year,
                "year_label": str(year),
                "title": fact.get("fact_label") or "Event",
                "description": " · ".join(detail_chunks),
                "place": place,
                "source_url": fact.get("fact_url"),
            }
        )

    relation_labels = {
        "father": "Father",
        "mother": "Mother",
        "spouses": "Spouse",
        "children": "Child",
    }
    for row in relationship_rows:
        relation_label = relation_labels.get(str(row["relation_type"] or ""), "Relative")
        relation_name = str(row["name"] or row["person_id"] or relation_label).strip()
        birth_year_related = _extract_year(row["birth_date"])
        death_year_related = _extract_year(row["death_date"])
        if birth_year_related is not None:
            known_years.append(birth_year_related)
            if row["birth_place"]:
                known_places.append(str(row["birth_place"]))
            add_entry(
                {
                    "entry_type": "family_event",
                    "year": birth_year_related,
                    "year_label": str(birth_year_related),
                    "title": f"{relation_label} born",
                    "description": relation_name,
                    "place": row["birth_place"] or "",
                    "source_url": None,
                }
            )
        if death_year_related is not None:
            known_years.append(death_year_related)
            if row["death_place"]:
                known_places.append(str(row["death_place"]))
            add_entry(
                {
                    "entry_type": "family_event",
                    "year": death_year_related,
                    "year_label": str(death_year_related),
                    "title": f"{relation_label} died",
                    "description": relation_name,
                    "place": row["death_place"] or "",
                    "source_url": None,
                }
            )

    if not known_years:
        timeline.sort(key=lambda item: (item.get("year") or 0, item.get("title") or ""))
        return timeline[: max(1, int(limit))]

    current_year = datetime.now().year
    span_start = birth_year or min(known_years)
    span_end = death_year or max(known_years)
    if death_year is None and birth_year is not None:
        span_end = max(span_end, current_year)
    span_start = max(1000, int(span_start))
    span_end = min(2100, int(span_end))

    historical_events, _historical_total = familybook_db.list_historical_events(
        db_path,
        start_year=span_start,
        end_year=span_end,
        limit=900,
    )
    normalized_places = [_normalize_match_text(place) for place in known_places if place]
    places_blob = f" {' '.join(part for part in normalized_places if part)} "
    selected_historical: List[Dict[str, Any]] = []
    taken_scope_year: set[tuple[str, int]] = set()
    for event in historical_events:
        scope = str(event.get("scope") or "global")
        start_year = int(event.get("start_year") or 0)
        end_year = int(event.get("end_year") or start_year)
        if start_year < span_start or start_year > span_end:
            continue
        include = scope == "global"
        if not include:
            terms = event.get("match_terms") or []
            if not terms:
                include = False
            else:
                include = any(
                    f" {_normalize_match_text(str(term))} " in places_blob
                    for term in terms
                    if str(term).strip()
                )
        if not include:
            continue
        scope_year_key = (scope, start_year)
        if scope_year_key in taken_scope_year:
            continue
        taken_scope_year.add(scope_year_key)
        year_label = str(start_year) if start_year == end_year else f"{start_year}-{end_year}"
        distance = min(abs(start_year - year) for year in known_years) if known_years else 0
        selected_historical.append(
            {
                "distance": distance,
                "entry_type": "historical_event",
                "historical_scope": scope,
                "year": start_year,
                "year_label": year_label,
                "title": event.get("title") or "Historical event",
                "description": event.get("description") or "",
                "place": "",
                "source_url": event.get("source_url"),
            }
        )

    selected_historical.sort(
        key=lambda item: (
            0 if item.get("historical_scope") == "local" else 1,
            int(item.get("distance") or 0),
            int(item.get("year") or 0),
            str(item.get("title") or ""),
        )
    )
    local_count = 0
    global_count = 0
    for item in selected_historical:
        scope = item.get("historical_scope")
        if scope == "local":
            if local_count >= 12:
                continue
            local_count += 1
        else:
            if global_count >= 6:
                continue
            global_count += 1
        item.pop("distance", None)
        add_entry(item)

    timeline.sort(
        key=lambda item: (
            int(item.get("year") or 0),
            0 if item.get("entry_type") != "historical_event" else 1,
            str(item.get("title") or ""),
        )
    )
    return timeline[: max(1, int(limit))]


def _load_people_map(conn: sqlite3.Connection, person_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not person_ids:
        return {}
    placeholders = ",".join("?" for _ in person_ids)
    rows = conn.execute(
        f"""
        SELECT p.person_id, p.name, p.gender, p.lifespan, p.birth_date, p.death_date,
               CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END AS is_stub,
               (
                   SELECT mi.local_path
                   FROM media_items mi
                   WHERE mi.person_id = p.person_id AND mi.media_role = 'portrait' AND mi.local_path IS NOT NULL
                   ORDER BY mi.updated_at DESC
                   LIMIT 1
               ) AS portrait_local_path
        FROM persons p
        LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
        WHERE p.person_id IN ({placeholders})
        """,
        person_ids,
    ).fetchall()
    return {row["person_id"]: dict(row) for row in rows}


def _pick_default_tree_root(conn: sqlite3.Connection) -> Optional[str]:
    meta = conn.execute(
        "SELECT value FROM metadata WHERE key = 'default_root_person_id'"
    ).fetchone()
    if meta and meta["value"]:
        pid = str(meta["value"])
        row = conn.execute(
            "SELECT person_id FROM persons WHERE person_id = ?",
            (pid,),
        ).fetchone()
        if row:
            return str(row["person_id"])

    row = conn.execute(
        """
        SELECT p.person_id
        FROM persons p
        LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
        ORDER BY
            CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END ASC,
            p.name COLLATE NOCASE,
            p.person_id
        LIMIT 1
        """
    ).fetchone()
    return str(row["person_id"]) if row else None


def get_tree_view(db_path: str, root_person_id: Optional[str], mode: str = "family", depth: int = 3) -> Optional[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        if not root_person_id:
            root_person_id = _pick_default_tree_root(conn)
        if not root_person_id:
            return None
        root = conn.execute(
            """
            SELECT person_id, name, lifespan, birth_date, death_date
            FROM persons
            WHERE person_id = ?
            """,
            (root_person_id,),
        ).fetchone()
        if not root:
            return None

        rel_rows = _rows_to_dicts(
            conn.execute(
                """
                SELECT person_id, relation_type, related_person_id
                FROM relationships
                """
            ).fetchall()
        )
        rel_index: Dict[str, Dict[str, List[str]]] = {}
        all_ids = {root_person_id}
        for row in rel_rows:
            rel_index.setdefault(row["person_id"], {}).setdefault(row["relation_type"], []).append(row["related_person_id"])
            all_ids.add(row["person_id"])
            all_ids.add(row["related_person_id"])
        people = _load_people_map(conn, sorted(all_ids))

        def unique(seq: List[str]) -> List[str]:
            seen = set()
            out = []
            for value in seq:
                if not value or value in seen:
                    continue
                seen.add(value)
                out.append(value)
            return out

        levels: List[Dict[str, Any]] = []
        if mode == "family":
            parents = unique((rel_index.get(root_person_id, {}).get("father") or []) + (rel_index.get(root_person_id, {}).get("mother") or []))
            spouses = unique(rel_index.get(root_person_id, {}).get("spouses") or [])
            children = unique(rel_index.get(root_person_id, {}).get("children") or [])
            levels = [
                {"label": "Parents", "nodes": [people[pid] for pid in parents if pid in people]},
                {"label": "Selected", "nodes": [people[root_person_id]]},
                {"label": "Spouses", "nodes": [people[pid] for pid in spouses if pid in people]},
                {"label": "Children", "nodes": [people[pid] for pid in children if pid in people]},
            ]
        elif mode == "ancestors":
            current = [root_person_id]
            for generation in range(depth + 1):
                label = "Selected" if generation == 0 else f"Generation {generation}"
                levels.append({"label": label, "nodes": [people[pid] for pid in current if pid in people]})
                next_ids: List[str] = []
                for pid in current:
                    next_ids.extend(rel_index.get(pid, {}).get("father") or [])
                    next_ids.extend(rel_index.get(pid, {}).get("mother") or [])
                current = unique(next_ids)
                if not current:
                    break
        else:
            current = [root_person_id]
            for generation in range(depth + 1):
                label = "Selected" if generation == 0 else f"Generation {generation}"
                levels.append({"label": label, "nodes": [people[pid] for pid in current if pid in people]})
                next_ids = []
                for pid in current:
                    next_ids.extend(rel_index.get(pid, {}).get("children") or [])
                current = unique(next_ids)
                if not current:
                    break

        return {
            "root_person_id": root_person_id,
            "root": people.get(root_person_id, dict(root)),
            "mode": mode,
            "depth": depth,
            "levels": levels,
        }
    finally:
        conn.close()


def _describe_path_relationship(edges: List[str]) -> str:
    def ancestor_label(levels: int) -> str:
        labels = ["parent", "grandparent"] + [("great-" * (n - 2)) + "grandparent" for n in range(3, levels + 1)]
        return labels[levels - 1] if levels - 1 < len(labels) else f"{levels}-gen ancestor"

    def descendant_label(levels: int) -> str:
        labels = ["child", "grandchild"] + [("great-" * (n - 2)) + "grandchild" for n in range(3, levels + 1)]
        return labels[levels - 1] if levels - 1 < len(labels) else f"{levels}-gen descendant"

    going_up = [e for e in edges if e in ("father", "mother", "parent")]
    going_down = [e for e in edges if e == "child"]
    sideways = [e for e in edges if e == "spouse"]
    up = len(going_up)
    down = len(going_down)
    side = len(sideways)

    if side > 0 and up == 0 and down == 0:
        return "spouse"

    if side == 1 and up == 0 and down > 0 and edges[-1] == "spouse" and all(edge == "child" for edge in edges[:-1]):
        return f"{descendant_label(down)}-in-law"

    if side == 1 and down == 0 and up > 0 and edges[-1] == "spouse" and all(edge in ('father', 'mother', 'parent') for edge in edges[:-1]):
        return f"{ancestor_label(up)}-in-law"

    if side > 0:
        return "in-law connection"

    if up > 0 and down == 0:
        return ancestor_label(up)

    if down > 0 and up == 0:
        return descendant_label(down)

    # Find pivot: first "child" after up sequence
    pivot = next((i for i, e in enumerate(edges) if e == "child"), None)
    if pivot is None or not all(edge in ("father", "mother", "parent") for edge in edges[:pivot]) or not all(edge == "child" for edge in edges[pivot:]):
        return f"{len(edges)} steps"

    p1 = pivot          # steps going up from source
    p2 = len(edges) - pivot  # steps going down to target
    degree = min(p1, p2) - 1
    removed = abs(p1 - p2)
    if degree < 0:
        return f"{len(edges)} steps"

    if degree == 0:
        if removed == 0:
            return "sibling"
        if removed == 1:
            return "aunt/uncle" if p1 > p2 else "niece/nephew"
        if removed == 2:
            return "grand-aunt/uncle" if p1 > p2 else "grand-niece/nephew"
        return f"{removed - 1}× grand-aunt/uncle" if p1 > p2 else f"{removed - 1}× grand-niece/nephew"

    ordinals = {1: "1st", 2: "2nd", 3: "3rd"}
    label = f"{ordinals.get(degree, str(degree) + 'th')} cousin"
    if removed:
        label += f" {removed}× removed"
    return label


def _build_relationship_graph(conn) -> Dict[str, List[Dict[str, str]]]:
    rows = _rows_to_dicts(
        conn.execute("SELECT person_id, relation_type, related_person_id FROM relationships").fetchall()
    )
    graph_map: Dict[str, Dict[str, str]] = {}
    reverse_labels = {"father": "child", "mother": "child", "children": "parent", "spouses": "spouse"}

    def canonical_label(label: str) -> str:
        raw = str(label or "").strip().lower()
        if raw in ("spouses", "spouse"):
            return "spouse"
        if raw in ("children", "child"):
            return "child"
        if raw in ("father", "mother", "parent"):
            return raw
        return raw

    def choose_label(existing: str | None, candidate: str) -> str:
        priorities = {"father": 5, "mother": 5, "parent": 4, "child": 3, "spouse": 2}
        candidate = canonical_label(candidate)
        if not existing:
            return candidate
        existing = canonical_label(existing)
        return candidate if priorities.get(candidate, 0) > priorities.get(existing, 0) else existing

    def add_edge(source: str, target: str, label: str) -> None:
        if not source or not target or source == target:
            return
        bucket = graph_map.setdefault(source, {})
        bucket[target] = choose_label(bucket.get(target), label)

    for row in rows:
        add_edge(str(row["person_id"] or ""), str(row["related_person_id"] or ""), str(row["relation_type"] or ""))
        reverse_label = reverse_labels.get(row["relation_type"], row["relation_type"])
        add_edge(str(row["related_person_id"] or ""), str(row["person_id"] or ""), str(reverse_label or ""))
    graph: Dict[str, List[Dict[str, str]]] = {}
    for source, targets in graph_map.items():
        graph[source] = [{"to": target, "label": label} for target, label in sorted(targets.items())]
    return graph


def _find_k_shortest_paths(
    graph: Dict[str, List[Dict[str, str]]],
    source: str,
    target: str,
    k: int = 3,
    max_depth: int = 24,
) -> List[tuple]:
    """Find up to k shortest simple paths using priority-queue BFS (Yen's variant)."""
    import heapq

    if source == target:
        return [([source], [])]

    counter = 0
    # (cost, tie_counter, path_nodes_tuple, path_edges_tuple)
    heap = [(0, counter, (source,), ())]
    pop_count: Dict[str, int] = {}
    paths = []

    while heap and len(paths) < k:
        cost, _, path_nodes, path_edges = heapq.heappop(heap)
        current = path_nodes[-1]
        pop_count[current] = pop_count.get(current, 0) + 1
        if pop_count[current] > k:
            continue
        if current == target:
            paths.append((list(path_nodes), list(path_edges)))
            continue
        if cost >= max_depth:
            continue
        for edge in graph.get(current, []):
            nxt = edge["to"]
            if nxt not in path_nodes:
                counter += 1
                heapq.heappush(heap, (
                    cost + 1, counter,
                    path_nodes + (nxt,),
                    path_edges + (edge["label"],),
                ))
    return paths


def _build_path_result(full_people: Dict, path_nodes: List[str], path_edges: List[str]) -> Dict[str, Any]:
    steps = []
    for i, pid in enumerate(path_nodes):
        node = dict(full_people.get(pid) or {"person_id": pid})
        steps.append({"person": node, "edge_to_next": path_edges[i] if i < len(path_edges) else None})
    return {
        "steps": steps,
        "relationship": _describe_path_relationship(path_edges),
        "length": len(path_nodes),
    }


def _filter_connection_paths(raw_paths: List[tuple[List[str], List[str]]]) -> List[tuple[List[str], List[str]]]:
    if not raw_paths:
        return []
    shortest_length = min(len(edges) for _, edges in raw_paths)
    filtered = [(nodes, edges) for nodes, edges in raw_paths if len(edges) == shortest_length]
    unique: List[tuple[List[str], List[str]]] = []
    seen_signatures: set[tuple[str, ...]] = set()
    for nodes, edges in filtered:
        signature = tuple(nodes)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        unique.append((nodes, edges))
    return unique


def get_connection_view(
    db_path: str, source_person_id: str, target_person_id: str, max_depth: int = 15, max_paths: int = 3
) -> Optional[Dict[str, Any]]:
    conn = _connect(db_path)
    try:
        people = _load_people_map(conn, [source_person_id, target_person_id])
        if source_person_id not in people or target_person_id not in people:
            return None

        graph = _build_relationship_graph(conn)
        max_depth = max(1, min(int(max_depth), 30))
        max_paths = max(1, min(int(max_paths), 5))

        raw_paths = _filter_connection_paths(
            _find_k_shortest_paths(graph, source_person_id, target_person_id, k=max_paths, max_depth=max_depth)
        )

        if not raw_paths:
            return {
                "source": people[source_person_id],
                "target": people[target_person_id],
                "path": [],
                "paths": [],
                "found": False,
            }

        all_node_ids = list({pid for path_nodes, _ in raw_paths for pid in path_nodes})
        full_people = _load_people_map(conn, all_node_ids)

        result_paths = [_build_path_result(full_people, pn, pe) for pn, pe in raw_paths]

        # Keep legacy `path` key (first path) for backward compat
        first_path_nodes, first_path_edges = raw_paths[0]
        legacy_path = []
        for i, pid in enumerate(first_path_nodes):
            node = dict(full_people.get(pid) or {"person_id": pid})
            legacy_path.append({"person": node, "edge_to_next": first_path_edges[i] if i < len(first_path_edges) else None})

        return {
            "source": full_people.get(source_person_id, people[source_person_id]),
            "target": full_people.get(target_person_id, people[target_person_id]),
            "path": legacy_path,
            "paths": result_paths,
            "found": True,
            "relationship": result_paths[0]["relationship"],
        }
    finally:
        conn.close()


def get_multi_connection_view(
    db_path: str, person_ids: List[str], max_depth: int = 15, max_paths: int = 2
) -> List[Dict[str, Any]]:
    """Find connections between all consecutive pairs in person_ids list."""
    conn = _connect(db_path)
    try:
        all_people = _load_people_map(conn, person_ids)
        graph = _build_relationship_graph(conn)
        max_depth = max(1, min(int(max_depth), 30))
        results = []
        for i in range(len(person_ids) - 1):
            src, tgt = person_ids[i], person_ids[i + 1]
            if src not in all_people or tgt not in all_people:
                results.append({"source": all_people.get(src, {"person_id": src}),
                                 "target": all_people.get(tgt, {"person_id": tgt}),
                                 "paths": [], "found": False})
                continue
            raw_paths = _filter_connection_paths(
                _find_k_shortest_paths(graph, src, tgt, k=max_paths, max_depth=max_depth)
            )
            if not raw_paths:
                results.append({"source": all_people[src], "target": all_people[tgt],
                                 "paths": [], "found": False})
                continue
            node_ids = list({pid for pn, _ in raw_paths for pid in pn})
            full_people = _load_people_map(conn, node_ids)
            result_paths = [_build_path_result(full_people, pn, pe) for pn, pe in raw_paths]
            results.append({
                "source": full_people.get(src, all_people[src]),
                "target": full_people.get(tgt, all_people[tgt]),
                "paths": result_paths,
                "found": True,
                "relationship": result_paths[0]["relationship"],
            })
        return results
    finally:
        conn.close()


_BOOK_I18N: Dict[str, Dict[str, Any]] = {
    "es": {
        "book_title": "Libro Genealógico de {name}",
        "generated_by": "Generado con FamilyBook · {date} · {count} personas",
        "toc": "Tabla de Contenidos",
        "personal_info": "Información personal",
        "events_facts": "Eventos y hechos",
        "family": "Familia",
        "notes": "Notas",
        "sources": "Fuentes",
        "lifespan": "Años de vida",
        "birth_date": "Nacimiento",
        "birth_place": "Lugar de nacimiento",
        "death_date": "Fallecimiento",
        "death_place": "Lugar de fallecimiento",
        "gender": "Género",
        "male": "Masculino",
        "female": "Femenino",
        "father": "Padre",
        "mother": "Madre",
        "spouses": "Cónyuge(s)",
        "children": "Hijos",
        "siblings": "Hermanos",
        "unknown": "Desconocido",
        "stub_notice": "Perfil básico — sincronización completa pendiente",
        "root_marker": "Persona raíz",
        "see_section": "ver sección",
        "no_further_data": "Sin datos adicionales registrados.",
        "generation_label": "Generación {roman} — {name}",
        "gen_name": {
            -7: "Séptimos abuelos",
            -6: "Sextos abuelos",
            -5: "Quintos abuelos",
            -4: "Tatarabuelos",
            -3: "Bisabuelos",
            -2: "Abuelos",
            -1: "Padres",
             0: "Persona raíz",
             1: "Hijos",
             2: "Nietos",
             3: "Bisnietos",
             4: "Tataranietos",
             5: "Quintos nietos",
        },
        "gen_name_default_anc": "Antecesores (gen. {n})",
        "gen_name_default_desc": "Descendientes (gen. {n})",
        "fact_labels": {
            "Birth": "Nacimiento",
            "Death": "Fallecimiento",
            "Burial": "Entierro",
            "Christening": "Bautismo",
            "Occupation": "Ocupación",
            "Residence": "Residencia",
            "Immigration": "Inmigración",
            "Emigration": "Emigración",
            "Ethnicity": "Etnia",
            "Religion": "Religión",
            "NationalId": "ID nacional",
            "LifeSketch": "Boceto de vida",
            "Affiliation": "Afiliación",
            "Marriage": "Matrimonio",
            "Divorce": "Divorcio",
            "Military": "Servicio militar",
            "Education": "Educación",
            "Graduation": "Graduación",
            "Event": "Evento",
        },
        "gender_map": {"Male": "Masculino", "Female": "Femenino", "male": "Masculino", "female": "Femenino"},
    },
    "en": {
        "book_title": "Genealogical Book of {name}",
        "generated_by": "Generated with FamilyBook · {date} · {count} people",
        "toc": "Table of Contents",
        "personal_info": "Personal information",
        "events_facts": "Events & facts",
        "family": "Family",
        "notes": "Notes",
        "sources": "Sources",
        "lifespan": "Lifespan",
        "birth_date": "Birth date",
        "birth_place": "Birth place",
        "death_date": "Death date",
        "death_place": "Death place",
        "gender": "Gender",
        "male": "Male",
        "female": "Female",
        "father": "Father",
        "mother": "Mother",
        "spouses": "Spouse(s)",
        "children": "Children",
        "siblings": "Siblings",
        "unknown": "Unknown",
        "stub_notice": "Basic profile — full sync pending",
        "root_marker": "Root person",
        "see_section": "see section",
        "no_further_data": "No further data recorded.",
        "generation_label": "Generation {roman} — {name}",
        "gen_name": {
            -7: "7th Great-grandparents",
            -6: "6th Great-grandparents",
            -5: "5th Great-grandparents",
            -4: "4th Great-grandparents",
            -3: "Great-great-grandparents",
            -2: "Great-grandparents",
            -1: "Grandparents",
             0: "Root person",
             1: "Children",
             2: "Grandchildren",
             3: "Great-grandchildren",
             4: "2nd Great-grandchildren",
             5: "3rd Great-grandchildren",
        },
        "gen_name_default_anc": "Ancestors (gen. {n})",
        "gen_name_default_desc": "Descendants (gen. {n})",
        "fact_labels": {
            "Birth": "Birth",
            "Death": "Death",
            "Burial": "Burial",
            "Christening": "Christening",
            "Occupation": "Occupation",
            "Residence": "Residence",
            "Immigration": "Immigration",
            "Emigration": "Emigration",
            "Ethnicity": "Ethnicity",
            "Religion": "Religion",
            "NationalId": "National ID",
            "LifeSketch": "Life sketch",
            "Affiliation": "Affiliation",
            "Marriage": "Marriage",
            "Divorce": "Divorce",
            "Military": "Military service",
            "Education": "Education",
            "Graduation": "Graduation",
            "Event": "Event",
        },
        "gender_map": {},
    },
}

_ROMAN = [(1000,"M"),(900,"CM"),(500,"D"),(400,"CD"),(100,"C"),(90,"XC"),(50,"L"),(40,"XL"),(10,"X"),(9,"IX"),(5,"V"),(4,"IV"),(1,"I")]


def _to_roman(n: int) -> str:
    if n <= 0:
        return str(n)
    result = ""
    for val, sym in _ROMAN:
        while n >= val:
            result += sym
            n -= val
    return result


def _bl(lx: Dict[str, Any], key: str, **kwargs: Any) -> str:
    """Get a label from the i18n dict, formatting with kwargs."""
    val = lx.get(key, key)
    if kwargs and isinstance(val, str):
        val = val.format(**kwargs)
    return str(val)


def _gen_name(lx: Dict[str, Any], gen_offset: int) -> str:
    gen_map = lx.get("gen_name", {})
    if gen_offset in gen_map:
        return gen_map[gen_offset]
    if gen_offset < 0:
        return _bl(lx, "gen_name_default_anc", n=abs(gen_offset))
    return _bl(lx, "gen_name_default_desc", n=gen_offset)


def _translate_fact_label(raw_label: str, lx: Dict[str, Any]) -> str:
    fact_labels = lx.get("fact_labels", {})
    return fact_labels.get(raw_label, raw_label)


def _assign_book_generations(
    root_id: str,
    person_ids_set: set,
    relationships: List[Dict[str, Any]],
) -> Dict[str, int]:
    """BFS from root assigning generation offsets. root=0, ancestors<0, descendants>0."""
    # Build adjacency: parent_of[pid] → set of parent IDs; children_of[pid] → set of child IDs
    parent_of: Dict[str, set] = {}
    children_of: Dict[str, set] = {}
    spouses_of: Dict[str, set] = {}
    for row in relationships:
        pid = str(row.get("person_id") or "").strip()
        rtype = str(row.get("relation_type") or "").strip()
        rpid = str(row.get("related_person_id") or "").strip()
        if not pid or not rpid:
            continue
        if rtype in ("father", "mother"):
            parent_of.setdefault(pid, set()).add(rpid)
        elif rtype == "children":
            children_of.setdefault(pid, set()).add(rpid)
        elif rtype == "spouses":
            spouses_of.setdefault(pid, set()).add(rpid)

    gen: Dict[str, int] = {root_id: 0}
    queue: deque = deque([root_id])
    while queue:
        pid = queue.popleft()
        current = gen[pid]
        for parent_id in parent_of.get(pid, set()):
            if parent_id not in gen and parent_id in person_ids_set:
                gen[parent_id] = current - 1
                queue.append(parent_id)
        for child_id in children_of.get(pid, set()):
            if child_id not in gen and child_id in person_ids_set:
                gen[child_id] = current + 1
                queue.append(child_id)
        for spouse_id in spouses_of.get(pid, set()):
            if spouse_id not in gen and spouse_id in person_ids_set:
                gen[spouse_id] = current
                queue.append(spouse_id)
    # Assign unresolved people to gen 0
    for pid in person_ids_set:
        if pid not in gen:
            gen[pid] = 0
    return gen


def _resolve_book_root_person_id(conn: sqlite3.Connection, preferred_person_id: Optional[str]) -> Optional[str]:
    preferred = str(preferred_person_id or "").strip()
    if preferred:
        row = conn.execute(
            "SELECT person_id FROM persons WHERE person_id = ?",
            (preferred,),
        ).fetchone()
        if row:
            return str(row["person_id"])
    fallback = _pick_default_tree_root(conn)
    if fallback:
        return fallback
    row = conn.execute(
        """
        SELECT p.person_id
        FROM persons p
        LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
        ORDER BY
            CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END,
            p.birth_date IS NULL,
            p.birth_date,
            p.name COLLATE NOCASE,
            p.person_id
        LIMIT 1
        """
    ).fetchone()
    return str(row["person_id"]) if row else None


def _collect_book_component_ids(conn: sqlite3.Connection, root_person_id: str) -> set[str]:
    related_rows = conn.execute(
        """
        SELECT person_id, relation_type, related_person_id
        FROM relationships
        WHERE person_id IS NOT NULL AND related_person_id IS NOT NULL
        """
    ).fetchall()
    relation_map: Dict[str, Dict[str, set[str]]] = {}
    for row in related_rows:
        left = str(row["person_id"] or "").strip()
        rel_type = str(row["relation_type"] or "").strip()
        right = str(row["related_person_id"] or "").strip()
        if not left or not right or not rel_type:
            continue
        relation_map.setdefault(left, {}).setdefault(rel_type, set()).add(right)

    seen: set[str] = set()

    def walk(person_id: str, relation_types: tuple[str, ...]) -> None:
        pending: List[str] = [person_id]
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            rels = relation_map.get(current, {})
            for relation_type in relation_types:
                for nxt in rels.get(relation_type, set()):
                    if nxt not in seen:
                        pending.append(nxt)

    walk(root_person_id, ("father", "mother"))
    ancestors = set(seen)
    for ancestor_id in list(ancestors):
        rels = relation_map.get(ancestor_id, {})
        seen.update(rels.get("spouses", set()))

    before_descendants = set(seen)
    walk(root_person_id, ("children",))
    descendants = set(seen) - before_descendants
    for descendant_id in list(descendants | {root_person_id}):
        rels = relation_map.get(descendant_id, {})
        seen.update(rels.get("spouses", set()))

    return seen or {root_person_id}


def generate_book_markdown(
    db_path: str,
    root_person_id: Optional[str] = None,
    lang: str = "es",
) -> str:
    """Generate a genealogically structured Markdown book from local mirror data."""
    lx = _BOOK_I18N.get(lang) or _BOOK_I18N["es"]
    conn = _connect(db_path)
    try:
        root_id = _resolve_book_root_person_id(conn, root_person_id)
        if not root_id:
            return "# " + _bl(lx, "book_title", name="?") + "\n\n_" + _bl(lx, "no_further_data") + "_\n"

        component_ids = sorted(_collect_book_component_ids(conn, root_id))
        if not component_ids:
            return "# " + _bl(lx, "book_title", name="?") + "\n\n_" + _bl(lx, "no_further_data") + "_\n"

        ph = ",".join("?" * len(component_ids))

        # --- Batch-load all data ---
        persons = _rows_to_dicts(
            conn.execute(
                f"""
                SELECT p.person_id, p.name, p.gender, p.lifespan, p.birth_date, p.birth_place,
                       p.death_date, p.death_place,
                       CASE WHEN pss.last_fetched_at IS NULL THEN 1 ELSE 0 END AS is_stub
                FROM persons p
                LEFT JOIN person_sync_state pss ON pss.person_id = p.person_id
                WHERE p.person_id IN ({ph})
                """,
                component_ids,
            ).fetchall()
        )
        persons_by_id: Dict[str, Dict[str, Any]] = {str(p["person_id"]): p for p in persons}

        root_person = persons_by_id.get(root_id)
        if not root_person:
            return "# " + _bl(lx, "book_title", name="?") + "\n\n_" + _bl(lx, "no_further_data") + "_\n"

        all_rels = _rows_to_dicts(
            conn.execute(
                f"""
                SELECT person_id, relation_type, related_person_id
                FROM relationships
                WHERE person_id IN ({ph}) AND related_person_id IS NOT NULL
                ORDER BY person_id, relation_type
                """,
                component_ids,
            ).fetchall()
        )
        # Group relationships by person_id → {rel_type: [related_id, ...]}
        rels_by_person: Dict[str, Dict[str, List[str]]] = {}
        for row in all_rels:
            pid = str(row["person_id"])
            rtype = str(row["relation_type"])
            rpid = str(row["related_person_id"])
            rels_by_person.setdefault(pid, {}).setdefault(rtype, []).append(rpid)

        all_facts = _rows_to_dicts(
            conn.execute(
                f"""
                SELECT person_id, seq, fact_type, date_original, place_original, value_json
                FROM person_facts
                WHERE person_id IN ({ph})
                ORDER BY person_id, seq
                """,
                component_ids,
            ).fetchall()
        )
        facts_by_person: Dict[str, List[Dict]] = {}
        for row in all_facts:
            pid = str(row["person_id"])
            raw_type = str(row["fact_type"] or "")
            raw_label = raw_type
            if raw_type.startswith("http://") or raw_type.startswith("https://"):
                tail = raw_type.rstrip("/").split("/")[-1]
                raw_label = tail or "Event"
            elif raw_type.startswith("data:,"):
                raw_label = unquote_plus(raw_type[len("data:,"):]).strip() or "Event"
            translated_label = _translate_fact_label(raw_label, lx)
            date_v = (str(row.get("date_original") or "")).strip() or None
            place_v = (str(row.get("place_original") or "")).strip() or None
            try:
                vj = json.loads(row.get("value_json") or "{}")
            except Exception:
                vj = {}
            raw_val = vj.get("value")
            fact_value = None
            if isinstance(raw_val, str):
                fact_value = raw_val.strip() or None
            elif isinstance(raw_val, dict):
                cand = raw_val.get("text") or raw_val.get("value")
                if isinstance(cand, str):
                    fact_value = cand.strip() or None
            if not (date_v or place_v or fact_value):
                continue
            facts_by_person.setdefault(pid, []).append({
                "label": translated_label,
                "date": date_v,
                "place": place_v,
                "value": fact_value,
            })

        all_notes = _rows_to_dicts(
            conn.execute(
                f"""
                SELECT person_id, subject, text_value
                FROM person_notes
                WHERE person_id IN ({ph})
                ORDER BY person_id, updated_at DESC
                """,
                component_ids,
            ).fetchall()
        )
        notes_by_person: Dict[str, List[Dict]] = {}
        for row in all_notes:
            pid = str(row["person_id"])
            notes_by_person.setdefault(pid, []).append({"subject": row["subject"], "text": row["text_value"]})

        all_sources = _rows_to_dicts(
            conn.execute(
                f"""
                SELECT person_id, title, citation
                FROM person_sources
                WHERE person_id IN ({ph})
                ORDER BY person_id, title
                """,
                component_ids,
            ).fetchall()
        )
        sources_by_person: Dict[str, List[Dict]] = {}
        for row in all_sources:
            pid = str(row["person_id"])
            sources_by_person.setdefault(pid, []).append({"title": row["title"], "citation": row["citation"]})

        # --- Assign generation levels ---
        person_ids_set = set(component_ids)
        gen_map = _assign_book_generations(root_id, person_ids_set, all_rels)

        # --- Sort people within each generation by birth date then name ---
        def _sort_key(person: Dict) -> tuple:
            return (
                gen_map.get(str(person["person_id"]), 0),
                bool(person.get("birth_date")) is False,
                str(person.get("birth_date") or ""),
                str(person.get("name") or ""),
            )

        ordered_people = sorted(persons, key=_sort_key)

        # Group by generation
        generations_grouped: List[tuple] = []
        for gen_offset, group in groupby(ordered_people, key=lambda p: gen_map.get(str(p["person_id"]), 0)):
            generations_grouped.append((gen_offset, list(group)))
        generations_grouped.sort(key=lambda x: x[0])  # oldest first

        root_name = root_person.get("name") or root_id
        today = datetime.now().strftime("%Y-%m-%d")

        # --- Build Markdown ---
        lines: List[str] = []

        # Cover
        lines.append(f"# {_bl(lx, 'book_title', name=root_name)}\n")
        lines.append(f"*{_bl(lx, 'generated_by', date=today, count=len(ordered_people))}*\n")
        lines.append("\n---\n")

        # Table of contents
        lines.append(f"## {_bl(lx, 'toc')}\n")
        roman_counter = 1
        gen_roman_map: Dict[int, str] = {}
        for gen_offset, _ in generations_grouped:
            gen_roman_map[gen_offset] = _to_roman(roman_counter)
            roman_counter += 1
        for gen_offset, group in generations_grouped:
            gen_label = _gen_name(lx, gen_offset)
            roman = gen_roman_map[gen_offset]
            root_marker = f" ◀ {_bl(lx, 'root_marker')}" if gen_offset == 0 else ""
            lines.append(f"### {_bl(lx, 'generation_label', roman=roman, name=gen_label)}{root_marker}\n")
            for person in group:
                name = person.get("name") or str(person["person_id"])
                anchor = re.sub(r"[^\w\s-]", "", name.lower()).strip().replace(" ", "-")
                lifespan = f" *({person['lifespan']})*" if person.get("lifespan") else ""
                lines.append(f"- [{name}](#{anchor}){lifespan}\n")
        lines.append("\n---\n")

        # Person profiles by generation
        for gen_offset, group in generations_grouped:
            gen_label = _gen_name(lx, gen_offset)
            roman = gen_roman_map[gen_offset]
            root_marker = f" ◀ {_bl(lx, 'root_marker')}" if gen_offset == 0 else ""
            lines.append(f"\n## {_bl(lx, 'generation_label', roman=roman, name=gen_label)}{root_marker}\n")

            for person in group:
                pid = str(person["person_id"])
                name = person.get("name") or pid
                anchor = re.sub(r"[^\w\s-]", "", name.lower()).strip().replace(" ", "-")
                lifespan_str = f" ({person['lifespan']})" if person.get("lifespan") else ""
                lines.append(f"\n### {name}{lifespan_str}\n")

                if person.get("is_stub"):
                    lines.append(f"*{_bl(lx, 'stub_notice')}*\n")

                # Basic info table
                table_rows: List[str] = []
                if person.get("lifespan"):
                    table_rows.append(f"| {_bl(lx, 'lifespan')} | {person['lifespan']} |")
                if person.get("birth_date"):
                    table_rows.append(f"| {_bl(lx, 'birth_date')} | {person['birth_date']} |")
                if person.get("birth_place"):
                    table_rows.append(f"| {_bl(lx, 'birth_place')} | {person['birth_place']} |")
                if person.get("death_date"):
                    table_rows.append(f"| {_bl(lx, 'death_date')} | {person['death_date']} |")
                if person.get("death_place"):
                    table_rows.append(f"| {_bl(lx, 'death_place')} | {person['death_place']} |")
                if person.get("gender"):
                    gender_raw = str(person["gender"])
                    gender_map = lx.get("gender_map", {})
                    gender_display = gender_map.get(gender_raw, gender_raw)
                    table_rows.append(f"| {_bl(lx, 'gender')} | {gender_display} |")
                if table_rows:
                    lines.append(f"#### {_bl(lx, 'personal_info')}\n")
                    lines.append("| | |")
                    lines.append("|---|---|")
                    lines.extend(table_rows)
                    lines.append("")

                # Events & Facts
                facts = facts_by_person.get(pid, [])
                if facts:
                    lines.append(f"\n#### {_bl(lx, 'events_facts')}\n")
                    for f in facts:
                        parts = []
                        if f["date"]:
                            parts.append(f["date"])
                        if f["place"]:
                            parts.append(f["place"])
                        if f["value"] and f["value"] not in (f["date"], f["place"]):
                            parts.append(f["value"])
                        detail = " · ".join(parts) if parts else ""
                        lines.append(f"- **{f['label']}**" + (f": {detail}" if detail else ""))
                    lines.append("")

                # Family
                person_rels = rels_by_person.get(pid, {})
                family_lines: List[str] = []
                for rel_key in ("father", "mother", "spouses", "children", "siblings"):
                    related_ids = person_rels.get(rel_key, [])
                    if not related_ids:
                        continue
                    label = _bl(lx, rel_key)
                    names_list = []
                    for rid in related_ids:
                        rp = persons_by_id.get(rid)
                        rname = (rp.get("name") or rid) if rp else rid
                        rlifespan = f" ({rp['lifespan']})" if rp and rp.get("lifespan") else ""
                        names_list.append(f"{rname}{rlifespan}")
                    family_lines.append(f"- **{label}**: {', '.join(names_list)}")
                if family_lines:
                    lines.append(f"\n#### {_bl(lx, 'family')}\n")
                    lines.extend(family_lines)
                    lines.append("")

                # Notes (up to 3)
                notes = notes_by_person.get(pid, [])[:3]
                if notes:
                    lines.append(f"\n#### {_bl(lx, 'notes')}\n")
                    for note in notes:
                        subj = str(note.get("subject") or _bl(lx, "notes"))
                        text = str(note.get("text") or "")[:400].strip()
                        lines.append(f"> **{subj}**: {text}")
                    lines.append("")

                # Sources (up to 5)
                srcs = sources_by_person.get(pid, [])[:5]
                if srcs:
                    lines.append(f"\n#### {_bl(lx, 'sources')}\n")
                    for src in srcs:
                        title = str(src.get("title") or _bl(lx, "sources"))
                        citation = str(src.get("citation") or "").strip()
                        lines.append(f"- {title}" + (f" — *{citation}*" if citation else ""))
                    lines.append("")

                lines.append("\n---\n")

        return "\n".join(lines)
    finally:
        conn.close()


def get_pedigree_view(db_path: str, root_person_id: Optional[str], generations: int = 4) -> Optional[Dict[str, Any]]:
    generations = max(2, min(int(generations), 8))
    conn = _connect(db_path)
    try:
        if not root_person_id:
            root_person_id = _pick_default_tree_root(conn)
        if not root_person_id:
            return None

        rel_rows = _rows_to_dicts(
            conn.execute(
                """
                SELECT person_id, relation_type, related_person_id
                FROM relationships
                WHERE relation_type IN ('father', 'mother')
                """
            ).fetchall()
        )
        parent_map: Dict[str, Dict[str, str]] = {}
        for row in rel_rows:
            parent_map.setdefault(row["person_id"], {})[row["relation_type"]] = row["related_person_id"]

        ahn: Dict[int, Optional[str]] = {1: root_person_id}
        max_slot = 2 ** generations
        for slot in range(1, max_slot):
            person_id = ahn.get(slot)
            if not person_id:
                continue
            gen = int(slot.bit_length() - 1)
            if gen >= generations - 1:
                continue
            parents = parent_map.get(person_id, {})
            ahn[slot * 2] = parents.get("father")
            ahn[slot * 2 + 1] = parents.get("mother")

        ids = sorted({pid for pid in ahn.values() if pid})
        people_map = _load_people_map(conn, ids)
        levels: List[Dict[str, Any]] = []
        for gen in range(generations):
            count = 2**gen
            nodes: List[Dict[str, Any]] = []
            for i in range(count):
                slot = count + i
                person_id = ahn.get(slot)
                node = {
                    "slot": slot,
                    "index": i,
                    "person_id": person_id,
                    "person": people_map.get(person_id) if person_id else None,
                }
                nodes.append(node)
            levels.append({"generation": gen, "nodes": nodes})

        edges: List[Dict[str, int]] = []
        for child_slot in range(1, 2 ** (generations - 1)):
            child_person = ahn.get(child_slot)
            if not child_person:
                continue
            for parent_slot in (child_slot * 2, child_slot * 2 + 1):
                if ahn.get(parent_slot):
                    edges.append({"from": parent_slot, "to": child_slot})

        root_person = people_map.get(root_person_id)
        if not root_person:
            row = conn.execute(
                "SELECT person_id, name, lifespan, birth_date, death_date FROM persons WHERE person_id = ?",
                (root_person_id,),
            ).fetchone()
            if not row:
                return None
            root_person = dict(row)

        return {
            "root_person_id": root_person_id,
            "root": root_person,
            "generations": generations,
            "levels": levels,
            "edges": edges,
        }
    finally:
        conn.close()
