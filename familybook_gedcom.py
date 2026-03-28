"""GEDCOM parser and DB importer for FamilyBook."""
from __future__ import annotations

from collections import defaultdict
import re
import sqlite3
import uuid
from typing import Any, Dict, List, Optional, Tuple

import familybook_db


def _parse_gedcom_lines(data: bytes) -> List[Tuple[int, Optional[str], str, str]]:
    """Parse GEDCOM file into (level, xref, tag, value) tuples."""
    lines = []
    for raw in data.decode("utf-8", errors="replace").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        # GEDCOM line: level [xref] tag [value]
        m = re.match(r"^(\d+)\s+(?:(@[^@]+@)\s+)?(\S+)\s*(.*)?$", raw)
        if not m:
            continue
        level = int(m.group(1))
        xref = m.group(2)
        tag = m.group(3)
        value = (m.group(4) or "").strip()
        lines.append((level, xref, tag, value))
    return lines


def parse_gedcom(data: bytes) -> Dict[str, Any]:
    """Returns {'individuals': {xref: {...}}, 'families': {xref: {...}}}"""
    lines = _parse_gedcom_lines(data)

    individuals: Dict[str, Dict[str, Any]] = {}
    families: Dict[str, Dict[str, Any]] = {}

    current_rec: Optional[Dict[str, Any]] = None
    current_xref: Optional[str] = None
    current_type: Optional[str] = None  # 'INDI' or 'FAM'
    current_tag_stack: List[str] = []  # track nested context

    def _get_context(depth: int) -> str:
        return current_tag_stack[depth - 1] if depth - 1 < len(current_tag_stack) else ""

    for level, xref, tag, value in lines:
        if level == 0:
            current_tag_stack = []
            if xref and tag == "INDI":
                current_rec = {"xref": xref, "name": None, "sex": None, "fsid": None,
                                "birt_date": None, "birt_place": None,
                                "deat_date": None, "deat_place": None}
                current_xref = xref
                current_type = "INDI"
                individuals[xref] = current_rec
            elif xref and tag == "FAM":
                current_rec = {"xref": xref, "husb": None, "wife": None, "chil": []}
                current_xref = xref
                current_type = "FAM"
                families[xref] = current_rec
            else:
                current_rec = None
                current_xref = None
                current_type = None
            continue

        if current_rec is None:
            continue

        # Resize stack to current depth
        if level - 1 < len(current_tag_stack):
            current_tag_stack = current_tag_stack[:level - 1]
        current_tag_stack.append(tag)

        parent_tag = current_tag_stack[level - 2] if level >= 2 else None

        if current_type == "INDI":
            if tag == "NAME" and level == 1:
                # GEDCOM name: /surname/ given
                name = value.replace("/", " ").strip()
                if name:
                    current_rec["name"] = name
            elif tag == "SEX" and level == 1:
                current_rec["sex"] = value.upper()[:1] if value else None
            elif tag in ("_FSID", "_FS_ID") and level == 1:
                current_rec["fsid"] = value or None
            elif tag == "BIRT" and level == 1:
                pass  # context set via stack
            elif tag == "DEAT" and level == 1:
                pass
            elif tag == "DATE" and level == 2:
                if parent_tag == "BIRT":
                    current_rec["birt_date"] = value or None
                elif parent_tag == "DEAT":
                    current_rec["deat_date"] = value or None
            elif tag == "PLAC" and level == 2:
                if parent_tag == "BIRT":
                    current_rec["birt_place"] = value or None
                elif parent_tag == "DEAT":
                    current_rec["deat_place"] = value or None

        elif current_type == "FAM":
            if tag == "HUSB" and level == 1:
                current_rec["husb"] = value or None
            elif tag == "WIFE" and level == 1:
                current_rec["wife"] = value or None
            elif tag == "CHIL" and level == 1:
                if value:
                    current_rec["chil"].append(value)

    return {"individuals": individuals, "families": families}


def _normalize_name(name: Optional[str]) -> str:
    if not name:
        return ""
    return re.sub(r"\s+", " ", name.strip().lower())


def _normalize_place(place: Optional[str]) -> str:
    if not place:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", place.strip().lower()).strip()


def _extract_birth_year(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    m = re.search(r"\b(\d{4})\b", date_str)
    return int(m.group(1)) if m else None


FSID_RE = re.compile(r"^[A-Z0-9]{4}-[A-Z0-9]{3,4}$")


def _looks_like_familysearch_person_id(person_id: Optional[str]) -> bool:
    return bool(person_id and FSID_RE.match(str(person_id).strip().upper()))


def _build_gedcom_parent_refs(families: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Optional[str]]]:
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for fam in families.values():
        father = fam.get("husb")
        mother = fam.get("wife")
        for child in fam.get("chil", []) or []:
            out[child] = {"father": father, "mother": mother}
    return out


def _gedcom_parent_name_pair(
    individuals: Dict[str, Dict[str, Any]],
    parent_refs: Dict[str, Dict[str, Optional[str]]],
    xref: str,
) -> Tuple[str, str]:
    refs = parent_refs.get(xref) or {}
    father_name = _normalize_name((individuals.get(refs.get("father") or "") or {}).get("name"))
    mother_name = _normalize_name((individuals.get(refs.get("mother") or "") or {}).get("name"))
    return father_name, mother_name


def _load_existing_parent_name_pairs(conn: sqlite3.Connection) -> Dict[str, Tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT
            c.person_id AS child_id,
            fp.name AS father_name,
            mp.name AS mother_name
        FROM persons c
        LEFT JOIN relationships rf
            ON rf.person_id = c.person_id AND rf.relation_type = 'father'
        LEFT JOIN persons fp
            ON fp.person_id = rf.related_person_id
        LEFT JOIN relationships rm
            ON rm.person_id = c.person_id AND rm.relation_type = 'mother'
        LEFT JOIN persons mp
            ON mp.person_id = rm.related_person_id
        """
    ).fetchall()
    out: Dict[str, Tuple[str, str]] = {}
    for row in rows:
        out[str(row["child_id"])] = (
            _normalize_name(row["father_name"]),
            _normalize_name(row["mother_name"]),
        )
    return out


def _resolve_root_person_id(
    conn: sqlite3.Connection,
    *,
    root_person_name: Optional[str],
    touched_person_ids: List[str],
) -> Optional[str]:
    if root_person_name:
        wanted = _normalize_name(root_person_name)
        if wanted:
            rows = conn.execute(
                """
                SELECT person_id, name, birth_date
                FROM persons
                WHERE name IS NOT NULL AND trim(name) <> ''
                """
            ).fetchall()
            exact: List[sqlite3.Row] = [row for row in rows if _normalize_name(row["name"]) == wanted]
            if exact:
                touched_set = set(touched_person_ids)
                exact.sort(
                    key=lambda row: (
                        0 if row["person_id"] in touched_set else 1,
                        0 if _extract_birth_year(row["birth_date"]) is not None else 1,
                        _normalize_name(row["name"]),
                        row["person_id"],
                    )
                )
                return str(exact[0]["person_id"])

    for pid in touched_person_ids:
        if pid:
            return pid

    row = conn.execute("SELECT value FROM metadata WHERE key = 'default_root_person_id'").fetchone()
    if row and row["value"]:
        exists = conn.execute("SELECT 1 FROM persons WHERE person_id = ?", (str(row["value"]),)).fetchone()
        if exists:
            return str(row["value"])
    return None


def import_gedcom_to_db(db_path: str, data: bytes, root_person_name: Optional[str] = None) -> Dict[str, Any]:
    """Upsert GEDCOM into local DB, complementing existing data and avoiding duplicates."""
    parsed = parse_gedcom(data)
    individuals = parsed["individuals"]
    families = parsed["families"]
    gedcom_parent_refs = _build_gedcom_parent_refs(families)

    conn = familybook_db.connect(db_path)
    now = familybook_db.utc_now_iso()
    persons_count_row = conn.execute("SELECT COUNT(*) FROM persons").fetchone()
    persons_count = int(persons_count_row[0]) if persons_count_row and persons_count_row[0] is not None else 0
    tree_was_empty = persons_count == 0

    # Build lookup tables for dedup
    existing_by_fsid: Dict[str, str] = {}
    existing_by_name_birth_year: Dict[Tuple[str, int], str] = {}
    existing_by_name_birth_place_year: Dict[Tuple[str, str, int], str] = {}
    existing_by_name_death_year: Dict[Tuple[str, int], str] = {}
    existing_by_name_birth_year_parents: Dict[Tuple[str, int, str, str], str] = {}
    existing_by_name_unique: Dict[str, str] = {}
    existing_by_name_all: Dict[str, List[str]] = defaultdict(list)
    existing_parent_names = _load_existing_parent_name_pairs(conn)

    for row in conn.execute("SELECT person_id, name, birth_date, birth_place, death_date FROM persons").fetchall():
        person_id = row["person_id"]
        name_norm = _normalize_name(row["name"])
        birth_year = _extract_birth_year(row["birth_date"])
        birth_place = _normalize_place(row["birth_place"])
        death_year = _extract_birth_year(row["death_date"])
        father_name, mother_name = existing_parent_names.get(str(person_id), ("", ""))
        if name_norm:
            existing_by_name_all[name_norm].append(person_id)
            if birth_year is not None:
                existing_by_name_birth_year[(name_norm, birth_year)] = person_id
                if birth_place:
                    existing_by_name_birth_place_year[(name_norm, birth_place, birth_year)] = person_id
                if father_name or mother_name:
                    existing_by_name_birth_year_parents[(name_norm, birth_year, father_name, mother_name)] = person_id
            if death_year is not None:
                existing_by_name_death_year[(name_norm, death_year)] = person_id

    for name_norm, person_ids in existing_by_name_all.items():
        unique = sorted(set(person_ids))
        if len(unique) == 1:
            existing_by_name_unique[name_norm] = unique[0]

    for row in conn.execute("SELECT key, value FROM metadata WHERE key LIKE 'fsid:%'").fetchall():
        key = str(row["key"] or "")
        pid = str(row["value"] or "")
        fsid = key.split(":", 1)[1] if ":" in key else ""
        if fsid and pid:
            existing_by_fsid[fsid] = pid

    for row in conn.execute("SELECT person_id FROM persons").fetchall():
        pid = str(row["person_id"] or "")
        if FSID_RE.match(pid):
            existing_by_fsid[pid] = pid

    imported = updated = skipped = 0
    # xref → resolved person_id
    xref_to_pid: Dict[str, str] = {}
    touched_person_ids: List[str] = []

    with conn:
        for xref, indi in individuals.items():
            name = indi.get("name") or ""
            sex = indi.get("sex")
            fsid = indi.get("fsid")
            birt_date = indi.get("birt_date")
            birt_place = indi.get("birt_place")
            deat_date = indi.get("deat_date")
            deat_place = indi.get("deat_place")
            birth_year = _extract_birth_year(birt_date)
            birth_place_norm = _normalize_place(birt_place)
            death_year = _extract_birth_year(deat_date)
            name_norm = _normalize_name(name)
            father_name_norm, mother_name_norm = _gedcom_parent_name_pair(individuals, gedcom_parent_refs, xref)

            existing_pid: Optional[str] = None

            # Dedup strategy 1: by FSID
            if fsid and fsid in existing_by_fsid:
                existing_pid = existing_by_fsid[fsid]
            # Dedup strategy 2: FSID as person_id
            elif fsid and FSID_RE.match(fsid):
                row = conn.execute("SELECT person_id FROM persons WHERE person_id = ?", (fsid,)).fetchone()
                if row:
                    existing_pid = fsid
            # Dedup strategy 3: name + birth year
            if not existing_pid and name_norm and birth_year is not None:
                existing_pid = existing_by_name_birth_year.get((name_norm, birth_year))
            # Dedup strategy 4: name + birth year + birth place
            if not existing_pid and name_norm and birth_year is not None and birth_place_norm:
                existing_pid = existing_by_name_birth_place_year.get((name_norm, birth_place_norm, birth_year))
            # Dedup strategy 5: name + birth year + parent names
            if not existing_pid and name_norm and birth_year is not None and (father_name_norm or mother_name_norm):
                existing_pid = existing_by_name_birth_year_parents.get(
                    (name_norm, birth_year, father_name_norm, mother_name_norm)
                )
            # Dedup strategy 4: name + death year
            if not existing_pid and name_norm and death_year is not None:
                existing_pid = existing_by_name_death_year.get((name_norm, death_year))
            # Dedup strategy 6: unique normalized name in DB
            if not existing_pid and name_norm:
                existing_pid = existing_by_name_unique.get(name_norm)

            if existing_pid:
                # Update existing
                conn.execute(
                    """
                    UPDATE persons SET
                        name = COALESCE(name, ?),
                        gender = COALESCE(gender, ?),
                        birth_date = COALESCE(birth_date, ?),
                        birth_place = COALESCE(birth_place, ?),
                        death_date = COALESCE(death_date, ?),
                        death_place = COALESCE(death_place, ?),
                        updated_at = ?
                    WHERE person_id = ?
                    """,
                    (name or None, sex, birt_date, birt_place, deat_date, deat_place, now, existing_pid),
                )
                xref_to_pid[xref] = existing_pid
                touched_person_ids.append(existing_pid)
                if name_norm:
                    existing_by_name_all[name_norm].append(existing_pid)
                    existing_by_name_unique[name_norm] = existing_pid
                    if birth_year is not None:
                        existing_by_name_birth_year[(name_norm, birth_year)] = existing_pid
                        if birth_place_norm:
                            existing_by_name_birth_place_year[(name_norm, birth_place_norm, birth_year)] = existing_pid
                        if father_name_norm or mother_name_norm:
                            existing_by_name_birth_year_parents[
                                (name_norm, birth_year, father_name_norm, mother_name_norm)
                            ] = existing_pid
                    if death_year is not None:
                        existing_by_name_death_year[(name_norm, death_year)] = existing_pid
                if fsid:
                    existing_by_fsid[fsid] = existing_pid
                    conn.execute(
                        """
                        INSERT INTO metadata(key, value) VALUES(?, ?)
                        ON CONFLICT(key) DO UPDATE SET value=excluded.value
                        """,
                        (f"fsid:{fsid}", existing_pid),
                    )
                updated += 1
            else:
                # Insert new person
                if fsid and FSID_RE.match(fsid):
                    new_pid = fsid
                else:
                    new_pid = str(uuid.uuid4())
                lifespan_parts = []
                if birt_date:
                    lifespan_parts.append(birt_date[:4] if len(birt_date) >= 4 else birt_date)
                if deat_date:
                    lifespan_parts.append(deat_date[:4] if len(deat_date) >= 4 else deat_date)
                lifespan = " – ".join(lifespan_parts) if lifespan_parts else None
                raw_json = familybook_db._json_dump({"gedcom_xref": xref, "name": name, "sex": sex})
                conn.execute(
                    """
                    INSERT INTO persons(person_id, name, gender, lifespan, birth_date, birth_place,
                        death_date, death_place, raw_json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(person_id) DO NOTHING
                    """,
                    (new_pid, name or None, sex, lifespan, birt_date, birt_place, deat_date, deat_place, raw_json, now),
                )
                xref_to_pid[xref] = new_pid
                touched_person_ids.append(new_pid)
                if name_norm:
                    existing_by_name_all[name_norm].append(new_pid)
                    unique_ids = sorted(set(existing_by_name_all[name_norm]))
                    if len(unique_ids) == 1:
                        existing_by_name_unique[name_norm] = unique_ids[0]
                    else:
                        existing_by_name_unique.pop(name_norm, None)
                    if birth_year is not None:
                        existing_by_name_birth_year[(name_norm, birth_year)] = new_pid
                        if birth_place_norm:
                            existing_by_name_birth_place_year[(name_norm, birth_place_norm, birth_year)] = new_pid
                        if father_name_norm or mother_name_norm:
                            existing_by_name_birth_year_parents[
                                (name_norm, birth_year, father_name_norm, mother_name_norm)
                            ] = new_pid
                    if death_year is not None:
                        existing_by_name_death_year[(name_norm, death_year)] = new_pid
                if fsid:
                    existing_by_fsid[fsid] = new_pid
                    conn.execute(
                        """
                        INSERT INTO metadata(key, value) VALUES(?, ?)
                        ON CONFLICT(key) DO UPDATE SET value=excluded.value
                        """,
                        (f"fsid:{fsid}", new_pid),
                    )
                imported += 1

        # Insert relationships from families
        for xref, fam in families.items():
            husb_xref = fam.get("husb")
            wife_xref = fam.get("wife")
            children_xrefs = fam.get("chil", [])

            husb_pid = xref_to_pid.get(husb_xref) if husb_xref else None
            wife_pid = xref_to_pid.get(wife_xref) if wife_xref else None
            child_pids = [xref_to_pid[c] for c in children_xrefs if c in xref_to_pid]

            def _upsert_rel(person_id: str, rel_type: str, related_id: str) -> None:
                conn.execute(
                    """
                    INSERT INTO relationships(person_id, relation_type, related_person_id, value_json, updated_at)
                    VALUES (?, ?, ?, '{}', ?)
                    ON CONFLICT(person_id, relation_type, related_person_id) DO NOTHING
                    """,
                    (person_id, rel_type, related_id, now),
                )

            if husb_pid and wife_pid:
                _upsert_rel(husb_pid, "spouses", wife_pid)
                _upsert_rel(wife_pid, "spouses", husb_pid)

            for child_pid in child_pids:
                if husb_pid:
                    _upsert_rel(husb_pid, "children", child_pid)
                    _upsert_rel(child_pid, "father", husb_pid)
                if wife_pid:
                    _upsert_rel(wife_pid, "children", child_pid)
                    _upsert_rel(child_pid, "mother", wife_pid)

        root_person_id = _resolve_root_person_id(
            conn,
            root_person_name=root_person_name,
            touched_person_ids=touched_person_ids,
        )
        if root_person_id:
            conn.execute(
                """
                INSERT INTO metadata(key, value) VALUES('default_root_person_id', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (root_person_id,),
            )

    conn.close()
    return {
        "imported": imported,
        "updated": updated,
        "skipped": skipped,
        "mode": "initialized" if tree_was_empty else "complemented",
        "root_person_id": root_person_id,
    }


def _gedcom_clean(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"[\r\n]+", " ", str(value)).strip()


def _gedcom_name(name: Optional[str]) -> str:
    clean = _gedcom_clean(name)
    if not clean:
        return "Unknown /Unknown/"
    if "/" in clean:
        return clean
    parts = clean.split()
    if len(parts) == 1:
        return f"{parts[0]} /Unknown/"
    return f"{' '.join(parts[:-1])} /{parts[-1]}/"


def _gender_code(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    token = str(value).strip().upper()[:1]
    if token in ("M", "F"):
        return token
    return None


def export_gedcom_from_db(db_path: str) -> bytes:
    """Export local mirror DB content to GEDCOM 5.5.1 bytes."""
    conn = familybook_db.connect(db_path)
    try:
        person_rows = conn.execute(
            """
            SELECT person_id, name, gender, birth_date, birth_place, death_date, death_place
            FROM persons
            ORDER BY name COLLATE NOCASE, person_id
            """
        ).fetchall()
        rel_rows = conn.execute(
            """
            SELECT person_id, relation_type, related_person_id
            FROM relationships
            WHERE relation_type IN ('father', 'mother', 'children', 'spouses')
            """
        ).fetchall()
    finally:
        conn.close()

    people: Dict[str, Dict[str, Any]] = {row["person_id"]: dict(row) for row in person_rows}
    genders = {pid: _gender_code(data.get("gender")) for pid, data in people.items()}

    parents_by_child: Dict[str, Dict[str, Optional[str]]] = defaultdict(lambda: {"father": None, "mother": None})
    children_by_parent: Dict[str, set[str]] = defaultdict(set)
    spouse_pairs: set[Tuple[str, str]] = set()

    for rel in rel_rows:
        src = rel["person_id"]
        rel_type = rel["relation_type"]
        dst = rel["related_person_id"]
        if rel_type in ("father", "mother"):
            parents_by_child[src][rel_type] = dst
        elif rel_type == "children":
            children_by_parent[src].add(dst)
        elif rel_type == "spouses" and src and dst and src != dst:
            spouse_pairs.add(tuple(sorted((src, dst))))

    for parent_id, children in children_by_parent.items():
        parent_gender = genders.get(parent_id)
        for child_id in children:
            holder = parents_by_child[child_id]
            if parent_gender == "M":
                if not holder["father"]:
                    holder["father"] = parent_id
            elif parent_gender == "F":
                if not holder["mother"]:
                    holder["mother"] = parent_id
            elif not holder["father"]:
                holder["father"] = parent_id
            elif not holder["mother"]:
                holder["mother"] = parent_id

    families: Dict[Tuple[Optional[str], Optional[str]], set[str]] = {}
    for child_id, refs in parents_by_child.items():
        father_id = refs.get("father")
        mother_id = refs.get("mother")
        key = (father_id, mother_id)
        families.setdefault(key, set()).add(child_id)

    for left, right in spouse_pairs:
        left_gender = genders.get(left)
        right_gender = genders.get(right)
        if left_gender == "M" and right_gender != "M":
            key = (left, right)
        elif right_gender == "M" and left_gender != "M":
            key = (right, left)
        else:
            key = (left, right)
        families.setdefault(key, set())

    person_ids = sorted(people.keys())
    person_xref = {person_id: f"@I{idx}@" for idx, person_id in enumerate(person_ids, start=1)}

    family_items = sorted(
        families.items(),
        key=lambda item: (
            item[0][0] or "",
            item[0][1] or "",
            ",".join(sorted(item[1])),
        ),
    )
    family_xref = {key: f"@F{idx}@" for idx, (key, _) in enumerate(family_items, start=1)}
    fams_by_person: Dict[str, List[str]] = defaultdict(list)
    famc_by_person: Dict[str, List[str]] = defaultdict(list)

    for (father_id, mother_id), children in family_items:
        xref = family_xref[(father_id, mother_id)]
        if father_id in person_xref:
            fams_by_person[father_id].append(xref)
        if mother_id in person_xref:
            fams_by_person[mother_id].append(xref)
        for child_id in sorted(children):
            if child_id in person_xref:
                famc_by_person[child_id].append(xref)

    lines: List[str] = [
        "0 HEAD",
        "1 SOUR FAMILYBOOK",
        "2 NAME Familybook Local Mirror",
        "1 GEDC",
        "2 VERS 5.5.1",
        "2 FORM LINEAGE-LINKED",
        "1 CHAR UTF-8",
    ]

    for person_id in person_ids:
        person = people[person_id]
        xref = person_xref[person_id]
        lines.append(f"0 {xref} INDI")
        lines.append(f"1 NAME {_gedcom_name(person.get('name'))}")
        gender = _gender_code(person.get("gender"))
        if gender:
            lines.append(f"1 SEX {gender}")
        lines.append(f"1 _FSID {person_id}")
        birth_date = _gedcom_clean(person.get("birth_date"))
        birth_place = _gedcom_clean(person.get("birth_place"))
        if birth_date or birth_place:
            lines.append("1 BIRT")
            if birth_date:
                lines.append(f"2 DATE {birth_date}")
            if birth_place:
                lines.append(f"2 PLAC {birth_place}")
        death_date = _gedcom_clean(person.get("death_date"))
        death_place = _gedcom_clean(person.get("death_place"))
        if death_date or death_place:
            lines.append("1 DEAT")
            if death_date:
                lines.append(f"2 DATE {death_date}")
            if death_place:
                lines.append(f"2 PLAC {death_place}")
        for fams in sorted(set(fams_by_person.get(person_id, []))):
            lines.append(f"1 FAMS {fams}")
        for famc in sorted(set(famc_by_person.get(person_id, []))):
            lines.append(f"1 FAMC {famc}")

    for (father_id, mother_id), children in family_items:
        xref = family_xref[(father_id, mother_id)]
        lines.append(f"0 {xref} FAM")
        if father_id in person_xref:
            lines.append(f"1 HUSB {person_xref[father_id]}")
        if mother_id in person_xref:
            lines.append(f"1 WIFE {person_xref[mother_id]}")
        for child_id in sorted(children):
            if child_id in person_xref:
                lines.append(f"1 CHIL {person_xref[child_id]}")

    lines.append("0 TRLR")
    return ("\n".join(lines) + "\n").encode("utf-8")
