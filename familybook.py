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
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import requests


DEFAULT_BASE_URL = os.getenv("FS_BASE_URL", "https://api.familysearch.org")
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"


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


def iso_year(date_str: str | None) -> Optional[int]:
    if not date_str:
        return None
    match = re.search(r"(\\d{4})", date_str)
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
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "familybook-cli/0.1",
        }
    )
    session.base_url = base_url.rstrip("/")  # type: ignore[attr-defined]
    return session


def fetch_person(session: requests.Session, person_id: str) -> Dict:
    url = f"{session.base_url}/platform/tree/persons/{person_id}"  # type: ignore[attr-defined]
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    persons = data.get("persons", [])
    if not persons:
        raise ValueError(f"No se encontró persona para ID {person_id}")
    return persons[0]


def fetch_relationships(session: requests.Session, person_id: str) -> Dict:
    url = f"{session.base_url}/platform/tree/persons/{person_id}/relationships"  # type: ignore[attr-defined]
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_ancestry(session: requests.Session, person_id: str, generations: int = 4) -> Dict[str, Dict]:
    url = f"{session.base_url}/platform/tree/ancestry"  # type: ignore[attr-defined]
    params = {"person": person_id, "generations": generations}
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    persons = data.get("persons", [])
    return {p["id"]: p for p in persons}


def fetch_descendancy(session: requests.Session, person_id: str, generations: int = 3) -> Dict[str, Dict]:
    url = f"{session.base_url}/platform/tree/descendancy"  # type: ignore[attr-defined]
    params = {"person": person_id, "generations": generations}
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    persons = data.get("persons", [])
    return {p["id"]: p for p in persons}


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


def extract_life_events(person: Dict) -> List[LifeEvent]:
    events: List[LifeEvent] = []
    for fact in person.get("facts", []):
        fact_type = fact.get("type", "")
        label = fact_type.split("/")[-1] or fact_type
        date = None
        if fact.get("date"):
            date = fact["date"].get("original") or fact["date"].get("normalized", [{}])[0].get("value")
        place = None
        if fact.get("place"):
            place = fact["place"].get("original") or fact["place"].get("normalized", [{}])[0].get("value")
        events.append(LifeEvent(label=label, date=date, place=place))
    events.sort(key=lambda e: (e.date or "9999"))
    return events


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
    resp = requests.get(WIKIDATA_SPARQL_URL, params={"query": query}, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()
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
) -> str:
    lines: List[str] = []
    lines.append(f"# Libro genealógico de {extract_name(persons[root_id])}\\n")
    for pid, person in persons.items():
        name = extract_name(person)
        lines.append(f"## {name} ({extract_lifespan(person)}) — {pid}")
        portrait = extract_portrait(person)
        if portrait:
            lines.append(f"![Foto de {name}]({portrait})")
        lines.append("")

        display = person.get("display", {})
        birth = display.get("birthPlace")
        death = display.get("deathPlace")
        lines.append("### Biografía básica")
        lines.append(f"- Nombre: {name}")
        lines.append(f"- Nacimiento: {display.get('birthDate', 'N/D')} en {birth or 'N/D'}")
        lines.append(f"- Fallecimiento: {display.get('deathDate', 'N/D')} en {death or 'N/D'}")
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
                lines.append(f"- {date_short}: {ev.label}{where}")
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
        if spouses:
            lines.append("- Cónyuges:")
            for sid in spouses:
                lines.append(f"  - {extract_name(persons.get(sid, {'display': {'name': 'Desconocido'}}))} ({sid})")
        if children:
            lines.append("- Hijos:")
            for cid in children:
                lines.append(f"  - {extract_name(persons.get(cid, {'display': {'name': 'Desconocido'}}))} ({cid})")
        if not (father or mother or spouses or children):
            lines.append("- Sin relaciones inmediatas registradas")
        lines.append("\\n---\\n")
    return "\\n".join(lines)


def parse_relationships(raw: Dict) -> Dict[str, Dict]:
    """
    Devuelve un índice simple {person_id: {father, mother, spouses, children}}
    a partir de la estructura de relationships.
    """
    rel_index: Dict[str, Dict] = {}
    for rel in raw.get("childAndParentsRelationships", []):
        child = rel.get("child", {}).get("resourceId")
        father = rel.get("father", {}).get("resourceId")
        mother = rel.get("mother", {}).get("resourceId")
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
    return rel_index


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera un libro genealógico desde FamilySearch")
    parser.add_argument("--person-id", required=True, help="ID de FamilySearch de la persona raíz")
    parser.add_argument("--generations", type=int, default=4, help="Número de generaciones a explorar")
    parser.add_argument("--output", default="family_book.md", help="Ruta de salida del libro (Markdown)")
    parser.add_argument("--context", action="store_true", help="Agregar contexto histórico usando Wikidata")
    args = parser.parse_args()

    token = os.getenv("FS_ACCESS_TOKEN")
    if not token:
        raise SystemExit("Falta token: exporta FS_ACCESS_TOKEN con tu token OAuth de FamilySearch.")

    session = build_session(token)

    # Reunir personas y relaciones
    root_person = fetch_person(session, args.person_id)
    persons: Dict[str, Dict] = {args.person_id: root_person}
    persons.update(fetch_ancestry(session, args.person_id, generations=args.generations))
    persons.update(fetch_descendancy(session, args.person_id, generations=args.generations))

    relationships_raw = fetch_relationships(session, args.person_id)
    relationships = parse_relationships(relationships_raw)

    # Eventos de contexto por persona
    context_events: Dict[str, List[ContextEvent]] = {}
    if args.context:
        for pid, person in persons.items():
            display = person.get("display", {})
            birth_place = display.get("birthPlace")
            birth_year = iso_year(display.get("birthDate"))
            death_year = iso_year(display.get("deathDate")) or (birth_year + 90 if birth_year else None)
            if not (birth_place and birth_year and death_year):
                continue
            try:
                context_events[pid] = query_wikidata_events(birth_place, birth_year, death_year)
            except requests.HTTPError:
                # No bloquea el libro si falla Wikidata
                context_events[pid] = []

    md = build_markdown(args.person_id, persons, relationships, context_events)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"Libro generado en {args.output}")


if __name__ == "__main__":
    main()
