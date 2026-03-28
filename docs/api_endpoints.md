# FamilyBook — API Endpoint Specification

> Spec de referencia para la reescritura en Rust.
> Servidor: `ThreadingHTTPServer` en `127.0.0.1:8766` (puerto configurable).
> Todas las respuestas son JSON salvo endpoints de descarga de archivos.

---

## Convenciones

- **Path params**: `{person_id}` — IDs de FamilySearch en mayúsculas (e.g. `XXXX-XXXX`).
- **Query params opcionales**: se omiten si no se indican.
- **Errores**: siempre `{"error": "<code>"}` con el status HTTP correspondiente.
- **Paginación**: `limit` y `offset` en todos los listados; límites por endpoint.
- **CORS**: headers presentes para `127.0.0.1`, `localhost` y `tauri://`.

---

## Errores comunes

| Código HTTP | `error` field | Significado |
|---|---|---|
| 400 | `missing_<field>` | Campo requerido ausente |
| 400 | `invalid_<field>` | Valor inválido |
| 401 | `oauth_login_required` | Se requiere autenticación |
| 404 | `not_found` | Recurso no encontrado |
| 409 | `sync_running` | Operación bloqueada por sync activo |
| 413 | `file_too_large` | Body excede 4 GB |
| 503 | `database_invalid` | DB corrupta o incompatible |
| 503 | `pandoc_unavailable` | pandoc no encontrado |
| 503 | `py7zr_not_installed` | py7zr no instalado |
| 500 | `internal_error` | Error interno del servidor |

---

## BOOTSTRAP & SISTEMA

### `GET /api/bootstrap/status`

Estado general del sistema al iniciar la app.

**Response 200**
```json
{
  "db": {
    "path": "/abs/path/familybook.sqlite",
    "exists": true,
    "valid": true,
    "error": null,
    "persons": 1234
  },
  "oauth": { "authenticated": true, "reason": null, "error": null, "client_id_set": true,
             "redirect_uri": "...", "ident_base_url": "...", "pending": null,
             "last_error": null, "last_event_at": "2024-01-01T00:00:00Z" },
  "sync_process": { "running": false, "pid": null, "returncode": null },
  "latest_run": { "id": 5, "started_at": "...", "finished_at": "...", "status": "completed",
                  "jobs_done": 100, "jobs_failed": 0 },
  "last_sync_at": "2024-01-01T00:00:00Z",
  "assets_root": "/abs/path/familybook_assets",
  "assets_exists": true,
  "ready_for_app": true,
  "has_people": true,
  "ready": true
}
```

---

### `POST /api/bootstrap/recreate-db`

Recrea la base de datos desde cero. Hace backup automático antes.

**Request body**
```json
{ "confirm": "RECREATE", "force": false }
```

- `confirm` (required): debe ser exactamente `"RECREATE"`.
- `force` (optional, bool): omite verificación de datos existentes.

**Responses**
- `200` → `{"ok": true, "backups": [...], "status": {...}}`
- `400` → `{"ok": false, "reason": "confirm_required"}`
- `409` → `{"ok": false, "reason": "sync_running" | "db_has_data"}`
- `500` → `{"ok": false, "reason": "recreate_failed", "error": "..."}`

**Side effects**: backup del sqlite existente, recreación del schema, creación de `assets/`.

---

## AUTENTICACIÓN (FamilySearch OAuth 2.0 + PKCE)

### `GET /api/auth/familysearch/status`

**Response 200**
```json
{
  "authenticated": true,
  "reason": null,
  "error": null,
  "client_id_set": true,
  "redirect_uri": "http://localhost:53682/auth/familysearch/callback",
  "ident_base_url": "https://ident.familysearch.org",
  "pending": null,
  "last_error": null,
  "last_event_at": "2024-01-01T00:00:00Z"
}
```

---

### `GET /api/auth/familysearch/start`
### `POST /api/auth/familysearch/start`

Inicia el flujo OAuth PKCE. GET acepta `?redirect=1` para redirigir directo.

**Query params (GET only)**
- `redirect` (optional): `"1"` | `"true"` | `"yes"` → HTTP 302 a `auth_url`.

**Response 200** (cuando no hay redirect)
```json
{
  "started": true,
  "auth_url": "https://ident.familysearch.org/cis-web/oauth2/v3/authorization?...",
  "state": "abc123",
  "redirect_uri": "http://localhost:53682/auth/familysearch/callback"
}
```

**Response 200** (ya autenticado)
```json
{ "started": false, "reason": "already_authenticated" }
```

---

### `GET /auth/familysearch/callback`

Manejador del callback OAuth. Recibe `code` y `state` de FamilySearch.

**Query params**: `code`, `state`, `error` (opcional), `error_description` (opcional).

**Response**: HTML con mensaje de éxito o error. No es JSON.

**Side effects**: intercambia code por tokens, guarda tokens en cache, setea `FS_ACCESS_TOKEN` en env.

---

### `POST /api/auth/familysearch/disconnect`

Elimina tokens y desconecta la sesión.

**Response 200**: `{"ok": true}`

**Side effects**: borra archivo de cache de tokens, limpia `FS_ACCESS_TOKEN` del env.

---

## SINCRONIZACIÓN

### `GET /api/sync/status`

**Response 200**
```json
{ "running": true, "pid": 12345, "returncode": null }
```

---

### `POST /api/sync/stop`

Detiene el proceso de sync activo (SIGTERM → SIGKILL).

**Response 200**: `{"stopped": true, "pid": 12345, "returncode": -15}`
**Response 409**: `{"stopped": false, "reason": "sync_not_running"}`

---

### `POST /api/sync`

Inicia sincronización con FamilySearch.

**Request body**
```json
{
  "job_limit": 100,
  "force": false,
  "stubs": false,
  "root_person_id": "XXXX-XXXX",
  "generations": 4
}
```

- `job_limit` (optional, int > 0)
- `force` (optional, bool)
- `stubs` (optional, bool): modo stub (descarga mínima)
- `root_person_id` (optional, string uppercase)
- `generations` (optional, int 1–12, default 4)

**Response 202**: `{"started": true, "pid": 12345}`
**Response 401**: OAuth requerido
**Response 409**: `{"started": false, "reason": "sync_already_running"}`

---

### `POST /api/import/familysearch`

Inicia sync desde una persona raíz específica.

**Request body**
```json
{
  "person_id": "XXXX-XXXX",
  "generations": 4,
  "force": false,
  "job_limit": null
}
```

- `person_id` (required)
- `generations` (optional, int 1–12, default 4)

**Response 202**
```json
{ "started": true, "pid": 12345, "root_person_id": "XXXX-XXXX", "generations": 4 }
```

**Side effects**: guarda `default_root_person_id` en metadata de la DB.

---

## DEDUPLICACIÓN

### `GET /api/dedupe/candidates`

Lista pares de personas posiblemente duplicadas.

**Query params**
- `limit` (optional, int 1–500, default 120)
- `offset` (optional, int >= 0, default 0)
- `min_score` (optional, int 1–100, default 55)

**Response 200**
```json
{
  "items": [
    { "person_id_a": "XXXX-XXXX", "person_id_b": "YYYY-YYYY", "score": 87, "reasons": [...] }
  ],
  "limit": 120, "offset": 0, "total": 5, "min_score": 55
}
```

---

### `POST /api/dedupe/ignore`

Marca un par como no-duplicado.

**Request body**
```json
{ "person_id_a": "XXXX-XXXX", "person_id_b": "YYYY-YYYY", "reason": "distintas personas" }
```

**Response 200**: `{"ignored": true, "person_id_a": "...", "person_id_b": "..."}`
**Response 400**: `{"error": "invalid_pair"}`

---

## EVENTOS HISTÓRICOS

### `GET /api/historical/events`

**Query params**
- `limit` (optional, int 1–1000, default 120)
- `offset` (optional, int >= 0, default 0)
- `scope` (optional): `"global"` | `"local"` | vacío (todos)
- `place` (optional, string)
- `year_from` (optional, int)
- `year_to` (optional, int)

**Response 200**
```json
{
  "items": [
    {
      "event_key": "ww2",
      "scope": "global",
      "title": "Segunda Guerra Mundial",
      "description": "...",
      "start_year": 1939,
      "end_year": 1945,
      "source_url": "https://...",
      "match_terms": ["guerra mundial", "ww2"]
    }
  ],
  "limit": 120, "offset": 0, "total": 42, "scope": null, "place": null
}
```

---

### `POST /api/historical/events`

Crea o actualiza un evento histórico.

**Request body**
```json
{
  "scope": "global",
  "title": "Nombre del evento",
  "description": "Descripción opcional",
  "start_year": 1939,
  "end_year": 1945,
  "source_url": "https://...",
  "match_terms": ["término1", "término2"],
  "event_key": "mi_evento"
}
```

- `scope` (required): `"global"` | `"local"`
- `title` (required)
- `start_year` (required, int 1000–2100)
- `end_year` (optional, default = start_year)
- `event_key` (optional, auto-generado si se omite)
- `match_terms` (optional): string CSV/newline/semicolon o array

**Validaciones**: `start_year <= end_year`, rango [1000, 2100], URL debe ser http/https.

**Response 200**: `{"saved": true, "item": {...}}`

---

### `POST /api/historical/sync`

Sincroniza eventos desde Wikipedia.

**Request body**
```json
{ "year_from": 1900, "year_to": 2025, "local_country": "Venezuela" }
```

**Response 200**: `{"counts_by_scope": {"global": 40, "local": 12}, "events_synced": 52}`

---

## BACKUP Y EXPORTACIÓN

### `GET /api/backup/capabilities`

**Response 200**
```json
{
  "py7zr": false,
  "pandoc": true,
  "pandoc_path": "/opt/homebrew/bin/pandoc",
  "pandoc_pdf_engine": "xelatex"
}
```

---

### `GET /api/backup/db`

Descarga el archivo SQLite.

**Response 200**: `application/x-sqlite3`, filename: `familybook.sqlite`

---

### `GET /api/backup/full`

Descarga ZIP con DB + assets.

**Response 200**: `application/zip`, filename: `familybook-backup.zip`

Contenido del ZIP:
```
familybook.sqlite
assets/
  portraits/
  media/
```

---

### `GET /api/backup/full7z`

Descarga 7z con DB + assets. Requiere `py7zr`.

**Response 200**: `application/x-7z-compressed`, filename: `familybook-backup.7z`
**Response 503**: `{"error": "py7zr_not_installed"}`

---

### `POST /api/import/backup`

Restaura desde un backup ZIP o 7z.

**Request body**: contenido binario del archivo ZIP o 7z (Content-Type auto-detectado).
**Límite**: 4 GB.

**Response 200**: `{"success": true, "persons": 1234}`
**Response 400**: `invalid_format` | `no_db_in_archive` | `empty_body`
**Response 409**: `sync_running`
**Response 413**: `file_too_large`
**Response 503**: `py7zr_not_installed`

**Side effects**: reemplaza DB y assets actuales con los del backup.

---

### `POST /api/import/gedcom`

Importa un archivo GEDCOM.

**Query params**
- `root_name` (optional): nombre de la persona raíz si se crea nueva

**Request body**: contenido binario del GEDCOM.

**Response 200**: objeto de estadísticas con conteos de personas/relaciones importadas.
**Response 400**: `empty_body`

---

### `GET /api/export/gedcom`

Exporta la DB completa en formato GEDCOM.

**Response 200**: `text/plain`, filename: `familybook-export.ged`

---

## LIBRO / BOOK

### `GET /api/book/markdown`

Genera el libro familiar en Markdown.

**Query params**
- `person_id` (optional): persona raíz (usa default si se omite)
- `lang` (optional, default `"es"`): código de idioma 2 letras

**Response 200**: `text/markdown`, filename: `familybook.md`

---

### `GET /api/book/pdf`

Genera el libro en PDF vía pandoc.

**Query params**: igual que `/api/book/markdown`.

**Response 200**: `application/pdf`, filename: `familybook.pdf`
**Response 503**: `pandoc_unavailable` | `pdf_engine_unavailable`

**Requisitos**: pandoc en PATH o `/opt/homebrew/bin/pandoc`, más un motor PDF (tectonic, xelatex, lualatex, pdflatex, wkhtmltopdf, weasyprint, prince).

---

## PERSONAS

### `GET /api/people`

Lista personas con búsqueda opcional.

**Query params**
- `q` (optional): texto libre, busca en nombre y datos
- `limit` (optional, int > 0, default 200)
- `offset` (optional, int >= 0, default 0)
- `include_total` (optional): `"1"` | `"true"` | `"yes"` → incluye campo `total`

**Response 200** (sin `include_total`)
```json
[
  {
    "person_id": "XXXX-XXXX",
    "name": "Juan Pérez",
    "gender": "Male",
    "lifespan": "1900–1980",
    "birth_date": "1 Jan 1900",
    "birth_place": "Caracas, Venezuela",
    "death_date": "15 Mar 1980",
    "death_place": null,
    "last_fetched_at": "2024-01-01T00:00:00Z",
    "is_stub": false,
    "portrait_url": "/assets/portraits/XXXX-XXXX.jpg"
  }
]
```

**Response 200** (con `include_total`)
```json
{ "items": [...], "total": 1234, "limit": 200, "offset": 0 }
```

---

### `GET /api/people/{person_id}`

Detalle completo de una persona.

**Response 200**
```json
{
  "person": {
    "person_id": "XXXX-XXXX",
    "name": "Juan Pérez",
    "gender": "Male",
    "lifespan": "1900–1980",
    "birth_date": "...", "birth_place": "...",
    "death_date": "...", "death_place": "...",
    "updated_at": "...", "last_fetched_at": "...",
    "is_stub": false, "portrait_url": "..."
  },
  "facts": [
    { "fact_type": "Birth", "date_original": "1 Jan 1900", "place_original": "Caracas", "value_json": null }
  ],
  "relationships": [...],
  "grouped_relationships": {
    "father": [...], "mother": [...], "spouses": [...], "children": [...]
  },
  "related_people": { "YYYY-YYYY": { "person_id": "...", "name": "..." } },
  "stats": { "sources_count": 3, "memories_count": 1, "media_count": 5 },
  "biography": "Texto de biografía..."
}
```

**Response 404**: `{"error": "person_not_found"}`

---

### `GET /api/people/{person_id}/media`

**Response 200**
```json
[
  {
    "media_key": "abc123",
    "media_role": "photo",
    "title": "Foto familiar",
    "remote_url": "https://...",
    "local_path": "portraits/XXXX-XXXX.jpg",
    "mime_type": "image/jpeg",
    "bytes_size": 204800,
    "status": "downloaded",
    "memory_key": null,
    "source_key": null,
    "updated_at": "2024-01-01T00:00:00Z",
    "asset_url": "/assets/portraits/XXXX-XXXX.jpg"
  }
]
```

---

### `GET /api/people/{person_id}/sources`

**Response 200**
```json
[
  {
    "source_key": "src_abc",
    "title": "Acta de nacimiento",
    "citation": "...",
    "raw_json": "...",
    "updated_at": "...",
    "source_url": "https://..."
  }
]
```

---

### `GET /api/people/{person_id}/notes`

**Response 200**
```json
[{ "subject": "Nota", "text_value": "Contenido de la nota..." }]
```

---

### `GET /api/people/{person_id}/memories`

**Response 200**: array de objetos memory con título, texto e imágenes asociadas.

---

### `GET /api/people/{person_id}/timeline`

Timeline combinado de hechos personales + eventos históricos del período de vida.

**Response 200**: array ordenado por fecha con hechos, eventos, fuentes.

---

### `GET /api/people/{person_id}/media/download`

Descarga ZIP con todos los archivos multimedia de la persona.

**Response 200**: `application/zip`, filename: `{nombre_safe}-media.zip`

---

## DNA

### `POST /api/people/{person_id}/dna`

Importa segmentos de DNA desde CSV. Auto-detecta formato.

**Request body**: texto CSV (23andMe, AncestryDNA, FTDNA, o genérico).

**Formatos soportados**:
- **23andMe**: columnas `chromosome, start, end, centimorgans, snps`
- **AncestryDNA**: columnas `chromosome, start, end, centimorgans, snps`
- **FTDNA**: columnas similares
- **Genérico**: columnas `chr/chrom/chromosome, start, end, cm/centimorgans`

**Response 200**: `{"stored": 42, "source": "23andme"}`
**Response 400**: `empty_body`

---

### `POST /api/people/{person_id}/dna/raw`

Importa SNPs crudos.

**Request body**: JSON array o CSV con columnas `rsid, chromosome, position, genotype`.

**Response 200**: `{"stored": 700000, "kind": "raw_snps"}`

---

### `POST /api/people/{person_id}/dna/ethnicity`

Importa estimaciones de etnicidad.

**Request body**: JSON array o CSV con campos `region, percentage` (+ opcionales: `reference_panel, generation_estimate, side, color_hint`).

**Response 200**: `{"stored": 12, "kind": "ethnicity"}`

---

### `POST /api/people/{person_id}/dna/haplogroups`

Importa haplogrupos.

**Request body**
```json
{
  "y_haplogroup": "R1b",
  "mt_haplogroup": "H1",
  "y_timeline": [{"label": "...", "year": 1200}],
  "mt_timeline": [],
  "notes": {}
}
```

---

### `POST /api/people/{person_id}/dna/matches`

Importa matches de DNA.

**Request body**: JSON array o CSV con datos de matches (nombre, cM, relación estimada, etc.).

**Response 200**: `{"stored": 150, "kind": "matches"}`

---

### `GET /api/people/{person_id}/dna`

Resumen general de datos DNA de la persona.

**Response 200**: objeto con resumen de segmentos, fuentes disponibles, última importación.

---

### `GET /api/people/{person_id}/dna/painter`

Datos para el pintor de cromosomas (visualización SVG).

**Response 200**: objeto con segmentos por cromosoma, listo para renderizar.

---

### `GET /api/people/{person_id}/dna/ethnicity`

**Response 200**: `{"regions": [{"region": "...", "percentage": 45.2}], "reference_panel": "..."}`

---

### `GET /api/people/{person_id}/dna/haplogroups`

**Response 200**
```json
{
  "y_haplogroup": "R1b",
  "mt_haplogroup": "H1",
  "y_timeline": [...],
  "mt_timeline": [...],
  "notes": {}
}
```

---

### `GET /api/people/{person_id}/dna/matches`

**Response 200**: array de matches con datos de conexión y parentesco estimado.

---

### `GET /api/people/{person_id}/dna/traits`

Predicciones de rasgos basadas en SNPs.

**Response 200**: objeto con predicciones de rasgos físicos y de salud.

---

### `DELETE /api/people/{person_id}/dna`

Elimina segmentos DNA de una fuente específica.

**Query params**
- `source` (required): nombre de la fuente (e.g. `"23andme"`, `"ancestrydna"`)

**Response 200**: `{"deleted": true, "source": "23andme"}`
**Response 400**: `{"error": "missing_source"}`

---

## ÁRBOL GENEALÓGICO

### `GET /api/tree`

Árbol familiar centrado en una persona.

**Query params**
- `root` (optional): person_id raíz
- `mode` (optional, default `"family"`): modo de visualización
- `depth` (optional, int 1–12, default 3)

**Response 200**
```json
{
  "root": "XXXX-XXXX",
  "levels": [
    {
      "nodes": [
        { "person_id": "XXXX-XXXX", "name": "Juan Pérez", "gender": "Male", ... }
      ]
    }
  ]
}
```

**Response 404**: persona raíz no encontrada.

---

### `GET /api/tree/pedigree`

Árbol de pedigrí (ancestros directos).

**Query params**
- `root` (optional): person_id raíz
- `generations` (optional, int 1–12, default 4)

**Response 200**: estructura similar a `/api/tree` con niveles de ancestros.

---

## CONEXIONES

### `GET /api/connection`

Encuentra la ruta de parentesco entre dos personas.

**Query params**
- `source` (required): person_id
- `target` (required): person_id
- `max_depth` (optional, int 1–30, default 15)
- `max_paths` (optional, int 1–5, default 3)

**Response 200**
```json
{
  "source": "XXXX-XXXX",
  "target": "YYYY-YYYY",
  "path": ["XXXX-XXXX", "ZZZZ-ZZZZ", "YYYY-YYYY"],
  "paths": [
    {
      "steps": [
        { "person": { "person_id": "...", "name": "..." }, "relationship": "padre" }
      ]
    }
  ]
}
```

**Response 400**: `missing_people`
**Response 404**: alguna persona no encontrada

---

### `GET /api/connections`

Conexiones entre múltiples personas.

**Query params**
- `people` (required): person IDs separados por coma (mínimo 2)
- `max_depth` (optional, int 1–30, default 15)
- `max_paths` (optional, int 1–5, default 2)

**Response 200**
```json
{
  "pairs": [
    { "source": "XXXX-XXXX", "target": "YYYY-YYYY", "paths": [...] }
  ]
}
```

**Response 400**: `{"error": "need_at_least_2"}`

---

## ESTADO Y METADATA

### `GET /api/status`

Estado completo del sistema + conteos de la DB.

**Response 200**
```json
{
  "db_path": "/abs/path/familybook.sqlite",
  "counts": {
    "persons": 1234,
    "relationships": 5678,
    "notes": 90,
    "person_sources": 300,
    "person_memories": 45,
    "media_items": 600
  },
  "metadata": {
    "last_sync_at": "2024-01-01T00:00:00Z"
  },
  "latest_run": { "id": 5, "status": "completed", ... },
  "active_run": {
    "id": 5,
    "progress_percent": 75,
    "queue": { "pending": 25, "done": 75 }
  },
  "queue_by_status": [
    { "status": "pending", "qty": 25 },
    { "status": "done", "qty": 75 }
  ],
  "sync_process": { "running": false, "pid": null, "returncode": null },
  "oauth": { "authenticated": true, ... }
}
```

---

### `GET /api/runs`

Historial de ejecuciones de sync.

**Query params**
- `limit` (optional, int 1–200, default 12)

**Response 200**
```json
[
  {
    "id": 5,
    "started_at": "2024-01-01T00:00:00Z",
    "finished_at": "2024-01-01T01:00:00Z",
    "status": "completed",
    "jobs_done": 100,
    "jobs_failed": 0,
    "persons_count": 1234,
    "relationships_count": 5678,
    "media_count": 600,
    "last_error": null
  }
]
```

---

### `GET /api/stub-count`

**Response 200**: `{"stub_count": 42}`

---

## RUTAS NO-API (Archivos estáticos)

| Path | Descripción |
|---|---|
| `GET /` | Sirve `ui/index.html` |
| `GET /index.html` | Sirve `ui/index.html` |
| `GET /assets/{path}` | Assets del proyecto (imágenes, media) desde `output/familybook_assets/` |
| `GET /files/{path}` | Archivos del directorio raíz del proyecto (con protección path traversal) |
| `GET /*` | Cualquier otra ruta → busca en `ui/` |

---

## Comportamientos especiales

- **Portrait URL**: se genera desde `local_path` en `/assets/portraits/` o fallback a URL remota.
- **Person IDs**: se convierten a mayúsculas automáticamente.
- **Generations**: siempre clampeado a rango [1, 12].
- **Archivos temporales**: los downloads los crean y eliminan automáticamente.
- **DNA CSV auto-detection**: detecta el formato por headers antes de parsear.
- **OAuth PKCE**: usa `code_verifier` + `code_challenge` S256, state aleatorio por sesión.
