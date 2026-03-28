# Plan de Migración a Rust — FamilyBook

> Creado: 2026-03-20
> Actualizado: 2026-03-20
> Estado: En curso — Track GUI nativa ✅ funcional, Track HTTP/Tauri ⏳ pendiente

## Resumen Ejecutivo

FamilyBook es actualmente un servidor HTTP en Python (~450 KB de código fuente) con SQLite, una UI vanilla JS, integración con FamilySearch (OAuth2/PKCE), y un desktop wrapper en Tauri. La migración a Rust sigue **dos tracks paralelos**:

- **Track A — GUI Nativa (Iced):** Aplicación de escritorio standalone en Rust puro. Reemplaza la necesidad de correr el servidor Python para uso local. Implementado en `familybook-rs/` con Iced 0.13 + rusqlite + tokio. **Estado: funcionalmente equivalente a la web UI para flujos de lectura, con features de escritura parcialmente implementadas.**
- **Track B — HTTP Server (axum):** Servidor REST compatible con la UI vanilla JS existente y con la integración Tauri. Reemplaza `familybook_app.py` como sidecar. **Estado: no iniciado.**

Ambos tracks comparten la misma capa `db/` y el mismo archivo SQLite. La elección entre tracks depende del caso de uso: la GUI nativa es el camino más corto para eliminar Python en desktop; el servidor HTTP es necesario para la integración Tauri y para usuarios que prefieren la web UI.

---

## 1. Inventario de Componentes a Migrar

| Componente Python | Tamaño | Responsabilidad | Equivalente Rust |
|---|---|---|---|
| `familybook_app.py` | 108 KB | HTTP server + todos los endpoints | Track B: `axum` — ⏳ pendiente |
| `familybook_db.py` | 137 KB | Esquema SQLite + 19 tablas + migraciones + queries | `rusqlite` + `refinery` — ✅ hecho |
| `familybook_mirror.py` | 71 KB | Queries read-side + generación de libro | `db/book.rs`, `db/connection.rs`, `db/tree.rs` — ✅ hecho |
| `familybook_gedcom.py` | 27 KB | Parser/importador/exportador GEDCOM 5.5.1 | `gedcom/` — ⏳ pendiente |
| `familybook.py` | 114 KB | CLI + FamilySearch OAuth2/PKCE + sync | `sync/` — ⏳ pendiente |
| `familybook_tree.py` | stub | Tree CLI stub | integrado en GUI — ✅ hecho |
| `familybook_book.py` | stub | Book CLI stub | integrado en GUI — ✅ hecho |
| `run_familybook.sh` | 162 líneas | Shell wrapper | eliminado, binario directo — ✅ hecho |
| `scripts/` | varios | Build tools Python | scripts Bash/Cargo — ⏳ pendiente |
| _(nuevo)_ | — | UI de escritorio standalone | Track A: Iced GUI — ✅ hecho |

**Lo que NO se migra:**
- `ui/` — HTML/JS/CSS permanece igual para Track B (es agnóstico al backend)
- `desktop/src-tauri/` — Ya es Rust; se expande en Track B
- Pandoc — Tool externa, se invoca por `std::process::Command`

---

## 2. Stack Rust

### 2.1 Track A — GUI Nativa (implementado)

| Propósito | Crate | Estado |
|---|---|---|
| Framework GUI | `iced 0.13` (features: tokio, image, svg, canvas) | ✅ |
| Async runtime | `tokio` (full) | ✅ |
| SQLite | `rusqlite` (bundled) | ✅ |
| Migraciones | `refinery` | ✅ |
| File dialogs | `rfd 0.15` | ✅ |
| ZIP backup | `zip 0.6` | ✅ |
| Normalización Unicode | `unicode-normalization` | ✅ |
| CLI | `clap 4` | ✅ |
| Serialización | `serde` + `serde_json` | ✅ |

Pendientes para Track A:
| Propósito | Crate | Estado |
|---|---|---|
| HTTP client (FamilySearch sync) | `reqwest` | ❌ |
| 7z backup | `sevenz-rust` | ❌ |
| GEDCOM encoding | `encoding_rs` | ❌ |
| Similitud nombres (dedup) | `strsim` | ❌ |
| OAuth2 PKCE (SHA256, base64) | `sha2`, `base64`, `rand` | ❌ |

### 2.2 Track B — HTTP Server (pendiente)

| Propósito | Crate |
|---|---|
| HTTP server | `axum` |
| CORS + archivos estáticos | `tower-http` (`CorsLayer`, `ServeDir`) |
| Pool de conexiones DB | `deadpool-sqlite` o `r2d2_sqlite` |
| HTTP client | `reqwest` |
| MIME types | `mime_guess` |
| Logging | `tracing` + `tracing-subscriber` |

### 2.3 Compartido entre tracks

| Propósito | Crate |
|---|---|
| Regex | `regex` |
| Fechas | `chrono` |
| UUID / tokens | `uuid`, `rand` |
| SHA2 | `sha2` |
| Base64 | `base64` |

---

## 3. Arquitectura del Proyecto Rust

```
familybook-rs/
├── Cargo.toml
├── Cargo.lock
├── migrations/              # SQL de migraciones (v1..v9) — ⏳ aún embebidas en código
├── src/
│   ├── main.rs              # CLI (clap) + arranque GUI — ✅
│   ├── lib.rs               # re-exports públicos — ✅
│   ├── db/                  # Capa de datos compartida — ✅
│   │   ├── mod.rs           # open_connection(), ensure_schema()
│   │   ├── people.rs        # list_people, get_person_*
│   │   ├── person_detail.rs # PersonDetail completo
│   │   ├── timeline.rs      # get_person_timeline
│   │   ├── tree.rs          # FamilyTree, PedigreeTree
│   │   ├── connection.rs    # BFS paths, relationship labels
│   │   ├── dna.rs           # dna_segments, etnias, haplogrupos, matches, traits
│   │   ├── events.rs        # historical_events
│   │   ├── dedupe.rs        # scoring, ignore_pair, candidates
│   │   ├── media.rs         # media_items, sources, notes, memories
│   │   ├── book.rs          # generate_book_markdown()
│   │   ├── export.rs        # export_gedcom (stub)
│   │   ├── runs.rs          # sync_runs, stub_count
│   │   └── status.rs        # StatusPayload
│   ├── gui/                 # Track A: GUI Iced — ✅ (ver sección 4A)
│   │   ├── mod.rs           # update() + view() + subscription()
│   │   ├── state.rs         # AppState, Message, View
│   │   ├── theme.rs
│   │   ├── views/           # Explorer, Tree, Connections, DNA, Historical,
│   │   │                    # Dedupe, Data, Book, Info, Settings
│   │   └── widgets/         # SearchBar, PersonCard, PersonDetail,
│   │                        # PedigreeCanvas, DnaPainter, Sidebar
│   ├── gedcom/              # Track A+B: GEDCOM — ❌ pendiente
│   │   ├── mod.rs
│   │   ├── parser.rs
│   │   ├── importer.rs
│   │   └── exporter.rs
│   ├── sync/                # Track A+B: FamilySearch — ❌ pendiente
│   │   ├── mod.rs
│   │   ├── oauth.rs         # OAuth2 PKCE
│   │   ├── familysearch.rs  # Cliente reqwest
│   │   └── queue.rs         # Worker async
│   ├── api/                 # Track B: HTTP server — ❌ pendiente
│   │   ├── mod.rs
│   │   ├── bootstrap.rs
│   │   ├── auth.rs
│   │   ├── sync.rs
│   │   ├── people.rs
│   │   ├── dna.rs
│   │   ├── tree.rs
│   │   ├── connections.rs
│   │   ├── dedupe.rs
│   │   ├── historical.rs
│   │   ├── book.rs
│   │   ├── import_export.rs
│   │   ├── backup.rs
│   │   └── status.rs
│   └── util/                # Compartido — ❌ pendiente (lógica inline hoy)
│       ├── text.rs          # normalize_text_for_identity
│       ├── hash.rs          # hash_file SHA256
│       └── pandoc.rs        # resolve_pandoc_path, run_pandoc
└── tests/
    ├── db_book.rs           # ✅ 4 tests
    ├── db_connection.rs     # ✅ 21 tests
    ├── db_dedupe.rs         # ✅ 26 tests
    ├── db_dna.rs            # ✅ 6 tests
    ├── db_events.rs         # ✅ 15 tests
    ├── db_media.rs          # ✅ 11 tests
    ├── db_migrations.rs     # ✅ 3 tests
    ├── db_people.rs         # ✅ 10 tests
    ├── db_person_detail.rs  # ✅ 4 tests
    ├── db_runs.rs           # ✅ 6 tests
    ├── db_timeline.rs       # ✅ 4 tests
    └── db_tree.rs           # ✅ 3 tests
                             # Total: 113 tests, todos passing
```

---

## 4. Plan de Fases

### Track A — GUI Nativa (Iced)

#### Fase A1 — Capa de Base de Datos ✅ COMPLETA
- ✅ `Cargo.toml` con dependencias base
- ✅ `db/mod.rs` — `open_connection()`, `ensure_schema()` via `refinery`
- ✅ `db/people.rs` — `list_people`, `get_person_detail`, timeline, media
- ✅ `db/tree.rs` — `FamilyTree`, `PedigreeTree`
- ✅ `db/connection.rs` — BFS paths, relationship labels
- ✅ `db/dna.rs` — segmentos, etnias, haplogrupos, matches, traits
- ✅ `db/dedupe.rs` — scoring, `ignore_pair`, candidatos paginados
- ✅ `db/events.rs` — `historical_events` con filtros y paginación
- ✅ `db/media.rs` — media, sources, notes, memories
- ✅ `db/book.rs` — `generate_book_markdown()`
- ✅ `db/runs.rs` — `sync_runs`, `stub_count`
- ✅ `db/status.rs` — `StatusPayload`
- ✅ 113 tests de integración (DB abierta con tempfile real)

#### Fase A2 — GUI Base ✅ COMPLETA
- ✅ `iced::application()` con `update()`, `view()`, `subscription()`
- ✅ Sidebar de navegación (10 vistas)
- ✅ Explorer: lista de personas paginada, búsqueda con debounce real (~400ms), detalle, timeline, media counts
- ✅ Tree: árbol familiar multinivel, pedigree chart con canvas
- ✅ Settings: idioma, persona raíz, feedback visual de guardado (✓ Guardado, 3s)

#### Fase A3 — Vistas de Datos ✅ COMPLETA
- ✅ Connections: búsqueda de camino entre dos personas; path 0 completo, paths 1..N colapsados
- ✅ DNA: tabs Summary / Painter (SVG) / Traits / Ethnicity / Haplogroups / Matches
- ✅ Historical: lista paginada con filtros por texto y scope
- ✅ Dedupe: candidatos paginados, acción ignore con reload
- ✅ Data: backup ZIP/SQLite, import backup, export GEDCOM (vía DB)
- ✅ Book: generación Markdown y PDF vía pandoc
- ✅ Info: estado de DB (counts por tabla)

#### Fase A4 — FamilySearch Sync en GUI ❌ PENDIENTE
**Entregable:** vista Sync con OAuth2 PKCE, trigger de sync, barra de progreso en tiempo real.

Pasos:
1. `src/sync/oauth.rs` — PKCE (code_verifier, code_challenge SHA256, state CSRF); abrir browser + capturar callback en servidor local efímero
2. `src/sync/familysearch.rs` — cliente `reqwest`: personas, relaciones, notas, fuentes, medios, recuerdos; manejo de tokens expirados y retry
3. `src/sync/queue.rs` — worker tokio que procesa `sync_queue`, escribe en DB, emite progreso vía canal
4. `db/sync.rs` — queries sobre `sync_runs`, `sync_queue`, `person_sync_state`
5. GUI: nueva vista `Sync` con estado de autenticación, botón Login FS, botón Sync, log de progreso en tiempo real, botón Stop
6. Mensajes nuevos: `FsAuthStart`, `FsAuthCallback(String)`, `FsAuthStatus(bool)`, `SyncStart`, `SyncStop`, `SyncProgress(SyncProgressEvent)`, `SyncDone`

**Riesgo:** Retry logic, rate limiting, manejo de tokens expirados. El sync actual Python tiene lógica compleja que debe reproducirse exactamente.

#### Fase A5 — GEDCOM en GUI ❌ PENDIENTE
**Entregable:** import/export GEDCOM desde la vista Data, con preview de resultados.

Pasos:
1. `src/gedcom/parser.rs` — tokenizer línea a línea: `(level, xref, tag, value)`; soporte ANSI/UTF-8/UTF-16 via `encoding_rs`
2. `src/gedcom/importer.rs` — 5 estrategias de dedup en cascada: FSID → nombre+año → nombre+lugar+año → padre+año → muerte
3. `src/gedcom/exporter.rs` — serialización a GEDCOM 5.5.1
4. GUI Data view: botón "Importar GEDCOM" con `rfd` file picker, progreso, reporte de personas importadas/actualizadas/ignoradas
5. GUI Data view: botón "Exportar GEDCOM" que genera y guarda con `rfd`
6. Tests: roundtrip importar→exportar, estrategias de dedup, encodings

**Riesgo:** Las 5 estrategias de dedup tienen fallbacks en cascada; traducir exactamente para preservar comportamiento de la GUI Python.

#### Fase A6 — DNA CSV Import en GUI ❌ PENDIENTE
**Entregable:** importar archivos CSV de DNA desde la vista DNA con auto-detección de formato.

Pasos:
1. `src/dna/parser.rs` — auto-detección de formato por cabecera CSV: 23andMe, AncestryDNA, FTDNA
2. GUI DNA view: botón "Importar CSV" con `rfd`, parseo en `spawn_blocking`, confirmación con count de segmentos
3. Tests: parseo de los 3 formatos con archivos de muestra

#### Fase A7 — Refinamientos GUI ❌ PENDIENTE
Features menores que completan paridad funcional con la web UI:

- [ ] Bootstrap view: botón recreate-db y recover-db para setup inicial y recuperación
- [ ] Media detail inline: ver notas, fuentes y memorias completas dentro del panel de persona (hoy solo se muestran counts)
- [ ] Historical events: botón "Sync Wikipedia" en la vista Historical
- [ ] `familybook.app.json` config loading: leer token FS, assets path, port desde config en lugar de solo args CLI
- [ ] 7z backup: reemplazar `py7zr` con `sevenz-rust` en la vista Data
- [ ] `src/util/text.rs`, `hash.rs`, `pandoc.rs`: extraer lógica inline de `gui/mod.rs` a módulos reutilizables

---

### Track B — HTTP Server / Tauri (pendiente)

> Prerrequisito: Track A Fases A4 y A5 completas (sync + GEDCOM en módulos reutilizables).
> Este track comparte `db/`, `sync/`, `gedcom/`, `dna/`, `util/` con Track A.

#### Fase B1 — Servidor HTTP Base ❌ PENDIENTE
**Entregable:** servidor axum que responde a todos los endpoints de lectura, compatible con la UI actual (`ui/`).

Pasos:
1. Agregar `axum`, `tower-http`, `deadpool-sqlite` a `Cargo.toml`
2. `src/api/mod.rs` — router con CORS + `ServeDir` apuntando a `ui/`
3. `src/api/status.rs` — `GET /api/status`, `GET /api/runs`, `GET /api/stub-count`
4. `src/api/people.rs` — `GET /api/people`, `GET /api/people/{id}` y sub-recursos
5. `src/api/tree.rs` — `GET /api/tree`, `GET /api/tree/pedigree`
6. `src/api/connections.rs` — `GET /api/connection`, `GET /api/connections`
7. `src/api/dedupe.rs` — `GET /api/dedupe/candidates`, `POST /api/dedupe/ignore`
8. `src/api/historical.rs` — `GET /api/historical/events`, `POST /api/historical/events`
9. `src/api/dna.rs` — todos los endpoints DNA (lectura)
10. `src/api/book.rs` — `GET /api/book/markdown`, `GET /api/book/pdf`

**Criterio de éxito:** la UI vanilla JS funciona sin modificaciones contra el servidor Rust para flujos de solo lectura.

#### Fase B2 — Endpoints de Escritura ❌ PENDIENTE
1. `src/api/import_export.rs` — `POST /api/import/gedcom`, `GET /api/export/gedcom`
2. `src/api/backup.rs` — backup ZIP, 7z, import backup, capabilities
3. `src/api/dna.rs` — import CSV (POST), delete
4. `src/api/historical.rs` — `POST /api/historical/sync`

#### Fase B3 — Auth y Sync HTTP ❌ PENDIENTE
1. `src/api/auth.rs` — endpoints `/api/auth/familysearch/*`
2. `src/api/sync.rs` — `POST /api/sync`, `POST /api/sync/stop`, `GET /api/sync/status`
3. `src/api/bootstrap.rs` — recreate-db, recover-db

#### Fase B4 — Integración Tauri ❌ PENDIENTE
1. Refactor a crate `familybook-core` (db + sync + gedcom + util)
2. En `src-tauri/`, invocar `familybook-core` directamente (sin `spawn` de proceso Python)
3. Eliminar `build_sidecar.py` y scripts Nuitka
4. Actualizar `tauri.conf.json` para el nuevo binario
5. Tests de empaquetado en macOS, Linux, Windows

#### Fase B5 — Paridad Total y Limpieza ❌ PENDIENTE
1. Suite de paridad: ejecutar ambos servidores contra la misma DB, comparar respuestas JSON endpoint por endpoint
2. Eliminar archivos Python del repo (`familybook_*.py`, `familybook.py`, `run_familybook.sh`)
3. CI/CD: GitHub Actions para builds multiplataforma (macOS `.dmg`, Linux `.AppImage`, Windows `.msi`)
4. README actualizado

---

## 5. Estado Actual Consolidado

| Área | Track A (GUI) | Track B (HTTP) |
|---|---|---|
| DB layer (queries) | ✅ completa | ✅ reutilizable |
| Migraciones | ✅ (via refinery) | ✅ reutilizable |
| Explorer / People | ✅ | ❌ |
| Tree + Pedigree | ✅ | ❌ |
| Connections | ✅ (paths colapsados) | ❌ |
| DNA viewer | ✅ | ❌ |
| Historical events | ✅ lectura | ❌ |
| Dedupe | ✅ | ❌ |
| Book (MD + PDF) | ✅ | ❌ |
| Backup ZIP/SQLite | ✅ | ❌ |
| Settings con feedback | ✅ | — |
| Búsqueda con debounce | ✅ | — |
| GEDCOM import/export | ❌ A5 | ❌ B2 |
| DNA CSV import | ❌ A6 | ❌ B2 |
| FamilySearch sync | ❌ A4 | ❌ B3 |
| 7z backup | ❌ A7 | ❌ B2 |
| HTTP server | — | ❌ B1 |
| Tauri integration | — | ❌ B4 |

**Tests:** 113 passing, 0 failing (cobertura DB completa; GUI no tiene tests unitarios).

---

## 6. Mapeo Detallado de Endpoints a Handlers Rust (Track B)

| Endpoint | Handler Python | Handler Rust |
|---|---|---|
| `GET /api/bootstrap/status` | `_handle_bootstrap_status` | `api::bootstrap::status` |
| `POST /api/bootstrap/recreate-db` | `_handle_bootstrap_recreate_db` | `api::bootstrap::recreate_db` |
| `POST /api/bootstrap/recover-db` | `_handle_bootstrap_recover_db` | `api::bootstrap::recover_db` |
| `GET /api/auth/familysearch/status` | `_handle_auth_fs_status` | `api::auth::fs_status` |
| `GET/POST /api/auth/familysearch/start` | `_handle_auth_fs_start` | `api::auth::fs_start` |
| `GET /auth/familysearch/callback` | `_handle_auth_fs_callback` | `api::auth::fs_callback` |
| `POST /api/auth/familysearch/disconnect` | `_handle_auth_fs_disconnect` | `api::auth::fs_disconnect` |
| `GET /api/sync/status` | `_handle_sync_status` | `api::sync::status` |
| `POST /api/sync` | `_handle_sync_start` | `api::sync::start` |
| `POST /api/sync/stop` | `_handle_sync_stop` | `api::sync::stop` |
| `POST /api/import/familysearch` | `_handle_import_familysearch` | `api::sync::import_person` |
| `GET /api/people` | `_handle_list_people` | `api::people::list` |
| `GET /api/people/{id}` | `_handle_get_person` | `api::people::get` |
| `GET /api/people/{id}/media` | `_handle_person_media` | `api::people::media` |
| `GET /api/people/{id}/sources` | `_handle_person_sources` | `api::people::sources` |
| `GET /api/people/{id}/notes` | `_handle_person_notes` | `api::people::notes` |
| `GET /api/people/{id}/memories` | `_handle_person_memories` | `api::people::memories` |
| `GET /api/people/{id}/timeline` | `_handle_person_timeline` | `api::people::timeline` |
| `GET /api/people/{id}/media/download` | `_handle_person_media_download` | `api::people::media_download` |
| `POST /api/people/{id}/dna` | `_handle_dna_import_segments` | `api::dna::import_segments` |
| `POST /api/people/{id}/dna/raw` | `_handle_dna_import_raw` | `api::dna::import_raw` |
| `POST /api/people/{id}/dna/ethnicity` | `_handle_dna_import_ethnicity` | `api::dna::import_ethnicity` |
| `POST /api/people/{id}/dna/haplogroups` | `_handle_dna_import_haplogroups` | `api::dna::import_haplogroups` |
| `POST /api/people/{id}/dna/matches` | `_handle_dna_import_matches` | `api::dna::import_matches` |
| `DELETE /api/people/{id}/dna` | `_handle_dna_delete` | `api::dna::delete` |
| `GET /api/people/{id}/dna` | `_handle_dna_summary` | `api::dna::summary` |
| `GET /api/people/{id}/dna/painter` | `_handle_dna_painter` | `api::dna::painter` |
| `GET /api/people/{id}/dna/ethnicity` | `_handle_dna_ethnicity` | `api::dna::ethnicity` |
| `GET /api/people/{id}/dna/haplogroups` | `_handle_dna_haplogroups` | `api::dna::haplogroups` |
| `GET /api/people/{id}/dna/matches` | `_handle_dna_matches` | `api::dna::matches` |
| `GET /api/people/{id}/dna/traits` | `_handle_dna_traits` | `api::dna::traits` |
| `GET /api/tree` | `_handle_tree` | `api::tree::get` |
| `GET /api/tree/pedigree` | `_handle_pedigree` | `api::tree::pedigree` |
| `GET /api/connection` | `_handle_connection` | `api::connections::single` |
| `GET /api/connections` | `_handle_connections` | `api::connections::hub` |
| `GET /api/dedupe/candidates` | `_handle_dedupe_candidates` | `api::dedupe::candidates` |
| `POST /api/dedupe/ignore` | `_handle_dedupe_ignore` | `api::dedupe::ignore` |
| `GET /api/historical/events` | `_handle_historical_events` | `api::historical::list` |
| `POST /api/historical/events` | `_handle_historical_save` | `api::historical::save` |
| `POST /api/historical/sync` | `_handle_historical_sync` | `api::historical::sync` |
| `GET /api/book/markdown` | `_handle_book_markdown` | `api::book::markdown` |
| `GET /api/book/pdf` | `_handle_book_pdf` | `api::book::pdf` |
| `POST /api/import/gedcom` | `_handle_import_gedcom` | `api::import_export::import_gedcom` |
| `POST /api/import/backup` | `_handle_import_backup` | `api::backup::import` |
| `GET /api/export/gedcom` | `_handle_export_gedcom` | `api::import_export::export_gedcom` |
| `GET /api/backup/db` | `_handle_backup_db` | `api::backup::db` |
| `GET /api/backup/full` | `_handle_backup_full` | `api::backup::full_zip` |
| `GET /api/backup/full7z` | `_handle_backup_full7z` | `api::backup::full_7z` |
| `GET /api/backup/capabilities` | `_handle_backup_capabilities` | `api::backup::capabilities` |
| `GET /api/status` | `_handle_status` | `api::status::full` |
| `GET /api/runs` | `_handle_runs` | `api::status::runs` |
| `GET /api/stub-count` | `_handle_stub_count` | `api::status::stub_count` |

---

## 7. Mapeo de Algoritmos Críticos

### 7.1 Scoring de Duplicados (`familybook_db.py` → `db/dedupe.rs`) ✅ Implementado

```rust
pub struct DedupeScore {
    pub person_id_a: String,
    pub person_id_b: String,
    pub score: f64,
    pub reasons: Vec<String>,
}

fn score_pair(a: &PersonProfile, b: &PersonProfile) -> DedupeScore {
    let mut score = 0.0;
    // 1. Nombre normalizado exacto: +40
    // 2. Año de nacimiento coincide: +20
    // 3. Lugar de nacimiento normalizado coincide: +15
    // 4. Año de muerte coincide: +15
    // 5. Padres coinciden: +10 por padre
    // 6. Nombre parcial (jaro-winkler via `strsim`): +variable
}
```

### 7.2 BFS de Conexiones Genealógicas (`familybook_mirror.py` → `db/connection.rs`) ✅ Implementado

```rust
pub fn find_paths(
    conn: &Connection,
    source_id: &str,
    target_id: &str,
    max_depth: usize,
    max_paths: usize,
) -> Vec<Vec<PathNode>> {
    // BFS bidireccional para eficiencia
    // std::collections::{VecDeque, HashSet, HashMap}
    // Cada nodo lleva: person_id, relation_type, depth
}
```

### 7.3 OAuth2 PKCE (`familybook.py` → `sync/oauth.rs`) ❌ Pendiente

```rust
pub struct PkceChallenge {
    pub code_verifier: String,   // 43-128 chars URL-safe random
    pub code_challenge: String,  // BASE64URL(SHA256(verifier))
    pub state: String,           // CSRF token
}

impl PkceChallenge {
    pub fn new() -> Self {
        let verifier = generate_url_safe_random(64); // rand + base64
        let challenge = base64url_encode(sha256(verifier.as_bytes()));
        Self { code_verifier: verifier, code_challenge: challenge, state: generate_url_safe_random(32) }
    }
}
```

### 7.4 GEDCOM Dedup Cascade (`familybook_gedcom.py` → `gedcom/importer.rs`) ❌ Pendiente

Estrategias en cascada (primera que hace match gana):
1. FSID exacto (`_1_EXTERN_ID FSID:xxxx`)
2. Nombre normalizado + año de nacimiento (±1)
3. Nombre normalizado + lugar de nacimiento normalizado + año (±2)
4. Nombre normalizado + padre conocido + año de nacimiento (±3)
5. Nombre normalizado + año de muerte (±1)

Si ninguna estrategia hace match → crear persona nueva.
