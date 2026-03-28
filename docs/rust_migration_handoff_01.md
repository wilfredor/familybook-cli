# Rust Migration Handoff 01 (Fase 1 iniciada)

Fecha: 2026-03-20

## Estado alcanzado en esta sesión

Se inició `familybook-rs/` y se completó un primer bloque de Fase 1 (DB foundation):

1. Crate Rust nuevo: `familybook-rs`.
2. Migraciones embebidas con `refinery` en `familybook-rs/migrations/`.
3. Esquema base SQLite v9 creado en `V1__initial_schema.sql`.
4. `ensure_schema` implementado en Rust con lógica de compatibilidad de `schema_version` (v4→v9) equivalente al backend Python para columnas/tablas legacy.
5. Upsert de `metadata.schema_version=9` y defaults de sync config.
6. Pruebas de integración DB creadas y verdes.

## Archivos creados/modificados

- `familybook-rs/Cargo.toml`
- `familybook-rs/src/main.rs`
- `familybook-rs/src/lib.rs`
- `familybook-rs/src/db/mod.rs`
- `familybook-rs/migrations/V1__initial_schema.sql`
- `familybook-rs/migrations/V2__reserved.sql`
- `familybook-rs/migrations/V3__reserved.sql`
- `familybook-rs/migrations/V4__reserved.sql`
- `familybook-rs/migrations/V5__reserved.sql`
- `familybook-rs/migrations/V6__reserved.sql`
- `familybook-rs/migrations/V7__reserved.sql`
- `familybook-rs/migrations/V8__reserved.sql`
- `familybook-rs/migrations/V9__reserved.sql`
- `familybook-rs/tests/db_migrations.rs`

## Verificación ejecutada

Comandos corridos:

```bash
cd familybook-rs
cargo fmt
cargo test
cargo run -- --db-path /tmp/smoke.sqlite
```

Resultado:
- `cargo test`: 3 tests OK.
- `cargo run`: DB inicializada con `schema_version=9`.

## Qué falta (siguiente sesión)

1. Refinar estrategia de migraciones SQL versionadas reales (V1..V9) para reflejar evolución histórica exacta en vez de placeholders reservados.
2. Crear `src/db/schema.rs` y `src/db/people.rs` con modelos y queries base:
   - `list_people`
   - `get_person`
   - `get_person_timeline` (si alcanza)
3. Introducir pool de conexiones (r2d2/deadpool) en `db/mod.rs`.
4. Arrancar Fase 2 mínima:
   - `clap` subcomando `serve`
   - `axum` con `GET /api/status`
   - estado compartido básico (`AppState`) con DB.
5. Agregar test de paridad mínimo Python↔Rust para `/api/status` y `/api/people`.

## Prompt sugerido para la próxima AI/sesión

```text
Continúa la migración Rust de FamilyBook desde el estado en `familybook-rs/`.

Contexto:
- Ya existe base de Fase 1: migraciones embebidas, `ensure_schema`, compatibilidad legacy v4-v9 y tests DB verdes.
- Punto de partida principal: `familybook-rs/src/db/mod.rs` y `familybook-rs/tests/db_migrations.rs`.

Objetivo de esta sesión:
1) Completar Fase 1 con capa `db/people.rs` (queries de lectura principales) y tests.
2) Iniciar Fase 2 con servidor `axum` mínimo que exponga `GET /api/status` y, si alcanza, `GET /api/people`.
3) Mantener compatibilidad del contrato JSON con el backend Python existente.

Restricciones:
- No tocar UI.
- Cambios pequeños, probados y con commits lógicos (si se está comiteando).
- Si el alcance crece demasiado, detenerse y dejar un nuevo handoff claro.

Validación obligatoria antes de cerrar:
- `cargo fmt`
- `cargo test`
- smoke run del binario (y del server si se implementa).
```
