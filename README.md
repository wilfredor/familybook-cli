# FamilyBook CLI

CLI para generar un libro genealógico (Markdown + PDF) desde FamilySearch, usando:
- una persona raíz (`--person-id`), o
- la persona actual del usuario autenticado (`--current-person`).

Este README está pensado también como documento de handoff para futuros agentes.

## Estado actual del proyecto (2026-03-16)
- Generación funcional de `output/family_book.md` y `output/family_book.pdf`.
- OAuth Authorization Code + PKCE funcionando con cache local de token.
- Soporte validado en beta con:
  - `FS_IDENT_BASE_URL=https://identbeta.familysearch.org`
  - `FS_BASE_URL=https://beta.familysearch.org`
  - callback `http://127.0.0.1:53682/callback` (registrado para AppKey `b008HHXNSAK8C1VCGDQ8`).
- Corrección aplicada para beta:
  - si `GET /platform/tree/persons/{pid}/relationships` devuelve `404`, se usa fallback a `GET /platform/tree/persons/{pid}/families`.
- Corrección aplicada para completitud de datos:
  - `ancestry/descendancy` se usa para descubrir IDs, luego se hidratan personas completas con `GET /platform/tree/persons?pids=...`.
- Retratos de perfil:
  - el libro ahora intenta incluir la foto de perfil de cada persona en Markdown/PDF.
  - primero usa `links.portrait`; si no viene, consulta `GET /platform/tree/persons/{pid}/portrait`.
- App web local refinada:
  - navegación separada por `Explorar`, `Herramientas` y `Preferencias`.
  - tema adicional `flat/sobrio` para mejor aprovechamiento de espacio.
  - importación FamilySearch por `person_id` raíz desde UI.
  - importación GEDCOM con raíz opcional por nombre.
  - deduplicación reforzada para evitar personas/relaciones duplicadas al complementar árboles.
  - vista `Connections` mejorada:
    - con 2 personas mantiene path detallado clásico;
    - con 3 o más personas usa vista hub con persona central seleccionable en la misma pantalla;
    - traducción de parentescos y flexión por género cuando `gender` está disponible.
  - vista `Book` corregida:
    - ahora permite elegir persona raíz desde UI;
    - Markdown/PDF se generan para la rama conectada a ese nodo (ancestros, descendientes y parejas directas), no para todo el mirror.
  - detalle de persona enriquecido:
    - biografía visible desde `LifeSketch` o notas largas locales;
    - timeline con más contexto: hechos familiares directos + más eventos históricos relevantes.
  - módulo DNA:
    - sigue ligado a una persona concreta;
    - se redujo redundancia visual interna del panel.

## Arquitectura rápida
- Único ejecutable: `familybook.py`.
- Flujo alto nivel:
  1. Resolver autenticación (token env/cache/OAuth login).
  2. Resolver persona raíz.
  3. Descubrir red (ancestros + descendencia) y obtener IDs.
  4. Leer personas completas en batch.
  5. Leer relaciones (con fallback en beta).
  6. Descargar retratos.
  7. Construir Markdown.
  8. Generar PDF (por defecto).

## Requisitos
- Python 3.10+.
- `requests`.
- Para PDF:
  - `pandoc` obligatorio.
  - engine recomendado: `weasyprint` (si está instalado se usa automáticamente).
  - fallback: `tectonic`.

Instalación mínima:
```bash
python3 -m pip install requests
```

En macOS/Homebrew (si falta PDF):
```bash
brew install pandoc weasyprint
```

## Configuración (variables de entorno)
- `FS_ACCESS_TOKEN`: token OAuth ya emitido.
- `FS_BASE_URL`: host API FamilySearch (default `https://api.familysearch.org`).
- `FS_PERSON_ID`: persona raíz por defecto.
- `FS_OAUTH_CLIENT_ID`: AppKey.
- `FS_OAUTH_REDIRECT_URI`: callback OAuth (default `http://127.0.0.1:53682/callback`).
- `FS_IDENT_BASE_URL`: host identidad (default `https://ident.familysearch.org`).
- `FS_OAUTH_SCOPE`: default `openid offline_access`.
- `FS_OAUTH_TIMEOUT_SECONDS`: timeout callback local (default `180`).
- `FS_TOKEN_CACHE_PATH`: default `~/.familybook/oauth_token.json`.

Perfil beta recomendado:
```bash
export FS_BASE_URL="https://beta.familysearch.org"
export FS_IDENT_BASE_URL="https://identbeta.familysearch.org"
export FS_OAUTH_CLIENT_ID="b008HHXNSAK8C1VCGDQ8"
export FS_OAUTH_REDIRECT_URI="http://127.0.0.1:53682/callback"
export FS_OAUTH_SCOPE="openid offline_access"
```

## Uso
Scripts dedicados:
- `familybook_book.py`: genera solo libro (Markdown/PDF).
- `familybook_tree.py`: genera solo árbol SVG.

Ejecutar con persona actual:
```bash
python3 familybook.py --current-person --generations 4 --context
```

Ejecutar con persona específica:
```bash
python3 familybook.py --person-id L29R-2Q6 --generations 4 --context
```

Login OAuth interactivo (sin browser auto-open):
```bash
python3 familybook.py --oauth-login --no-browser --oauth-client-id "$FS_OAUTH_CLIENT_ID" --current-person
```

Solo Markdown (sin PDF):
```bash
python3 familybook.py --current-person --no-pdf
```

Árbol de ancestros en SVG para impresión:
```bash
python3 familybook.py --current-person --tree-svg --tree-generations 6 --tree-svg-path output/family_tree.svg
```

Solo libro (entrypoint dedicado):
```bash
python3 familybook_book.py --current-person --generations 4
```

Solo árbol (entrypoint dedicado):
```bash
python3 familybook_tree.py --current-person --tree-generations 6 --tree-svg-path output/family_tree.svg
```

Salida por defecto:
- `output/family_book.md`
- `output/family_book.pdf`
- assets: `output/family_book_assets/portraits/` (si hay retratos descargables)
- base local opcional: `output/familybook.sqlite`
- assets de base local: `output/familybook_assets/`

## Base local SQLite
El proyecto puede sincronizar un mirror local inicial del subárbol consultado.

Incluye:
- personas completas (`persons`)
- facts (`person_facts`)
- relaciones normalizadas (`relationships`)
- notas (`person_notes`)
- fuentes (`person_sources`)
- memorias / stories / documentos textuales (`person_memories`)
- retratos descargados y catalogados (`media_items`)

Ejemplo:
```bash
python3 familybook.py --current-person --generations 4 --sync-local-db
```

Con ruta personalizada:
```bash
python3 familybook.py --person-id L29R-2Q6 --sync-local-db --local-db-path output/genealogy.sqlite
```

Con política de frescura:
```bash
python3 familybook.py --current-person --sync-local-db --sync-stale-person-hours 48
```

Con TTLs específicos por tipo:
```bash
python3 familybook.py --current-person --sync-local-db \
  --sync-stale-person-hours 24 \
  --sync-stale-notes-hours 72 \
  --sync-stale-sources-hours 72 \
  --sync-stale-memories-hours 168 \
  --sync-stale-portraits-hours 720
```

Persistencia de configuración:
- si pasas TTLs por CLI, esos valores se guardan en la base SQLite local;
- lo mismo aplica para política de reintentos (`--sync-max-retries`, `--sync-retry-delay-minutes`);
- si no pasas TTLs en corridas futuras, se reutiliza la configuración persistida de esa base;
- si la base no tiene configuración previa, se usan defaults iniciales `24/72/72/168/720` y `3` reintentos con `10` minutos de delay.

Procesar en tandas:
```bash
python3 familybook.py --current-person --sync-local-db --sync-job-limit 50
```
Eso procesa hasta 50 jobs y deja el resto en cola para reanudar después.

Forzando refresh completo:
```bash
python3 familybook.py --current-person --sync-local-db --sync-force
```

Notas:
- esta primera versión sincroniza retratos locales;
- memorias/documentos quedan indexados en base de datos a nivel de metadatos/payload;
- si los payloads de memorias/stories/documentos traen links descargables, la sync ahora intenta bajar esos binarios al blob store local.

### Sync incremental y resumible
La sync local usa una cola persistente en SQLite para poder continuar si el proceso se corta.

Tablas nuevas:
- `sync_runs`: ejecuciones con estado (`running`, `completed`, `failed`, `aborted`)
- `sync_queue`: jobs persistentes por persona/fase
- `person_sync_state`: marcas temporales por persona
- `metadata`: configuración persistida de sync y estado global

Fases actuales por persona:
- `fetch_person`
- `fetch_relationships`
- `fetch_notes`
- `fetch_sources`
- `fetch_memories`
- `download_memory_media`
- `download_portrait`

Comportamiento:
- cada job se marca `pending`, `in_progress`, `done` o `failed`;
- si un proceso muere, jobs `in_progress` viejos vuelven a `pending` al reanudar;
- cada unidad de trabajo hace commit por separado, así que no se pierde el avance previo.
- en corridas nuevas, solo se reencolan fases vencidas según TTL por tipo de fase;
- los jobs retryable respetan `sync_max_retries` y `sync_retry_delay_minutes`;
- `--sync-job-limit` permite cortar una tanda de forma controlada sin perder estado;
- `--sync-force` ignora frescura local y fuerza refresh total.

## App local
Existe un MVP de entorno gráfico local, sin dependencias externas adicionales, servido desde Python estándar.

Arranque simple:
```bash
python3 familybook_app.py
# o
./run_familybook.sh
```

URL por defecto:
```bash
http://127.0.0.1:53682
```

Configuración opcional por archivo:
- crea `familybook.app.json` (se ignora en git) a partir de `familybook.app.json.example`.
- puedes definir: `host`, `port`, `db_path`, `assets_root`.
- prioridad de configuración: `CLI > variables de entorno > familybook.app.json > defaults`.

Ejemplo:
```json
{
  "host": "127.0.0.1",
  "port": 53682,
  "db_path": "output/familybook.sqlite",
  "assets_root": "output/familybook_assets"
}
```

## Desktop empaquetado (Linux/macOS/Windows)
Para distribuir sin exponer `.py` sueltos:
- backend compilado con Nuitka (one-file);
- UI minificada para release;
- shell desktop con Tauri + sidecar;
- instaladores por plataforma (`.dmg`, `.msi`, `.deb/.rpm/.AppImage` según target).

Preflight rápido de dependencias:
```bash
python3 scripts/check_desktop_prereqs.py
```

Comando rápido:
```bash
./scripts/build_desktop.sh
```
El script crea y usa `.venv-build` automáticamente (no usa `pip` global).
En macOS, genera `.app` y luego crea `Familybook.dmg` con `hdiutil`.

Con hardening Cython opcional:
```bash
./scripts/build_desktop.sh --with-native
```

En Windows:
```powershell
.\scripts\build_desktop.ps1
```

Detalle completo:
- [docs/desktop_packaging.md](docs/desktop_packaging.md)

Incluye:
- estado del mirror;
- listado de personas;
- detalle de persona;
- media, fuentes, notas y memorias;
- biografía renderizada desde `LifeSketch`/notas cuando exista;
- timeline local más rica;
- conexiones en modo clásico y modo hub;
- libro con raíz seleccionable desde la UI;
- botón para lanzar sync local en background.
- importación desde FamilySearch por ID raíz (complementaria, incremental).
- importación GEDCOM (inicializa si DB vacía; complementa si ya existe).
- exportación GEDCOM y backups (`.zip`/`.7z`).

### Importación FamilySearch desde la app web
La app web ahora asegura OAuth antes de iniciar la importación:
- usa `FS_ACCESS_TOKEN` si ya existe;
- si hay cache y está vencido, intenta refresh token;
- si no hay token, inicia login OAuth Authorization Code + PKCE.

Regla operativa explicita para renovar token:
- si el token cacheado ya no sirve, si FamilySearch responde `401`, o si faltan descargas remotas (por ejemplo retratos) por auth, NO asumir que la app puede seguir en silencio;
- es necesario abrir el splash/popup de login exactamente como hoy lo hace la interfaz web desde `FamilySearch -> Conectar FamilySearch`;
- ese flujo debe abrir la ventana de autenticacion, pedir login en FamilySearch y volver por el callback registrado;
- solo despues de ese splash/login debe reintentarse la sync/importacion/descarga de retratos.

Variables críticas para este flujo:
- `FS_OAUTH_CLIENT_ID` (AppKey)
- `FS_OAUTH_REDIRECT_URI` (debe estar registrado en FamilySearch)
- `FS_BASE_URL` y `FS_IDENT_BASE_URL` coherentes con el entorno (beta/prod)
- opcional: `FS_TOKEN_CACHE_PATH`

Para entorno beta:
```bash
export FS_BASE_URL="https://beta.familysearch.org"
export FS_IDENT_BASE_URL="https://identbeta.familysearch.org"
export FS_OAUTH_CLIENT_ID="b008HHXNSAK8C1VCGDQ8"
export FS_OAUTH_REDIRECT_URI="http://127.0.0.1:53682/callback"
```

Nota importante:
- FamilySearch beta requiere callback/realm previamente registrados.
- Si el callback no está registrado, el login OAuth fallará con error de `redirect_uri`.

Adjuntos binarios:
- los retratos se guardan en `..._assets/portraits/`;
- adjuntos de memorias/documentos se guardan en `..._assets/memories/<person_id>/`;
- ambos quedan catalogados en `media_items` con `remote_url`, `local_path`, hash y MIME type cuando está disponible.

Estado operativo observado en este repo:
- hay muchas personas completas sin retrato local todavía;
- si FamilySearch devuelve `401`, la cola de `download_portrait` no podrá completarse hasta renovar sesión vía splash/login web.

## Flags CLI
- `--person-id`
- `--current-person`
- `--generations`
- `--context`
- `--output` (default `output/family_book.md`)
- `--pdf` (opcional, PDF ya está activo por defecto)
- `--no-pdf`
- `--prompt-token`
- `--oauth-login`
- `--no-browser`
- `--oauth-client-id`
- `--oauth-redirect-uri`
- `--oauth-scope`
- `--ident-base-url`
- `--oauth-timeout-seconds`
- `--token-cache-path`
- `--no-token-cache`
- `--tree-svg`
- `--tree-svg-path` (default `output/family_tree.svg`)
- `--tree-generations` (default: usa `--generations`)
- `--sync-local-db`
- `--local-db-path` (default `output/familybook.sqlite`)
- `--sync-stale-person-hours` (default `24`)
- `--sync-stale-notes-hours` (default `72`)
- `--sync-stale-sources-hours` (default `72`)
- `--sync-stale-memories-hours` (default `168`)
- `--sync-stale-portraits-hours` (default `720`)
- `--sync-max-retries` (default persistido `3`)
- `--sync-retry-delay-minutes` (default persistido `10`)
- `--sync-job-limit`
- `--sync-force`
- `--book-only`
- `--tree-only`

## Endpoints oficiales usados por el proyecto
Referencia base API:  
`https://developers.familysearch.org/main/reference/api-reference-guide`

Autenticación:
- `GET /cis-web/oauth2/v3/authorization`
  - Doc: `https://developers.familysearch.org/main/reference/getauthorizationpage`
  - Uso: iniciar Authorization Code + PKCE.
- `POST /cis-web/oauth2/v3/token`
  - Doc: `https://developers.familysearch.org/main/reference/getaccesstoken`
  - Uso: exchange `code -> access_token` y refresh token.

Family Tree:
- `GET /platform/tree/current-person`
  - Doc: `https://developers.familysearch.org/main/reference/readcurrenttreeperson`
  - Uso: obtener persona del usuario autenticado.
- `GET /platform/tree/persons/{pid}`
  - Doc: `https://developers.familysearch.org/main/reference/readperson`
  - Uso: leer persona individual completa.
- `GET /platform/tree/persons?pids={id1,id2,...}`
  - Doc: `https://developers.familysearch.org/main/reference/readpersons`
  - Uso: hidratación masiva de personas (facts, names, display, links).
- `GET /platform/tree/ancestry?person={pid}&generations={n}`
  - Doc: `https://developers.familysearch.org/main/reference/readancestry`
  - Uso: descubrir IDs de ancestros.
- `GET /platform/tree/descendancy?person={pid}&generations={n}`
  - Doc: `https://developers.familysearch.org/main/reference/readdescendancy`
  - Uso: descubrir IDs de descendencia.
- `GET /platform/tree/persons/{pid}/relationships`
  - Uso: relaciones inmediatas (cuando disponible).
- `GET /platform/tree/persons/{pid}/families`
  - Doc: `https://developers.familysearch.org/main/reference/readpersonfamilies`
  - Uso: fallback en beta cuando `relationships` no está habilitado.
- `GET /platform/tree/persons/{pid}/portrait`
  - Doc: `https://developers.familysearch.org/main/reference/readpersonportrait`
  - Uso: resolver retrato de perfil cuando no llega en `links.portrait`.

Wikidata (contexto opcional):
- `GET https://query.wikidata.org/sparql`
  - Uso: eventos históricos por lugar/rango de años.

## Qué datos se extraen hoy
Persona (`persons`):
- `id`
- `display.name`
- `display.gender`
- `display.birthDate`, `display.birthPlace`
- `display.deathDate`, `display.deathPlace`
- `display.lifespan`
- `facts[]` (`type`, `date`, `place`)
- `links.portrait.href`

Relaciones:
- `childAndParentsRelationships` (`father/mother` o `parent1/parent2`)
- `coupleRelationships`
- `parentChildRelationships`
- `relationships` genérico (respuesta típica de `/families` en beta)

Salida Markdown por persona:
- biografía básica
- línea de vida (orden por fecha string)
- contexto histórico (si `--context`)
- familia inmediata (padres, cónyuges, hijos)

Vista de detalle en app local:
- facts visibles filtrados;
- `LifeSketch` expuesto como bloque de biografía;
- notas, fuentes, memorias y media;
- timeline con hechos personales, familiares e históricos;
- conexiones con traducción de parentesco y fallback por género desconocido.

## Endpoints relevantes disponibles para futuras mejoras
Todos con docs oficiales en el mismo portal:
- `readpersonchildren`
- `readpersonparents`
- `readpersonspouses`
- `readpersonsources`
- `readpersonportrait` / `readpersonportraits`
- `searchtreepersons`
- `findrelationship`
- `readpersonchangehistory`

Estos pueden ayudar a enriquecer:
- familia inmediata más precisa por persona,
- fuentes y evidencia,
- historial de cambios,
- búsqueda de personas fuera del subárbol inicial.

Inventario completo de endpoints (para agentes):
```bash
curl -Ls https://developers.familysearch.org/main/reference/api-reference-guide \
  | rg -o 'href="/main/reference/[^"]+"' \
  | sed 's/href="//;s/"$//' \
  | sort -u
```
Nota: al 2026-03-09 este método devuelve ~203 slugs de endpoints de referencia.

## Decisiones técnicas importantes
- Se evita depender solo de `ancestry/descendancy` porque frecuentemente devuelven objetos resumidos.
- Se prioriza `readpersons` por lotes para completitud y rendimiento.
- En beta, se contempla explícitamente que algunos recursos de relaciones respondan `404`.
- En `Connections`, para 3+ personas ya no se usa la cadena secuencial `A->B->C` como visualización principal; se prioriza hub `centro -> cada participante`.
- La generación local de libro desde la app debe partir de un nodo raíz explícito.
- PDF se intenta siempre (a menos que `--no-pdf`):
  - prioridad `weasyprint`,
  - fallback `tectonic`,
  - si no hay engines adecuados, el `.md` sí queda generado.

## Troubleshooting rápido
- Error `401` en tree:
  - verificar que `FS_BASE_URL` y `FS_IDENT_BASE_URL` correspondan al mismo entorno.
  - ejemplo beta/beta: `https://beta.familysearch.org` + `https://identbeta.familysearch.org`.
  - si el token en cache existe pero FamilySearch igual responde `401`, renovar la sesion abriendo el splash de login desde `Conectar FamilySearch` en la interfaz web; no confiar en que el cache local siga siendo valido.
- Faltan retratos aunque exista cola pendiente:
  - revisar primero auth/OAuth; los jobs `download_portrait` no progresarán con token inválido;
  - después del relogin, reanudar la sync en lugar de reconstruir la base.
- Error de callback local:
  - confirmar que el puerto no esté ocupado.
  - confirmar callback exacto registrado en FamilySearch.
- Falta de datos (fechas/eventos):
  - verificar que el flujo esté usando `readpersons` (ya implementado).
  - revisar si el perfil realmente tiene facts en FamilySearch.
- Falla PDF:
  - instalar `pandoc` y `weasyprint`.
  - usar `--no-pdf` para no bloquear generación del Markdown.

## Archivos clave para agentes
- `familybook.py`: lógica completa.
- `README.md`: este documento técnico/handoff.
- `output/`: artefactos generados localmente.

## Convenciones operativas para futuros agentes
- No asumir que beta y producción son intercambiables.
- Siempre dejar explícito en comandos el entorno cuando se depura auth (`FS_BASE_URL` + `FS_IDENT_BASE_URL`).
- Si hay que renovar token, documentar y pedir explicitamente abrir el splash/popup de `Conectar FamilySearch`; no describirlo como refresh automatico si en la practica requiere login web interactivo.
- Si se toca `Connections`, validar ambos modos:
  - 2 personas -> path detallado clásico;
  - 3+ personas -> hub con selector de persona central.
- Si se toca traducción de parentescos, validar con género masculino, femenino y desconocido.
- Si se modifica lógica de relaciones, validar ambos caminos:
  - `/relationships`
  - `/families` fallback.
- Si se modifica salida Markdown, regenerar al menos una corrida real y validar legibilidad.
- Mantener ejemplos de ejecución reproducibles en este README.

## Seguridad
- No commitear tokens ni cache de OAuth.
- `~/.familybook/oauth_token.json` se guarda con permisos `0600`.
- Usar solo OAuth oficial; no usar usuario/clave directa.
- Mantener `output/`, `.env`, DBs locales y backups fuera de git (`.gitignore`).

## Roadmap sugerido
- Filtrado de facts ruidosos (`data:,...`) para salida más legible.
- Tests automáticos para parsing de relaciones (incluyendo payload `/families`).
- Modo debug opcional para guardar payloads JSON crudos.
- Internacionalización/normalización de etiquetas de facts (`Birth`, `Residence`, etc.).
