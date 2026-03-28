const state = {
  people: [],
  allPeople: [],
  selectedPersonId: null,
  language: "es",
  peopleQuery: "",
  peoplePage: 1,
  peoplePageSize: 60,
  peopleTotal: 0,
  treeRootId: null,
  connectionSourceId: null,
  connectionTargetId: null,
  connectionPersonIds: [],
  connectionHubId: null,
  defaultStartPersonId: null,
  autocompleteResults: {},
  dedupePage: 1,
  dedupePageSize: 24,
  dedupeTotal: 0,
  historicalPage: 1,
  historicalPageSize: 40,
  historicalTotal: 0,
  mediaFilter: "all",
  memoryFilter: "all",
  currentDna: null,
  dnaMatchMinCm: 0,
  dnaMatchSide: "all",
  dnaSelectedMatch: null,
  currentMedia: [],
  bookRootId: null,
  currentView: "explorer",
  statusSnapshot: null,
  bootstrapSnapshot: null,
  bootstrapGateDismissed: false,
  appDataLoaded: false,
  statusIntervalId: null,
};
let fsImportProgressIntervalId = null;
let bootstrapImportIntervalId = null;
const VIEW_LABEL_KEYS = {
  explorer: "explorer",
  dna: "dna",
  tree: "tree",
  connections: "connections",
  data: "data_center",
  book: "generate_book",
  info: "information",
  settings: "preferences",
  historical: "historical_events",
  dedupe: "dedupe_review",
};

const I18N = {
  es: {
    main_navigation: "Navegación principal",
    close_menu: "Cerrar menú",
    explorer: "Explorador",
    dna: "ADN",
    tree: "Árbol",
    connections: "Conexiones",
    tools: "Herramientas",
    familysearch: "FamilySearch",
    gedcom: "GEDCOM",
    preferences: "Preferencias",
    current_view: "Vista",
    selected_person: "Seleccionado",
    menu_section_explore: "Explorar",
    menu_section_analysis: "Análisis",
    menu_section_data: "Datos",
    menu_section_publish: "Salida",
    menu_section_system: "Sistema",
    data_center: "Centro de datos",
    information: "Información",
    status: "Estado",
    statusbar_ready: "Listo",
    statusbar_syncing: "Sincronizando (pid {pid})",
    statusbar_last_run: "Última ejecución: {status} (#{id})",
    refresh: "Actualizar",
    reset: "Restablecer",
    sync_history: "Historial de sincronización",
    sync: "Sincronizar",
    familysearch_account: "Cuenta FamilySearch",
    connect_familysearch: "Conectar FamilySearch",
    disconnect: "Desconectar",
    connected_short: "Conectado",
    authenticating_short: "Autenticando…",
    oauth_connected: "FamilySearch conectado",
    oauth_not_connected: "FamilySearch no conectado",
    oauth_pending: "Autenticación en progreso…",
    oauth_connecting: "Abriendo autenticación…",
    oauth_popup_blocked: "El navegador bloqueó la ventana de autenticación. Permite popups.",
    oauth_missing_client_id: "Falta FS_OAUTH_CLIENT_ID (AppKey) en el entorno.",
    import_from_familysearch: "Importar desde FamilySearch",
    fs_root_person_id: "ID persona raíz",
    fs_person_id_placeholder: "ID de FamilySearch (ej. L29R-2Q6)",
    generations: "Generaciones",
    collateral_depth: "Profundidad colateral",
    start_import: "Iniciar importación",
    starting_import: "Iniciando importación…",
    import_preparing_queue: "Sincronizando… preparando cola",
    import_progress_line: "{pct}% · trabajos {done}/{total} · pendientes {pending} · en progreso {in_progress} · fallidos {failed}",
    import_completed_line: "Importación completada · personas {persons} · relaciones {relationships} · media {media} · trabajos {done}",
    import_failed_line: "Importación {status}: {error}",
    please_enter_person_id: "Escribe un ID de persona válido.",
    gedcom_root_name: "Nombre persona raíz",
    gedcom_root_name_placeholder: "Nombre completo opcional de persona raíz",
    gedcom_root_name_hint: "Si lo indicas, esa persona quedará como raíz por defecto del árbol.",
    gedcom_root_set: "Raíz del árbol: {id}",
    job_limit: "Límite de trabajos",
    force_refresh: "Forzar actualización",
    start_sync: "Iniciar sincronización",
    stop_sync: "Detener sync",
    sync_stopped: "Sincronización detenida (pid {pid})",
    open_menu: "Abrir menú",
    local_archive: "Archivo local",
    search_name_or_id: "Buscar por nombre o ID",
    previous: "Anterior",
    next: "Siguiente",
    select_person_inspect_mirror: "Selecciona una persona para inspeccionar el espejo local.",
    select_person_open_dna: "Selecciona una persona para abrir el módulo ADN.",
    use_selected_person: "Usar persona seleccionada",
    root: "Raíz",
    search_person: "Buscar persona…",
    search_root_person: "Buscar persona raíz",
    mode: "Modo",
    pedigree: "Pedigrí",
    family: "Familia",
    ancestors: "Ancestros",
    descendants: "Descendientes",
    depth: "Profundidad",
    select_person_open_tree: "Selecciona una persona para abrir el árbol.",
    from: "Desde",
    to: "Hasta",
    search_person_by_name_or_id: "Buscar persona por nombre o ID",
    max_depth: "Profundidad máxima",
    find_connection: "Buscar conexión",
    pick_two_people_connection: "Elige dos personas para revisar su conexión local.",
    add_person: "Agregar persona",
    remove_person: "Eliminar persona",
    hub_person: "Persona central",
    hub_view: "Vista hub",
    hub_connections_from: "Conexiones desde {name}",
    primary_path: "Camino principal",
    connection_step: "paso",
    connection_steps: "pasos",
    appearance: "Apariencia",
    navigation: "Navegación",
    language: "Idioma",
    place: "Lugar",
    all: "Todos",
    lang_spanish: "Español",
    lang_english: "Inglés",
    theme: "Tema",
    light: "Claro",
    dark: "Oscuro",
    flat: "Sobrio",
    win95: "Windows 95",
    win311: "Windows 3.11",
    font_size: "Tamaño de fuente",
    small: "Pequeño",
    normal: "Normal",
    large: "Grande",
    x_large: "Extra grande",
    start_person: "Persona inicial",
    clear: "Limpiar",
    backup: "Respaldo",
    download_database: "↓ Descargar base de datos (.sqlite)",
    download_full_backup: "↓ Descargar respaldo completo (.zip)",
    download_7z_backup: "↓ Descargar respaldo 7z",
    choose_backup_file: "Elegir archivo de respaldo",
    browse_backup_file: "Examinar...",
    import_backup: "Importar respaldo",
    import_gedcom: "Importar GEDCOM",
    choose_gedcom_file: "Elegir archivo GEDCOM",
    browse_gedcom_file: "Examinar...",
    import: "Importar",
    export_gedcom: "Exportar GEDCOM",
    download_gedcom: "↓ Descargar GEDCOM (.ged)",
    generate_book: "Generar libro",
    book_root_person: "Persona raíz",
    book_root_current: "Libro desde: {name}",
    book_root_missing: "Selecciona una persona raíz para generar el libro.",
    download_markdown: "↓ Descargar Markdown",
    download_pdf: "↓ Descargar PDF",
    sync_missing_profiles: "Sincronizar perfiles faltantes",
    sync_missing_profiles_action: "Sincronizar perfiles faltantes",
    close: "Cerrar",
    gallery: "Galería",
    download_all: "↓ Descargar todo",
    sync_started_pid: "Sincronización iniciada (pid {pid})",
    no_queue: "Sin cola",
    no_sync_runs_yet: "Aún no hay sincronizaciones",
    people: "Personas",
    relationships: "Relaciones",
    sources: "Fuentes",
    memories: "Recuerdos",
    media: "Media",
    queue: "Cola",
    last_sync: "Última sincronización",
    latest_run: "Última ejecución",
    process: "Proceso",
    idle: "inactivo",
    never: "Nunca",
    none: "Ninguna",
    run: "Ejecución",
    jobs: "trabajos",
    failed: "fallidos",
    no_lifespan: "Sin fechas",
    no_dates: "Sin fechas",
    no_local_media: "Sin media local",
    no_media_files: "No hay archivos de media",
    open_in_familysearch: "Abrir en FamilySearch",
    open_gallery: "Abrir galería",
    open_image: "Abrir imagen",
    open_source: "Abrir origen",
    current_start_person_none: "Persona inicial actual: ninguna",
    current_start_person: "Persona inicial actual: {name} ({id})",
    current_start_person_id_only: "Persona inicial actual: {id}",
    start_person_not_unique: "No hay coincidencia única. Escribe nombre completo o ID.",
    root_label: "Raíz: {name}",
    no_nodes: "Sin nodos",
    no_local_connection_path: "No hay conexión local entre estas dos personas con el espejo actual.",
    search_pick_two_people: "Busca y selecciona dos personas para ver su conexión local.",
    pick_two_different_people: "Selecciona dos personas diferentes.",
    unable_to_load_tree: "No se pudo cargar el árbol: {error}",
    familysearch_id: "ID de FamilySearch: {id}",
    open_tree: "Abrir árbol",
    no_local_detail_available: "Aún no hay detalle local para esta persona.",
    running_pid: "ejecutando (pid {pid})",
    loading: "Cargando…",
    unable_to_load_stub_count: "No se pudo cargar el conteo de perfiles básicos",
    stub_people_summary: "{count} personas con perfil básico (aún no sincronizadas por completo)",
    py7zr_not_installed: "py7zr no instalado — ejecuta: pip install py7zr",
    pandoc_not_in_path: "pandoc no encontrado en PATH",
    pandoc_available: "pandoc disponible en {path}{engine}",
    pandoc_available_engine: " · motor {engine}",
    pandoc_unavailable_msg: "pandoc no encontrado — PDF no disponible; Markdown sí disponible",
    please_select_backup_file: "Primero selecciona un archivo de respaldo.",
    uploading: "Subiendo…",
    error_prefix: "Error: {error}",
    imported_people_reloading: "Importado. {count} personas. Recargando…",
    please_select_gedcom_file: "Primero selecciona un archivo GEDCOM.",
    importing: "Importando…",
    import_done_stats: "Listo — importados: {imported}, actualizados: {updated}, omitidos: {skipped}",
    dedupe_review: "Revisión de duplicados",
    refresh_candidates: "Actualizar candidatos",
    no_duplicate_candidates: "No hay candidatos de duplicados con el umbral actual.",
    ignore_pair: "Ignorar par",
    open_left: "Abrir izquierda",
    open_right: "Abrir derecha",
    duplicate_score: "Puntaje {score}",
    historical_events: "Eventos históricos",
    historical_sync: "Sincronizar desde servicio",
    historical_syncing: "Sincronizando eventos históricos…",
    historical_sync_done: "Sincronizado {years} años · global {global} · local {local}",
    year_from: "Desde",
    year_to: "Hasta",
    local_country: "País local",
    no_historical_events: "No hay eventos históricos cargados en ese rango.",
    delete_event: "Eliminar",
    historical_add_event: "Guardar evento",
    historical_saved: "Evento guardado.",
    historical_missing_title: "Escribe un título para el evento.",
    historical_invalid_year: "El año no es válido.",
    scope_label: "Ámbito",
    historical_event_year: "Año",
    historical_event_end_year: "Año fin",
    historical_event_title: "Título",
    historical_event_title_placeholder: "Evento",
    historical_event_description: "Descripción",
    historical_event_description_placeholder: "Resumen breve",
    historical_source_url: "URL fuente",
    historical_source_url_placeholder: "https://...",
    historical_match_terms: "Términos locales",
    historical_match_terms_placeholder: "venezuela, caracas, falcón",
    autocomplete_no_match: "Sin coincidencia única",
    accepts_dna_csv: "Acepta CSV de 23andMe, AncestryDNA, FamilyTreeDNA o formato genérico",
    replace_data: "Reemplazar datos",
    upload_dna_csv: "Subir CSV de ADN",
    dna_module: "Módulo ADN",
    biography: "Biografía",
    biography_source_life_sketch: "Biografía importada desde FamilySearch",
    biography_source_note: "Biografía armada desde notas locales",
    dna_privacy_notice: "Privacidad genética",
    dna_privacy_body: "Todos los archivos de ADN se procesan localmente en este dispositivo. La información de salud y comportamiento es orientativa y no constituye diagnóstico.",
    dna_imports: "Importaciones",
    dna_import_segments: "Segmentos",
    dna_import_raw: "SNPs crudos",
    dna_import_ethnicity: "Etnicidad",
    dna_import_haplogroups: "Haplogrupos",
    dna_import_matches: "Matches",
    dna_import_hint_segments: "CSV de segmentos para el painter",
    dna_import_hint_raw: "TXT/CSV crudo tipo 23andMe o genérico",
    dna_import_hint_ethnicity: "JSON/CSV con región y porcentaje",
    dna_import_hint_haplogroups: "JSON/CSV con Y y mtDNA",
    dna_import_hint_matches: "JSON/CSV con matches y segmentos compartidos",
    upload: "Subir",
    no_dna_data: "Aún no hay datos de ADN cargados.",
    chromosome_painter: "Pintor de cromosomas",
    please_select_csv_file: "Selecciona un archivo CSV.",
    stored_segments: "Guardados {count} segmentos ({source})",
    dna_traits: "Características heredadas",
    dna_ethnicity: "Desglose étnico",
    dna_haplogroups: "Linaje uniparental",
    dna_matches: "DNA Relatives",
    dna_total_snps: "SNPs crudos",
    dna_regions: "Regiones",
    dna_segment_detail: "Detalle del segmento",
    dna_segment_detail_placeholder: "Haz clic en un segmento para ver su detalle.",
    dna_probable_ancestor: "Ancestro probable",
    dna_genomic_range: "Rango genómico",
    dna_associated_traits: "Características asociadas",
    dna_select_segment: "Pasa el mouse para ver el tooltip y haz clic para fijar el detalle.",
    dna_no_traits: "Aún no hay traits inferidos. Importa SNPs crudos para habilitar esta vista.",
    dna_no_ethnicity: "Aún no hay desglose étnico importado.",
    dna_no_haplogroups: "Aún no hay haplogrupos importados.",
    dna_no_matches: "Aún no hay matches importados.",
    dna_disclaimer_health: "Información orientativa, no diagnóstico médico.",
    dna_disclaimer_behavior: "Los rasgos cognitivos o conductuales son probabilísticos, no deterministas.",
    dna_generation_breakdown: "Por generación estimada",
    dna_reference_panel: "Panel de referencia",
    dna_haplogroup_y: "Cromosoma Y",
    dna_haplogroup_mt: "ADN mitocondrial",
    dna_lineage_route: "Ruta estimada",
    dna_match_browser: "Chromosome browser",
    dna_shared_cm: "cM compartidos",
    dna_predicted_relationship: "Relación probable",
    dna_side: "Lado",
    dna_filter_min_cm: "Mínimo cM",
    dna_filter_side: "Filtrar lado",
    dna_all_sides: "Todos",
    dna_maternal: "Materno",
    dna_paternal: "Paterno",
    dna_unassigned: "Sin asignar",
    dna_unknown: "Desconocido",
    dna_probabilistic: "probabilístico",
    dna_copy_count: "{count} copias del alelo",
    dna_matches_found: "{count} matches",
    dna_traits_found: "{count} rasgos detectados",
    dna_import_saved: "Guardados {count} registros en {target}",
    dna_no_match_segments: "Este match no trae segmentos comparables.",
    page_of_total: "{from}-{to} de {total} · página {page}/{pages}",
    total: "total",
    visible_total: "{visible} visibles · {total} total",
    all_media: "Todo el media",
    all_memories: "Todos los recuerdos",
    unnamed_relative: "Pariente sin nombre",
    facts: "Hechos",
    timeline: "Línea de tiempo",
    historical_global: "Global",
    historical_local: "Local",
    notes: "Notas",
    note: "Nota",
    tl_label_map: {
      Birth: "Nacimiento", Death: "Fallecimiento", Burial: "Entierro",
      Christening: "Bautismo", Occupation: "Ocupación", Residence: "Residencia",
      Immigration: "Inmigración", Emigration: "Emigración", Ethnicity: "Etnia",
      Religion: "Religión", "National ID": "ID nacional", "Life Sketch": "Boceto de vida",
      Affiliation: "Afiliación", Marriage: "Matrimonio", Divorce: "Divorcio",
      "Military": "Servicio militar", Education: "Educación", Graduation: "Graduación",
      Event: "Evento",
      "Father born": "Padre nació", "Father died": "Padre falleció",
      "Mother born": "Madre nació", "Mother died": "Madre falleció",
      "Spouse born": "Cónyuge nació", "Spouse died": "Cónyuge falleció",
      "Child born": "Hijo/a nació", "Child died": "Hijo/a falleció",
    },
    family_section: "Familia",
    parents: "Padres",
    spouses: "Parejas",
    children: "Hijos",
    father: "Padre",
    mother: "Madre",
    spouse: "Pareja",
    basic_profile: "perfil básico",
    basic_profile_only: "Solo perfil básico",
    basic_profile_only_desc: "Esta persona está disponible solo como perfil local básico. Aún no se sincronizaron notas, fuentes, recuerdos ni hechos detallados para esta rama.",
    relative_outside_local_mirror: "Pariente fuera del espejo local",
    no_local_profile_cached: "Aún no hay un perfil local para este pariente.",
    unknown: "Desconocido",
    media_item: "media",
    gallery_suffix: "{name} — Media",
    no_value: "Sin valor",
    reset_zoom_hint: "Restablecer zoom (doble clic en SVG también restablece)",
    download_svg: "↓ SVG",
    print: "Imprimir",
    portrait: "Retrato",
    oauth_login_required: "Inicia sesión FamilySearch para continuar.",
    bootstrap_title: "Configuración inicial",
    bootstrap_db_title: "1. Base de datos local",
    bootstrap_oauth_title: "2. Conexión FamilySearch",
    bootstrap_import_title: "3. Importar información",
    bootstrap_recover_db: "Recuperar desde backups",
    bootstrap_recreate_db: "Recrear base de datos",
    bootstrap_reconnect_familysearch: "Reconectar FamilySearch",
    bootstrap_open_data_center: "Abrir Centro de datos",
    bootstrap_continue: "Continuar sin importar",
    bootstrap_loading: "Verificando estado inicial…",
    bootstrap_db_ok_people: "Base válida con {count} personas.",
    bootstrap_db_ok_empty: "Base válida, pero sin personas todavía.",
    bootstrap_db_invalid: "Base inválida: {error}",
    bootstrap_import_ready: "Datos cargados: {count} personas.",
    bootstrap_import_missing: "Aún no hay personas importadas.",
    bootstrap_import_running: "Importación en curso (pid {pid})…",
    bootstrap_need_oauth: "Conecta FamilySearch para importar desde ID raíz.",
    bootstrap_ready_summary: "Configuración lista.",
    bootstrap_empty_summary: "Sin datos aún. Importa una raíz o abre Centro de datos.",
    bootstrap_invalid_summary: "La base local está inválida. Recréala para continuar.",
    bootstrap_recreate_confirm: "Esto puede reemplazar la base local actual. ¿Deseas continuar?",
    bootstrap_recreate_done: "Base recreada correctamente.",
    bootstrap_recreate_failed: "No se pudo recrear la base: {error}",
    bootstrap_recover_trying: "Intentando recuperar base desde backups…",
    bootstrap_recover_done: "Recuperación completada desde {source}.",
    bootstrap_recover_failed: "No se pudo recuperar base: {error}",
    bootstrap_import_started: "Importación iniciada (pid {pid}).",
    bootstrap_import_failed: "No se pudo iniciar importación: {error}",
    bootstrap_check_failed: "No se pudo leer el estado inicial: {error}",
    bootstrap_continue_hint: "Puedes continuar y cargar respaldo/GEDCOM desde Data Center.",
  },
  en: {
    main_navigation: "Main navigation",
    close_menu: "Close menu",
    explorer: "Explorer",
    dna: "DNA",
    tree: "Tree",
    connections: "Connections",
    tools: "Tools",
    familysearch: "FamilySearch",
    gedcom: "GEDCOM",
    preferences: "Preferences",
    current_view: "View",
    selected_person: "Selected",
    menu_section_explore: "Explore",
    menu_section_analysis: "Analysis",
    menu_section_data: "Data",
    menu_section_publish: "Publish",
    menu_section_system: "System",
    data_center: "Data Center",
    information: "Information",
    status: "Status",
    statusbar_ready: "Ready",
    statusbar_syncing: "Syncing (pid {pid})",
    statusbar_last_run: "Last run: {status} (#{id})",
    refresh: "Refresh",
    reset: "Reset",
    sync_history: "Sync History",
    sync: "Sync",
    familysearch_account: "FamilySearch Account",
    connect_familysearch: "Connect FamilySearch",
    disconnect: "Disconnect",
    connected_short: "Connected",
    authenticating_short: "Authenticating…",
    oauth_connected: "FamilySearch connected",
    oauth_not_connected: "FamilySearch not connected",
    oauth_pending: "Authentication in progress…",
    oauth_connecting: "Opening authentication…",
    oauth_popup_blocked: "Popup blocked by browser. Allow popups for this site.",
    oauth_missing_client_id: "FS_OAUTH_CLIENT_ID (AppKey) is missing in environment.",
    import_from_familysearch: "Import from FamilySearch",
    fs_root_person_id: "Root person ID",
    fs_person_id_placeholder: "FamilySearch person ID (e.g. L29R-2Q6)",
    generations: "Generations",
    collateral_depth: "Collateral depth",
    start_import: "Start import",
    starting_import: "Starting import…",
    import_preparing_queue: "Syncing… preparing queue",
    import_progress_line: "{pct}% · jobs {done}/{total} · pending {pending} · in progress {in_progress} · failed {failed}",
    import_completed_line: "Import completed · people {persons} · relationships {relationships} · media {media} · jobs {done}",
    import_failed_line: "Import {status}: {error}",
    please_enter_person_id: "Please enter a valid person ID.",
    gedcom_root_name: "Root person name",
    gedcom_root_name_placeholder: "Optional root person full name",
    gedcom_root_name_hint: "If provided, this person will be set as the default tree root.",
    gedcom_root_set: "Tree root: {id}",
    job_limit: "Job limit",
    force_refresh: "Force refresh",
    start_sync: "Start Sync",
    stop_sync: "Stop Sync",
    sync_stopped: "Sync stopped (pid {pid})",
    open_menu: "Open menu",
    local_archive: "Local Archive",
    search_name_or_id: "Search by name or person ID",
    previous: "Previous",
    next: "Next",
    select_person_inspect_mirror: "Select a person to inspect the local mirror.",
    select_person_open_dna: "Select a person to open the DNA module.",
    use_selected_person: "Use selected person",
    root: "Root",
    search_person: "Search person…",
    search_root_person: "Search root person",
    mode: "Mode",
    pedigree: "Pedigree",
    family: "Family",
    ancestors: "Ancestors",
    descendants: "Descendants",
    depth: "Depth",
    select_person_open_tree: "Select a person to open the tree.",
    from: "From",
    to: "To",
    search_person_by_name_or_id: "Search person by name or ID",
    max_depth: "Max depth",
    find_connection: "Find connection",
    pick_two_people_connection: "Pick two people to inspect their local connection.",
    add_person: "Add person",
    remove_person: "Remove person",
    hub_person: "Hub person",
    hub_view: "Hub view",
    hub_connections_from: "Connections from {name}",
    primary_path: "Primary path",
    connection_step: "step",
    connection_steps: "steps",
    appearance: "Appearance",
    navigation: "Navigation",
    language: "Language",
    place: "Place",
    all: "All",
    lang_spanish: "Spanish",
    lang_english: "English",
    theme: "Theme",
    light: "Light",
    dark: "Dark",
    flat: "Flat",
    win95: "Windows 95",
    win311: "Windows 3.11",
    font_size: "Font size",
    small: "Small",
    normal: "Normal",
    large: "Large",
    x_large: "X-Large",
    start_person: "Start person",
    clear: "Clear",
    backup: "Backup",
    download_database: "↓ Download database (.sqlite)",
    download_full_backup: "↓ Download full backup (.zip)",
    download_7z_backup: "↓ Download 7z backup",
    choose_backup_file: "Choose backup file",
    browse_backup_file: "Browse...",
    import_backup: "Import backup",
    import_gedcom: "Import GEDCOM",
    choose_gedcom_file: "Choose GEDCOM file",
    browse_gedcom_file: "Browse...",
    import: "Import",
    export_gedcom: "Export GEDCOM",
    download_gedcom: "↓ Download GEDCOM (.ged)",
    generate_book: "Generate Book",
    book_root_person: "Root person",
    book_root_current: "Book root: {name}",
    book_root_missing: "Select a root person to generate the book.",
    download_markdown: "↓ Download Markdown",
    download_pdf: "↓ Download PDF",
    sync_missing_profiles: "Sync missing profiles",
    sync_missing_profiles_action: "Sync missing profiles",
    close: "Close",
    gallery: "Gallery",
    download_all: "↓ Download all",
    sync_started_pid: "Sync started (pid {pid})",
    no_queue: "No queue",
    no_sync_runs_yet: "No sync runs yet",
    people: "People",
    relationships: "Relationships",
    sources: "Sources",
    memories: "Memories",
    media: "Media",
    queue: "Queue",
    last_sync: "Last sync",
    latest_run: "Latest run",
    process: "Process",
    idle: "idle",
    never: "Never",
    none: "None",
    run: "Run",
    jobs: "jobs",
    failed: "failed",
    no_lifespan: "No lifespan",
    no_dates: "No dates",
    no_local_media: "No local media",
    no_media_files: "No media files available",
    open_in_familysearch: "Open in FamilySearch",
    open_gallery: "Open gallery",
    open_image: "Open image",
    open_source: "Open source",
    current_start_person_none: "Current start person: none",
    current_start_person: "Current start person: {name} ({id})",
    current_start_person_id_only: "Current start person: {id}",
    start_person_not_unique: "No unique person match. Type full name or ID.",
    root_label: "Root: {name}",
    no_nodes: "No nodes",
    no_local_connection_path: "No local connection path found between these two people in the current mirror.",
    search_pick_two_people: "Search and pick two people to inspect their local connection.",
    pick_two_different_people: "Pick two different people.",
    unable_to_load_tree: "Unable to load tree: {error}",
    familysearch_id: "FamilySearch ID: {id}",
    open_tree: "Open tree",
    no_local_detail_available: "No local detail available for this person yet.",
    running_pid: "running (pid {pid})",
    loading: "Loading…",
    unable_to_load_stub_count: "Unable to load stub count",
    stub_people_summary: "{count} people with basic profile only (not yet fully synced)",
    py7zr_not_installed: "py7zr not installed — run: pip install py7zr",
    pandoc_not_in_path: "pandoc not found in PATH",
    pandoc_available: "pandoc available at {path}{engine}",
    pandoc_available_engine: " · engine {engine}",
    pandoc_unavailable_msg: "pandoc not found — PDF download unavailable; Markdown download still works",
    please_select_backup_file: "Please select a backup file first.",
    uploading: "Uploading…",
    error_prefix: "Error: {error}",
    imported_people_reloading: "Imported! {count} people. Reloading…",
    please_select_gedcom_file: "Please select a GEDCOM file first.",
    importing: "Importing…",
    import_done_stats: "Done — imported: {imported}, updated: {updated}, skipped: {skipped}",
    dedupe_review: "Duplicate Review",
    refresh_candidates: "Refresh candidates",
    no_duplicate_candidates: "No duplicate candidates found with current threshold.",
    ignore_pair: "Ignore pair",
    open_left: "Open left",
    open_right: "Open right",
    duplicate_score: "Score {score}",
    historical_events: "Historical Events",
    historical_sync: "Sync from service",
    historical_syncing: "Syncing historical events…",
    historical_sync_done: "Synced {years} years · global {global} · local {local}",
    year_from: "From",
    year_to: "To",
    local_country: "Local country",
    no_historical_events: "No historical events loaded for this range.",
    delete_event: "Delete",
    historical_add_event: "Save event",
    historical_saved: "Event saved.",
    historical_missing_title: "Enter an event title.",
    historical_invalid_year: "Invalid year.",
    scope_label: "Scope",
    historical_event_year: "Year",
    historical_event_end_year: "End year",
    historical_event_title: "Title",
    historical_event_title_placeholder: "Event",
    historical_event_description: "Description",
    historical_event_description_placeholder: "Short summary",
    historical_source_url: "Source URL",
    historical_source_url_placeholder: "https://...",
    historical_match_terms: "Local terms",
    historical_match_terms_placeholder: "venezuela, caracas, falcon",
    autocomplete_no_match: "No unique match",
    accepts_dna_csv: "Accepts 23andMe, AncestryDNA, FamilyTreeDNA, or generic CSV files",
    replace_data: "Replace data",
    upload_dna_csv: "Upload DNA CSV",
    dna_module: "DNA Module",
    biography: "Biography",
    biography_source_life_sketch: "Biography imported from FamilySearch",
    biography_source_note: "Biography assembled from local notes",
    dna_privacy_notice: "Genetic privacy",
    dna_privacy_body: "All DNA files are processed locally on this device. Health and behavior output is informational only and is not medical advice.",
    dna_imports: "Imports",
    dna_import_segments: "Segments",
    dna_import_raw: "Raw SNPs",
    dna_import_ethnicity: "Ethnicity",
    dna_import_haplogroups: "Haplogroups",
    dna_import_matches: "Matches",
    dna_import_hint_segments: "Segment CSV for the painter",
    dna_import_hint_raw: "23andMe-style raw TXT/CSV or generic",
    dna_import_hint_ethnicity: "JSON/CSV with region and percentage",
    dna_import_hint_haplogroups: "JSON/CSV with Y and mtDNA",
    dna_import_hint_matches: "JSON/CSV with relatives and shared segments",
    upload: "Upload",
    no_dna_data: "No DNA data uploaded yet.",
    chromosome_painter: "Chromosome Painter",
    please_select_csv_file: "Please select a CSV file.",
    stored_segments: "Stored {count} segments ({source})",
    dna_traits: "Inherited traits",
    dna_ethnicity: "Ethnicity breakdown",
    dna_haplogroups: "Uniparental lineage",
    dna_matches: "DNA relatives",
    dna_total_snps: "Raw SNPs",
    dna_regions: "Regions",
    dna_segment_detail: "Segment detail",
    dna_segment_detail_placeholder: "Click a segment to inspect it.",
    dna_probable_ancestor: "Probable ancestor",
    dna_genomic_range: "Genomic range",
    dna_associated_traits: "Associated traits",
    dna_select_segment: "Hover for tooltip and click to pin details.",
    dna_no_traits: "No inferred traits yet. Import raw SNPs to enable this view.",
    dna_no_ethnicity: "No ethnicity breakdown imported yet.",
    dna_no_haplogroups: "No haplogroups imported yet.",
    dna_no_matches: "No DNA relatives imported yet.",
    dna_disclaimer_health: "Informational only, not a medical diagnosis.",
    dna_disclaimer_behavior: "Cognitive or behavioral traits are probabilistic, not deterministic.",
    dna_generation_breakdown: "Estimated generation",
    dna_reference_panel: "Reference panel",
    dna_haplogroup_y: "Y chromosome",
    dna_haplogroup_mt: "Mitochondrial DNA",
    dna_lineage_route: "Estimated route",
    dna_match_browser: "Chromosome browser",
    dna_shared_cm: "Shared cM",
    dna_predicted_relationship: "Predicted relationship",
    dna_side: "Side",
    dna_filter_min_cm: "Minimum cM",
    dna_filter_side: "Filter side",
    dna_all_sides: "All",
    dna_maternal: "Maternal",
    dna_paternal: "Paternal",
    dna_unassigned: "Unassigned",
    dna_unknown: "Unknown",
    dna_probabilistic: "probabilistic",
    dna_copy_count: "{count} allele copies",
    dna_matches_found: "{count} matches",
    dna_traits_found: "{count} traits detected",
    dna_import_saved: "Stored {count} records in {target}",
    dna_no_match_segments: "This match has no comparable segments.",
    page_of_total: "{from}-{to} of {total} · page {page}/{pages}",
    total: "total",
    visible_total: "{visible} visible · {total} total",
    all_media: "All media",
    all_memories: "All memories",
    unnamed_relative: "Unnamed relative",
    facts: "Facts",
    timeline: "Timeline",
    historical_global: "Global",
    tl_label_map: {},
    historical_local: "Local",
    notes: "Notes",
    note: "Note",
    family_section: "Family",
    parents: "Parents",
    spouses: "Spouses",
    children: "Children",
    father: "Father",
    mother: "Mother",
    spouse: "Spouse",
    basic_profile: "basic profile",
    basic_profile_only: "Basic profile only",
    basic_profile_only_desc: "This person is available as a basic local profile only. Full notes, sources, memories, and detailed facts were not synced for this branch yet.",
    relative_outside_local_mirror: "Relative outside the local mirror",
    no_local_profile_cached: "No local profile has been cached for this relative yet.",
    unknown: "Unknown",
    media_item: "media",
    gallery_suffix: "{name} — Media",
    no_value: "No value",
    reset_zoom_hint: "Reset zoom (double-click SVG also resets)",
    download_svg: "↓ SVG",
    print: "Print",
    portrait: "Portrait",
    oauth_login_required: "Sign in to FamilySearch to continue.",
    bootstrap_title: "Initial setup",
    bootstrap_db_title: "1. Local database",
    bootstrap_oauth_title: "2. FamilySearch connection",
    bootstrap_import_title: "3. Import data",
    bootstrap_recover_db: "Recover from backups",
    bootstrap_recreate_db: "Recreate database",
    bootstrap_reconnect_familysearch: "Reconnect FamilySearch",
    bootstrap_open_data_center: "Open Data Center",
    bootstrap_continue: "Continue without import",
    bootstrap_loading: "Checking startup status…",
    bootstrap_db_ok_people: "Database is valid with {count} people.",
    bootstrap_db_ok_empty: "Database is valid, but empty.",
    bootstrap_db_invalid: "Database is invalid: {error}",
    bootstrap_import_ready: "Data loaded: {count} people.",
    bootstrap_import_missing: "No people imported yet.",
    bootstrap_import_running: "Import is running (pid {pid})…",
    bootstrap_need_oauth: "Connect FamilySearch to import from a root ID.",
    bootstrap_ready_summary: "Setup is complete.",
    bootstrap_empty_summary: "No data yet. Import a root person or open Data Center.",
    bootstrap_invalid_summary: "Local database is invalid. Recreate it to continue.",
    bootstrap_recreate_confirm: "This may replace your current local database. Continue?",
    bootstrap_recreate_done: "Database recreated successfully.",
    bootstrap_recreate_failed: "Could not recreate database: {error}",
    bootstrap_recover_trying: "Trying to recover database from backups…",
    bootstrap_recover_done: "Recovery completed from {source}.",
    bootstrap_recover_failed: "Could not recover database: {error}",
    bootstrap_import_started: "Import started (pid {pid}).",
    bootstrap_import_failed: "Could not start import: {error}",
    bootstrap_check_failed: "Could not read startup status: {error}",
    bootstrap_continue_hint: "You can continue and import backup/GEDCOM from Data Center.",
  },
};

function detectDefaultLanguage() {
  const raw = (navigator.language || navigator.languages?.[0] || "es").toLowerCase();
  return raw.startsWith("es") ? "es" : "en";
}

function t(key, vars = {}) {
  const lang = state.language in I18N ? state.language : "en";
  const base = I18N[lang]?.[key] ?? I18N.en?.[key] ?? key;
  return String(base).replace(/\{(\w+)\}/g, (_, name) => String(vars[name] ?? ""));
}

function isClassicWindowsTheme() {
  const theme = document.documentElement.dataset.theme;
  return theme === "win95" || theme === "win311";
}

function themedText(key, vars = {}, options = {}) {
  const text = t(key, vars);
  return options.ellipsis && isClassicWindowsTheme() ? `${text}...` : text;
}

function translateLabel(englishLabel) {
  const lang = state.language in I18N ? state.language : "en";
  const map = I18N[lang]?.tl_label_map || {};
  return map[englishLabel] ?? englishLabel;
}

function showToast(msg, type = "info", duration = 4000) {
  const el = document.createElement("div");
  el.className = `toast toast-${type}`;
  el.textContent = msg;
  document.body.appendChild(el);
  requestAnimationFrame(() => el.classList.add("toast-visible"));
  setTimeout(() => {
    el.classList.remove("toast-visible");
    setTimeout(() => el.remove(), 300);
  }, duration);
}

function applyStaticTranslations() {
  for (const node of document.querySelectorAll("[data-i18n]")) {
    const key = isClassicWindowsTheme() ? (node.getAttribute("data-win95-i18n") || node.getAttribute("data-i18n")) : node.getAttribute("data-i18n");
    if (!key) continue;
    node.textContent = themedText(key, {}, { ellipsis: node.hasAttribute("data-win95-ellipsis") });
  }
  for (const node of document.querySelectorAll("[data-i18n-placeholder]")) {
    const key = node.getAttribute("data-i18n-placeholder");
    if (!key) continue;
    node.setAttribute("placeholder", t(key));
  }
  for (const node of document.querySelectorAll("[data-i18n-aria-label]")) {
    const key = node.getAttribute("data-i18n-aria-label");
    if (!key) continue;
    node.setAttribute("aria-label", t(key));
  }
  decorateMenuButtons();
  updateStatusBar();
}

function decorateMenuButtons() {
  for (const button of document.querySelectorAll(".app-menu-button")) {
    const label = button.textContent || "";
    const index = label.search(/\S/);
    if (index < 0) continue;
    button.innerHTML = `${escapeHtml(label.slice(0, index))}<span class="menu-hotkey">${escapeHtml(label.slice(index, index + 1))}</span>${escapeHtml(label.slice(index + 1))}`;
    button.setAttribute("aria-label", label);
  }
}

async function api(path, options = {}) {
  let response;
  try {
    response = await fetch(path, options);
  } catch (error) {
    throw new Error(`Request failed for ${path}: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!response.ok) {
    throw new Error(`Request failed for ${path}: HTTP ${response.status}`);
  }
  return response.json();
}

function syncStartErrorMessage(result) {
  const reason = String(result?.reason || "");
  if (reason === "oauth_login_required") return t("oauth_login_required");
  if (reason === "missing_oauth_client_id") return t("oauth_missing_client_id");
  return String(result?.error || result?.reason || "unknown");
}

function wait(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function renderOAuthStatus(status) {
  const node = document.getElementById("oauth-status");
  const connectBtn = document.getElementById("oauth-connect-btn");
  const disconnectBtn = document.getElementById("oauth-disconnect-btn");
  const resetConnectBtn = () => {
    if (!connectBtn) return;
    connectBtn.classList.remove("oauth-connect-connected", "oauth-connect-pending");
    connectBtn.textContent = themedText("connect_familysearch", {}, { ellipsis: true });
  };
  if (!node) return;
  resetConnectBtn();
  if (status?.authenticated) {
    node.textContent = t("oauth_connected");
    if (connectBtn) {
      connectBtn.disabled = true;
      connectBtn.classList.add("oauth-connect-connected");
      connectBtn.textContent = t("connected_short");
    }
    if (disconnectBtn) disconnectBtn.disabled = false;
    return;
  }
  if (status?.pending) {
    node.textContent = t("oauth_pending");
    if (connectBtn) {
      connectBtn.disabled = true;
      connectBtn.classList.add("oauth-connect-pending");
      connectBtn.textContent = t("authenticating_short");
    }
    if (disconnectBtn) disconnectBtn.disabled = true;
    return;
  }
  node.textContent = t("oauth_not_connected");
  if (connectBtn) connectBtn.disabled = false;
  if (disconnectBtn) disconnectBtn.disabled = true;
}

async function loadOAuthStatus() {
  try {
    const status = await api("/api/auth/familysearch/status");
    renderOAuthStatus(status);
    return status;
  } catch {
    renderOAuthStatus(null);
    return null;
  }
}

async function startOAuthFlow() {
  const statusNode = document.getElementById("oauth-status");
  if (statusNode) {
    statusNode.textContent = t("oauth_connecting");
  }
  let preStatus = null;
  try {
    preStatus = await api("/api/auth/familysearch/status");
  } catch {
    preStatus = null;
  }
  if (preStatus?.authenticated) {
    renderOAuthStatus(preStatus);
    return true;
  }
  if (preStatus && preStatus.client_id_set === false) {
    if (statusNode) {
      statusNode.textContent = t("oauth_missing_client_id");
    }
    return false;
  }

  const popup = window.open("about:blank", "familybook-oauth", "popup=yes,width=640,height=760");
  if (!popup) {
    if (statusNode) {
      statusNode.textContent = t("oauth_popup_blocked");
    }
    return false;
  }
  try {
    popup.document.title = "Familybook OAuth";
    popup.document.body.innerHTML = "<p style='font-family: sans-serif; padding: 16px;'>Abriendo autenticación…</p>";
  } catch {
    // ignore cross-browser popup document restrictions
  }

  let startResp;
  let start = {};
  try {
    startResp = await fetch("/api/auth/familysearch/start", { method: "POST" });
    start = await startResp.json();
  } catch (error) {
    try {
      popup.close();
    } catch {
      // ignore
    }
    if (statusNode) {
      statusNode.textContent = t("error_prefix", { error: String(error) });
    }
    return false;
  }

  if (!startResp.ok) {
    try {
      popup.close();
    } catch {
      // ignore
    }
    if (statusNode) {
      statusNode.textContent = syncStartErrorMessage(start);
    }
    return false;
  }
  if (!start.started) {
    try {
      popup.close();
    } catch {
      // ignore
    }
    if (start.reason === "already_authenticated") {
      await loadOAuthStatus();
      return true;
    }
    if (statusNode) {
      statusNode.textContent = syncStartErrorMessage(start);
    }
    return false;
  }
  if (!start.auth_url) {
    try {
      popup.close();
    } catch {
      // ignore
    }
    if (statusNode) {
      statusNode.textContent = t("error_prefix", { error: "oauth_missing_auth_url" });
    }
    return false;
  }

  try {
    popup.location.href = start.auth_url;
  } catch {
    if (statusNode) {
      statusNode.textContent = t("oauth_popup_blocked");
    }
    return false;
  }

  const postMessagePromise = new Promise((resolve) => {
    const onMessage = (event) => {
      const data = event?.data || {};
      if (!data || data.source !== "familybook-oauth") return;
      window.removeEventListener("message", onMessage);
      resolve(Boolean(data.ok));
    };
    window.addEventListener("message", onMessage);
    window.setTimeout(() => {
      window.removeEventListener("message", onMessage);
      resolve(false);
    }, 190000);
  });

  let connected = false;
  const startedAt = Date.now();
  while (Date.now() - startedAt < 185000) {
    await wait(1300);
    const auth = await loadOAuthStatus();
    if (auth?.authenticated) {
      connected = true;
      break;
    }
  }
  const msgConnected = await postMessagePromise;
  connected = connected || msgConnected;
  await loadOAuthStatus();
  await loadBootstrapStatus().catch(() => null);
  return connected;
}

async function ensureOAuthConnected() {
  const current = await loadOAuthStatus();
  if (current?.authenticated) return true;
  return startOAuthFlow();
}

async function disconnectOAuth() {
  await fetch("/api/auth/familysearch/disconnect", { method: "POST" });
  await loadOAuthStatus();
  await loadBootstrapStatus().catch(() => null);
}

function setBootstrapGateVisible(visible) {
  const gate = document.getElementById("bootstrap-gate");
  if (!gate) return;
  gate.classList.toggle("hidden", !visible);
  document.body.classList.toggle("bootstrap-lock", visible);
}

function bootstrapNeedsGate(info) {
  if (!info) return true;
  if (!info.ready_for_app) return true;
  if (info.has_people) return false;
  return !state.bootstrapGateDismissed;
}

function renderBootstrapStatus(info) {
  state.bootstrapSnapshot = info || null;
  const summaryNode = document.getElementById("bootstrap-summary");
  const dbNode = document.getElementById("bootstrap-db-status");
  const oauthNode = document.getElementById("bootstrap-oauth-status");
  const importNode = document.getElementById("bootstrap-import-status");
  const recoverBtn = document.getElementById("bootstrap-recover-db-btn");
  const recreateBtn = document.getElementById("bootstrap-recreate-db-btn");
  const connectBtn = document.getElementById("bootstrap-connect-btn");
  const importBtn = document.getElementById("bootstrap-import-btn");
  const openDataBtn = document.getElementById("bootstrap-open-data-btn");
  const continueBtn = document.getElementById("bootstrap-continue-btn");

  if (!summaryNode || !dbNode || !oauthNode || !importNode) {
    return;
  }

  if (!info) {
    summaryNode.textContent = t("bootstrap_loading");
    dbNode.textContent = t("loading");
    oauthNode.textContent = t("loading");
    oauthNode.dataset.state = "";
    importNode.textContent = t("loading");
    setBootstrapGateVisible(true);
    return;
  }

  const db = info.db || {};
  const oauth = info.oauth || {};
  const sync = info.sync_process || {};
  const dbValid = Boolean(db.valid);
  const peopleCount = Number(db.persons || 0);
  const hasPeople = peopleCount > 0;
  const oauthConnected = Boolean(oauth.authenticated);

  if (!dbValid) {
    const reason = String(db.error || "unknown");
    summaryNode.textContent = t("bootstrap_invalid_summary");
    dbNode.textContent = t("bootstrap_db_invalid", { error: reason });
  } else if (hasPeople) {
    summaryNode.textContent = t("bootstrap_ready_summary");
    dbNode.textContent = t("bootstrap_db_ok_people", { count: formatCount(peopleCount) });
  } else {
    summaryNode.textContent = t("bootstrap_empty_summary");
    dbNode.textContent = t("bootstrap_db_ok_empty");
  }

  if (oauthConnected) {
    oauthNode.textContent = t("oauth_connected");
    oauthNode.dataset.state = "ok";
  } else if (oauth?.client_id_set === false) {
    oauthNode.textContent = t("oauth_missing_client_id");
    oauthNode.dataset.state = "warn";
  } else {
    oauthNode.textContent = t("oauth_not_connected");
    oauthNode.dataset.state = "error";
  }

  if (sync?.running) {
    importNode.textContent = t("bootstrap_import_running", { pid: sync.pid || "?" });
  } else if (hasPeople) {
    importNode.textContent = t("bootstrap_import_ready", { count: formatCount(peopleCount) });
  } else if (!oauthConnected) {
    importNode.textContent = t("bootstrap_need_oauth");
  } else {
    importNode.textContent = t("bootstrap_import_missing");
  }

  if (recoverBtn) recoverBtn.disabled = dbValid || Boolean(sync?.running);
  if (recreateBtn) recreateBtn.disabled = Boolean(sync?.running);
  if (connectBtn) {
    connectBtn.disabled = !dbValid || Boolean(sync?.running);
    connectBtn.textContent = oauthConnected ? t("bootstrap_reconnect_familysearch") : t("connect_familysearch");
  }
  if (importBtn) importBtn.disabled = !dbValid || !oauthConnected || Boolean(sync?.running);
  if (openDataBtn) openDataBtn.disabled = !dbValid;
  if (continueBtn) continueBtn.disabled = !dbValid;

  if (hasPeople) {
    state.bootstrapGateDismissed = false;
  }
  setBootstrapGateVisible(bootstrapNeedsGate(info));
}

async function loadBootstrapStatus() {
  try {
    const info = await api("/api/bootstrap/status");
    renderBootstrapStatus(info);
    return info;
  } catch (error) {
    renderBootstrapStatus(null);
    const summaryNode = document.getElementById("bootstrap-summary");
    if (summaryNode) {
      summaryNode.textContent = t("bootstrap_check_failed", {
        error: error instanceof Error ? error.message : String(error),
      });
    }
    throw error;
  }
}

async function initializeReadyAppData(forceReload = false) {
  if (!state.bootstrapSnapshot?.ready_for_app) {
    return;
  }
  if (!state.appDataLoaded || forceReload) {
    await Promise.all([loadStatus(), loadPeople("", 1), loadOAuthStatus()]);
    await ensurePersonCached(state.defaultStartPersonId);
    state.appDataLoaded = true;
  } else {
    await Promise.all([loadStatus(), loadOAuthStatus()]);
  }
  if (!state.statusIntervalId) {
    state.statusIntervalId = window.setInterval(() => {
      loadStatus().catch(console.error);
    }, 5000);
  }
}

function stopBootstrapImportMonitor() {
  if (bootstrapImportIntervalId) {
    window.clearInterval(bootstrapImportIntervalId);
    bootstrapImportIntervalId = null;
  }
}

function startBootstrapImportMonitor() {
  stopBootstrapImportMonitor();
  const tick = async () => {
    try {
      const info = await loadBootstrapStatus();
      if (info?.has_people) {
        stopBootstrapImportMonitor();
        await initializeReadyAppData(true);
        setBootstrapGateVisible(false);
      }
    } catch (error) {
      console.error(error);
    }
  };
  tick().catch(console.error);
  bootstrapImportIntervalId = window.setInterval(() => {
    tick().catch(console.error);
  }, 2500);
}

async function recreateDatabaseFromBootstrap() {
  const resultNode = document.getElementById("bootstrap-action-result");
  if (!window.confirm(t("bootstrap_recreate_confirm"))) {
    return;
  }
  if (resultNode) {
    resultNode.textContent = t("bootstrap_loading");
  }
  try {
    const response = await fetch("/api/bootstrap/recreate-db", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirm: "RECREATE", force: false }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      const detail = payload.error || payload.reason || `HTTP ${response.status}`;
      if (resultNode) {
        resultNode.textContent = t("bootstrap_recreate_failed", { error: detail });
      }
      return;
    }
    state.bootstrapGateDismissed = false;
    if (resultNode) {
      resultNode.textContent = t("bootstrap_recreate_done");
    }
    await loadBootstrapStatus();
    await initializeReadyAppData(true);
  } catch (error) {
    if (resultNode) {
      resultNode.textContent = t("bootstrap_recreate_failed", { error: String(error) });
    }
  }
}

async function recoverDatabaseFromBootstrap() {
  const resultNode = document.getElementById("bootstrap-action-result");
  if (resultNode) {
    resultNode.textContent = t("bootstrap_recover_trying");
  }
  try {
    const response = await fetch("/api/bootstrap/recover-db", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ allow_repair: true }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      const detail = payload.error || payload.reason || `HTTP ${response.status}`;
      if (resultNode) {
        resultNode.textContent = t("bootstrap_recover_failed", { error: detail });
      }
      await loadBootstrapStatus().catch(() => null);
      return;
    }
    if (resultNode) {
      resultNode.textContent = t("bootstrap_recover_done", { source: payload.source || "backup" });
    }
    await loadBootstrapStatus();
    await initializeReadyAppData(true);
  } catch (error) {
    if (resultNode) {
      resultNode.textContent = t("bootstrap_recover_failed", { error: String(error) });
    }
  }
}

function dismissBootstrapGate() {
  state.bootstrapGateDismissed = true;
  setBootstrapGateVisible(false);
  const resultNode = document.getElementById("bootstrap-action-result");
  if (resultNode) {
    resultNode.textContent = t("bootstrap_continue_hint");
  }
}

function openDataCenterFromBootstrap() {
  dismissBootstrapGate();
  activateView("data");
}

async function connectFromBootstrap() {
  const resultNode = document.getElementById("bootstrap-action-result");
  if (resultNode) {
    resultNode.textContent = t("oauth_connecting");
  }
  try {
    if (state.bootstrapSnapshot?.oauth?.authenticated) {
      await disconnectOAuth();
    }
    const connected = await startOAuthFlow();
    await loadBootstrapStatus().catch(() => null);
    if (resultNode) {
      resultNode.textContent = connected ? t("oauth_connected") : t("oauth_not_connected");
    }
  } catch (error) {
    if (resultNode) {
      resultNode.textContent = t("error_prefix", { error: String(error) });
    }
  }
}

function formatCount(value) {
  return new Intl.NumberFormat().format(value ?? 0);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderStatus(status) {
  state.statusSnapshot = status;
  const latestRun = status.latest_run;
  const activeRun = status.active_run;
  const queue = activeRun?.queue
    ? Object.entries(activeRun.queue).map(([k, v]) => `${k}: ${v}`).join(" · ")
    : status.queue_by_status.map((item) => `${item.status}: ${item.qty}`).join(" · ");
  const sync = status.sync_process;
  document.getElementById("status-content").innerHTML = `
    <div><strong>${escapeHtml(t("people"))}</strong> ${formatCount(status.counts.persons)}</div>
    <div><strong>${escapeHtml(t("relationships"))}</strong> ${formatCount(status.counts.relationships)}</div>
    <div><strong>${escapeHtml(t("sources"))}</strong> ${formatCount(status.counts.person_sources)}</div>
    <div><strong>${escapeHtml(t("memories"))}</strong> ${formatCount(status.counts.person_memories)}</div>
    <div><strong>${escapeHtml(t("media"))}</strong> ${formatCount(status.counts.media_items)}</div>
    <div><strong>${escapeHtml(t("queue"))}</strong> ${escapeHtml(queue || t("no_queue"))}</div>
    <div><strong>${escapeHtml(t("last_sync"))}</strong> ${escapeHtml(status.metadata.last_sync_at || t("never"))}</div>
    <div><strong>${escapeHtml(t("latest_run"))}</strong> ${escapeHtml(latestRun ? `${latestRun.status} (#${latestRun.id})` : t("none"))}</div>
    <div><strong>${escapeHtml(t("process"))}</strong> ${escapeHtml(sync.running ? t("running_pid", { pid: sync.pid }) : t("idle"))}</div>
  `;
  updateStatusBar();
}

function renderRuns(runs) {
  const node = document.getElementById("runs-content");
  if (!runs.length) {
    node.innerHTML = `<div class="muted">${escapeHtml(t("no_sync_runs_yet"))}</div>`;
    return;
  }
  node.innerHTML = runs
    .map((run) => {
      return `
        <div class="run-card">
          <div class="run-card-header">
            <strong>${escapeHtml(t("run"))} #${escapeHtml(run.id)}</strong>
            <span class="badge">${escapeHtml(run.status)}</span>
          </div>
          <div class="run-meta">
            ${escapeHtml(t("jobs"))} ${formatCount(run.jobs_done)} · ${escapeHtml(t("failed"))} ${formatCount(run.jobs_failed)}<br>
            ${escapeHtml(t("media"))} ${formatCount(run.media_count)} · ${escapeHtml(t("memories"))} ${formatCount(run.memories_count)}
          </div>
        </div>
      `;
    })
    .join("");
}

function shortPath(value) {
  if (!value) return "";
  const parts = String(value).split(/[\\/]/);
  return parts[parts.length - 1] || value;
}

function genderToneClass(personOrGender) {
  const raw =
    typeof personOrGender === "string"
      ? personOrGender
      : (personOrGender?.gender || "");
  const token = String(raw || "").trim().toLowerCase();
  if (token.startsWith("f")) return "thumb-female";
  if (token.startsWith("m")) return "thumb-male";
  return "thumb-unknown";
}

function fallbackThumbHtml(personOrGender, className = "person-thumb") {
  return `<span class="${escapeHtml(className)} thumb-fallback ${escapeHtml(genderToneClass(personOrGender))}" aria-hidden="true"></span>`;
}

function personThumb(person, className = "person-thumb") {
  if (person?.portrait_url) {
    return `<img class="${escapeHtml(className)}" src="${escapeHtml(person.portrait_url)}" alt="${escapeHtml(person.name || t("portrait"))}" data-person-gender="${escapeHtml(person.gender || "")}" data-thumb-class="${escapeHtml(className)}" data-thumb-tone="${escapeHtml(genderToneClass(person))}">`;
  }
  return fallbackThumbHtml(person, className);
}

function ancestorByClass(node, className) {
  let current = node?.parentElement || null;
  while (current) {
    if (current.classList && current.classList.contains(className)) {
      return current;
    }
    current = current.parentElement;
  }
  return null;
}

function handlePortraitError(img) {
  if (!img) return;
  const className =
    img.dataset.thumbClass
    || (img.classList.contains("person-thumb") ? "person-thumb" : "")
    || (img.classList.contains("family-thumb") ? "family-thumb" : "")
    || (img.classList.contains("person-hero-portrait") ? "person-hero-portrait" : "")
    || (img.classList.contains("tree-thumb") ? "tree-thumb" : "")
    || "person-thumb";
  const toneClass = img.dataset.thumbTone || genderToneClass(img.dataset.personGender || "");
  const fallback = document.createElement("span");
  fallback.className = `${className} thumb-fallback ${toneClass}`;
  fallback.setAttribute("aria-hidden", "true");
  if (img.parentElement) {
    img.parentElement.insertBefore(fallback, img);
  }
  const card = ancestorByClass(img, "person-card");
  if (card) {
    card.classList.remove("no-thumb");
    card.classList.add("with-thumb");
  }
  img.remove();
}

function renderPeople() {
  const list = document.getElementById("people-list");
  document.getElementById("people-count").textContent = `${formatCount(state.peopleTotal)} ${t("total")}`;
  list.innerHTML = state.people
    .map((person) => {
      const active = person.person_id === state.selectedPersonId ? " active" : "";
      const disabled = person.is_stub ? " disabled" : "";
      const stubBadge = person.is_stub ? `<span class="stub-badge">${escapeHtml(t("basic_profile"))}</span>` : "";
      const thumb = personThumb(person);
      const thumbClass = thumb ? " with-thumb" : " no-thumb";
      return `
        <div class="person-card${active}${disabled}${thumbClass}" data-person-id="${escapeHtml(person.person_id)}">
          ${thumb}
          <div class="person-card-copy">
            <div class="person-card-name">${escapeHtml(person.name || person.person_id)} ${stubBadge}</div>
            <div class="person-card-meta">${escapeHtml(person.lifespan || t("no_lifespan"))}</div>
          </div>
        </div>
      `;
    })
    .join("");
  for (const node of list.querySelectorAll(".person-card")) {
    if (node.classList.contains("disabled")) {
      continue;
    }
    node.addEventListener("click", () => selectPerson(node.dataset.personId).catch(console.error));
  }
  renderPeoplePager();
}

function renderPeoplePager() {
  const prev = document.getElementById("people-prev-page");
  const next = document.getElementById("people-next-page");
  const info = document.getElementById("people-page-info");
  if (!prev || !next || !info) return;
  const totalPages = Math.max(1, Math.ceil((state.peopleTotal || 0) / state.peoplePageSize));
  const clampedPage = Math.min(Math.max(1, state.peoplePage), totalPages);
  const from = state.peopleTotal ? (clampedPage - 1) * state.peoplePageSize + 1 : 0;
  const to = state.peopleTotal ? Math.min(clampedPage * state.peoplePageSize, state.peopleTotal) : 0;
  info.textContent = t("page_of_total", {
    from: formatCount(from),
    to: formatCount(to),
    total: formatCount(state.peopleTotal),
    page: clampedPage,
    pages: totalPages,
  });
  prev.disabled = clampedPage <= 1;
  next.disabled = clampedPage >= totalPages;
}

function setView(view) {
  state.currentView = view;
  for (const name of ["explorer", "dna", "tree", "connections", "data", "book", "info", "settings", "historical", "dedupe"]) {
    const node = document.getElementById(`view-${name}`);
    if (!node) continue;
    node.classList.toggle("hidden", name !== view);
  }
  syncMenuViewState();
  updateStatusBar();
}

function fullPeople() {
  const source = state.allPeople.length ? state.allPeople : state.people;
  return source.filter((person) => !person.is_stub);
}

async function ensureAllPeopleLoaded() {
  if (state.allPeople.length) {
    return;
  }
  const all = [];
  let offset = 0;
  let total = 1;
  const chunkSize = 500;
  while (all.length < total) {
    const payload = await api(`/api/people?limit=${chunkSize}&offset=${offset}&include_total=1`);
    const items = payload.items || [];
    total = Number(payload.total || 0);
    all.push(...items);
    offset += items.length;
    if (!items.length) {
      break;
    }
  }
  state.allPeople = all;
}

function upsertPeopleCache(items) {
  if (!Array.isArray(items) || !items.length) return;
  const byId = new Map();
  for (const person of state.allPeople) {
    byId.set(person.person_id, person);
  }
  for (const person of items) {
    if (!person?.person_id) continue;
    byId.set(person.person_id, person);
  }
  state.allPeople = Array.from(byId.values());
}

async function ensurePersonCached(personId) {
  const normalized = String(personId || "").trim();
  if (!normalized || findPersonById(normalized)) return;
  try {
    await searchPeople(normalized, 4);
  } catch {
    // Best effort only for label rendering.
  }
}

function findPersonById(personId) {
  if (!personId) return null;
  const source = state.allPeople.length ? state.allPeople : state.people;
  return source.find((person) => person.person_id === personId) || null;
}

function currentViewLabel() {
  return t(VIEW_LABEL_KEYS[state.currentView] || state.currentView || "explorer");
}

function selectedPersonLabel() {
  const person = findPersonById(state.selectedPersonId);
  if (person?.name) {
    return `${person.name} (${person.person_id || t("unknown")})`;
  }
  return state.selectedPersonId || t("none");
}

function statusBarSummary() {
  const status = state.statusSnapshot;
  if (!status) return t("loading");
  const sync = status.sync_process || {};
  const latest = status.latest_run;
  if (sync.running) return t("statusbar_syncing", { pid: sync.pid || "?" });
  if (latest?.status && latest?.id) return t("statusbar_last_run", { status: latest.status, id: latest.id });
  return t("statusbar_ready");
}

function syncMenuViewState() {
  for (const button of document.querySelectorAll(".menu-nav-button")) {
    const active = button.dataset.view === state.currentView;
    button.classList.toggle("active", active);
    if (active) {
      button.setAttribute("aria-current", "page");
    } else {
      button.removeAttribute("aria-current");
    }
  }
}

function updateStatusBar() {
  const summaryNode = document.getElementById("statusbar-summary");
  const peopleNode = document.getElementById("statusbar-people");
  const viewNode = document.getElementById("statusbar-view");
  const selectionNode = document.getElementById("statusbar-selection");
  if (!summaryNode || !peopleNode || !viewNode || !selectionNode) return;
  const totalPeople = state.statusSnapshot?.counts?.persons ?? state.peopleTotal ?? 0;
  summaryNode.textContent = statusBarSummary();
  peopleNode.textContent = formatCount(totalPeople);
  viewNode.textContent = currentViewLabel();
  selectionNode.textContent = selectedPersonLabel();
}

function preferredStartPersonId() {
  if (!state.defaultStartPersonId) return null;
  const person = findPersonById(state.defaultStartPersonId);
  if (!person && !state.allPeople.length) {
    return state.defaultStartPersonId;
  }
  if (person && !person.is_stub) {
    return person.person_id;
  }
  return null;
}

function resolveBookRootId() {
  return state.bookRootId || state.selectedPersonId || preferredStartPersonId() || state.treeRootId || fullPeople()[0]?.person_id || null;
}

function connectionPersonLabel(person) {
  if (!person) return "";
  const name = person.name || person.person_id;
  return `${name} — ${person.person_id}`;
}

function normalizeSearchText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function filterPeopleLocally(rawValue, limit = 12) {
  const query = normalizeSearchText(rawValue);
  if (!query) return [];
  const matches = [];
  for (const person of fullPeople()) {
    const personId = String(person.person_id || "");
    const name = String(person.name || "");
    const quick = autocompleteLabel(person);
    const connection = connectionPersonLabel(person);
    const haystacks = [
      normalizeSearchText(personId),
      normalizeSearchText(name),
      normalizeSearchText(quick),
      normalizeSearchText(connection),
    ];
    let rank = -1;
    if (haystacks.some((value) => value === query)) {
      rank = 0;
    } else if (haystacks.some((value) => value.startsWith(query))) {
      rank = 1;
    } else if (haystacks.some((value) => value.includes(query))) {
      rank = 2;
    }
    if (rank >= 0) {
      matches.push({ person, rank });
    }
  }
  matches.sort((left, right) => (
    left.rank - right.rank
    || String(left.person.name || "").localeCompare(String(right.person.name || ""))
    || String(left.person.person_id || "").localeCompare(String(right.person.person_id || ""))
  ));
  return matches.slice(0, limit).map((item) => item.person);
}

function resolveConnectionPersonId(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) return null;
  const people = fullPeople();
  return resolvePersonIdFromItems(value, people);
}

function resolvePersonIdFromItems(rawValue, items) {
  const value = String(rawValue || "").trim();
  if (!value) return null;
  const normalized = normalizeSearchText(value);
  const byId = items.find((person) => normalizeSearchText(person.person_id) === normalized);
  if (byId) return byId.person_id;
  const byConnectionLabel = items.find((person) => normalizeSearchText(connectionPersonLabel(person)) === normalized);
  if (byConnectionLabel) return byConnectionLabel.person_id;
  const byQuickLabel = items.find((person) => normalizeSearchText(autocompleteLabel(person)) === normalized);
  if (byQuickLabel) return byQuickLabel.person_id;
  const bySuffix = items.find((person) => normalized.endsWith(normalizeSearchText(person.person_id)));
  if (bySuffix) return bySuffix.person_id;
  const exactNameMatches = items.filter((person) => normalizeSearchText(person.name) === normalized);
  if (exactNameMatches.length === 1) return exactNameMatches[0].person_id;
  const startsWithMatches = items.filter((person) => normalizeSearchText(person.name).startsWith(normalized));
  if (startsWithMatches.length === 1) return startsWithMatches[0].person_id;
  const includesMatches = items.filter((person) => normalizeSearchText(person.name).includes(normalized));
  if (includesMatches.length === 1) return includesMatches[0].person_id;
  return null;
}

async function searchPeople(rawValue, limit = 12) {
  const value = String(rawValue || "").trim();
  if (!value) return [];
  const payload = await api(`/api/people?q=${encodeURIComponent(value)}&limit=${encodeURIComponent(limit)}`);
  const items = (payload.items || []).filter((person) => !person.is_stub);
  upsertPeopleCache(items);
  return items;
}

function extractIdFromLabelFormat(value) {
  // "Name · lifespan · PERSON_ID" (personQuickLabel format used in datalist options)
  const dotParts = value.split(" · ");
  if (dotParts.length >= 3) return dotParts[dotParts.length - 1].trim() || null;
  // "Name — PERSON_ID" (connectionPersonLabel format set after resolve)
  const dashIdx = value.lastIndexOf(" — ");
  if (dashIdx > 0) return value.slice(dashIdx + 3).trim() || null;
  return null;
}

async function resolvePersonId(rawValue, inputId = "") {
  const value = String(rawValue || "").trim();
  if (!value) return null;
  // Fast path: extract person_id from known label formats (datalist selection)
  // This is cache-independent and works regardless of async state
  const extractedId = extractIdFromLabelFormat(value);
  if (extractedId) {
    const fromAllPeople = findPersonById(extractedId);
    if (fromAllPeople) return fromAllPeople.person_id;
    const fromAutoCache = (state.autocompleteResults[inputId] || []).find(
      (p) => String(p.person_id || "").toLowerCase() === extractedId.toLowerCase()
    );
    if (fromAutoCache) return fromAutoCache.person_id;
  }
  const localResolved = resolveConnectionPersonId(value);
  if (localResolved) return localResolved;
  const cachedAutocomplete = inputId ? (state.autocompleteResults[inputId] || []) : [];
  const autocompleteResolved = resolvePersonIdFromItems(value, cachedAutocomplete);
  if (autocompleteResolved) return autocompleteResolved;
  const searchQuery = extractedId || value;
  const matches = await searchPeople(searchQuery, 12);
  // When we searched by extracted ID, check for exact person_id match first
  if (extractedId && matches.length) {
    const byId = matches.find((p) => String(p.person_id || "").toLowerCase() === extractedId.toLowerCase());
    if (byId) return byId.person_id;
  }
  const resolved = resolvePersonIdFromItems(value, matches);
  return resolved || (matches.length === 1 ? matches[0].person_id : null);
}

function autocompleteLabel(person) {
  return personQuickLabel(person);
}

async function getAutocompleteMatches(rawValue, options = {}) {
  const limit = Math.max(1, Number(options.limit || 8));
  const localMatches = filterPeopleLocally(rawValue, limit);
  if (options.localOnly) {
    return localMatches;
  }
  if (localMatches.length >= limit || state.allPeople.length) {
    return localMatches;
  }
  const remoteMatches = await searchPeople(rawValue, limit);
  const merged = [];
  const seen = new Set();
  for (const person of [...localMatches, ...remoteMatches]) {
    const personId = String(person?.person_id || "");
    if (!personId || seen.has(personId)) continue;
    seen.add(personId);
    merged.push(person);
    if (merged.length >= limit) break;
  }
  return merged;
}

function getAutocompleteListId(inputId) {
  return `${inputId}-list`;
}

function ensureAutocompleteList(inputId) {
  let list = document.getElementById(getAutocompleteListId(inputId));
  if (list) return list;
  const input = document.getElementById(inputId);
  if (!input) return null;
  list = document.createElement("datalist");
  list.id = getAutocompleteListId(inputId);
  input.setAttribute("list", list.id);
  input.insertAdjacentElement("afterend", list);
  return list;
}

async function updatePersonAutocomplete(inputId) {
  const input = document.getElementById(inputId);
  const list = ensureAutocompleteList(inputId);
  if (!input || !list) return;
  const query = String(input.value || "").trim();
  if (query.length < 2) {
    list.innerHTML = "";
    state.autocompleteResults[inputId] = [];
    return;
  }
  const cached = state.autocompleteResults[inputId] || [];
  if (cached.some((p) => autocompleteLabel(p) === query || connectionPersonLabel(p) === query)) return;
  const items = await searchPeople(query, 8);
  state.autocompleteResults[inputId] = items;
  list.innerHTML = items
    .map((person) => `<option value="${escapeHtml(autocompleteLabel(person))}"></option>`)
    .join("");
}

function setupPersonAutocomplete(inputId, onSelect, options = {}) {
  const input = document.getElementById(inputId);
  if (!input) return;
  input.setAttribute("autocomplete", "off");
  const list = ensureAutocompleteList(inputId);
  const anchor =
    input.closest(".cp-person-row, .connection-search-control, .tree-root-control, .inline-control, .pref-start-person-block")
    || input.parentElement;
  if (!anchor) return;
  anchor.classList.add("person-ac-anchor");
  let dropdown = document.getElementById(`${inputId}-ac`);
  if (!dropdown) {
    dropdown = document.createElement("div");
    dropdown.id = `${inputId}-ac`;
    dropdown.className = "person-ac-dropdown";
    dropdown.hidden = true;
  }
  if (dropdown.parentElement !== anchor) {
    anchor.appendChild(dropdown);
  }
  let searchTimer = null;
  let activeIndex = -1;
  const minQueryLength = Math.max(1, Number(options.minQueryLength || 2));

  async function fetchMatches(query) {
    return getAutocompleteMatches(query, {
      limit: options.limit || 8,
      localOnly: Boolean(options.localOnly),
    });
  }

  function positionDropdown() {
    dropdown.style.top = `${input.offsetTop + input.offsetHeight + 4}px`;
    dropdown.style.left = `${input.offsetLeft}px`;
    dropdown.style.width = `${input.offsetWidth}px`;
  }

  function closeDropdown() {
    dropdown.hidden = true;
    activeIndex = -1;
  }

  function syncList(items) {
    if (!list) return;
    list.innerHTML = (items || [])
      .map((person) => `<option value="${escapeHtml(autocompleteLabel(person))}"></option>`)
      .join("");
  }

  function highlightItem(idx) {
    const items = dropdown.querySelectorAll(".person-ac-item");
    items.forEach((el, i) => el.classList.toggle("person-ac-active", i === idx));
    activeIndex = idx;
  }

  function renderItems(items) {
    syncList(items);
    if (!items.length) { closeDropdown(); return; }
    dropdown.innerHTML = items.map((p, i) => `
      <div class="person-ac-item" data-person-id="${escapeHtml(p.person_id)}" data-index="${i}">
        <span class="person-ac-name">${escapeHtml(p.name || p.person_id)}</span>
        ${p.lifespan ? `<span class="person-ac-meta">${escapeHtml(p.lifespan)}</span>` : ""}
      </div>
    `).join("");
    positionDropdown();
    dropdown.hidden = false;
    activeIndex = -1;
    for (const item of dropdown.querySelectorAll(".person-ac-item")) {
      item.addEventListener("mousedown", (e) => {
        e.preventDefault();
        closeDropdown();
        onSelect(item.dataset.personId);
      });
    }
  }

  input.addEventListener("input", () => {
    clearTimeout(searchTimer);
    const query = String(input.value || "").trim();
    if (query.length < minQueryLength) { closeDropdown(); state.autocompleteResults[inputId] = []; return; }
    searchTimer = setTimeout(async () => {
      try {
        const items = await fetchMatches(query);
        state.autocompleteResults[inputId] = items;
        renderItems(items);
      } catch {
        closeDropdown();
      }
    }, 180);
  });

  input.addEventListener("keydown", (e) => {
    const items = dropdown.querySelectorAll(".person-ac-item");
    const cachedItems = state.autocompleteResults[inputId] || [];
    if (e.key === "Escape") { closeDropdown(); return; }
    if (e.key === "Enter") {
      const target = (!dropdown.hidden && items.length)
        ? (activeIndex >= 0 ? items[activeIndex] : items[0])
        : null;
      const fallbackPersonId = target?.dataset.personId
        || cachedItems[0]?.person_id
        || (options.localOnly ? filterPeopleLocally(input.value, 1)[0]?.person_id : null);
      if (fallbackPersonId) {
        e.preventDefault();
        closeDropdown();
        onSelect(fallbackPersonId);
      }
      return;
    }
    if (dropdown.hidden || !items.length) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      highlightItem(Math.min(activeIndex + 1, items.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      highlightItem(Math.max(activeIndex - 1, 0));
    }
  });

  input.addEventListener("change", async () => {
    const resolved = await resolvePersonId(input.value, inputId);
    if (resolved) {
      closeDropdown();
      onSelect(resolved);
    }
  });

  input.addEventListener("blur", () => setTimeout(closeDropdown, 150));
  input.addEventListener("focus", async () => {
    const query = String(input.value || "").trim();
    if (query.length >= minQueryLength) {
      try {
        const items = await fetchMatches(query);
        state.autocompleteResults[inputId] = items;
        renderItems(items);
        return;
      } catch {
        closeDropdown();
      }
    }
    if (!dropdown.hidden) positionDropdown();
  });
  if (dropdown.dataset.resizeWired !== "true") {
    dropdown.dataset.resizeWired = "true";
    window.addEventListener("resize", positionDropdown);
  }
}

function setConnectionInputValue(inputId, personId) {
  const input = document.getElementById(inputId);
  if (!input) return;
  input.value = connectionPersonLabel(findPersonById(personId));
}

function syncConnectionInputSelection(inputId, personId) {
  const input = document.getElementById(inputId);
  if (!input) return;
  if (!personId) {
    input.value = "";
    return;
  }
  input.value = connectionPersonLabel(findPersonById(personId));
}

function nextConnectionEmptyIndex(fromIndex) {
  const afterCurrent = state.connectionPersonIds.findIndex((personId, idx) => idx > fromIndex && !personId);
  if (afterCurrent >= 0) return afterCurrent;
  return state.connectionPersonIds.findIndex((personId, idx) => idx !== fromIndex && !personId);
}

function selectedConnectionIds() {
  return [...new Set(state.connectionPersonIds.filter(Boolean))];
}

function resolveConnectionHubId(ids = selectedConnectionIds()) {
  if (ids.includes(state.connectionHubId)) {
    return state.connectionHubId;
  }
  return ids[0] || null;
}

function focusConnectionInput(index) {
  if (index < 0) return;
  const input = document.getElementById(`connection-person-${index}`);
  if (!input) return;
  input.focus();
  input.select();
}

function syncConnectionSearchInputs() {
  const sourceInput = document.getElementById("connection-source-search");
  const targetInput = document.getElementById("connection-target-search");
  if (!sourceInput || !targetInput) return;
  setConnectionInputValue("connection-source-search", state.connectionSourceId);
  setConnectionInputValue("connection-target-search", state.connectionTargetId);
}

async function applyConnectionSearchInput(inputId, key) {
  const input = document.getElementById(inputId);
  if (!input) return;
  const resolved = await resolvePersonId(input.value, inputId);
  if (!resolved) return;
  if (key === "source") {
    state.connectionSourceId = resolved;
  } else {
    state.connectionTargetId = resolved;
  }
  input.value = connectionPersonLabel(findPersonById(resolved));
}

async function showTreeForPerson(personId = null) {
  if (!state.allPeople.length) {
    await ensureAllPeopleLoaded();
  }
  const fallback = fullPeople()[0]?.person_id || null;
  const chosen = personId || state.treeRootId || state.selectedPersonId || preferredStartPersonId() || fallback;
  state.treeRootId = chosen || null;
  const search = document.getElementById("tree-root-search");
  if (search && state.treeRootId) {
    setConnectionInputValue("tree-root-search", state.treeRootId);
  }
  setView("tree");
  await loadTree();
}

function buildMediaCards(media) {
  const filtered = state.mediaFilter === "all" ? media : media.filter((item) => item.media_role === state.mediaFilter);
  if (!filtered.length) {
    return `<div class="muted">${escapeHtml(t("no_local_media"))}</div>`;
  }
  const roles = [...new Set(media.map((item) => item.media_role))].sort();
  return `
    <div class="subpanel-toolbar">
      <div class="muted">${escapeHtml(t("visible_total", { visible: filtered.length, total: media.length }))}</div>
      <select id="media-filter">
        <option value="all">${escapeHtml(t("all_media"))}</option>
        ${roles.map((role) => `<option value="${escapeHtml(role)}" ${role === state.mediaFilter ? "selected" : ""}>${escapeHtml(role)}</option>`).join("")}
      </select>
    </div>
    <div class="media-grid">
      ${filtered
        .slice(0, 24)
        .map((item, index) => {
          const isImage = (item.mime_type || "").startsWith("image/") || /\.(jpg|jpeg|png|gif|webp)$/i.test(item.local_path || "");
          const src = item.asset_url || "";
          return `
            <div class="media-card" data-media-index="${index}">
              ${isImage && src ? `<img src="${escapeHtml(src)}" alt="${escapeHtml(item.title || item.media_key)}">` : ""}
              <div class="media-caption">
                <div class="badge">${escapeHtml(item.media_role)}</div>
                <div>${escapeHtml(item.title || item.media_key)}</div>
              </div>
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function filteredMediaForCurrentState(media) {
  return state.mediaFilter === "all" ? media : media.filter((item) => item.media_role === state.mediaFilter);
}

function extractYearFromText(value) {
  const match = String(value || "").match(/\b(\d{4})\b/);
  return match ? Number(match[1]) : null;
}

function filterTimelineForPerson(person, timeline) {
  const items = Array.isArray(timeline) ? timeline : [];
  const birthYear = extractYearFromText(person?.birth_date) || extractYearFromText(person?.lifespan);
  const deathYear = extractYearFromText(person?.death_date);
  const currentYear = new Date().getFullYear();
  const spanStart = birthYear;
  const spanEnd = deathYear || (birthYear ? currentYear : null);
  if (!spanStart || !spanEnd) {
    return items;
  }
  return items.filter((item) => {
    if (item?.entry_type !== "historical_event") {
      return true;
    }
    const year = Number(item.year || 0);
    return year >= spanStart && year <= spanEnd;
  });
}

function relationshipLabel(item) {
  const relPerson = item.related_person;
  if (!relPerson) {
    return "";
  }
  return relPerson.name || relPerson.person_id;
}

function formatStubSummary(person) {
  const name = person.name || t("unnamed_relative");
  const lifespan = person.lifespan || [person.birth_date, person.death_date].filter(Boolean).join(" - ") || t("no_dates");
  return { name, lifespan };
}

function renderRichTextParagraphs(text) {
  return String(text || "")
    .split(/\n\s*\n/)
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((chunk) => `<p>${escapeHtml(chunk).replace(/\n/g, "<br>")}</p>`)
    .join("");
}

function buildBiographySection(biography) {
  if (!biography?.text) {
    return "";
  }
  const sourceLabel = biography.source === "life_sketch"
    ? t("biography_source_life_sketch")
    : t("biography_source_note");
  return `
    <section class="subpanel biography-section">
      <div class="panel-section-head">
        <h4>${escapeHtml(t("biography"))}</h4>
        <div class="muted">${escapeHtml(sourceLabel)}</div>
      </div>
      <div class="biography-copy">${renderRichTextParagraphs(biography.text)}</div>
    </section>
  `;
}

function buildFactsSection(facts) {
  const visibleFacts = facts.filter((fact) => {
    if ((fact.fact_type || "").includes("LifeSketch") || fact.fact_label === "Life Sketch") {
      return false;
    }
    const hasLabel = fact.fact_label && fact.fact_label !== "Event";
    const hasValue = fact.fact_value || fact.date_original || fact.place_original;
    return hasLabel && hasValue;
  });
  if (!visibleFacts.length) {
    return "";
  }
  return `
    <section class="subpanel">
      <h4>${escapeHtml(t("facts"))}</h4>
      <ul class="fact-list">
        ${visibleFacts
          .slice(0, 20)
          .map((fact) => {
            const lines = [];
            if (fact.fact_value) {
              if (fact.fact_url) {
                lines.push(`<a href="${escapeHtml(fact.fact_url)}" target="_blank" rel="noreferrer">${escapeHtml(fact.fact_value)}</a>`);
              } else {
                lines.push(escapeHtml(fact.fact_value));
              }
            }
            const details = [fact.date_original, fact.place_original].filter(Boolean).join(" · ");
            if (details) {
              lines.push(escapeHtml(details));
            }
            return `
              <li class="fact-row">
                <div class="fact-label">${escapeHtml(translateLabel(fact.fact_label))}</div>
                <div class="fact-lines">${lines.join("<br>")}</div>
              </li>
            `;
          })
          .join("")}
      </ul>
    </section>
  `;
}

function historicalScopeLabel(scope) {
  return scope === "local" ? t("historical_local") : t("historical_global");
}

function buildTimelineSection(timeline) {
  if (!timeline.length) {
    return "";
  }
  return `
    <section class="subpanel">
      <h4>${escapeHtml(t("timeline"))}</h4>
      <ul class="timeline-list">
        ${timeline
          .slice(0, 120)
          .map((item) => {
            const isHistorical = item.entry_type === "historical_event";
            const scopeBadge = isHistorical
              ? `<span class="badge">${escapeHtml(historicalScopeLabel(item.historical_scope || "global"))}</span>`
              : "";
            return `
              <li class="timeline-item ${isHistorical ? "historical" : "personal"}">
                <div class="timeline-year">${escapeHtml(item.year_label || String(item.year || ""))}</div>
                <div class="timeline-copy">
                  <div class="timeline-title">${escapeHtml(item.entry_type !== "historical_event" ? translateLabel(item.title || "") : (item.title || ""))} ${scopeBadge}</div>
                  ${item.place ? `<div class="entry-body">${escapeHtml(item.place)}</div>` : ""}
                  ${item.description ? `<div class="entry-body">${escapeHtml(item.description)}</div>` : ""}
                  ${item.source_url ? `<div class="entry-link"><a href="${escapeHtml(item.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(t("open_source"))}</a></div>` : ""}
                </div>
              </li>
            `;
          })
          .join("")}
      </ul>
    </section>
  `;
}

function buildRelationshipGroups(detail) {
  const grouped = detail.grouped_relationships || {};
  const order = [
    ["father", t("father")],
    ["mother", t("mother")],
    ["spouses", t("spouses")],
    ["children", t("children")],
  ];
  const groups = order
    .map(([key, label]) => {
      const items = grouped[key] || [];
      if (!items.length) {
        return "";
      }
      return `
        <div class="relationship-group">
          <h5>${escapeHtml(label)}</h5>
          <ul>
            ${items
              .map((item) => {
                const relPerson = item.related_person;
                if (relPerson) {
                  if (relPerson.is_stub) {
                    const stub = formatStubSummary(relPerson);
                    return `
                      <li class="unknown-relative-card">
                        ${personThumb(relPerson, "family-thumb")}
                        <span class="unknown-relative">${escapeHtml(stub.name)}</span>
                        <span class="muted">${escapeHtml(stub.lifespan)}</span>
                        <span class="technical-id">${escapeHtml(t("basic_profile_only"))}</span>
                      </li>
                    `;
                  }
                  return `<li><button class="person-link person-link-row" data-person-link="${escapeHtml(relPerson.person_id)}">${personThumb(relPerson, "family-thumb")}<span>${escapeHtml(relationshipLabel(item))}</span></button></li>`;
                }
                return `
                  <li class="unknown-relative-card">
                    <span class="unknown-relative">${escapeHtml(t("relative_outside_local_mirror"))}</span>
                    <span class="muted">${escapeHtml(t("no_local_profile_cached"))}</span>
                    ${item.related_person_id ? `<span class="technical-id">${escapeHtml(t("familysearch_id", { id: item.related_person_id }))}</span>` : ""}
                  </li>
                `;
              })
              .join("")}
          </ul>
        </div>
      `;
    })
    .filter(Boolean)
    .join("");
  if (!groups) {
    return "";
  }
  return `<section class="subpanel"><h4>${escapeHtml(t("family_section"))}</h4><div class="relationship-groups">${groups}</div></section>`;
}

function buildFamilyNav(detail) {
  const grouped = detail.grouped_relationships || {};
  const order = [
    ["father", t("father")],
    ["mother", t("mother")],
    ["spouses", t("spouse")],
    ["children", t("children")],
  ];
  const blocks = order
    .map(([key, label]) => {
      const items = (grouped[key] || []).filter((item) => item.related_person);
      if (!items.length) {
        return "";
      }
      return `
        <div class="family-nav-group">
          <div class="family-nav-label">${escapeHtml(label)}</div>
          <div class="family-nav-items">
            ${items
              .map((item) => {
                const person = item.related_person;
                return `<button class="person-link family-nav-link" data-person-link="${escapeHtml(person.person_id)}">${personThumb(
                  person,
                  "family-thumb"
                )}<span>${escapeHtml(person.name || person.person_id)}</span></button>`;
              })
              .join("")}
          </div>
        </div>
      `;
    })
    .filter(Boolean)
    .join("");
  if (!blocks) {
    return "";
  }
  return `<div class="family-nav">${blocks}</div>`;
}

function buildNotesSection(notes) {
  if (!notes.length) {
    return "";
  }
  return `
    <section class="subpanel">
      <h4>${escapeHtml(t("notes"))}</h4>
      <ul class="note-list">
        ${notes
          .map((item) => `<li><strong>${escapeHtml(item.subject || t("note"))}</strong><div class="entry-body">${escapeHtml((item.text_value || "").slice(0, 240))}</div></li>`)
          .join("")}
      </ul>
    </section>
  `;
}

function buildSourcesSection(sources) {
  if (!sources.length) {
    return "";
  }
  return `
    <section class="subpanel sources-panel">
      <h4>${escapeHtml(t("sources"))}</h4>
      <ul class="source-list">
        ${sources
          .slice(0, 25)
          .map((item) => `<li><strong>${escapeHtml(item.title || item.source_key)}</strong><div class="entry-body">${escapeHtml((item.citation || "").slice(0, 180))}</div>${item.source_url ? `<div class="entry-link"><a href="${escapeHtml(item.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(t("open_in_familysearch"))}</a></div>` : ""}</li>`)
          .join("")}
      </ul>
    </section>
  `;
}

function buildMemoriesSection(memories) {
  if (!memories.length) {
    return "";
  }
  const filteredMemories =
    state.memoryFilter === "all"
      ? memories
      : memories.filter((item) => (item.memory_type || "unknown") === state.memoryFilter);
  const memoryTypes = [...new Set(memories.map((item) => item.memory_type || "unknown"))].sort();
  return `
    <section class="subpanel">
      <div class="subpanel-toolbar">
        <h4>${escapeHtml(t("memories"))}</h4>
        <select id="memory-filter">
          <option value="all">${escapeHtml(t("all_memories"))}</option>
          ${memoryTypes.map((type) => `<option value="${escapeHtml(type)}" ${type === state.memoryFilter ? "selected" : ""}>${escapeHtml(type)}</option>`).join("")}
        </select>
      </div>
      <ul class="memory-list">
        ${filteredMemories
          .slice(0, 40)
          .map((item) => `<li><strong>${escapeHtml(item.title || item.memory_key)}</strong><div class="entry-body">${escapeHtml((item.description || item.text_value || "").slice(0, 240))}</div></li>`)
          .join("")}
      </ul>
    </section>
  `;
}

function buildMediaSection(media, personId) {
  if (!media.length) {
    return "";
  }
  return `
    <section class="subpanel">
      <div class="subpanel-toolbar">
        <h4>${escapeHtml(t("media"))}</h4>
        <button class="secondary open-gallery-btn" type="button" data-person-gallery="${escapeHtml(personId || "")}">${escapeHtml(themedText("open_gallery", {}, { ellipsis: true }))}</button>
      </div>
      ${buildMediaCards(media)}
    </section>
  `;
}

function buildStatChips(stats, mediaCount) {
  const chips = [
    [t("facts"), stats.facts],
    [t("parents"), stats.parents],
    [t("spouses"), stats.spouses],
    [t("children"), stats.children],
    [t("media"), mediaCount],
  ].filter(([, value]) => Number(value || 0) > 0);
  if (!chips.length) {
    return "";
  }
  return `
    <div class="person-stats">
      ${chips.map(([label, value]) => `<div class="stat-chip">${escapeHtml(label)} ${formatCount(value)}</div>`).join("")}
    </div>
  `;
}

function openMediaModal(item) {
  if (!item) return;
  const modal = document.getElementById("media-modal");
  const body = document.getElementById("media-modal-body");
  const isImage = (item.mime_type || "").startsWith("image/") || /\.(jpg|jpeg|png|gif|webp)$/i.test(item.local_path || "");
  body.innerHTML = `
    ${isImage && item.asset_url ? `<img src="${escapeHtml(item.asset_url)}" alt="${escapeHtml(item.title || item.media_key)}">` : ""}
    <div class="media-modal-meta">
      <div><strong>${escapeHtml(item.title || item.media_key)}</strong></div>
      <div class="badge">${escapeHtml(item.media_role || t("media_item"))}</div>
      <div class="meta-actions">
        ${item.asset_url ? `<a href="${escapeHtml(item.asset_url)}" target="_blank" rel="noreferrer">${escapeHtml(t("open_image"))}</a>` : ""}
        ${item.remote_url ? `<a href="${escapeHtml(item.remote_url)}" target="_blank" rel="noreferrer">${escapeHtml(t("open_source"))}</a>` : ""}
      </div>
    </div>
  `;
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
}

function closeMediaModal() {
  const modal = document.getElementById("media-modal");
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

function closeAppMenus() {
  for (const group of document.querySelectorAll(".app-menu-group")) {
    group.classList.remove("open");
  }
}

function toggleAppMenu(menuName) {
  let opened = false;
  for (const group of document.querySelectorAll(".app-menu-group")) {
    const shouldOpen = group.querySelector(".app-menu-button")?.dataset.menu === menuName && !group.classList.contains("open");
    group.classList.toggle("open", shouldOpen);
    if (shouldOpen) {
      opened = true;
    }
  }
  return opened;
}

function activateView(view) {
  setView(view);
  closeAppMenus();
  if (view === "dna") {
    loadDnaView().catch(console.error);
    return;
  }
  if (view === "data") {
    wireDataSubsections();
    loadStubCount().catch(console.error);
    loadCapabilities().catch(console.error);
    loadOAuthStatus().catch(console.error);
    return;
  }
  if (view === "tree") {
    showTreeForPerson().catch(console.error);
    return;
  }
  if (view === "connections") {
    ensureAllPeopleLoaded()
      .then(() => {
        renderConnectionPeople();
        return loadConnections();
      })
      .catch(console.error);
    return;
  }
  if (view === "book") {
    loadCapabilities().catch(console.error);
    return;
  }
  if (view === "info") {
    loadStatus().catch(console.error);
    return;
  }
  if (view === "historical") {
    loadHistoricalEvents().catch(console.error);
    return;
  }
  if (view === "dedupe") {
    loadDedupeCandidates().catch(console.error);
    return;
  }
  if (view === "settings") {
    Promise.resolve()
      .then(() => ensurePersonCached(state.defaultStartPersonId))
      .then(() => syncPreferenceSelects())
      .catch(console.error);
  }
}

async function loadDnaView(personId = null) {
  const targetId =
    personId
    || state.selectedPersonId
    || preferredStartPersonId()
    || state.people.find((person) => !person.is_stub)?.person_id
    || state.people[0]?.person_id
    || null;
  const container = document.getElementById("dna-view-content");
  if (!container) return;
  if (!targetId) {
    container.className = "empty-state";
    container.textContent = t("select_person_open_dna");
    return;
  }
  container.className = "";
  container.innerHTML = `<div class="muted">${escapeHtml(t("loading"))}</div>`;
  const detail = await api(`/api/people/${encodeURIComponent(targetId)}`);
  const person = detail.person || {};
  const dnaHtml = await buildDnaSection(targetId);
  container.innerHTML = `
    <div class="panel-title-row">
      <h2>${escapeHtml(t("dna_module"))}</h2>
      <div class="muted">${escapeHtml(person.name || targetId)} · ${escapeHtml(person.person_id || targetId)}</div>
    </div>
    ${dnaHtml}
  `;
  await wireDnaModule(targetId);
}

function loadPreferences() {
  const language = localStorage.getItem("fb_language") || detectDefaultLanguage();
  const theme = localStorage.getItem("fb_theme") || "light";
  const size = localStorage.getItem("fb_font_size") || "16";
  state.language = language in I18N ? language : detectDefaultLanguage();
  state.defaultStartPersonId = localStorage.getItem("fb_start_person_id") || null;
  document.documentElement.lang = state.language;
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.setProperty("--ui-font-size", size + "px");
  applyStaticTranslations();
}

function savePreference(key, value) {
  localStorage.setItem(key, value);
  loadPreferences();
}

function syncPreferenceSelects() {
  const languageSelect = document.getElementById("pref-language");
  const themeSelect = document.getElementById("pref-theme");
  const fontSelect = document.getElementById("pref-font-size");
  if (languageSelect) languageSelect.value = localStorage.getItem("fb_language") || state.language || detectDefaultLanguage();
  if (themeSelect) themeSelect.value = localStorage.getItem("fb_theme") || "light";
  if (fontSelect) fontSelect.value = localStorage.getItem("fb_font_size") || "16";
  syncStartPersonPreferenceUi();
}

async function refreshTranslatedUi() {
  applyStaticTranslations();
  renderPeople();
  syncStartPersonPreferenceUi();
  await loadStatus();
  if (state.selectedPersonId) {
    await selectPerson(state.selectedPersonId);
  }
  if (state.currentView === "tree") {
    await loadTree();
  } else if (state.currentView === "dna") {
    await loadDnaView();
  } else if (state.currentView === "connections") {
    await loadConnection();
  } else if (state.currentView === "data") {
    await Promise.all([loadStubCount(), loadCapabilities(), loadOAuthStatus()]);
  } else if (state.currentView === "info") {
    await loadStatus();
  } else if (state.currentView === "historical") {
    await loadHistoricalEvents();
  } else if (state.currentView === "dedupe") {
    await loadDedupeCandidates();
  } else if (state.currentView === "book") {
    await loadCapabilities();
  }
}

function syncStartPersonPreferenceUi(message = "") {
  const input = document.getElementById("pref-start-person-search");
  const current = document.getElementById("pref-start-person-current");
  const person = findPersonById(state.defaultStartPersonId);
  if (input) {
    input.value = person ? connectionPersonLabel(person) : (state.defaultStartPersonId || "");
  }
  if (current) {
    if (message) {
      current.textContent = message;
    } else if (person) {
      current.textContent = t("current_start_person", { name: person.name || person.person_id, id: person.person_id });
    } else if (state.defaultStartPersonId) {
      current.textContent = t("current_start_person_id_only", { id: state.defaultStartPersonId });
    } else {
      current.textContent = t("current_start_person_none");
    }
  }
}

function setDefaultStartPerson(personId) {
  const normalized = (personId || "").trim();
  savePreference("fb_start_person_id", normalized);
  state.defaultStartPersonId = normalized || null;
  syncStartPersonPreferenceUi();
}

async function applyDefaultStartPersonFromInput() {
  const input = document.getElementById("pref-start-person-search");
  if (!input) return;
  const raw = input.value.trim();
  if (!raw) {
    setDefaultStartPerson("");
    return;
  }
  const resolved = await resolvePersonId(raw, "pref-start-person-search");
  if (!resolved) {
    syncStartPersonPreferenceUi(t("start_person_not_unique"));
    return;
  }
  setDefaultStartPerson(resolved);
  await selectPerson(resolved);
}

function renderGalleryGrid(media) {
  if (!media.length) {
    return `<div class="muted">${escapeHtml(t("no_media_files"))}</div>`;
  }
  return media
    .map((item, index) => {
      const isImage = (item.mime_type || "").startsWith("image/") || /\.(jpg|jpeg|png|gif|webp)$/i.test(item.local_path || "");
      const src = item.asset_url || "";
      return `
        <div class="media-card" data-gallery-index="${index}">
          ${isImage && src ? `<img src="${escapeHtml(src)}" alt="${escapeHtml(item.title || item.media_key)}">` : ""}
          <div class="media-caption">
            <div class="badge">${escapeHtml(item.media_role)}</div>
            <div>${escapeHtml(item.title || item.media_key)}</div>
          </div>
        </div>
      `;
    })
    .join("");
}

function openGallery(personId, media) {
  const title = state.allPeople.find((p) => p.person_id === personId)?.name || personId;
  document.getElementById("gallery-modal-title").textContent = t("gallery_suffix", { name: title });
  document.getElementById("gallery-download-all").href = `/api/people/${encodeURIComponent(personId)}/media/download`;
  const body = document.getElementById("gallery-modal-body");
  body.innerHTML = renderGalleryGrid(media);
  const modal = document.getElementById("gallery-modal");
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  for (const node of body.querySelectorAll("[data-gallery-index]")) {
    node.addEventListener("click", () => {
      openMediaModal(media[Number(node.dataset.galleryIndex)]);
    });
  }
}

function closeGallery() {
  const modal = document.getElementById("gallery-modal");
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
}

function buildTreeNode(node) {
  const disabled = node.is_stub ? " disabled" : "";
  return `
    <button class="tree-node${disabled}" data-tree-person="${escapeHtml(node.person_id)}" ${node.is_stub ? 'type="button" disabled' : 'type="button"'}>
      ${personThumb(node, "tree-thumb")}
      <span class="tree-node-name">${escapeHtml(node.name || node.person_id)}</span>
      <span class="tree-node-meta">${escapeHtml(node.lifespan || [node.birth_date, node.death_date].filter(Boolean).join(" - ") || t("no_lifespan"))}</span>
    </button>
  `;
}

function renderTree(tree) {
  const rootLabel = document.getElementById("tree-root-label");
  rootLabel.textContent = tree.root?.name ? t("root_label", { name: tree.root.name }) : "";
  if (tree.root_person_id) {
    state.treeRootId = tree.root_person_id;
    setConnectionInputValue("tree-root-search", tree.root_person_id);
  }
  const node = document.getElementById("tree-view");
  node.classList.remove("empty-state");
  node.innerHTML = `
    <div class="tree-columns tree-mode-${escapeHtml(tree.mode)}">
      ${tree.levels
        .map(
          (level) => `
            <section class="tree-column">
              <h3>${escapeHtml(level.label)}</h3>
              <div class="tree-nodes">
                ${level.nodes.length ? level.nodes.map(buildTreeNode).join("") : `<div class="muted">${escapeHtml(t("no_nodes"))}</div>`}
              </div>
            </section>
          `
        )
        .join("")}
    </div>
  `;
  for (const button of node.querySelectorAll("[data-tree-person]")) {
    button.addEventListener("click", () => {
      const personId = button.dataset.treePerson;
      if (!personId) return;
      selectPerson(personId).catch(console.error);
    });
  }
}

function escapeAttr(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function wrapPedigreeName(name, maxChars = 16, maxLines = 2) {
  const words = String(name || "").trim().split(/\s+/).filter(Boolean);
  if (!words.length) {
    return [t("unknown")];
  }
  const lines = [];
  let current = words[0];
  for (let i = 1; i < words.length; i += 1) {
    const next = words[i];
    if ((current + " " + next).length <= maxChars) {
      current += " " + next;
    } else {
      lines.push(current);
      current = next;
      if (lines.length >= maxLines - 1) {
        break;
      }
    }
  }
  lines.push(current);
  if (lines.length > maxLines) {
    lines.length = maxLines;
  }
  if (words.join(" ").length > lines.join(" ").length && lines.length) {
    const last = lines[lines.length - 1];
    lines[lines.length - 1] = last.length >= maxChars ? `${last.slice(0, maxChars - 1)}…` : `${last}…`;
  }
  return lines;
}

function renderPedigreeTree(tree) {
  const rootLabel = document.getElementById("tree-root-label");
  rootLabel.textContent = tree.root?.name ? t("root_label", { name: tree.root.name }) : "";
  if (tree.root_person_id) {
    state.treeRootId = tree.root_person_id;
    setConnectionInputValue("tree-root-search", tree.root_person_id);
  }
  const node = document.getElementById("tree-view");
  node.classList.remove("empty-state");

  const generations = tree.generations || tree.levels.length;
  const leaves = 2 ** Math.max(0, generations - 1);
  const width = Math.max(1200, leaves * 240);
  const rowGap = 260;
  const top = 70;
  const height = top + generations * rowGap + 140;
  const radiusByGen = (gen) => (gen === 0 ? 56 : gen === 1 ? 50 : 44);
  const positions = new Map();

  for (const level of tree.levels) {
    const gen = level.generation;
    const y = top + (generations - 1 - gen) * rowGap;
    const count = level.nodes.length;
    const step = width / count;
    for (const n of level.nodes) {
      const x = step * (n.index + 0.5);
      positions.set(n.slot, { x, y, gen });
    }
  }

  const defs = [];
  let clipCount = 0;
  const circles = [];
  const labels = [];
  for (const level of tree.levels) {
    for (const n of level.nodes) {
      if (!n.person) continue;
      const pos = positions.get(n.slot);
      if (!pos) continue;
      const r = radiusByGen(pos.gen);
      circles.push(`<circle class="pedigree-ring" cx="${pos.x}" cy="${pos.y}" r="${r}"></circle>`);
      circles.push(`<circle class="pedigree-empty ${escapeAttr(genderToneClass(n.person))}" cx="${pos.x}" cy="${pos.y}" r="${r - 3}"></circle>`);
      if (n.person.portrait_url) {
        const clipId = `pedigree_clip_${clipCount++}`;
        defs.push(`<clipPath id="${clipId}"><circle cx="${pos.x}" cy="${pos.y}" r="${r - 3}"></circle></clipPath>`);
        circles.push(
          `<image href="${escapeAttr(n.person.portrait_url)}" x="${pos.x - (r - 3)}" y="${pos.y - (r - 3)}" width="${2 * (r - 3)}" height="${2 * (r - 3)}" preserveAspectRatio="xMidYMid slice" clip-path="url(#${clipId})" onerror="this.remove();"></image>`
        );
      }
      const name = n.person.name || n.person.person_id || t("unknown");
      const lifespan = n.person.lifespan || [n.person.birth_date, n.person.death_date].filter(Boolean).join(" - ") || t("no_lifespan");
      const wrapped = wrapPedigreeName(name, pos.gen <= 1 ? 18 : 15, 2);
      const nameY = pos.y + r + 26;
      const lineGap = 18;
      const metaY = nameY + (wrapped.length - 1) * lineGap + 24;
      const nameSize = pos.gen === 0 ? 16 : 14;
      labels.push(`
        <g class="pedigree-hit ${n.person.is_stub ? "disabled" : ""}" data-tree-person="${escapeAttr(n.person.person_id || "")}">
          <title>${escapeHtml(name)} (${escapeHtml(lifespan)})</title>
          <rect x="${pos.x - r}" y="${pos.y - r}" width="${2 * r}" height="${2 * r}" rx="${r}" ry="${r}" fill="transparent"></rect>
        </g>
        <text class="pedigree-name" x="${pos.x}" y="${nameY}" text-anchor="middle" style="font-size:${nameSize}px">
          ${wrapped.map((line, idx) => `<tspan x="${pos.x}" dy="${idx === 0 ? 0 : lineGap}">${escapeHtml(line)}</tspan>`).join("")}
        </text>
        <text class="pedigree-meta" x="${pos.x}" y="${metaY}" text-anchor="middle">${escapeHtml(lifespan)}</text>
      `);
    }
  }

  const paths = [];
  // Group edges by child to detect complete couples
  const edgesByChild = new Map();
  for (const edge of tree.edges || []) {
    if (!edgesByChild.has(edge.to)) edgesByChild.set(edge.to, []);
    edgesByChild.get(edge.to).push(edge.from);
  }
  for (const [childSlot, parentSlots] of edgesByChild) {
    const to = positions.get(childSlot);
    if (!to) continue;
    const rTo = radiusByGen(to.gen);
    const childTop = to.y - rTo;
    if (parentSlots.length === 2) {
      // Both parents: draw couple bracket
      const p1 = positions.get(parentSlots[0]);
      const p2 = positions.get(parentSlots[1]);
      if (!p1 || !p2) continue;
      const [left, right] = p1.x <= p2.x ? [p1, p2] : [p2, p1];
      const rP = radiusByGen(left.gen);
      const bracketY = left.y + rP + 14;
      // Tick lines from each circle down to bracket, connected horizontally
      paths.push(`<path class="pedigree-edge pedigree-couple-bracket" d="M${left.x},${left.y + rP} V${bracketY} H${right.x} V${right.y + rP}"></path>`);
      // Center line down to child
      const midX = (left.x + right.x) / 2;
      paths.push(`<path class="pedigree-edge" d="M${midX},${bracketY} V${childTop}"></path>`);
    } else {
      // Single parent: staircase connector
      for (const parentSlot of parentSlots) {
        const from = positions.get(parentSlot);
        if (!from) continue;
        const rFrom = radiusByGen(from.gen);
        const y1 = from.y + rFrom;
        const mid = y1 + (childTop - y1) * 0.42;
        paths.push(`<path class="pedigree-edge" d="M${from.x},${y1} V${mid} H${to.x} V${childTop}"></path>`);
      }
    }
  }

  const origVB = `0 0 ${width} ${height}`;
  node.innerHTML = `
    <div class="pedigree-wrap">
      <button class="pedigree-reset secondary" type="button" title="${escapeAttr(t("reset_zoom_hint"))}">⌖ ${escapeHtml(t("reset"))}</button>
      <svg class="pedigree-svg" width="${width}" height="${height}" viewBox="${origVB}" data-orig-view-box="${origVB}" preserveAspectRatio="xMinYMin meet">
        <defs>${defs.join("")}</defs>
        ${paths.join("")}
        ${circles.join("")}
        ${labels.join("")}
      </svg>
    </div>
  `;
  setupPedigreeZoom(node);
  setupPedigreeActions(node);

  for (const hit of node.querySelectorAll(".pedigree-hit")) {
    if (hit.classList.contains("disabled")) continue;
    hit.addEventListener("click", () => {
      const personId = hit.dataset.treePerson;
      if (!personId) return;
      selectPerson(personId).catch(console.error);
    });
  }
}

// ── Connection people panel ──────────────────────────────────
function renderConnectionPeople() {
  const list = document.getElementById("connection-people-list");
  if (!list) return;
  if (!state.connectionPersonIds.length) state.connectionPersonIds = [null, null];
  const uniqueIds = selectedConnectionIds();
  const hubId = resolveConnectionHubId(uniqueIds);
  state.connectionHubId = hubId;
  const hubControls = uniqueIds.length >= 3 ? `
    <div class="cp-hub-controls">
      <div class="cp-hub-label">${escapeHtml(t("hub_person"))}</div>
      <div class="cp-hub-chip-row">
        ${uniqueIds.map((personId) => {
          const person = findPersonById(personId);
          return `
            <button type="button" class="cp-hub-chip ${personId === hubId ? "active" : ""}" data-connection-hub="${escapeAttr(personId)}">
              ${personThumb(person || { person_id: personId, name: personId }, "cp-hub-chip-portrait")}
              <span>${escapeHtml((person?.name || personId).split(" ").slice(0, 3).join(" "))}</span>
            </button>
          `;
        }).join("")}
      </div>
    </div>
  ` : "";
  list.innerHTML = `${state.connectionPersonIds.map((pid, idx) => {
    const person = pid ? findPersonById(pid) : null;
    const displayVal = person ? connectionPersonLabel(person) : "";
    return `
      <div class="cp-person-row" data-index="${idx}">
        <span class="cp-person-num">${idx + 1}</span>
        <input type="search" id="connection-person-${idx}" class="cp-person-input"
          placeholder="${escapeHtml(t("search_person_by_name_or_id"))}"
          value="${escapeHtml(displayVal)}">
        ${state.connectionPersonIds.length > 2
          ? `<button class="cp-person-remove icon-btn" data-remove="${idx}" title="${escapeHtml(t('remove_person'))}">✕</button>`
          : ""}
      </div>`;
  }).join("")}${hubControls}`;

  state.connectionPersonIds.forEach((_, idx) => {
    setupPersonAutocomplete(`connection-person-${idx}`, (pid) => {
      state.connectionPersonIds[idx] = pid;
      let nextIndex = nextConnectionEmptyIndex(idx);
      if (nextIndex < 0 && state.connectionPersonIds.length < 6) {
        state.connectionPersonIds.push(null);
        nextIndex = state.connectionPersonIds.length - 1;
      }
      renderConnectionPeople();
      syncConnectionInputSelection(`connection-person-${idx}`, pid);
      if (nextIndex >= 0) {
        window.setTimeout(() => focusConnectionInput(nextIndex), 0);
      }
      loadConnections().catch(console.error);
    }, { localOnly: true, minQueryLength: 1, limit: 10 });
  });
  for (const btn of list.querySelectorAll("[data-connection-hub]")) {
    btn.addEventListener("click", () => {
      state.connectionHubId = btn.dataset.connectionHub || null;
      renderConnectionPeople();
      loadConnections().catch(console.error);
    });
  }
  for (const btn of list.querySelectorAll("[data-remove]")) {
    btn.addEventListener("click", () => {
      state.connectionPersonIds.splice(Number(btn.dataset.remove), 1);
      renderConnectionPeople();
      loadConnections().catch(console.error);
    });
  }
}

function translateConnectionEdgeLabel(edge) {
  const normalized = String(edge || "").trim().toLowerCase();
  if (!normalized) return "";
  const edgeMap = {
    father: t("father"),
    mother: t("mother"),
    parent: state.language === "es" ? "Progenitor" : "Parent",
    child: state.language === "es" ? "Hijo" : "Child",
    children: t("children"),
    spouse: t("spouse"),
    spouses: t("spouse"),
  };
  return edgeMap[normalized] || normalized;
}

function translateRelationshipLabel(label) {
  const normalized = String(label || "").trim().toLowerCase();
  if (!normalized) return "?";
  if (state.language !== "es") {
    return normalized;
  }
  if (normalized === "in-law connection") {
    return "conexión por afinidad";
  }
  const inLaw = normalized.endsWith("-in-law");
  const base = inLaw ? normalized.slice(0, -"-in-law".length) : normalized;

  function ancestorLabel(level) {
    if (level <= 1) return "padre/madre";
    if (level === 2) return "abuelo/a";
    if (level === 3) return "bisabuelo/a";
    if (level === 4) return "tatarabuelo/a";
    return `${level - 2}° abuelo/a`;
  }

  function descendantLabel(level) {
    if (level <= 1) return "hijo/a";
    if (level === 2) return "nieto/a";
    if (level === 3) return "bisnieto/a";
    if (level === 4) return "tataranieto/a";
    return `${level - 2}° nieto/a`;
  }

  let translated = base;
  if (base === "parent") translated = "padre/madre";
  else if (base === "child") translated = "hijo/a";
  else if (base === "grandparent") translated = "abuelo/a";
  else if (base === "grandchild") translated = "nieto/a";
  else if (/^(great-)+grandparent$/.test(base)) {
    translated = ancestorLabel((base.match(/great-/g) || []).length + 2);
  } else if (/^(great-)+grandchild$/.test(base)) {
    translated = descendantLabel((base.match(/great-/g) || []).length + 2);
  }

  return inLaw ? `${translated} político/a` : translated;
}

function translateRelationshipLabelForPerson(label, person) {
  const normalized = String(label || "").trim().toLowerCase();
  if (!normalized || state.language !== "es") {
    return translateRelationshipLabel(label);
  }
  const gender = String(person?.gender || "").trim().toLowerCase();
  const feminine = gender === "female" || gender === "f";
  const masculine = gender === "male" || gender === "m";

  function ancestorLabel(level) {
    if (!feminine && !masculine) {
      if (level <= 1) return "padre/madre";
      if (level === 2) return "abuelo/a";
      if (level === 3) return "bisabuelo/a";
      if (level === 4) return "tatarabuelo/a";
      return `${level - 2}° abuelo/a`;
    }
    if (level <= 1) return feminine ? "madre" : "padre";
    if (level === 2) return feminine ? "abuela" : "abuelo";
    if (level === 3) return feminine ? "bisabuela" : "bisabuelo";
    if (level === 4) return feminine ? "tatarabuela" : "tatarabuelo";
    return `${level - 2}° ${feminine ? "abuela" : "abuelo"}`;
  }

  function descendantLabel(level) {
    if (!feminine && !masculine) {
      if (level <= 1) return "hijo/a";
      if (level === 2) return "nieto/a";
      if (level === 3) return "bisnieto/a";
      if (level === 4) return "tataranieto/a";
      return `${level - 2}° nieto/a`;
    }
    if (level <= 1) return feminine ? "hija" : "hijo";
    if (level === 2) return feminine ? "nieta" : "nieto";
    if (level === 3) return feminine ? "bisnieta" : "bisnieto";
    if (level === 4) return feminine ? "tataranieta" : "tataranieto";
    return `${level - 2}° ${feminine ? "nieta" : "nieto"}`;
  }

  if (normalized === "in-law connection") {
    return "conexión por afinidad";
  }
  const inLaw = normalized.endsWith("-in-law");
  const base = inLaw ? normalized.slice(0, -"-in-law".length) : normalized;

  let translated = base;
  if (base === "parent") translated = ancestorLabel(1);
  else if (base === "child") translated = descendantLabel(1);
  else if (base === "grandparent") translated = ancestorLabel(2);
  else if (base === "grandchild") translated = descendantLabel(2);
  else if (/^(great-)+grandparent$/.test(base)) {
    translated = ancestorLabel((base.match(/great-/g) || []).length + 2);
  } else if (/^(great-)+grandchild$/.test(base)) {
    translated = descendantLabel((base.match(/great-/g) || []).length + 2);
  } else {
    translated = translateRelationshipLabel(base);
  }

  if (!inLaw) {
    return translated;
  }
  if (!feminine && !masculine) {
    return `${translated} político/a`;
  }
  return `${translated} ${feminine ? "política" : "político"}`;
}

// ── Path chain rendering ──────────────────────────────────────
function renderPathChain(pathObj, pathIndex) {
  const { steps, relationship, length } = pathObj;
  const edges = steps.map((s) => s.edge_to_next).filter(Boolean);
  const pivotEdgeIdx = edges.findIndex((e) => e === "child" || e === "children");
  const pivotNodeIdx = pivotEdgeIdx >= 0 ? pivotEdgeIdx : -1;
  const pathColors = ["#3a86ff", "#ff6b6b", "#2ec4b6", "#ff9f1c", "#a855f7"];
  const color = pathColors[pathIndex % pathColors.length];

  const stepsHtml = steps.map((step, i) => {
    const p = step.person;
    const isPivot = i === pivotNodeIdx;
    const edge = step.edge_to_next;
    const isUp = edge && (edge === "father" || edge === "mother" || edge === "parent");
    const isDown = edge && (edge === "child" || edge === "children");
    const isSide = edge && (edge === "spouse" || edge === "spouses");
    const edgeClass = isUp ? "cp-edge-up" : isDown ? "cp-edge-down" : isSide ? "cp-edge-side" : "cp-edge-other";
    return `
      <div class="cp-step">
        <div class="cp-node${isPivot ? " cp-pivot" : ""}" data-person-link="${escapeHtml(p.person_id)}"
             title="${escapeHtml(p.name || p.person_id)}">
          ${personThumb(p, "cp-portrait")}
          <div class="cp-name">${escapeHtml((p.name || p.person_id || "").split(" ").slice(0, 2).join(" "))}</div>
          ${p.lifespan ? `<div class="cp-dates">${escapeHtml(p.lifespan)}</div>` : ""}
        </div>
        ${edge ? `<div class="cp-edge ${edgeClass}">
            <span class="cp-edge-label">${escapeHtml(translateConnectionEdgeLabel(edge))}</span>
            <span class="cp-edge-arrow">›</span>
          </div>` : ""}
      </div>`;
  }).join("");

  return `
    <div class="cp-path" style="--path-color:${color}">
      <div class="cp-path-header">
        <span class="cp-relationship badge" style="background:${color};color:#fff;border:none">${escapeHtml(translateRelationshipLabelForPerson(relationship, steps[steps.length - 1]?.person))}</span>
        <span class="cp-length muted">${length} ${length === 1 ? t("connection_step") : t("connection_steps")}</span>
      </div>
      <div class="cp-chain-scroll"><div class="cp-chain">${stepsHtml}</div></div>
    </div>`;
}

function renderConnectionResult(pairData) {
  const { source, target, paths, found } = pairData;
  const headerHtml = `
    <div class="cp-pair-header">
      ${personThumb(source, "cp-portrait-sm")}
      <span class="cp-pair-names">${escapeHtml(source.name || source.person_id)}</span>
      <span class="cp-pair-sep">↔</span>
      ${personThumb(target, "cp-portrait-sm")}
      <span class="cp-pair-names">${escapeHtml(target.name || target.person_id)}</span>
    </div>`;
  if (!found || !paths.length) {
    return `<div class="cp-pair-block">${headerHtml}
      <div class="muted cp-no-path">${escapeHtml(t("no_local_connection_path"))}</div></div>`;
  }
  return `<div class="cp-pair-block">${headerHtml}
    <div class="cp-paths">${paths.map((p, i) => renderPathChain(p, i)).join("")}</div></div>`;
}

function renderHubConnectionCard(pairData, index) {
  const { source, target, paths, found } = pairData;
  const primaryPath = paths?.[0] || null;
  return `
    <article class="cp-hub-card ${found && primaryPath ? "" : "cp-hub-card-empty"}">
      <div class="cp-hub-card-header">
        <div class="cp-hub-card-route">
          ${personThumb(source, "cp-hub-mini-portrait")}
          <span class="cp-hub-mini-arrow">→</span>
          ${personThumb(target, "cp-hub-mini-portrait")}
        </div>
        <div class="cp-hub-card-target" data-person-link="${escapeHtml(target.person_id || "")}">
          <strong>${escapeHtml(target.name || target.person_id)}</strong>
          ${target.lifespan ? `<span class="muted">${escapeHtml(target.lifespan)}</span>` : ""}
        </div>
      </div>
      ${found && primaryPath
        ? `<div class="cp-hub-card-body">
            <div class="cp-hub-card-kicker">${escapeHtml(t("primary_path"))}</div>
            ${renderPathChain(primaryPath, index)}
          </div>`
        : `<div class="muted cp-no-path">${escapeHtml(t("no_local_connection_path"))}</div>`}
    </article>
  `;
}

function renderHubConnections(hubPerson, pairResults) {
  const hubName = hubPerson?.name || hubPerson?.person_id || t("unknown");
  return `
    <div class="cp-hub-view">
      <div class="cp-hub-view-header">
        <span class="relationship-summary">${escapeHtml(t("hub_view"))}</span>
        <div class="muted">${escapeHtml(t("hub_connections_from", { name: hubName }))}</div>
      </div>
      <div class="cp-hub-center-wrap">
        <button type="button" class="cp-hub-center-card" data-person-link="${escapeHtml(hubPerson?.person_id || "")}">
          ${personThumb(hubPerson || {}, "cp-hub-center-portrait")}
          <span class="cp-hub-center-label">${escapeHtml(hubName)}</span>
          ${hubPerson?.lifespan ? `<span class="cp-hub-center-dates">${escapeHtml(hubPerson.lifespan)}</span>` : ""}
        </button>
      </div>
      <div class="cp-hub-grid">
        ${pairResults.map((pair, idx) => renderHubConnectionCard(pair, idx)).join("")}
      </div>
    </div>
  `;
}

async function loadConnections() {
  const ids = state.connectionPersonIds.filter(Boolean);
  const node = document.getElementById("connection-view");
  if (ids.length < 2) {
    node.className = "panel empty-state";
    node.textContent = t("search_pick_two_people");
    return;
  }
  const unique = [...new Set(ids)];
  if (unique.length < 2) {
    node.className = "panel empty-state";
    node.textContent = t("pick_two_different_people");
    return;
  }
  node.className = "panel";
  node.innerHTML = `<div class="muted">${escapeHtml(t("loading") || "Loading…")}</div>`;
  try {
    let html = "";
    if (unique.length === 2) {
      const data = await api(
        `/api/connection?source=${encodeURIComponent(unique[0])}&target=${encodeURIComponent(unique[1])}&max_depth=24&max_paths=3`
      );
      html = renderConnectionResult(data);
    } else {
      const hubId = resolveConnectionHubId(unique);
      state.connectionHubId = hubId;
      const targets = unique.filter((personId) => personId !== hubId);
      const pairs = await Promise.all(
        targets.map((targetId) => api(
          `/api/connection?source=${encodeURIComponent(hubId)}&target=${encodeURIComponent(targetId)}&max_depth=24&max_paths=2`
        ))
      );
      const hubPerson = pairs[0]?.source || findPersonById(hubId) || { person_id: hubId, name: hubId };
      html = renderHubConnections(hubPerson, pairs);
    }
    node.innerHTML = `<div class="cp-results">${html}</div>`;
    for (const el of node.querySelectorAll("[data-person-link]")) {
      el.addEventListener("click", () => selectPerson(el.dataset.personLink).catch(console.error));
    }
  } catch (err) {
    node.innerHTML = `<div class="muted">${escapeHtml(String(err))}</div>`;
  }
}

function renderConnection(connection) {
  // Legacy compatibility wrapper
  const node = document.getElementById("connection-view");
  node.className = "panel";
  node.innerHTML = `<div class="cp-results">${renderConnectionResult(connection)}</div>`;
  for (const el of node.querySelectorAll("[data-person-link]")) {
    el.addEventListener("click", () => selectPerson(el.dataset.personLink).catch(console.error));
  }
}

async function loadTree() {
  if (!state.allPeople.length) {
    await ensureAllPeopleLoaded();
  }
  const fallback = fullPeople()[0]?.person_id || null;
  const requestedRoot = state.treeRootId || state.selectedPersonId || preferredStartPersonId() || fallback;
  const root = findPersonById(requestedRoot)?.person_id || requestedRoot || fallback;
  const node = document.getElementById("tree-view");
  try {
    state.treeRootId = root || null;
    const mode = document.getElementById("tree-mode").value;
    const depth = Number(document.getElementById("tree-depth").value) || 3;
    const rootParam = root ? `root=${encodeURIComponent(root)}&` : "";
    if (mode === "pedigree") {
      const tree = await api(`/api/tree/pedigree?${rootParam}generations=${encodeURIComponent(depth + 1)}`);
      renderPedigreeTree(tree);
    } else {
      const tree = await api(`/api/tree?${rootParam}mode=${encodeURIComponent(mode)}&depth=${encodeURIComponent(depth)}`);
      renderTree(tree);
    }
  } catch (error) {
    node.className = "tree-view panel empty-state";
    node.textContent = t("unable_to_load_tree", { error: String(error) });
  }
}

async function loadConnection() {
  return loadConnections();
}

function renderPersonLoadError(personId, error) {
  const node = document.getElementById("person-detail");
  if (!node) return;
  node.className = "empty-state";
  node.innerHTML = `
    <section class="subpanel">
      <h3>${escapeHtml(personId || t("unknown"))}</h3>
      <div class="muted">${escapeHtml(t("error_prefix", { error: String(error) }))}</div>
    </section>
  `;
}

function renderPerson(detail, media, sources, notes, memories, timeline, dnaHtml = "") {
  const person = detail.person;
  const facts = detail.facts || [];
  const biography = detail.biography || null;
  const stats = detail.stats || {};
  const filteredTimeline = filterTimelineForPerson(person, timeline);
  state.currentMedia = media;
  const personName = person.name || person.person_id;
  const sections = [
    buildBiographySection(biography),
    buildFactsSection(facts),
    buildTimelineSection(filteredTimeline),
    buildRelationshipGroups(detail),
    buildNotesSection(notes),
    buildSourcesSection(sources),
    buildMemoriesSection(memories),
    buildMediaSection(media, person.person_id),
  ]
    .filter(Boolean)
    .join("");
  document.getElementById("person-detail").innerHTML = `
    <div class="person-header">
      <div class="person-header-main">
        <div class="person-portrait-wrap">
          ${personThumb(person, "person-hero-portrait")}
        </div>
        <div>
          <h3>${escapeHtml(personName)}</h3>
          <div class="person-meta">
            ${escapeHtml(person.lifespan || t("no_lifespan"))}
            ${person.is_stub ? `<span class="stub-badge">${escapeHtml(t("basic_profile"))}</span>` : ""}
            <div class="technical-id">${escapeHtml(t("familysearch_id", { id: person.person_id || t("unknown") }))}</div>
          </div>
          ${!person.is_stub ? `<div class="person-actions"><button id="open-tree-from-person" class="secondary" type="button">${escapeHtml(t("open_tree"))}</button></div>` : ""}
        </div>
      </div>
      <div class="person-header-side">
        ${buildFamilyNav(detail)}
      </div>
    </div>
    ${buildStatChips(stats, media.length)}
    ${person.is_stub ? `<section class="subpanel"><div class="muted">${escapeHtml(t("basic_profile_only_desc"))}</div></section>` : ""}
    <div class="section-grid">
      ${sections || `<section class="subpanel"><div class="muted">${escapeHtml(t("no_local_detail_available"))}</div></section>`}
    </div>
  `;
  const mediaFilterNode = document.getElementById("media-filter");
  if (mediaFilterNode) {
    mediaFilterNode.addEventListener("change", (event) => {
      state.mediaFilter = event.target.value;
      renderPerson(detail, media, sources, notes, memories, timeline, dnaHtml);
    });
  }
  const memoryFilterNode = document.getElementById("memory-filter");
  if (memoryFilterNode) {
    memoryFilterNode.addEventListener("change", (event) => {
      state.memoryFilter = event.target.value;
      renderPerson(detail, media, sources, notes, memories, timeline, dnaHtml);
    });
  }
  for (const node of document.querySelectorAll("[data-person-link]")) {
    node.addEventListener("click", () => selectPerson(node.dataset.personLink).catch(console.error));
  }
  for (const node of document.querySelectorAll("[data-media-index]")) {
    node.addEventListener("click", () => {
      const filtered = filteredMediaForCurrentState(media).slice(0, 24);
      openMediaModal(filtered[Number(node.dataset.mediaIndex)]);
    });
  }
  const openTreeButton = document.getElementById("open-tree-from-person");
  if (openTreeButton) {
    openTreeButton.addEventListener("click", () => showTreeForPerson(person.person_id).catch(console.error));
  }
  for (const btn of document.querySelectorAll(".open-gallery-btn")) {
    btn.addEventListener("click", () => openGallery(btn.dataset.personGallery, media));
  }
}

async function loadStatus() {
  const status = await api("/api/status");
  renderStatus(status);
  if (status?.oauth) {
    renderOAuthStatus(status.oauth);
  }
  const runs = await api("/api/runs?limit=8");
  renderRuns(runs);
}

async function loadPeople(query = state.peopleQuery, page = state.peoplePage) {
  state.peopleQuery = query;
  state.peoplePage = Math.max(1, page);
  const offset = (state.peoplePage - 1) * state.peoplePageSize;
  const payload = await api(
    `/api/people?q=${encodeURIComponent(state.peopleQuery)}&limit=${encodeURIComponent(state.peoplePageSize)}&offset=${encodeURIComponent(offset)}&include_total=1`
  );
  state.people = payload.items || [];
  state.peopleTotal = Number(payload.total || 0);
  updateStatusBar();
  const maxPage = Math.max(1, Math.ceil((state.peopleTotal || 0) / state.peoplePageSize));
  if (state.peoplePage > maxPage) {
    state.peoplePage = maxPage;
    return loadPeople(state.peopleQuery, state.peoplePage);
  }
  const hashPersonId = decodeURIComponent(window.location.hash.replace(/^#person:/, ""));
  if (hashPersonId) {
    state.selectedPersonId = hashPersonId;
  } else if (!state.selectedPersonId) {
    state.selectedPersonId =
      preferredStartPersonId()
      || (state.people.find((person) => !person.is_stub)?.person_id ?? state.people[0]?.person_id ?? null);
  }
  renderPeople();
  const resolvedOnPage = state.people.find((person) => person.person_id === state.selectedPersonId);
  const resolvedGlobal = state.allPeople.find((person) => person.person_id === state.selectedPersonId);
  const resolved = resolvedOnPage || resolvedGlobal;
  if (state.selectedPersonId && (!resolved || !resolved.is_stub)) {
    try {
      await selectPerson(state.selectedPersonId);
    } catch {
      const fallback = state.people.find((person) => !person.is_stub)?.person_id || state.people[0]?.person_id || null;
      if (fallback && fallback !== state.selectedPersonId) {
        state.selectedPersonId = fallback;
        await selectPerson(fallback);
      }
    }
  } else if (state.currentView === "tree") {
    await loadTree();
  } else if (state.currentView === "connections") {
    renderConnectionPeople();
    await loadConnections();
  }
}

async function selectPerson(personId) {
  state.selectedPersonId = personId;
  state.mediaFilter = "all";
  state.memoryFilter = "all";
  state.dnaSelectedMatch = null;
  window.location.hash = `person:${encodeURIComponent(personId)}`;
  renderPeople();
  updateStatusBar();
  try {
    const [detail, media, sources, notes, memories, timeline] = await Promise.all([
      api(`/api/people/${encodeURIComponent(personId)}`),
      api(`/api/people/${encodeURIComponent(personId)}/media`),
      api(`/api/people/${encodeURIComponent(personId)}/sources`),
      api(`/api/people/${encodeURIComponent(personId)}/notes`),
      api(`/api/people/${encodeURIComponent(personId)}/memories`),
      api(`/api/people/${encodeURIComponent(personId)}/timeline`),
    ]);
    renderPerson(detail, media, sources, notes, memories, timeline);
    updateStatusBar();
    await syncBookControls();
    if (state.currentView === "dna") {
      loadDnaView(personId).catch(console.error);
    } else if (state.currentView === "tree") {
      loadTree().catch(console.error);
    } else if (state.currentView === "connections") {
      const emptySlot = state.connectionPersonIds.findIndex((id) => !id);
      if (emptySlot >= 0) {
        state.connectionPersonIds[emptySlot] = personId;
      } else if (!state.connectionPersonIds.length) {
        state.connectionPersonIds = [personId, null];
      }
      renderConnectionPeople();
      loadConnections().catch(console.error);
    }
  } catch (error) {
    renderPersonLoadError(personId, error);
    throw error;
  }
}

async function startSync() {
  const jobLimit = Number(document.getElementById("job-limit").value) || null;
  const generations = Math.max(1, Math.min(12, Number(document.getElementById("sync-generations")?.value) || 4));
  const syncCollateralRaw = Number(document.getElementById("sync-collateral-depth")?.value);
  const collateralDepth = Math.max(0, Math.min(12, Number.isFinite(syncCollateralRaw) ? syncCollateralRaw : generations));
  const force = document.getElementById("sync-force").checked;
  const resultNode = document.getElementById("sync-result");
  const connected = await ensureOAuthConnected();
  if (!connected) {
    resultNode.textContent = t("oauth_login_required");
    return;
  }
  try {
    const response = await fetch("/api/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        job_limit: jobLimit,
        force,
        generations,
        collateral_depth: collateralDepth,
      }),
    });
    const result = await response.json();
    resultNode.textContent = result.started ? t("sync_started_pid", { pid: result.pid }) : syncStartErrorMessage(result);
  } catch (error) {
    resultNode.textContent = String(error);
  }
  await loadStatus();
}

async function stopSync() {
  const resultNode = document.getElementById("sync-result");
  try {
    const response = await fetch("/api/sync/stop", { method: "POST" });
    const result = await response.json();
    resultNode.textContent = result.stopped ? t("sync_stopped", { pid: result.pid }) : String(result.reason || "");
  } catch (error) {
    resultNode.textContent = String(error);
  }
  await loadStatus();
}

function setupPedigreeZoom(container) {
  const svg = container.querySelector(".pedigree-svg");
  const resetBtn = container.querySelector(".pedigree-reset");
  if (!svg) return;

  function getVB() {
    const b = svg.viewBox.baseVal;
    return { x: b.x, y: b.y, w: b.width, h: b.height };
  }
  function setVB({ x, y, w, h }) {
    svg.setAttribute("viewBox", `${x} ${y} ${w} ${h}`);
  }
  function resetVB() {
    const orig = svg.dataset.origViewBox;
    if (orig) svg.setAttribute("viewBox", orig);
  }

  svg.addEventListener(
    "wheel",
    (e) => {
      e.preventDefault();
      const factor = e.deltaY < 0 ? 0.82 : 1.22;
      const rect = svg.getBoundingClientRect();
      const vb = getVB();
      const mx = vb.x + ((e.clientX - rect.left) / rect.width) * vb.w;
      const my = vb.y + ((e.clientY - rect.top) / rect.height) * vb.h;
      setVB({
        x: mx + (vb.x - mx) * factor,
        y: my + (vb.y - my) * factor,
        w: vb.w * factor,
        h: vb.h * factor,
      });
    },
    { passive: false }
  );

  let dragging = false;
  let hasDragged = false;
  let dragStart = null;
  let vbStart = null;

  svg.addEventListener("pointerdown", (e) => {
    if (e.button !== 0) return;
    dragging = true;
    hasDragged = false;
    dragStart = { x: e.clientX, y: e.clientY };
    vbStart = getVB();
    svg.setPointerCapture(e.pointerId);
  });

  svg.addEventListener("pointermove", (e) => {
    if (!dragging || !vbStart) return;
    const dx = e.clientX - dragStart.x;
    const dy = e.clientY - dragStart.y;
    if (!hasDragged && Math.abs(dx) + Math.abs(dy) < 5) return;
    hasDragged = true;
    svg.classList.add("pedigree-dragging");
    const rect = svg.getBoundingClientRect();
    const panDx = (dx / rect.width) * vbStart.w;
    const panDy = (dy / rect.height) * vbStart.h;
    setVB({ x: vbStart.x - panDx, y: vbStart.y - panDy, w: vbStart.w, h: vbStart.h });
  });

  svg.addEventListener("pointerup", () => {
    dragging = false;
    svg.classList.remove("pedigree-dragging");
  });

  // Block click from reaching pedigree-hit elements when a drag just ended
  svg.addEventListener(
    "click",
    (e) => {
      if (hasDragged) {
        e.stopPropagation();
        hasDragged = false;
      }
    },
    true
  );

  svg.addEventListener("dblclick", resetVB);
  if (resetBtn) resetBtn.addEventListener("click", resetVB);
}

async function downloadPedigreeSVG() {
  const svgEl = document.querySelector(".pedigree-svg");
  if (!svgEl) return;
  const clone = svgEl.cloneNode(true);
  const origVB = svgEl.dataset.origViewBox;
  if (origVB) clone.setAttribute("viewBox", origVB);
  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  clone.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");

  // Inline pedigree CSS
  const style = document.createElement("style");
  const cssRules = Array.from(document.styleSheets)
    .flatMap((sheet) => {
      try {
        return Array.from(sheet.cssRules);
      } catch {
        return [];
      }
    })
    .filter((rule) => rule.selectorText && rule.selectorText.match(/pedigree|tree-thumb/))
    .map((rule) => rule.cssText)
    .join("\n");
  style.textContent = cssRules;
  clone.prepend(style);

  // Convert image hrefs to data URIs
  const images = Array.from(clone.querySelectorAll("image[href]"));
  await Promise.all(
    images.map(async (img) => {
      const href = img.getAttribute("href");
      if (!href || href.startsWith("data:")) return;
      try {
        const resp = await fetch(href);
        const blob = await resp.blob();
        await new Promise((resolve) => {
          const reader = new FileReader();
          reader.onload = () => {
            img.setAttribute("href", reader.result);
            resolve();
          };
          reader.onerror = resolve;
          reader.readAsDataURL(blob);
        });
      } catch {
        // leave href as-is if fetch fails
      }
    })
  );

  const serializer = new XMLSerializer();
  const svgStr = serializer.serializeToString(clone);
  const blob = new Blob([svgStr], { type: "image/svg+xml" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "family-tree.svg";
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 10000);
}

function printPedigree() {
  const svgEl = document.querySelector(".pedigree-svg");
  const origVB = svgEl?.dataset.origViewBox;
  if (svgEl && origVB) svgEl.setAttribute("viewBox", origVB);
  window.print();
}

function setupPedigreeActions(container) {
  const wrap = container.querySelector(".pedigree-wrap");
  if (!wrap) return;
  const actions = document.createElement("div");
  actions.className = "pedigree-actions";
  actions.innerHTML = `
    <button class="secondary pedigree-action-svg" type="button">${escapeHtml(t("download_svg"))}</button>
    <button class="secondary pedigree-action-print" type="button">⎙ ${escapeHtml(t("print"))}</button>
  `;
  wrap.appendChild(actions);
  actions.querySelector(".pedigree-action-svg").addEventListener("click", () => downloadPedigreeSVG().catch(console.error));
  actions.querySelector(".pedigree-action-print").addEventListener("click", printPedigree);
}

async function loadStubCount() {
  const node = document.getElementById("stub-count");
  if (!node) return;
  try {
    const result = await api("/api/stub-count");
    node.textContent = t("stub_people_summary", { count: result.stub_count });
  } catch {
    node.textContent = t("unable_to_load_stub_count");
  }
}

async function loadCapabilities() {
  try {
    const caps = await api("/api/backup/capabilities");
    const link7z = document.getElementById("backup-7z-link");
    if (link7z && !caps.py7zr) {
      link7z.classList.add("disabled");
      link7z.title = t("py7zr_not_installed");
    } else if (link7z) {
      link7z.classList.remove("disabled");
      link7z.title = "";
    }
    const pdfLink = document.getElementById("book-pdf-link");
    const bookStatus = document.getElementById("book-status");
    if (pdfLink && !caps.pandoc) {
      pdfLink.classList.add("disabled");
      pdfLink.title = t("pandoc_not_in_path");
    } else if (pdfLink) {
      pdfLink.classList.remove("disabled");
      pdfLink.title = "";
    }
    if (bookStatus) {
      bookStatus.textContent = caps.pandoc
        ? t("pandoc_available", {
          path: caps.pandoc_path,
          engine: caps.pandoc_pdf_engine ? t("pandoc_available_engine", { engine: caps.pandoc_pdf_engine }) : "",
        })
        : t("pandoc_unavailable_msg");
    }
    await syncBookControls();
  } catch {
    const bookStatus = document.getElementById("book-status");
    if (bookStatus) bookStatus.textContent = "";
    await syncBookControls();
  }
}

function personQuickLabel(person) {
  if (!person) return "";
  const name = person.name || person.person_id;
  const life = person.lifespan || [person.birth_date, person.death_date].filter(Boolean).join(" - ") || t("no_lifespan");
  return `${name} · ${life} · ${person.person_id}`;
}

async function syncBookControls() {
  const rootId = resolveBookRootId();
  const rootInput = document.getElementById("book-root-search");
  const rootStatus = document.getElementById("book-root-current");
  const markdownLink = document.getElementById("book-markdown-link");
  const pdfLink = document.getElementById("book-pdf-link");
  if (rootId) {
    state.bookRootId = rootId;
    await ensurePersonCached(rootId);
  }
  const person = findPersonById(rootId);
  if (rootInput && rootId) {
    rootInput.value = connectionPersonLabel(person || { person_id: rootId, name: rootId });
  }
  if (rootStatus) {
    rootStatus.textContent = rootId
      ? t("book_root_current", { name: personQuickLabel(person || { person_id: rootId, name: rootId }) })
      : t("book_root_missing");
  }
  const lang = state.language || "es";
  const query = (rootId ? `?person_id=${encodeURIComponent(rootId)}&lang=${lang}` : `?lang=${lang}`);
  if (markdownLink) markdownLink.href = `/api/book/markdown${query}`;
  if (pdfLink) pdfLink.href = `/api/book/pdf${query}`;
}

function renderDedupeCandidates(items) {
  const node = document.getElementById("dedupe-list");
  if (!node) return;
  if (!items.length) {
    node.innerHTML = `<div class="muted">${escapeHtml(t("no_duplicate_candidates"))}</div>`;
    return;
  }
  node.innerHTML = items
    .map((item, idx) => {
      const left = item.left || {};
      const right = item.right || {};
      const score = Number(item.score || 0);
      return `
        <div class="run-card dedupe-card">
          <div class="run-card-header">
            <strong>${escapeHtml(left.name || right.name || left.person_id || right.person_id || t("unknown"))}</strong>
            <span class="badge">${escapeHtml(t("duplicate_score", { score }))}</span>
          </div>
          <div class="run-meta">
            <div>${escapeHtml(personQuickLabel(left))}</div>
            <div>${escapeHtml(personQuickLabel(right))}</div>
          </div>
          <div class="settings-links dedupe-actions">
            <button type="button" class="secondary" data-dedupe-open="${escapeHtml(left.person_id || "")}" data-dedupe-index="${idx}" data-side="left">${escapeHtml(t("open_left"))}</button>
            <button type="button" class="secondary" data-dedupe-open="${escapeHtml(right.person_id || "")}" data-dedupe-index="${idx}" data-side="right">${escapeHtml(t("open_right"))}</button>
            <button type="button" class="secondary" data-dedupe-ignore="${idx}">${escapeHtml(t("ignore_pair"))}</button>
          </div>
        </div>
      `;
    })
    .join("");

  for (const btn of node.querySelectorAll("[data-dedupe-open]")) {
    btn.addEventListener("click", () => {
      const personId = btn.getAttribute("data-dedupe-open");
      if (!personId) return;
      setView("explorer");
      selectPerson(personId).catch(console.error);
    });
  }
  for (const btn of node.querySelectorAll("[data-dedupe-ignore]")) {
    btn.addEventListener("click", async () => {
      const index = Number(btn.getAttribute("data-dedupe-ignore"));
      const item = items[index];
      if (!item?.left?.person_id || !item?.right?.person_id) return;
      await fetch("/api/dedupe/ignore", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          person_id_a: item.left.person_id,
          person_id_b: item.right.person_id,
          reason: "manual_ignore",
        }),
      });
      await loadDedupeCandidates();
    });
  }
}

function renderSimplePager(infoId, prevId, nextId, page, pageSize, total) {
  const info = document.getElementById(infoId);
  const prev = document.getElementById(prevId);
  const next = document.getElementById(nextId);
  if (!info || !prev || !next) return;
  const totalPages = Math.max(1, Math.ceil((total || 0) / pageSize));
  const currentPage = Math.min(Math.max(1, page), totalPages);
  const from = total ? (currentPage - 1) * pageSize + 1 : 0;
  const to = total ? Math.min(currentPage * pageSize, total) : 0;
  info.textContent = t("page_of_total", {
    from: formatCount(from),
    to: formatCount(to),
    total: formatCount(total),
    page: currentPage,
    pages: totalPages,
  });
  prev.disabled = currentPage <= 1;
  next.disabled = currentPage >= totalPages;
}

async function loadDedupeCandidates(page = state.dedupePage) {
  const node = document.getElementById("dedupe-list");
  if (!node) return;
  node.textContent = t("loading");
  try {
    state.dedupePage = Math.max(1, page);
    const offset = (state.dedupePage - 1) * state.dedupePageSize;
    const payload = await api(
      `/api/dedupe/candidates?limit=${encodeURIComponent(state.dedupePageSize)}&offset=${encodeURIComponent(offset)}&min_score=55`
    );
    state.dedupeTotal = Number(payload.total || 0);
    renderDedupeCandidates(payload.items || []);
    renderSimplePager("dedupe-page-info", "dedupe-prev-page", "dedupe-next-page", state.dedupePage, state.dedupePageSize, state.dedupeTotal);
  } catch (error) {
    node.textContent = String(error);
  }
}

function renderHistoricalEvents(items) {
  const node = document.getElementById("historical-events-list");
  if (!node) return;
  if (!items.length) {
    node.innerHTML = `<div class="muted">${escapeHtml(t("no_historical_events"))}</div>`;
    return;
  }
  const sorted = [...items].sort((a, b) => {
    const ay = Number(a.start_year || 0);
    const by = Number(b.start_year || 0);
    if (ay !== by) return by - ay;
    return String(a.scope || "").localeCompare(String(b.scope || ""));
  });
  node.innerHTML = sorted
    .slice(0, 80)
    .map((item) => {
      const yearLabel = Number(item.start_year) === Number(item.end_year)
        ? `${item.start_year}`
        : `${item.start_year}-${item.end_year}`;
      const scopeLabel = item.scope === "local" ? t("historical_local") : t("historical_global");
      return `
        <div class="run-card historical-card">
          <div class="run-card-header">
            <strong>${escapeHtml(yearLabel)} · ${escapeHtml(item.title || "")}</strong>
            <span class="badge">${escapeHtml(scopeLabel)}</span>
          </div>
          ${item.description ? `<div class="run-meta">${escapeHtml(item.description)}</div>` : ""}
          ${item.match_terms?.length ? `<div class="run-meta">${escapeHtml(item.match_terms.join(", "))}</div>` : ""}
          <div class="settings-links dedupe-actions">
            ${item.source_url ? `<a class="settings-download-link" href="${escapeHtml(item.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(t("open_source"))}</a>` : ""}
          </div>
        </div>
      `;
    })
    .join("");
}

async function loadHistoricalEvents(page = state.historicalPage) {
  const node = document.getElementById("historical-events-list");
  if (!node) return;
  const from = Number(document.getElementById("historical-year-from")?.value) || 1900;
  const to = Number(document.getElementById("historical-year-to")?.value) || 2026;
  const place = String(document.getElementById("historical-place")?.value || "").trim();
  const scope = String(document.getElementById("historical-scope-filter")?.value || "").trim();
  node.textContent = t("loading");
  try {
    state.historicalPage = Math.max(1, page);
    const offset = (state.historicalPage - 1) * state.historicalPageSize;
    const payload = await api(
      `/api/historical/events?year_from=${encodeURIComponent(from)}&year_to=${encodeURIComponent(to)}&place=${encodeURIComponent(place)}&scope=${encodeURIComponent(scope)}&limit=${encodeURIComponent(state.historicalPageSize)}&offset=${encodeURIComponent(offset)}`
    );
    state.historicalTotal = Number(payload.total || 0);
    renderHistoricalEvents(payload.items || []);
    renderSimplePager(
      "historical-page-info",
      "historical-prev-page",
      "historical-next-page",
      state.historicalPage,
      state.historicalPageSize,
      state.historicalTotal,
    );
  } catch (error) {
    node.textContent = String(error);
  }
}

async function syncHistoricalEvents() {
  const resultNode = document.getElementById("historical-sync-result");
  const yearFrom = Number(document.getElementById("historical-year-from")?.value) || 1900;
  const yearTo = Number(document.getElementById("historical-year-to")?.value) || 2026;
  const localCountry = String(document.getElementById("historical-place")?.value || "").trim() || "Venezuela";
  resultNode.textContent = t("historical_syncing");
  try {
    const response = await fetch("/api/historical/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        year_from: yearFrom,
        year_to: yearTo,
        local_country: localCountry,
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      resultNode.textContent = t("error_prefix", { error: payload.error || `HTTP ${response.status}` });
      return;
    }
    resultNode.textContent = t("historical_sync_done", {
      years: payload.years_processed || 0,
      global: payload.stored_global || 0,
      local: payload.stored_local || 0,
    });
    await loadHistoricalEvents(1);
    if (state.selectedPersonId) {
      await selectPerson(state.selectedPersonId);
    }
  } catch (error) {
    resultNode.textContent = String(error);
  }
}

function parseLocalTerms(rawTerms) {
  return String(rawTerms || "")
    .split(/[,;\n]+/)
    .map((term) => term.trim().toLowerCase())
    .filter(Boolean);
}

async function createHistoricalEvent() {
  const resultNode = document.getElementById("historical-sync-result");
  const scope = String(document.getElementById("historical-create-scope")?.value || "global").trim().toLowerCase();
  const title = String(document.getElementById("historical-create-title")?.value || "").trim();
  const startYear = Number(document.getElementById("historical-create-year")?.value);
  const endYearRaw = Number(document.getElementById("historical-create-end-year")?.value);
  const endYear = Number.isFinite(endYearRaw) ? endYearRaw : startYear;
  const description = String(document.getElementById("historical-create-description")?.value || "").trim();
  const sourceUrl = String(document.getElementById("historical-create-source-url")?.value || "").trim();
  const terms = parseLocalTerms(document.getElementById("historical-create-terms")?.value || "");
  if (!title) {
    resultNode.textContent = t("historical_missing_title");
    return;
  }
  if (!Number.isFinite(startYear) || startYear < 1000 || startYear > 2100) {
    resultNode.textContent = t("historical_invalid_year");
    return;
  }
  if (!Number.isFinite(endYear) || endYear < 1000 || endYear > 2100) {
    resultNode.textContent = t("historical_invalid_year");
    return;
  }
  try {
    const response = await fetch("/api/historical/events", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        scope: scope === "local" ? "local" : "global",
        title,
        description,
        start_year: startYear,
        end_year: endYear,
        source_url: sourceUrl || null,
        match_terms: scope === "local" ? terms : [],
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      resultNode.textContent = t("error_prefix", { error: payload.error || `HTTP ${response.status}` });
      return;
    }
    resultNode.textContent = t("historical_saved");
    document.getElementById("historical-create-title").value = "";
    document.getElementById("historical-create-description").value = "";
    document.getElementById("historical-create-source-url").value = "";
    document.getElementById("historical-create-terms").value = "";
    await loadHistoricalEvents(1);
    if (state.selectedPersonId) {
      await selectPerson(state.selectedPersonId);
    }
  } catch (error) {
    resultNode.textContent = String(error);
  }
}

function stopFsImportProgressMonitor() {
  if (fsImportProgressIntervalId) {
    window.clearInterval(fsImportProgressIntervalId);
    fsImportProgressIntervalId = null;
  }
}

async function refreshFsImportProgress() {
  const resultNode = document.getElementById("fs-import-result");
  if (!resultNode) return;
  const status = await api("/api/status");
  const syncProcess = status.sync_process || {};
  const activeRun = status.active_run;
  if (activeRun) {
    const queue = activeRun.queue || {};
    resultNode.textContent = t("import_progress_line", {
      pct: Math.round(Number(activeRun.progress_percent || 0)),
      done: formatCount(activeRun.completed_jobs || 0),
      total: formatCount(activeRun.total_jobs || 0),
      pending: formatCount(queue.pending || 0),
      in_progress: formatCount(queue.in_progress || 0),
      failed: formatCount(queue.failed || 0),
    });
    return;
  }
  if (syncProcess.running) {
    const queueRows = Array.isArray(status.queue_by_status) ? status.queue_by_status : [];
    if (queueRows.length) {
      const queue = { pending: 0, in_progress: 0, done: 0, failed: 0 };
      for (const item of queueRows) {
        const key = String(item.status || "");
        if (key in queue) {
          queue[key] = Number(item.qty || 0);
        }
      }
      const total = queue.pending + queue.in_progress + queue.done + queue.failed;
      const done = queue.done + queue.failed;
      if (total > 0) {
        const pct = Math.round((done * 100) / total);
        resultNode.textContent = t("import_progress_line", {
          pct,
          done: formatCount(done),
          total: formatCount(total),
          pending: formatCount(queue.pending),
          in_progress: formatCount(queue.in_progress),
          failed: formatCount(queue.failed),
        });
        return;
      }
    }
    resultNode.textContent = t("import_preparing_queue");
    return;
  }
  const latest = status.latest_run;
  if (!latest) {
    stopFsImportProgressMonitor();
    return;
  }
  if (latest.status === "completed") {
    resultNode.textContent = t("import_completed_line", {
      persons: formatCount(latest.persons_count || 0),
      relationships: formatCount(latest.relationships_count || 0),
      media: formatCount(latest.media_count || 0),
      done: formatCount(latest.jobs_done || 0),
    });
    stopFsImportProgressMonitor();
    return;
  }
  if (latest.status === "failed" || latest.status === "aborted") {
    resultNode.textContent = t("import_failed_line", {
      status: latest.status,
      error: latest.last_error || "unknown",
    });
    stopFsImportProgressMonitor();
    return;
  }
  resultNode.textContent = `${latest.status} (#${latest.id})`;
}

function startFsImportProgressMonitor() {
  stopFsImportProgressMonitor();
  refreshFsImportProgress().catch(console.error);
  fsImportProgressIntervalId = window.setInterval(() => {
    refreshFsImportProgress().catch(console.error);
  }, 2000);
}

async function importBackup() {
  const fileInput = document.getElementById("backup-import-file");
  const resultNode = document.getElementById("backup-import-result");
  const file = fileInput && fileInput.files && fileInput.files[0];
  if (!file) {
    resultNode.textContent = t("please_select_backup_file");
    return;
  }
  resultNode.textContent = t("uploading");
  try {
    const buffer = await file.arrayBuffer();
    const contentType = file.name.endsWith(".7z") ? "application/x-7z-compressed" : "application/zip";
    const response = await fetch("/api/import/backup", {
      method: "POST",
      headers: { "Content-Type": contentType, "Content-Length": String(buffer.byteLength) },
      body: buffer,
    });
    const data = await response.json();
    if (!response.ok || !data.success) {
      resultNode.textContent = t("error_prefix", { error: data.error || "unknown" });
      return;
    }
    resultNode.textContent = t("imported_people_reloading", { count: data.persons });
    setTimeout(() => window.location.reload(), 1500);
  } catch (error) {
    resultNode.textContent = String(error);
  }
}

async function importGedcom() {
  const fileInput = document.getElementById("gedcom-import-file");
  const rootNameInput = document.getElementById("gedcom-root-name");
  const resultNode = document.getElementById("gedcom-import-result");
  const file = fileInput && fileInput.files && fileInput.files[0];
  const rootName = rootNameInput ? rootNameInput.value.trim() : "";
  if (!file) {
    resultNode.textContent = t("please_select_gedcom_file");
    return;
  }
  resultNode.textContent = t("importing");
  try {
    const buffer = await file.arrayBuffer();
    const endpoint = rootName ? `/api/import/gedcom?root_name=${encodeURIComponent(rootName)}` : "/api/import/gedcom";
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "text/plain", "Content-Length": String(buffer.byteLength) },
      body: buffer,
    });
    const data = await response.json();
    if (!response.ok) {
      resultNode.textContent = t("error_prefix", { error: data.error || "unknown" });
      return;
    }
    let message = t("import_done_stats", { imported: data.imported, updated: data.updated, skipped: data.skipped });
    if (data.root_person_id) {
      message += ` · ${t("gedcom_root_set", { id: data.root_person_id })}`;
    }
    resultNode.textContent = message;
    await loadPeople("", 1);
    await loadStatus();
    await loadBootstrapStatus().catch(() => null);
  } catch (error) {
    resultNode.textContent = String(error);
  }
}

async function startFamilySearchImportRequest(personId, generations, force, collateralDepth = null) {
  const payload = {
    person_id: personId,
    generations,
    force,
  };
  if (Number.isFinite(Number(collateralDepth))) {
    payload.collateral_depth = Math.max(0, Math.min(12, Number(collateralDepth)));
  }
  const response = await fetch("/api/import/familysearch", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  let result = {};
  try {
    result = await response.json();
  } catch {
    result = {};
  }
  if (!response.ok || !result.started) {
    return {
      ok: false,
      result,
      message: syncStartErrorMessage(result) || `HTTP ${response.status}`,
    };
  }
  return {
    ok: true,
    result,
    message: `${t("sync_started_pid", { pid: result.pid })} · ${t("gedcom_root_set", { id: result.root_person_id || personId })}`,
  };
}

async function importFamilySearchRoot() {
  const personIdInput = document.getElementById("fs-import-person-id");
  const generationsInput = document.getElementById("fs-import-generations");
  const collateralDepthInput = document.getElementById("fs-import-collateral-depth");
  const forceInput = document.getElementById("fs-import-force");
  const resultNode = document.getElementById("fs-import-result");
  stopFsImportProgressMonitor();
  const personId = String(personIdInput?.value || "").trim().toUpperCase();
  if (!personId) {
    resultNode.textContent = t("please_enter_person_id");
    return;
  }
  const generations = Math.max(1, Math.min(12, Number(generationsInput?.value) || 4));
  const fsCollateralRaw = Number(collateralDepthInput?.value);
  const collateralDepth = Math.max(0, Math.min(12, Number.isFinite(fsCollateralRaw) ? fsCollateralRaw : generations));
  const force = Boolean(forceInput?.checked);
  resultNode.textContent = t("starting_import");
  const connected = await ensureOAuthConnected();
  if (!connected) {
    resultNode.textContent = t("oauth_login_required");
    return;
  }
  try {
    const start = await startFamilySearchImportRequest(personId, generations, force, collateralDepth);
    if (!start.ok) {
      resultNode.textContent = start.message;
      return;
    }
    resultNode.textContent = start.message;
    startFsImportProgressMonitor();
    await loadStatus();
    await loadBootstrapStatus().catch(() => null);
  } catch (error) {
    resultNode.textContent = String(error);
    stopFsImportProgressMonitor();
  }
}

async function importFromBootstrap() {
  const personIdInput = document.getElementById("bootstrap-person-id");
  const generationsInput = document.getElementById("bootstrap-generations");
  const collateralDepthInput = document.getElementById("bootstrap-collateral-depth");
  const resultNode = document.getElementById("bootstrap-action-result");
  const personId = String(personIdInput?.value || "").trim().toUpperCase();
  if (!personId) {
    if (resultNode) resultNode.textContent = t("please_enter_person_id");
    return;
  }
  const generations = Math.max(1, Math.min(12, Number(generationsInput?.value) || 4));
  const bootstrapCollateralRaw = Number(collateralDepthInput?.value);
  const collateralDepth = Math.max(0, Math.min(12, Number.isFinite(bootstrapCollateralRaw) ? bootstrapCollateralRaw : generations));
  if (resultNode) resultNode.textContent = t("starting_import");
  const connected = await ensureOAuthConnected();
  if (!connected) {
    if (resultNode) resultNode.textContent = t("oauth_login_required");
    return;
  }
  const fsPersonInput = document.getElementById("fs-import-person-id");
  const fsGenerationsInput = document.getElementById("fs-import-generations");
  const fsCollateralDepthInput = document.getElementById("fs-import-collateral-depth");
  if (fsPersonInput) fsPersonInput.value = personId;
  if (fsGenerationsInput) fsGenerationsInput.value = String(generations);
  if (fsCollateralDepthInput) fsCollateralDepthInput.value = String(collateralDepth);
  try {
    const start = await startFamilySearchImportRequest(personId, generations, false, collateralDepth);
    if (!start.ok) {
      if (resultNode) resultNode.textContent = t("bootstrap_import_failed", { error: start.message });
      return;
    }
    if (resultNode) {
      resultNode.textContent = t("bootstrap_import_started", { pid: start.result.pid || "?" });
    }
    startBootstrapImportMonitor();
    startFsImportProgressMonitor();
    await loadStatus().catch(() => null);
  } catch (error) {
    if (resultNode) resultNode.textContent = t("bootstrap_import_failed", { error: String(error) });
  }
}

// GRCh38 approximate chromosome lengths in bp
const CHR_LENGTHS = {
  "1": 248956422, "2": 242193529, "3": 198295559, "4": 190214555, "5": 181538259,
  "6": 170805979, "7": 159345973, "8": 145138636, "9": 138394717, "10": 133797422,
  "11": 135086622, "12": 133275309, "13": 114364328, "14": 107043718, "15": 101991189,
  "16": 90338345, "17": 83257441, "18": 80373285, "19": 58617616, "20": 64444167,
  "21": 46709983, "22": 40924039, "X": 156040895,
};
const CHR_ORDER = ["1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","X"];
const DNA_PALETTE = [
  "#e76f51","#2a9d8f","#e9c46a","#264653","#f4a261","#a8dadc","#457b9d","#e63946",
  "#48cae4","#b5838d","#52b788","#c77dff",
];
function formatBp(value) {
  return new Intl.NumberFormat().format(Number(value || 0));
}

function formatCm(value) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? `${num.toFixed(1)} cM` : "0.0 cM";
}

function formatPct(value) {
  const num = Number(value || 0);
  return `${num.toFixed(1)}%`;
}

async function safeApi(path, fallback) {
  try {
    return await api(path);
  } catch {
    return fallback;
  }
}

function dnaLabelForSide(side) {
  const token = String(side || "").toLowerCase();
  if (token === "maternal") return t("dna_maternal");
  if (token === "paternal") return t("dna_paternal");
  return t("dna_unassigned");
}

function dnaColorsForLegend(legend) {
  return Object.fromEntries((legend || []).map((item, index) => [
    item.branch_key,
    DNA_PALETTE[index % DNA_PALETTE.length],
  ]));
}

function renderDnaUploadCard(target, labelKey, hintKey, accept = ".csv,.txt,.json") {
  return `
    <div class="dna-import-card">
      <div class="dna-import-title">${escapeHtml(t(labelKey))}</div>
      <div class="dna-import-hint">${escapeHtml(t(hintKey))}</div>
      <label class="import-file-label dna-import-label">
        <input id="dna-file-${escapeAttr(target)}" type="file" accept="${escapeAttr(accept)}">
        <span>${escapeHtml(t("replace_data"))}</span>
      </label>
      <button type="button" class="secondary" data-dna-upload-target="${escapeAttr(target)}">${escapeHtml(t("upload"))}</button>
      <div id="dna-upload-result-${escapeAttr(target)}" class="muted"></div>
    </div>
  `;
}

function renderSegmentDetail(segment) {
  if (!segment) {
    return `<div class="muted">${escapeHtml(t("dna_segment_detail_placeholder"))}</div>`;
  }
  const traits = (segment.traits || []).length
    ? `<ul class="dna-inline-list">${segment.traits.map((item) => `<li>${escapeHtml(item.trait_name)} <span class="muted">(${escapeHtml(item.rsid)})</span></li>`).join("")}</ul>`
    : `<div class="muted">${escapeHtml(t("none"))}</div>`;
  return `
    <div class="dna-detail-stack">
      <div class="dna-detail-line"><strong>${escapeHtml(segment.branch_label || t("dna_unassigned"))}</strong></div>
      <div class="dna-detail-line">${escapeHtml(t("dna_probable_ancestor"))}: ${escapeHtml(segment.probable_ancestor || t("dna_unknown"))}</div>
      <div class="dna-detail-line">${escapeHtml(t("dna_genomic_range"))}: ${escapeHtml(segment.genomic_range)}</div>
      <div class="dna-detail-line">${escapeHtml(formatCm(segment.centimorgans || 0))} · ${escapeHtml(segment.source || "")}</div>
      <div class="dna-detail-line">${escapeHtml(t("dna_associated_traits"))}: ${traits}</div>
    </div>
  `;
}

function renderChromosomePainter(personId, painterData) {
  const allSegments = painterData.segments || [];
  if (!allSegments.length) {
    return `<div class="muted">${escapeHtml(t("no_dna_data"))}</div>`;
  }
  const legend = painterData.legend || [];
  const colorMap = dnaColorsForLegend(legend);
  const svgWidth = 900;
  const labelWidth = 34;
  const barWidth = svgWidth - labelWidth - 28;
  const barH = 14;
  const gapH = 10;
  const totalH = CHR_ORDER.length * (barH + gapH) + 32;
  const maxLen = Math.max(...CHR_ORDER.map((c) => CHR_LENGTHS[c] || 1));
  const rows = CHR_ORDER.map((chr, idx) => {
    const y = 14 + idx * (barH + gapH);
    const chrLen = CHR_LENGTHS[chr] || maxLen;
    const scale = barWidth / maxLen;
    const barScaled = chrLen * scale;
    const segBars = allSegments
      .filter((segment) => String(segment.chromosome) === chr)
      .map((segment) => {
        const x = labelWidth + segment.start_pos * scale;
        const w = Math.max(1, (segment.end_pos - segment.start_pos) * scale);
        const fill = colorMap[segment.branch_key] || "#7a7a7a";
        const title = [
          segment.branch_label,
          segment.probable_ancestor,
          segment.centimorgans ? formatCm(segment.centimorgans) : "",
        ].filter(Boolean).join(" · ");
        const index = allSegments.indexOf(segment);
        return `<rect class="chr-seg" data-segment-index="${index}" x="${x.toFixed(1)}" y="${y}" width="${w.toFixed(1)}" height="${barH}" fill="${escapeAttr(fill)}"><title>${escapeHtml(title)}</title></rect>`;
      })
      .join("");
    return `
      <text class="chr-label" x="${labelWidth - 6}" y="${y + barH - 2}" text-anchor="end">${escapeHtml(chr)}</text>
      <rect class="chr-bg" x="${labelWidth}" y="${y}" width="${barScaled.toFixed(1)}" height="${barH}" rx="5"></rect>
      ${segBars}
    `;
  }).join("");
  const legendItems = legend.map((item) =>
    `<div class="dna-legend-item"><span class="dna-legend-swatch" style="background:${escapeAttr(colorMap[item.branch_key] || "#7a7a7a")}"></span>${escapeHtml(item.branch_label)}</div>`
  ).join("");
  return `
    <div class="dna-painter-layout">
      <div class="dna-painter-visual">
        <div class="chromosome-painter" id="dna-painter-shell">
          <svg id="dna-painter-svg" width="${svgWidth}" height="${totalH}" viewBox="0 0 ${svgWidth} ${totalH}">
            ${rows}
          </svg>
          <div id="dna-segment-tooltip" class="dna-tooltip hidden"></div>
        </div>
        ${legendItems ? `<div class="dna-legend">${legendItems}</div>` : ""}
        <div class="dna-hint">${escapeHtml(t("dna_select_segment"))}</div>
      </div>
      <aside class="dna-detail-panel">
        <h5>${escapeHtml(t("dna_segment_detail"))}</h5>
        <div id="dna-segment-detail">${renderSegmentDetail(allSegments[0])}</div>
      </aside>
    </div>
  `;
}

function renderTraitsPanel(traitsData) {
  const categories = traitsData.categories || {};
  const allItems = Object.values(categories).flat();
  if (!allItems.length) {
    return `<div class="muted">${escapeHtml(t("dna_no_traits"))}</div>`;
  }
  const labels = {
    health: state.language === "es" ? "Salud" : "Health",
    physical: state.language === "es" ? "Físicas" : "Physical",
    behavior: state.language === "es" ? "Comportamentales / cognitivas" : "Behavioral / cognitive",
  };
  return `
    <div class="dna-traits-grid">
      ${Object.entries(labels).map(([key, label]) => `
        <section class="dna-mini-panel">
          <div class="dna-panel-head">
            <strong>${escapeHtml(label)}</strong>
            <span class="muted">${escapeHtml(t("dna_traits_found", { count: (categories[key] || []).length }))}</span>
          </div>
          ${(categories[key] || []).length ? `
            <div class="dna-chip-list">
              ${(categories[key] || []).map((item) => `
                <article class="dna-chip-card">
                  <div><strong>${escapeHtml(item.trait_name)}</strong></div>
                  <div class="muted">${escapeHtml(item.rsid)} · ${escapeHtml(item.genotype)} · ${escapeHtml(t("dna_copy_count", { count: item.copies }))}</div>
                  <div>${escapeHtml(item.effect)}</div>
                  <div class="dna-tag-row">
                    <span class="dna-tag">${escapeHtml(item.confidence || "medium")}</span>
                    ${key !== "physical" ? `<span class="dna-tag dna-tag-soft">${escapeHtml(t("dna_probabilistic"))}</span>` : ""}
                  </div>
                </article>
              `).join("")}
            </div>
          ` : `<div class="muted">${escapeHtml(t("none"))}</div>`}
          ${key === "health" ? `<div class="dna-hint">${escapeHtml(traitsData.disclaimers?.health || t("dna_disclaimer_health"))}</div>` : ""}
          ${key === "behavior" ? `<div class="dna-hint">${escapeHtml(traitsData.disclaimers?.behavior || t("dna_disclaimer_behavior"))}</div>` : ""}
        </section>
      `).join("")}
    </div>
  `;
}

function renderEthnicityPanel(ethnicityData) {
  const items = ethnicityData.items || [];
  if (!items.length) {
    return `<div class="muted">${escapeHtml(t("dna_no_ethnicity"))}</div>`;
  }
  const slices = [];
  let offset = 0;
  items.forEach((item, index) => {
    const color = item.color_hint || DNA_PALETTE[index % DNA_PALETTE.length];
    const end = offset + Number(item.percentage || 0);
    slices.push(`${color} ${offset}% ${end}%`);
    offset = end;
  });
  const gradient = `conic-gradient(${slices.join(", ")})`;
  return `
    <div class="dna-ethnicity-layout">
      <div class="dna-donut-wrap">
        <div class="dna-donut" style="background:${escapeAttr(gradient)}"></div>
        <div class="dna-donut-center">${escapeHtml(formatPct(ethnicityData.total_percentage || 0))}</div>
      </div>
      <div class="dna-ethnicity-list">
        ${items.map((item, index) => `
          <div class="dna-ethnicity-row">
            <span class="dna-legend-swatch" style="background:${escapeAttr(item.color_hint || DNA_PALETTE[index % DNA_PALETTE.length])}"></span>
            <span>${escapeHtml(item.region)}</span>
            <strong>${escapeHtml(formatPct(item.percentage))}</strong>
          </div>
        `).join("")}
        <div class="dna-hint">${escapeHtml(t("dna_reference_panel"))}: ${escapeHtml(items[0].reference_panel || t("dna_unknown"))}</div>
        ${(ethnicityData.generation_breakdown || []).length ? `
          <div class="dna-generation-block">
            <strong>${escapeHtml(t("dna_generation_breakdown"))}</strong>
            ${(ethnicityData.generation_breakdown || []).map((item) => `
              <div class="dna-generation-row">
                <span>${escapeHtml(item.generation)}</span>
                <strong>${escapeHtml(formatPct(item.percentage))}</strong>
              </div>
            `).join("")}
          </div>
        ` : ""}
      </div>
    </div>
  `;
}

function projectMapPoint(point, width = 320, height = 150) {
  const lon = Number(point.lon || 0);
  const lat = Number(point.lat || 0);
  return {
    x: ((lon + 180) / 360) * width,
    y: ((90 - lat) / 180) * height,
  };
}

function renderHaplogroupRoute(points, cssClass) {
  if (!points.length) return `<div class="muted">${escapeHtml(t("none"))}</div>`;
  const width = 320;
  const height = 150;
  const coords = points.map((point) => ({ ...point, ...projectMapPoint(point, width, height) }));
  const path = coords.map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  return `
    <svg class="dna-route-map ${escapeAttr(cssClass)}" viewBox="0 0 ${width} ${height}">
      <rect x="0" y="0" width="${width}" height="${height}" rx="12"></rect>
      <path d="${escapeAttr(path)}"></path>
      ${coords.map((point) => `<circle cx="${point.x.toFixed(1)}" cy="${point.y.toFixed(1)}" r="4"></circle>`).join("")}
    </svg>
  `;
}

function renderHaplogroupsPanel(haplogroupsData) {
  const hasY = haplogroupsData.y_haplogroup || (haplogroupsData.y_timeline || []).length;
  const hasMt = haplogroupsData.mt_haplogroup || (haplogroupsData.mt_timeline || []).length;
  if (!hasY && !hasMt) {
    return `<div class="muted">${escapeHtml(t("dna_no_haplogroups"))}</div>`;
  }
  const cards = [
    {
      title: t("dna_haplogroup_y"),
      value: haplogroupsData.y_haplogroup,
      route: haplogroupsData.y_timeline || [],
      cssClass: "dna-route-y",
    },
    {
      title: t("dna_haplogroup_mt"),
      value: haplogroupsData.mt_haplogroup,
      route: haplogroupsData.mt_timeline || [],
      cssClass: "dna-route-mt",
    },
  ];
  return `
    <div class="dna-haplogroup-grid">
      ${cards.map((item) => `
        <article class="dna-mini-panel">
          <div class="dna-panel-head">
            <strong>${escapeHtml(item.title)}</strong>
            <span class="dna-code">${escapeHtml(item.value || t("dna_unknown"))}</span>
          </div>
          <div class="dna-hint">${escapeHtml(t("dna_lineage_route"))}</div>
          ${renderHaplogroupRoute(item.route, item.cssClass)}
          <div class="dna-inline-list-wrap">
            ${(item.route || []).map((point) => `<span class="dna-pill">${escapeHtml(point.label)} · ${escapeHtml(point.period)}</span>`).join("")}
          </div>
        </article>
      `).join("")}
    </div>
  `;
}

function renderMatchBrowser(match) {
  if (!match) return `<div class="muted">${escapeHtml(t("dna_no_matches"))}</div>`;
  const segments = match.segments || [];
  if (!segments.length) return `<div class="muted">${escapeHtml(t("dna_no_match_segments"))}</div>`;
  const svgWidth = 760;
  const labelWidth = 26;
  const barWidth = svgWidth - labelWidth - 20;
  const barH = 10;
  const gapH = 6;
  const totalH = CHR_ORDER.length * (barH + gapH) + 18;
  const maxLen = Math.max(...CHR_ORDER.map((c) => CHR_LENGTHS[c] || 1));
  const rows = CHR_ORDER.map((chr, idx) => {
    const y = 8 + idx * (barH + gapH);
    const chrLen = CHR_LENGTHS[chr] || maxLen;
    const scale = barWidth / maxLen;
    const segBars = segments
      .filter((segment) => String(segment.chromosome) === chr)
      .map((segment) => {
        const x = labelWidth + segment.start_pos * scale;
        const w = Math.max(1, (segment.end_pos - segment.start_pos) * scale);
        return `<rect class="match-seg" x="${x.toFixed(1)}" y="${y}" width="${w.toFixed(1)}" height="${barH}" rx="3"></rect>`;
      })
      .join("");
    return `
      <text class="chr-label" x="${labelWidth - 4}" y="${y + barH - 1}" text-anchor="end">${escapeHtml(chr)}</text>
      <rect class="chr-bg" x="${labelWidth}" y="${y}" width="${(chrLen * (barWidth / maxLen)).toFixed(1)}" height="${barH}" rx="3"></rect>
      ${segBars}
    `;
  }).join("");
  return `
    <div class="dna-browser-head">
      <strong>${escapeHtml(match.match_name)}</strong>
      <span class="muted">${escapeHtml(formatCm(match.total_cm || 0))} · ${escapeHtml(match.predicted_relationship || t("dna_unknown"))}</span>
    </div>
    <div class="chromosome-painter dna-match-browser">
      <svg width="${svgWidth}" height="${totalH}" viewBox="0 0 ${svgWidth} ${totalH}">
        ${rows}
      </svg>
    </div>
  `;
}

function filteredMatches(matches) {
  return (matches || []).filter((item) => {
    const totalCm = Number(item.total_cm || 0);
    const side = String(item.side || "").toLowerCase();
    if (totalCm < Number(state.dnaMatchMinCm || 0)) return false;
    if (state.dnaMatchSide !== "all" && side !== state.dnaMatchSide) return false;
    return true;
  });
}

function renderMatchesPanel(matchesData) {
  const matches = filteredMatches(matchesData.items || []);
  if (!(matchesData.items || []).length) {
    return `<div class="muted">${escapeHtml(t("dna_no_matches"))}</div>`;
  }
  const selected = matches.find((item) => item.match_name === state.dnaSelectedMatch) || matches[0] || null;
  if (selected) state.dnaSelectedMatch = selected.match_name;
  return `
    <div class="dna-match-controls">
      <label class="inline-control">
        <span>${escapeHtml(t("dna_filter_min_cm"))}</span>
        <input id="dna-match-min-cm" type="number" min="0" step="1" value="${escapeAttr(state.dnaMatchMinCm || 0)}">
      </label>
      <label class="inline-control">
        <span>${escapeHtml(t("dna_filter_side"))}</span>
        <select id="dna-match-side">
          <option value="all"${state.dnaMatchSide === "all" ? " selected" : ""}>${escapeHtml(t("dna_all_sides"))}</option>
          <option value="maternal"${state.dnaMatchSide === "maternal" ? " selected" : ""}>${escapeHtml(t("dna_maternal"))}</option>
          <option value="paternal"${state.dnaMatchSide === "paternal" ? " selected" : ""}>${escapeHtml(t("dna_paternal"))}</option>
        </select>
      </label>
      <div class="muted">${escapeHtml(t("dna_matches_found", { count: matches.length }))}</div>
    </div>
    <div class="dna-match-layout">
      <div class="dna-match-list">
        ${matches.map((item) => `
          <button type="button" class="dna-match-card ${item.match_name === selected?.match_name ? "active" : ""}" data-dna-match-name="${escapeAttr(item.match_name)}">
            <strong>${escapeHtml(item.match_name)}</strong>
            <span>${escapeHtml(formatCm(item.total_cm || 0))}</span>
            <span class="muted">${escapeHtml(dnaLabelForSide(item.side))} · ${escapeHtml(item.predicted_relationship || t("dna_unknown"))}</span>
          </button>
        `).join("")}
      </div>
      <div class="dna-browser-card">
        <h5>${escapeHtml(t("dna_match_browser"))}</h5>
        ${renderMatchBrowser(selected)}
      </div>
    </div>
  `;
}

async function buildDnaSection(personId) {
  const [overview, painter, traits, ethnicity, haplogroups, matches] = await Promise.all([
    safeApi(`/api/people/${encodeURIComponent(personId)}/dna`, { sources: {}, summary: {} }),
    safeApi(`/api/people/${encodeURIComponent(personId)}/dna/painter`, { segments: [], legend: [], summary: {} }),
    safeApi(`/api/people/${encodeURIComponent(personId)}/dna/traits`, { categories: {}, summary: {} }),
    safeApi(`/api/people/${encodeURIComponent(personId)}/dna/ethnicity`, { items: [], generation_breakdown: [] }),
    safeApi(`/api/people/${encodeURIComponent(personId)}/dna/haplogroups`, { y_timeline: [], mt_timeline: [] }),
    safeApi(`/api/people/${encodeURIComponent(personId)}/dna/matches`, { items: [] }),
  ]);
  state.currentDna = { overview, painter, traits, ethnicity, haplogroups, matches };
  const summary = overview.summary || {};
  return `
    <section class="subpanel dna-section">
      <div class="dna-summary-strip muted">${escapeHtml(summary.segments || 0)} seg · ${escapeHtml(summary.raw_snps || 0)} ${escapeHtml(t("dna_total_snps"))} · ${escapeHtml(summary.matches || 0)} matches</div>
      <nav class="dna-subnav">
        <button type="button" class="secondary" data-dna-subsection="dna-imports">${escapeHtml(t("dna_imports"))}</button>
        <button type="button" class="secondary" data-dna-subsection="dna-painter-block">${escapeHtml(t("chromosome_painter"))}</button>
        <button type="button" class="secondary" data-dna-subsection="dna-ethnicity-block">${escapeHtml(t("dna_ethnicity"))}</button>
        <button type="button" class="secondary" data-dna-subsection="dna-haplogroups-block">${escapeHtml(t("dna_haplogroups"))}</button>
        <button type="button" class="secondary" data-dna-subsection="dna-traits-block">${escapeHtml(t("dna_traits"))}</button>
        <button type="button" class="secondary" data-dna-subsection="dna-matches-block">${escapeHtml(t("dna_matches"))}</button>
      </nav>
      <div class="dna-privacy-note">
        <strong>${escapeHtml(t("dna_privacy_notice"))}</strong>
        <span>${escapeHtml(t("dna_privacy_body"))}</span>
      </div>
      <div id="dna-imports" class="dna-import-grid">
        ${renderDnaUploadCard("segments", "dna_import_segments", "dna_import_hint_segments", ".csv,.txt")}
        ${renderDnaUploadCard("raw", "dna_import_raw", "dna_import_hint_raw", ".txt,.csv,.json")}
        ${renderDnaUploadCard("ethnicity", "dna_import_ethnicity", "dna_import_hint_ethnicity", ".json,.csv,.txt")}
        ${renderDnaUploadCard("haplogroups", "dna_import_haplogroups", "dna_import_hint_haplogroups", ".json,.csv,.txt")}
        ${renderDnaUploadCard("matches", "dna_import_matches", "dna_import_hint_matches", ".json,.csv,.txt")}
      </div>
      <div class="dna-overview-grid">
        <div class="dna-overview-card"><span>${escapeHtml(t("chromosome_painter"))}</span><strong>${escapeHtml(summary.segments || 0)}</strong></div>
        <div class="dna-overview-card"><span>${escapeHtml(t("dna_total_snps"))}</span><strong>${escapeHtml(summary.raw_snps || 0)}</strong></div>
        <div class="dna-overview-card"><span>${escapeHtml(t("dna_regions"))}</span><strong>${escapeHtml(summary.ethnicity_regions || 0)}</strong></div>
        <div class="dna-overview-card"><span>${escapeHtml(t("dna_matches"))}</span><strong>${escapeHtml(summary.matches || 0)}</strong></div>
      </div>
      <div class="dna-module-grid">
        <section id="dna-painter-block" class="dna-block">
          <h5>${escapeHtml(t("chromosome_painter"))}</h5>
          ${renderChromosomePainter(personId, painter)}
        </section>
        <section id="dna-ethnicity-block" class="dna-block">
          <h5>${escapeHtml(t("dna_ethnicity"))}</h5>
          ${renderEthnicityPanel(ethnicity)}
        </section>
        <section id="dna-haplogroups-block" class="dna-block">
          <h5>${escapeHtml(t("dna_haplogroups"))}</h5>
          ${renderHaplogroupsPanel(haplogroups)}
        </section>
        <section id="dna-traits-block" class="dna-block">
          <h5>${escapeHtml(t("dna_traits"))}</h5>
          ${renderTraitsPanel(traits)}
        </section>
        <section id="dna-matches-block" class="dna-block dna-block-wide">
          <h5>${escapeHtml(t("dna_matches"))}</h5>
          ${renderMatchesPanel(matches)}
        </section>
      </div>
    </section>
  `;
}

async function refreshDnaSection(personId) {
  const panel = document.querySelector("#dna-view-content .dna-section");
  if (!panel) return;
  panel.outerHTML = await buildDnaSection(personId);
  await wireDnaModule(personId);
}

function wireDnaPainter() {
  const painter = state.currentDna?.painter;
  if (!painter?.segments?.length) return;
  const shell = document.getElementById("dna-painter-shell");
  const tooltip = document.getElementById("dna-segment-tooltip");
  const detail = document.getElementById("dna-segment-detail");
  if (!shell || !tooltip || !detail) return;
  const segments = painter.segments;
  const moveTooltip = (event, segment) => {
    const rect = shell.getBoundingClientRect();
    tooltip.innerHTML = `
      <strong>${escapeHtml(segment.branch_label || t("dna_unassigned"))}</strong><br>
      ${escapeHtml(segment.probable_ancestor || t("dna_unknown"))}<br>
      ${escapeHtml(segment.genomic_range)} · ${escapeHtml(formatCm(segment.centimorgans || 0))}
    `;
    tooltip.classList.remove("hidden");
    tooltip.style.left = `${event.clientX - rect.left + 12}px`;
    tooltip.style.top = `${event.clientY - rect.top + 12}px`;
  };
  for (const node of document.querySelectorAll(".chr-seg[data-segment-index]")) {
    const index = Number(node.getAttribute("data-segment-index"));
    const segment = segments[index];
    if (!segment) continue;
    node.addEventListener("mouseenter", (event) => moveTooltip(event, segment));
    node.addEventListener("mousemove", (event) => moveTooltip(event, segment));
    node.addEventListener("mouseleave", () => tooltip.classList.add("hidden"));
    node.addEventListener("click", () => {
      detail.innerHTML = renderSegmentDetail(segment);
      document.querySelectorAll(".chr-seg.selected").forEach((el) => el.classList.remove("selected"));
      node.classList.add("selected");
    });
  }
}

function wireDnaMatches(personId) {
  const matchMin = document.getElementById("dna-match-min-cm");
  const matchSide = document.getElementById("dna-match-side");
  if (matchMin) {
    matchMin.addEventListener("change", async (event) => {
      state.dnaMatchMinCm = Number(event.target.value || 0);
      await refreshDnaSection(personId);
    });
  }
  if (matchSide) {
    matchSide.addEventListener("change", async (event) => {
      state.dnaMatchSide = event.target.value || "all";
      await refreshDnaSection(personId);
    });
  }
  for (const node of document.querySelectorAll("[data-dna-match-name]")) {
    node.addEventListener("click", async () => {
      state.dnaSelectedMatch = node.getAttribute("data-dna-match-name");
      await refreshDnaSection(personId);
    });
  }
}

async function wireDnaImports(personId) {
  for (const button of document.querySelectorAll("[data-dna-upload-target]")) {
    button.addEventListener("click", async () => {
      const target = button.getAttribute("data-dna-upload-target");
      const fileInput = document.getElementById(`dna-file-${target}`);
      const resultNode = document.getElementById(`dna-upload-result-${target}`);
      const file = fileInput && fileInput.files && fileInput.files[0];
      if (!file) {
        resultNode.textContent = t("please_select_csv_file");
        return;
      }
      resultNode.textContent = t("uploading");
      try {
        const text = await file.text();
        const endpoint = target === "segments"
          ? `/api/people/${encodeURIComponent(personId)}/dna`
          : `/api/people/${encodeURIComponent(personId)}/dna/${encodeURIComponent(target)}`;
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "text/plain" },
          body: text,
        });
        const data = await response.json();
        if (!response.ok) {
          resultNode.textContent = t("error_prefix", { error: data.error || "unknown" });
          return;
        }
        const count = data.stored ?? 1;
        resultNode.textContent = target === "segments"
          ? t("stored_segments", { count: data.stored, source: data.source })
          : t("dna_import_saved", { count, target: t(`dna_import_${target}`) });
        await refreshDnaSection(personId);
      } catch (error) {
        resultNode.textContent = String(error);
      }
    });
  }
}

function wireDnaSubsections() {
  for (const button of document.querySelectorAll("[data-dna-subsection]")) {
    button.addEventListener("click", () => {
      const target = document.getElementById(button.getAttribute("data-dna-subsection") || "");
      if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  }
}

function wireDataSubsections() {
  for (const button of document.querySelectorAll("[data-data-subsection]")) {
    if (button.dataset.wired === "true") continue;
    button.dataset.wired = "true";
    button.addEventListener("click", () => {
      const target = document.getElementById(button.getAttribute("data-data-subsection") || "");
      if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  }
}

async function wireDnaModule(personId) {
  await wireDnaImports(personId);
  wireDnaSubsections();
  wireDnaPainter();
  wireDnaMatches(personId);
}

async function startStubSync() {
  const resultNode = document.getElementById("stub-sync-result");
  const connected = await ensureOAuthConnected();
  if (!connected) {
    resultNode.textContent = t("oauth_login_required");
    return;
  }
  try {
    const response = await fetch("/api/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stubs: true }),
    });
    const result = await response.json();
    resultNode.textContent = result.started ? t("sync_started_pid", { pid: result.pid }) : syncStartErrorMessage(result);
  } catch (error) {
    resultNode.textContent = String(error);
  }
}

function wireEvents() {
  document.addEventListener(
    "error",
    (event) => {
      const target = event.target;
      if (!(target instanceof HTMLImageElement)) {
        return;
      }
      if (!target.classList.contains("person-thumb")
          && !target.classList.contains("family-thumb")
          && !target.classList.contains("person-hero-portrait")
          && !target.classList.contains("tree-thumb")) {
        return;
      }
      handlePortraitError(target);
    },
    true
  );

  for (const button of document.querySelectorAll(".app-menu-button")) {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      toggleAppMenu(button.dataset.menu || "");
    });
  }
  for (const group of document.querySelectorAll(".app-menu-group")) {
    group.addEventListener("mouseenter", () => {
      const _t = document.documentElement.dataset.theme;
      if (_t !== "win95" && _t !== "win311") return;
      const anyOpen = document.querySelector(".app-menu-group.open");
      if (!anyOpen) return;
      toggleAppMenu(group.querySelector(".app-menu-button")?.dataset.menu || "");
    });
  }
  for (const node of document.querySelectorAll(".menu-nav-button")) {
    node.addEventListener("click", () => {
      activateView(node.dataset.view);
    });
  }
  wireDataSubsections();
  document.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) return;
    if (event.target.closest(".app-menubar")) return;
    closeAppMenus();
  });
  const stubSyncBtn = document.getElementById("start-stub-sync");
  if (stubSyncBtn) {
    stubSyncBtn.addEventListener("click", () => startStubSync().catch((e) => showToast(String(e), "error")));
  }
  const backupImportBtn = document.getElementById("backup-import-btn");
  if (backupImportBtn) {
    backupImportBtn.addEventListener("click", () => importBackup().catch((e) => showToast(String(e), "error")));
  }
  // Update file label text when file is chosen
  const backupImportFile = document.getElementById("backup-import-file");
  if (backupImportFile) {
    backupImportFile.addEventListener("change", () => {
      const span = backupImportFile.closest(".import-file-label")?.querySelector("span");
      if (span && backupImportFile.files[0]) span.textContent = backupImportFile.files[0].name;
    });
  }
  const gedcomImportBtn = document.getElementById("gedcom-import-btn");
  if (gedcomImportBtn) {
    gedcomImportBtn.addEventListener("click", () => importGedcom().catch((e) => showToast(String(e), "error")));
  }
  const fsImportBtn = document.getElementById("fs-import-btn");
  if (fsImportBtn) {
    fsImportBtn.addEventListener("click", () => importFamilySearchRoot().catch((e) => showToast(String(e), "error")));
  }
  const gedcomImportFile = document.getElementById("gedcom-import-file");
  if (gedcomImportFile) {
    gedcomImportFile.addEventListener("change", () => {
      const span = gedcomImportFile.closest(".import-file-label")?.querySelector("span");
      if (span && gedcomImportFile.files[0]) span.textContent = gedcomImportFile.files[0].name;
    });
  }
  document.getElementById("refresh-status").addEventListener("click", loadStatus);
  document.getElementById("start-sync").addEventListener("click", startSync);
  const stopSyncBtn = document.getElementById("stop-sync");
  if (stopSyncBtn) {
    stopSyncBtn.addEventListener("click", () => stopSync().catch((e) => showToast(String(e), "error")));
  }
  const oauthConnectBtn = document.getElementById("oauth-connect-btn");
  if (oauthConnectBtn) {
    oauthConnectBtn.addEventListener("click", () => startOAuthFlow().catch((e) => showToast(String(e), "error")));
  }
  const oauthDisconnectBtn = document.getElementById("oauth-disconnect-btn");
  if (oauthDisconnectBtn) {
    oauthDisconnectBtn.addEventListener("click", () => disconnectOAuth().catch((e) => showToast(String(e), "error")));
  }
  const bootstrapRefreshBtn = document.getElementById("bootstrap-refresh-btn");
  if (bootstrapRefreshBtn) {
    bootstrapRefreshBtn.addEventListener("click", () => loadBootstrapStatus().catch((e) => showToast(String(e), "error")));
  }
  const bootstrapRecoverBtn = document.getElementById("bootstrap-recover-db-btn");
  if (bootstrapRecoverBtn) {
    bootstrapRecoverBtn.addEventListener("click", () => recoverDatabaseFromBootstrap().catch((e) => showToast(String(e), "error")));
  }
  const bootstrapRecreateBtn = document.getElementById("bootstrap-recreate-db-btn");
  if (bootstrapRecreateBtn) {
    bootstrapRecreateBtn.addEventListener("click", () => recreateDatabaseFromBootstrap().catch((e) => showToast(String(e), "error")));
  }
  const bootstrapConnectBtn = document.getElementById("bootstrap-connect-btn");
  if (bootstrapConnectBtn) {
    bootstrapConnectBtn.addEventListener("click", () => connectFromBootstrap().catch((e) => showToast(String(e), "error")));
  }
  const bootstrapImportBtn = document.getElementById("bootstrap-import-btn");
  if (bootstrapImportBtn) {
    bootstrapImportBtn.addEventListener("click", () => importFromBootstrap().catch((e) => showToast(String(e), "error")));
  }
  const bootstrapOpenDataBtn = document.getElementById("bootstrap-open-data-btn");
  if (bootstrapOpenDataBtn) {
    bootstrapOpenDataBtn.addEventListener("click", openDataCenterFromBootstrap);
  }
  const bootstrapContinueBtn = document.getElementById("bootstrap-continue-btn");
  if (bootstrapContinueBtn) {
    bootstrapContinueBtn.addEventListener("click", dismissBootstrapGate);
  }
  const dedupeRefreshBtn = document.getElementById("dedupe-refresh-btn");
  if (dedupeRefreshBtn) {
    dedupeRefreshBtn.addEventListener("click", () => loadDedupeCandidates(1).catch(console.error));
  }
  const historicalSyncBtn = document.getElementById("historical-sync-btn");
  if (historicalSyncBtn) {
    historicalSyncBtn.addEventListener("click", () => syncHistoricalEvents().catch((e) => showToast(String(e), "error")));
  }
  const historicalCreateBtn = document.getElementById("historical-create-btn");
  if (historicalCreateBtn) {
    historicalCreateBtn.addEventListener("click", () => createHistoricalEvent().catch((e) => showToast(String(e), "error")));
  }
  if (document.getElementById("tree-root-search")) {
    setupPersonAutocomplete("tree-root-search", async (pid) => {
      if (!pid || pid === state.treeRootId) return;
      state.treeRootId = pid;
      setConnectionInputValue("tree-root-search", pid);
      await selectPerson(pid);
      if (state.currentView === "tree") await loadTree();
    });
  }
  document.getElementById("tree-mode").addEventListener("change", () => loadTree().catch(console.error));
  document.getElementById("tree-depth").addEventListener("change", () => loadTree().catch(console.error));
  // Connection people panel — rendered dynamically by renderConnectionPeople()
  const addPersonBtn = document.getElementById("connection-add-person");
  if (addPersonBtn) {
    addPersonBtn.addEventListener("click", () => {
      if (state.connectionPersonIds.length < 6) {
        state.connectionPersonIds.push(null);
        renderConnectionPeople();
      }
    });
  }
  renderConnectionPeople();
  if (document.getElementById("pref-start-person-search")) {
    setupPersonAutocomplete("pref-start-person-search", async (pid) => {
      setConnectionInputValue("pref-start-person-search", pid);
      setDefaultStartPerson(pid);
      await selectPerson(pid);
    });
  }
  if (document.getElementById("book-root-search")) {
    setupPersonAutocomplete("book-root-search", async (pid) => {
      state.bookRootId = pid;
      setConnectionInputValue("book-root-search", pid);
      await syncBookControls();
    }, { localOnly: true });
  }
  document.getElementById("people-prev-page").addEventListener("click", () => {
    if (state.peoplePage <= 1) return;
    loadPeople(state.peopleQuery, state.peoplePage - 1).catch(console.error);
  });
  document.getElementById("people-next-page").addEventListener("click", () => {
    const totalPages = Math.max(1, Math.ceil((state.peopleTotal || 0) / state.peoplePageSize));
    if (state.peoplePage >= totalPages) return;
    loadPeople(state.peopleQuery, state.peoplePage + 1).catch(console.error);
  });
  document.getElementById("people-search").addEventListener("input", (event) => {
    const query = event.target.value || "";
    setView("explorer");
    updatePersonAutocomplete("people-search").catch(console.error);
    loadPeople(query, 1).catch(console.error);
  });
  document.getElementById("people-search").addEventListener("focus", () => {
    updatePersonAutocomplete("people-search").catch(console.error);
  });
  document.getElementById("historical-prev-page").addEventListener("click", () => {
    if (state.historicalPage <= 1) return;
    loadHistoricalEvents(state.historicalPage - 1).catch(console.error);
  });
  document.getElementById("historical-next-page").addEventListener("click", () => {
    const pages = Math.max(1, Math.ceil((state.historicalTotal || 0) / state.historicalPageSize));
    if (state.historicalPage >= pages) return;
    loadHistoricalEvents(state.historicalPage + 1).catch(console.error);
  });
  document.getElementById("dedupe-prev-page").addEventListener("click", () => {
    if (state.dedupePage <= 1) return;
    loadDedupeCandidates(state.dedupePage - 1).catch(console.error);
  });
  document.getElementById("dedupe-next-page").addEventListener("click", () => {
    const pages = Math.max(1, Math.ceil((state.dedupeTotal || 0) / state.dedupePageSize));
    if (state.dedupePage >= pages) return;
    loadDedupeCandidates(state.dedupePage + 1).catch(console.error);
  });
  document.getElementById("historical-year-from").addEventListener("change", () => loadHistoricalEvents(1).catch(console.error));
  document.getElementById("historical-year-to").addEventListener("change", () => loadHistoricalEvents(1).catch(console.error));
  document.getElementById("historical-place").addEventListener("change", () => loadHistoricalEvents(1).catch(console.error));
  document.getElementById("historical-scope-filter").addEventListener("change", () => loadHistoricalEvents(1).catch(console.error));
  window.addEventListener("hashchange", () => {
    const hashPersonId = decodeURIComponent(window.location.hash.replace(/^#person:/, ""));
    if (hashPersonId && hashPersonId !== state.selectedPersonId) {
      selectPerson(hashPersonId).catch(console.error);
    }
  });
  document.getElementById("media-modal-close").addEventListener("click", closeMediaModal);
  for (const node of document.querySelectorAll("[data-close-media]")) {
    node.addEventListener("click", closeMediaModal);
  }
  document.getElementById("gallery-modal-close").addEventListener("click", closeGallery);
  for (const node of document.querySelectorAll("[data-close-gallery]")) {
    node.addEventListener("click", closeGallery);
  }
  const prefTheme = document.getElementById("pref-theme");
  const prefLanguage = document.getElementById("pref-language");
  if (prefLanguage) {
    prefLanguage.addEventListener("change", (e) => {
      savePreference("fb_language", e.target.value);
      refreshTranslatedUi().catch(console.error);
    });
  }
  if (prefTheme) {
    prefTheme.addEventListener("change", (e) => savePreference("fb_theme", e.target.value));
  }
  const prefFontSize = document.getElementById("pref-font-size");
  if (prefFontSize) {
    prefFontSize.addEventListener("change", (e) => savePreference("fb_font_size", e.target.value));
  }
  const prefStartClear = document.getElementById("pref-start-person-clear");
  if (prefStartClear) {
    prefStartClear.addEventListener("click", () => {
      setDefaultStartPerson("");
      syncStartPersonPreferenceUi();
    });
  }
  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeMediaModal();
      closeGallery();
      closeAppMenus();
    }
  });
}

async function main() {
  loadPreferences();
  wireEvents();
  setView("explorer");
  renderBootstrapStatus(null);
  const bootstrap = await loadBootstrapStatus();
  if (bootstrap?.ready_for_app) {
    await initializeReadyAppData(true);
  }
}

main().catch((error) => {
  console.error(error);
  document.getElementById("person-detail").textContent = String(error);
  showToast(String(error), "error", 8000);
});
