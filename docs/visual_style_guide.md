# FamilyBook RS — Visual Style Guide

> Guía de diseño para la interfaz de escritorio Rust/Iced. Define paleta, tipografía, espaciado, componentes y layout. Todo el trabajo en `src/gui/theme.rs` debe seguir este documento.

---

## 1. Dirección visual

La app debe sentirse **oscura, sobria y técnica** — más una herramienta de escritorio moderna que un panel administrativo clásico.

**Principios rectores:**
- Minimalismo funcional: solo lo necesario, nada decorativo
- Alta legibilidad sobre fondos oscuros
- Sin bordes innecesarios — separar con espacio, no con líneas
- Densidad media: información densa pero con aire suficiente
- Controles discretos pero con respuesta visual clara

**Lo que NO debe verse:**
- Bordes en todos los paneles
- Cajas dentro de cajas dentro de cajas
- Líneas divisorias gruesas
- Botones con relieve fuerte o sombras grandes
- Demasiados tonos distintos de gris sin propósito

---

## 2. Jerarquía visual (4 niveles, no más)

```
Nivel 1 — Fondo general de la app         #0F1115
Nivel 2 — Sidebar / status bar            #13161B
Nivel 3 — Superficie principal            #171B21
Nivel 4 — Tarjetas, inputs, elevated      #1D232B
```

Usar hover solo para indicar interactividad: `#242B35`

---

## 3. Paleta de colores (dark theme)

### Fondos

| Token             | Hex         | Uso                                      |
|-------------------|-------------|------------------------------------------|
| `--bg-app`        | `#0F1115`   | Fondo raíz de la aplicación              |
| `--bg-sidebar`    | `#13161B`   | Sidebar izquierdo, topbar                |
| `--bg-surface`    | `#171B21`   | Área principal de contenido              |
| `--bg-elevated`   | `#1D232B`   | Tarjetas, inputs, paneles elevated       |
| `--bg-hover`      | `#242B35`   | Hover sobre ítems interactivos           |

### Bordes

| Token              | Valor                      | Uso                                  |
|--------------------|----------------------------|--------------------------------------|
| `--border-subtle`  | `#2A313C`                  | Inputs, tablas, divisores concretos  |
| `--border-soft`    | `rgba(255,255,255,0.06)`   | Separaciones muy sutiles             |

**Regla:** borde solo en inputs, tablas, y divisores específicos. No en paneles generales.

### Texto

| Token              | Hex       | Uso                                   |
|--------------------|-----------|---------------------------------------|
| `--text-primary`   | `#E6EAF0` | Texto principal, labels importantes   |
| `--text-secondary` | `#A8B0BC` | Texto de soporte, subtítulos          |
| `--text-muted`     | `#7D8794` | Placeholders, metadatos, timestamps   |
| `--text-disabled`  | `#5F6975` | Controles deshabilitados              |

### Acento (azul frío)

| Token              | Hex / Valor                   | Uso                               |
|--------------------|-------------------------------|-----------------------------------|
| `--accent`         | `#4C8DFF`                     | Color de acento principal         |
| `--accent-hover`   | `#6AA1FF`                     | Hover sobre acento                |
| `--accent-pressed` | `#3A78E6`                     | Estado pressed                    |
| `--accent-soft`    | `rgba(76,141,255,0.14)`       | Fondo de ítem activo en sidebar   |

### Estados

| Token        | Hex       | Uso                                 |
|--------------|-----------|-------------------------------------|
| `--success`  | `#35C47C` | Conectado, éxito, OK                |
| `--warning`  | `#E6B450` | Advertencia, pendiente              |
| `--error`    | `#E05D5D` | Error, acción destructiva           |
| `--info`     | `#57A6FF` | Sincronizando, informativo          |

### Tokens CSS de referencia (para documentación y prototipado)

```css
:root {
  --bg-app:        #0F1115;
  --bg-sidebar:    #13161B;
  --bg-surface:    #171B21;
  --bg-elevated:   #1D232B;
  --bg-hover:      #242B35;

  --border-subtle: #2A313C;
  --border-soft:   rgba(255,255,255,0.06);

  --text-primary:   #E6EAF0;
  --text-secondary: #A8B0BC;
  --text-muted:     #7D8794;
  --text-disabled:  #5F6975;

  --accent:         #4C8DFF;
  --accent-hover:   #6AA1FF;
  --accent-pressed: #3A78E6;
  --accent-soft:    rgba(76,141,255,0.14);

  --success: #35C47C;
  --warning: #E6B450;
  --error:   #E05D5D;
  --info:    #57A6FF;

  --radius-sm: 4px;
  --radius-md: 6px;
  --radius-lg: 10px;

  --space-1: 4px;   --space-2: 8px;   --space-3: 12px;
  --space-4: 16px;  --space-5: 20px;  --space-6: 24px;
  --space-8: 32px;

  --font-ui: "Inter", "Segoe UI", sans-serif;
}
```

### Equivalencias en Iced (`Color::from_rgb8`)

```rust
// Fondos
pub const BG_APP:      Color = Color::from_rgb8(0x0F, 0x11, 0x15);
pub const BG_SIDEBAR:  Color = Color::from_rgb8(0x13, 0x16, 0x1B);
pub const BG_SURFACE:  Color = Color::from_rgb8(0x17, 0x1B, 0x21);
pub const BG_ELEVATED: Color = Color::from_rgb8(0x1D, 0x23, 0x2B);
pub const BG_HOVER:    Color = Color::from_rgb8(0x24, 0x2B, 0x35);

// Bordes
pub const BORDER_SUBTLE: Color = Color::from_rgb8(0x2A, 0x31, 0x3C);

// Texto
pub const TEXT_PRIMARY:   Color = Color::from_rgb8(0xE6, 0xEA, 0xF0);
pub const TEXT_SECONDARY: Color = Color::from_rgb8(0xA8, 0xB0, 0xBC);
pub const TEXT_MUTED:     Color = Color::from_rgb8(0x7D, 0x87, 0x94);
pub const TEXT_DISABLED:  Color = Color::from_rgb8(0x5F, 0x69, 0x75);

// Acento
pub const ACCENT:         Color = Color::from_rgb8(0x4C, 0x8D, 0xFF);
pub const ACCENT_HOVER:   Color = Color::from_rgb8(0x6A, 0xA1, 0xFF);
pub const ACCENT_PRESSED: Color = Color::from_rgb8(0x3A, 0x78, 0xE6);

// Estados
pub const SUCCESS: Color = Color::from_rgb8(0x35, 0xC4, 0x7C);
pub const WARNING: Color = Color::from_rgb8(0xE6, 0xB4, 0x50);
pub const ERROR:   Color = Color::from_rgb8(0xE0, 0x5D, 0x5D);
pub const INFO:    Color = Color::from_rgb8(0x57, 0xA6, 0xFF);
```

---

## 4. Tipografía

### Fuente recomendada

**Inter** (primera opción) → `"Inter"`, `"Segoe UI"`, `sans-serif`

Inter es la fuente de referencia para interfaces de escritorio modernas. Legible en tamaños pequeños, neutral, sin excentricidades.

### Escala tipográfica

| Nivel                  | Tamaño | Peso | Color            | Uso                                  |
|------------------------|--------|------|------------------|--------------------------------------|
| Título de app          | 28–32px | 600  | `--text-primary` | Nombre "FamilyBook" en sidebar       |
| Subtítulo de app       | 12px   | 400  | `--text-muted`   | "Rust desktop" bajo el título        |
| Título de página       | 22px   | 600  | `--text-primary` | H1 de cada vista (`Configuración`)   |
| Subtítulo de bloque    | 17px   | 600  | `--text-primary` | Título de sección/tarjeta            |
| Texto base             | 14px   | 400  | `--text-primary` | Contenido principal                  |
| Labels / soporte       | 13px   | 500  | `--text-secondary`| Labels de inputs, metadatos         |
| Microtexto / estado    | 12px   | 500  | `--text-muted`   | Timestamps, badges de estado         |

### Reglas tipográficas

- Usar solo pesos: 400, 500, 600 — nunca 700 en UI general
- No usar mayúsculas forzadas (ALL CAPS) excepto en badges muy cortos
- Titles cortos y directos — sin puntuación al final
- No combinar más de 2 tamaños dentro de un mismo bloque

---

## 5. Espaciado

Escala base: múltiplos de 4.

| Token      | Valor | Uso típico                                        |
|------------|-------|---------------------------------------------------|
| `space-1`  | 4px   | Gap mínimo entre íconos y texto                   |
| `space-2`  | 8px   | Gap entre badge y texto, padding mínimo           |
| `space-3`  | 12px  | Gap entre elementos dentro de un formulario       |
| `space-4`  | 16px  | Separación entre grupos de controles              |
| `space-5`  | 20px  | Padding interno de tarjetas pequeñas              |
| `space-6`  | 24px  | Padding interno de bloques / separación secciones |
| `space-8`  | 32px  | Separación entre bloques grandes, padding outer   |

### Reglas prácticas

- Padding interno de tarjetas/bloques: `20–24px`
- Separación entre tarjetas / secciones: `24px`
- Gap entre grandes bloques visuales: `32px`
- Altura mínima de controles (inputs, botones): `40px`
- Altura cómoda de botones primarios: `40–44px`
- Padding horizontal de botones: mínimo `16px`

---

## 6. Radios y sombras

### Radios

| Elemento          | Radio   |
|-------------------|---------|
| Inputs            | 6px     |
| Botones           | 6px     |
| Paneles / tarjetas | 10px   |
| Badges            | 999px   |
| Tabs segmentadas  | 6px     |

En Iced: `border.radius = 6.0.into()` o `10.0.into()`

### Sombras

En dark theme, sombras muy sutiles o ninguna. El contraste de fondo hace el trabajo.

```rust
// Sombra mínima aceptable
Shadow {
    color: Color::from_rgba(0.0, 0.0, 0.0, 0.25),
    offset: Vector::new(0.0, 1.0),
    blur_radius: 2.0,
}
```

- No usar sombras grandes tipo web de marketing
- Preferir `shadow: Shadow::default()` cuando haya suficiente contraste de fondo
- Sombra solo en elementos que flotan sobre contenido (modales, dropdowns)

---

## 7. Componentes

### 7.1 Status bar (bottom)

```
Alto:   48px
Ubicación: parte inferior de la ventana (bottom status bar)
Fondo:  --bg-sidebar  (#13161B)
Texto:  --text-secondary

Contenido: Vista actual · Persona seleccionada · Estado sync
Separación entre datos: espacio + punto medio ·
Sin cajas por ítem — todo en línea horizontal
```

Ejemplo visual:
```
  Vista: Explorer  ·  Persona: García, Juan  ·  FamilySearch: Conectado  ·  Sync: Inactivo
```

### 7.2 Sidebar

```
Ancho:         240–260px
Fondo:         --bg-sidebar  (#13161B)
Padding outer: 12px horizontal
```

**Título del producto:**
```
"FamilyBook"        28px / 600 / --text-primary
"Rust desktop"      12px / 400 / --text-muted
Padding top:        20px
Padding bottom:     16px
```

**Ítem de navegación:**
```
Alto:              40px
Padding horizontal: 14–16px
Radio:             6px
Fuente:            14px / 500
Color texto:        --text-secondary  (inactivo)
                    --text-primary    (activo)
Fondo activo:      rgba(76,141,255,0.16)
Fondo hover:       --bg-elevated (#1D232B)
Sin borde completo en cada ítem
Indicador activo:  barra izquierda de 3px en --accent  O  solo cambio de fondo
```

**Vistas disponibles (orden en sidebar):**
1. Explorer
2. Tree
3. Connections
4. DNA
5. Historical
6. Dedupe
7. ── separador sutil ──
8. Sync
9. Book
10. Data
11. Settings
12. Info

### 7.3 Área principal

```
Padding exterior:  24–32px
Max-width cómodo:  1100–1200px
Fondo:             --bg-surface (#171B21)
```

**Encabezado de página:**
```
Título:        22px / 600 / --text-primary
Descripción:   14px / 400 / --text-secondary  (opcional, 1 línea)
Sin caja       — no meterlo en un contenedor con borde
Margin bottom: 24px antes del contenido
```

### 7.4 Tabs segmentadas

```
Contenedor fondo:  --bg-sidebar (#13161B)
Radio contenedor:  8px
Padding interno:   4px

Tab inactiva:
  fondo:     transparente
  texto:     --text-secondary
  hover:     --bg-elevated

Tab activa:
  fondo:     --bg-hover (#242B35)  o  accent-soft
  texto:     --text-primary
  radio:     6px
  padding:   10px vertical / 16px horizontal
Sin línea inferior — el cambio de fondo es suficiente
```

### 7.5 Tarjetas / bloques de contenido

```
Fondo:   --bg-elevated (#1D232B)  o  --bg-surface (#171B21)
Borde:   opcional — 1px solid --border-subtle si es necesario
Radio:   10px
Padding: 20–24px
Separación entre tarjetas: 24px
```

Jerarquía dentro de la tarjeta:
```
Título bloque       17px / 600 / --text-primary
Subtexto de apoyo   13px / 400 / --text-secondary
Contenido           14px / 400 / --text-primary
```

El título del bloque hace el trabajo de delimitación — no hace falta borde superior grueso.

### 7.6 Botones

**Primario** — acciones principales ("Iniciar sync", "Guardar")
```
Fondo:            #4C8DFF  (--accent)
Hover:            #6AA1FF
Pressed:          #3A78E6
Texto:            blanco
Alto:             40–44px
Padding horiz:    16px
Radio:            6px
Sin sombra pesada
```

**Secundario** — acciones de soporte ("Cancelar", "Ver más")
```
Fondo:            --bg-hover (#242B35)
Texto:            --text-primary
Borde:            1px solid --border-subtle  (opcional)
Hover:            aclarar fondo ligeramente
```

**Peligro** — acciones destructivas confirmadas
```
Fondo:            rgba(224, 93, 93, 0.15)  (rojo tenue)
Texto:            --error (#E05D5D)
Hover:            rgba(224, 93, 93, 0.25)
No usar rojo sólido brillante salvo confirmación activa
```

**Ghost / subtle** — acciones terciarias, dentro de listas
```
Fondo:            transparente
Texto:            --text-secondary
Hover:            --bg-elevated
Sin borde
```

**Regla:** no más de un botón primario fuerte por bloque/tarjeta.

**Disabled:**
```
Opacidad texto:   0.45
Opacidad fondo:   0.55
cursor:           not-allowed
```

### 7.7 Inputs y campos

```
Fondo:             --bg-elevated (#1D232B)
Borde:             1px solid --border-subtle (#2A313C)
Radio:             6px
Alto:              40px
Padding horiz:     12–14px
Texto:             --text-primary
Placeholder:       --text-muted

Focus:
  borde:           1px solid --accent (#4C8DFF)
  glow (web):      0 0 0 3px rgba(76,141,255,0.15)
  glow (Iced):     sombra con accent y alpha bajo
```

**Labels:**
```
Posición:   encima del input, nunca flotando dentro
Tamaño:     13px / 500
Color:      --text-secondary
Margin-b:   6px antes del input
```

Separación entre campos de formulario: `16px`.

**Combobox / PickList:**
```
Fondo:             --bg-elevated (#1D232B)
Borde:             1px solid --border-subtle (#2A313C)
Radio:             6px
Texto:             --text-primary
Placeholder:       --text-muted
Icono (handle):    --text-secondary

Hover:             borde --border-soft
Opened/focus:      borde --accent
Menu desplegable:  fondo --bg-elevated, opción activa --accent-soft
```

### 7.8 Badges de estado

Usar para mostrar estados discretos como "Conectado", "Inactivo", "0 registros".

```
Alto:              24px
Padding horiz:     10px
Radio:             999px  (píldora)
Texto:             12px / 600

Conectado:         bg rgba(53,196,124,0.15)   texto #35C47C
Inactivo:          bg rgba(168,176,188,0.12)  texto #A8B0BC
Sincronizando:     bg rgba(76,141,255,0.15)   texto #4C8DFF
Error:             bg rgba(224,93,93,0.15)    texto #E05D5D
Advertencia:       bg rgba(230,180,80,0.15)   texto #E6B450
```

Nunca mostrar un estado solo con texto plano si tiene implicación de salud del sistema.

### 7.9 Separadores y divisores

**Usar:**
- Espacio vertical entre secciones (`24–32px`)
- Cambio de fondo suave entre áreas
- Diferencia de tamaño/peso tipográfico
- Línea `1px solid --border-subtle` solo cuando haya ambigüedad real

**No usar:**
- Borde rectangular alrededor de todos los paneles
- Divisiones de `2px` o más
- Marcos completos en listas o menús

### 7.10 Log / consola de progreso

```
Fondo:         --bg-app (#0F1115)  o  #0C0E12  (más oscuro que la superficie)
Fuente:        monoespaciada — "JetBrains Mono", "Fira Code", "Cascadia Code", monospace
Tamaño:        13px / 400
Color texto:   --text-muted (#7D8794)  →  líneas recientes en --text-secondary
Padding:       16px
Radio bloque:  6px

Sin eventos:
  mensaje centrado-izquierda en --text-muted
  ej: "No hay eventos aún"
```

---

## 8. Layout por vista

### Estructura global

```
┌────────────┬────────────────────────────────────────┐
│            │                                        │
│  Sidebar   │  Main surface                          │
│  240px     │  padding: 24–32px                      │
│            │  max-width: 1200px                     │
│            │                                        │
├────────────┴────────────────────────────────────────┤
│  Status bar 48px (bottom)                           │
└─────────────────────────────────────────────────────┘
```

### Vista: Settings / Sync

Orden de bloques (separados por `24px`):

1. Encabezado de página (título + descripción)
2. Tabs de sección (General / Sync / etc.)
3. Bloque: Estado de sincronización
4. Bloque: Configuración FamilySearch
5. Bloque: Log de progreso

En `General`, el selector de idioma debe ser **combobox** (`es`, `en`) y no input libre.

**Bloque "Estado de sincronización":**
```
Título:     "FamilySearch Sync"                    17px / 600
Estado:     badge "Conectado" / "Inactivo"
Acciones:   botones alineados horizontalmente

Metadatos en grilla 2 columnas:
  Estado del worker:        Inactivo
  Última sincronización:    n/d
  Registros sincronizados:  0
  Próxima ejecución:        n/d
```

**Bloque "Configuración FamilySearch":**
```
Formulario vertical:
  label → input      (Client ID)
  label → input      (Authorization URL)
  label → input      (Token URL)
  label → input      (Callback URL)

Separación entre campos: 16px
Botón guardar: primario, alineado a la derecha o al inicio del bloque
```

### Vista: Explorer

```
Barra de búsqueda:    input ancho completo en la parte superior
Lista de personas:    items con alto 40–48px, fondo --bg-elevated, radio 10px
Detalle de persona:   panel derecho, padding 24px
```

### Vista: Tree / Pedigree

```
Canvas:         fondo --bg-app, máximo espacio disponible
Nodos persona:  tarjetas compactas con fondo --bg-elevated
Líneas de árbol: color --border-subtle o ligeramente más claro
```

### Vista: DNA

```
Tabs:           Summary / Painter / Traits / Ethnicity / Haplogroups / Matches
Chromosome painter:  canvas SVG oscuro, segmentos con colores de estado
Tablas:         fondo alternado --bg-surface / --bg-elevated, borde --border-subtle
```

---

## 9. Iconografía

Usar iconos simples para reducir carga textual.

**Contextos de uso:**
```
sync         → ícono de flechas circulares
conexión     → plug / enlace
árbol        → árbol o jerarquía
DNA          → doble hélice o cadena
eventos      → calendario o reloj
duplicados   → dos personas superpuestas
configuración → engranaje
información  → círculo i
libro        → libro abierto
búsqueda     → lupa
```

**Estilo:**
- Trazo simple (outline), no sólido por defecto
- Tamaño: 18–20px
- Color normal: `--text-muted`
- Color activo/hover: `--text-secondary` o `--accent`
- No usar íconos muy detallados o con relleno complejo

**Bibliotecas recomendadas:** Lucide, Phosphor, Heroicons (iconos compatibles exportables a path SVG para Iced).

---

## 10. Accesibilidad

No sacrificar accesibilidad en nombre del minimalismo.

- Contraste texto/fondo: mínimo 4.5:1 para texto base, 3:1 para texto grande
- Tamaño mínimo de texto en UI: 13px (preferible 14px)
- Área clicable mínima: 40×40px
- Estado `focus` visible: borde o glow en `--accent`
- No depender únicamente del color para comunicar un estado — acompañar con texto o ícono
- Separación mínima entre controles clicables adyacentes: 8px

---

## 11. Qué cambiar en `theme.rs` (migración a dark)

El archivo actual usa un tema claro (`#F4F6F8` como fondo base). Los cambios necesarios son:

| Actual                          | Nuevo (dark)              |
|---------------------------------|---------------------------|
| `background: rgb(244,246,248)`  | `BG_SURFACE (#171B21)`    |
| `sidebar: rgb(237,241,245)`     | `BG_SIDEBAR (#13161B)`    |
| `card: rgb(250,251,252)`        | `BG_ELEVATED (#1D232B)`   |
| `text: rgb(28,35,43)`           | `TEXT_PRIMARY (#E6EAF0)`  |
| `primary: rgb(49,92,128)`       | `ACCENT (#4C8DFF)`        |
| `border: rgb(220,225,231)`      | `BORDER_SUBTLE (#2A313C)` |
| `radius: 0.0` (en muchos sitios)| `6.0` o `10.0`            |

La función `rounded_border` actualmente fuerza `radius: 0.0` ignorando el parámetro `_radius`. Corregir esto es parte del rediseño.

---

## 12. Personalización y gestión de temas

El tema **por defecto** sigue siendo `dark-default` (esta guía). La personalización no debe romper jerarquía visual, contraste ni legibilidad.

### 12.1 Personalizar el tema actual (dark-default)

Exponer una sección de **Apariencia** en `Settings` con vista previa en vivo.

Controles mínimos:
- Color de acento (`--accent`, `--accent-hover`, `--accent-pressed`, `--accent-soft`)
- Fondo de superficies (`--bg-surface`, `--bg-elevated`)
- Texto secundario (`--text-secondary`)
- Radio base (`--radius-md`, `--radius-lg`) en rangos acotados

Flujo recomendado:
1. Cambiar valores en controles
2. Ver resultado inmediato en preview (botones, input, tarjeta, badge)
3. `Aplicar` para usar en sesión
4. `Restaurar base` para volver a `dark-default`

Reglas de seguridad visual:
- Mantener contraste mínimo WCAG (4.5:1 texto normal)
- No permitir `--bg-surface` y `--bg-elevated` con diferencia menor a 3-4 puntos RGB
- No permitir acento con contraste insuficiente sobre `--bg-hover`
- Mantener una sola escala de radios (sin mezclar estilos redondeado/cuadrado)

### 12.2 Cambiar entre temas (presets)

Agregar selector de tema en `Settings > General` o pestaña `Appearance`:

- `dark-default` (actual)
- `dark-graphite` (menos saturado)
- `light-neutral` (claro sobrio)
- `high-contrast` (accesibilidad)

Comportamiento esperado:
- Cambio inmediato sin reiniciar app
- Persistir el tema activo al guardar settings
- Si el tema no existe/corrupto: fallback automático a `dark-default`
- Los presets son de solo lectura (no se sobrescriben)

### 12.3 Guardar temas personalizados

Permitir `Guardar como tema...` con nombre y slug (`mi-tema`).

Persistencia recomendada:
- Guardar `theme_id` activo en `metadata` (ej. `ui_theme_active`)
- Guardar temas custom en tabla propia (`ui_themes`) o JSON versionado
- Marcar origen del tema: `builtin` o `custom`

Estructura sugerida (JSON de tema):

```json
{
  "id": "mi-tema",
  "name": "Mi tema",
  "kind": "custom",
  "tokens": {
    "bg_app": "#0F1115",
    "bg_sidebar": "#13161B",
    "bg_surface": "#171B21",
    "bg_elevated": "#1D232B",
    "text_primary": "#E6EAF0",
    "text_secondary": "#A8B0BC",
    "accent": "#4C8DFF",
    "accent_hover": "#6AA1FF",
    "accent_pressed": "#3A78E6"
  },
  "radius": {
    "sm": 4,
    "md": 6,
    "lg": 10
  },
  "updated_at": "2026-03-20T12:00:00Z"
}
```

Operaciones mínimas:
- Crear tema desde el actual (`Duplicar y editar`)
- Renombrar tema custom
- Eliminar tema custom (si está activo, volver a `dark-default`)
- Exportar/importar tema (JSON) para compartir estilos entre instalaciones

---

## 13. Las 8 acciones de mayor impacto

Hacer estas 8 cosas transforma la percepción visual de la app:

1. **Eliminar la mayoría de bordes** — dejar solo en inputs y tablas
2. **Cambiar fondos a 3 niveles dark** — `#0F1115` / `#171B21` / `#1D232B`
3. **Usar Inter como fuente** — o Segoe UI si se prefiere integración nativa
4. **Aumentar padding interno** — mínimo 20px en tarjetas, 24px en bloques
5. **Convertir el sidebar en lista suave** — sin borde en cada ítem
6. **Rehacer tabs con estilo segmentado** — discretas, sin línea inferior
7. **Transformar estados en badges** — conectado/inactivo/error como píldoras de color
8. **Ordenar formularios** — label arriba, input debajo, 16px entre campos

---

*Última actualización: 2026-03-20. Aplicable a `familybook-rs` (Iced 0.13 + Rust).*
