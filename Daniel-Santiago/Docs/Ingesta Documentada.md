# Documentación de Ingesta — Bronze Layer

# Version 1.0 - 07/10/2026

## El Dataset

El dataset con el que trabajamos es **`instagram_reels_clean.csv`**, un archivo CSV que contiene datos de publicaciones de Instagram Reels. Cada fila representa un Reel publicado por un creador de contenido, e incluye información sobre el propio post (identificador, caption, duración del video, fecha) y sobre su rendimiento (vistas, likes, comentarios, tasa de engagement, score de viralidad y el tier de viralidad asignado).

El archivo vive en el volumen de Unity Catalog: `/Volumes/dtasociate/default/sources/instagram_reels_clean.csv` y contiene **88 registros** con **16 columnas**:

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `post_id` | Long | Identificador único del Reel |
| `shortcode` | String | Código corto de la URL del post (ej. `Dcd_FfvP96F`) |
| `creator_handle` | String | Nombre de usuario del creador (ej. `techburner`) |
| `caption` | String | Texto del caption, puede contener múltiples líneas y hashtags |
| `timestamp_utc` | Timestamp | Fecha y hora de publicación en UTC |
| `video_duration_sec` | Double | Duración del video en segundos |
| `views` | Integer | Número de vistas |
| `likes` | Integer | Número de likes |
| `comments` | Integer | Número de comentarios |
| `caption_word_count` | Integer | Cantidad de palabras en el caption |
| `hashtag_count` | Integer | Cantidad de hashtags usados |
| `niche` | String | Categoría del contenido (ej. "Technology & Gadgets", "Entertainment") |
| `engagement_rate_pct` | Double | Tasa de engagement en porcentaje |
| `virality_score` | Double | Score de viralidad calculado |
| `virality_tier` | String | Clasificación de viralidad (ej. "Viral Hit", "High Reach") |
| `_rescued_data` | String | Columna de rescate de datos con valores problemáticos del parsing original |

## Objetivo

El objetivo de esta capa **Bronze** es sencillo pero importante: **llevar los datos crudos desde el archivo CSV hacia una tabla Delta gestionada en Unity Catalog**, sin transformaciones. Es el primer paso de una arquitectura medallón (bronze → silver → gold) donde:

- **Bronze** recibe los datos tal como vienen, con el único añadido de una columna de auditoría (`_ingest_timestamp`).
- **Silver** (próximo paso) limpiará y castea estrictamente las métricas, eliminando registros con valores negativos.
- **Gold** (próximo paso) calculará la Tasa de Interacción por usuario y mantendrá las métricas actualizadas con `MERGE INTO`.

## Técnicas Utilizadas

### Auto Loader con esquema explícito

Usamos **Auto Loader** (`cloudFiles`) en lugar de un `spark.read` tradicional porque necesitamos que la ingesta sea **incremental**. Es decir, si mañana se agregan nuevos archivos CSV al directorio de origen, Auto Loader los detectará automáticamente y solo cargará los nuevos, sin reprocesar los que ya fueron ingeridos. Esto lo logra gracias al **checkpoint** que mantiene en `/Volumes/dtasociate/default/checkpoints/bronze_daniel/`.

### `schemaEvolutionMode = "none"`

Esta configuración le dice a Auto Loader: **no infieras ni modifiques el esquema**. Queremos control total sobre los tipos de datos, así que pasamos un esquema explícito construido con `StructType` y `StructField`. Si el archivo de origen cambia de estructura en el futuro (por ejemplo, aparece una columna nueva), Auto Loader no la agregará automáticamente — simplemente la ignorará. Esto es una decisión deliberada para evitar que datos inesperados rompan el pipeline downstream.

### `multiLine = true`

Las captions de los Reels contienen saltos de línea (los creadores escriben textos largos con párrafos y hashtags separados por líneas en blanco). Si no activamos esta opción, el parser CSV interpretaría cada salto de línea como un nuevo registro, rompiendo filas enteras. Con `multiLine = true`, Spark respeta las comillas del CSV y mantiene el caption completo en una sola fila.

### `pathGlobFilter = "*.csv"`

Auto Loader monitorea todo el directorio de origen, no un solo archivo. Con este filtro le decimos: solo preocúpate por los archivos con extensión `.csv`. Si en el futuro hay otros archivos en el mismo directorio (logs, JSONs, etc.), los ignorará.

### `trigger(availableNow = True)`

En lugar de mantener un stream corriendo continuamente, usamos el trigger `availableNow = True`. Esto significa que cuando el notebook se ejecuta:

1. Procesa todos los archivos disponibles que aún no ha ingerido.
2. Se detiene cuando termina.
3. En la próxima ejecución, solo procesa los archivos nuevos que hayan aparecido.

Es el patrón ideal para un job programado (batch incremental) — obtienes los beneficios de la ingesta incremental sin tener un stream activo 24/7.

### Columna de auditoría: `_ingest_timestamp`

Además de las 16 columnas del CSV, añadimos `_ingest_timestamp` con `current_timestamp()`. Esta columna registra **cuándo** cada fila fue cargada en la tabla bronze. Es una buena práctica en capas bronze porque permite rastrear la antigüedad de los datos y facilita el debugging si algo se carga mal.

## Estructura Creada

| Recurso | Nombre |
| --- | --- |
| Catálogo | `dtasociate` |
| Schema Bronze | `dtasociate.bronze` |
| Schema Silver | `dtasociate.silver` |
| Schema Gold | `dtasociate.gold` |
| Tabla Bronze | `dtasociate.bronze.bronze_daniel` |
| Volumen de checkpoints | `dtasociate.default.checkpoints` |

## Reutilización

Todo el notebook está parametrizado con **widgets de Databricks** (`dbutils.widgets`). Esto significa que si otro colega quiere replicar el mismo pipeline para un dataset diferente, solo necesita cambiar los valores de los widgets en la parte superior del notebook — sin tocar la lógica. Los parámetros disponibles son:

- `catalog_name`: nombre del catálogo de Unity Catalog
- `bronze_schema`, `silver_schema`, `gold_schema`: nombres de los schemas
- `bronze_table`: nombre de la tabla bronze
- `source_path`: ruta del directorio de origen