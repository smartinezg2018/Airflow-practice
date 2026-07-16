# Airflow practice
Proyecto: Pipeline de scraping y monitoreo de precios con Airflow
Objetivo del proyecto: construir un pipeline productivo (no solo un script) que scrapee datos de un sitio web de forma recurrente, los valide, transforme, almacene históricamente, y genere alertas — todo orquestado y observable a través de Airflow. La meta no es solo "aprender a hacer un scraper", sino entender cómo se ve un pipeline de datos real en producción: reintentos, idempotencia, logging, particionado por fecha, y monitoreo.
Fase 0: Definición del caso de uso
Antes de tocar código, conviene decidir qué vas a scrapear porque eso condiciona el diseño:

Datos que cambian en el tiempo (precios, stock, ofertas de trabajo, clima, criptomonedas) son ideales porque te obligan a pensar en series temporales y en "qué cambió desde la última corrida".
Ejemplos concretos:

books.toscrape.com — sandbox diseñado para practicar, sin fricciones legales ni de robots.txt. Ideal si quieres enfocarte 100% en Airflow y no en técnicas de scraping avanzadas.
Precios de un producto en Mercado Libre o Amazon — más realista, pero necesitas manejar rate limiting, headers, y posibles bloqueos.
Ofertas de empleo en un portal (LinkedIn, Computrabajo) — bueno para practicar paginación y scraping con JS (Playwright).
Clima histórico vía scraping de una web pública — bueno si quieres combinarlo con series temporales y visualización.



Te recomiendo empezar con books.toscrape.com para el DAG inicial y, una vez que funcione end-to-end, migrar la lógica de extracción a un sitio real más complejo. Así separas "aprender Airflow" de "pelear con anti-scraping".
Fase 1: Arquitectura general del pipeline
                    ┌─────────────┐
                    │   extract   │  (dynamic task mapping por URL/categoría)
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  validate   │  (branch: ok / retry / fail)
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  transform  │  (limpieza, normalización)
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │    load     │  (upsert en Postgres, particionado por fecha)
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │check_deltas │  (comparar con corrida anterior)
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              │                         │
      ┌───────▼──────┐         ┌────────▼───────┐
      │notify_changes│         │  skip_notify    │
      └──────────────┘         └────────────────┘
Fase 2: Detalle de cada tarea
1. Extract

Usa requests + BeautifulSoup para sitios estáticos, o Playwright/Selenium si el contenido se renderiza con JS.
Implementa dynamic task mapping (.expand() en Airflow 2.3+) para lanzar una task por cada URL o categoría a scrapear en paralelo, en vez de un loop for dentro de una sola task. Esto te da paralelismo real y visibilidad individual de cada scrape en la UI.
Guarda el HTML crudo (o el JSON extraído) en un data lake local particionado por fecha de ejecución, por ejemplo data/raw/{{ ds }}/categoria.html. Esto es clave: nunca sobrescribas el dato crudo, guárdalo versionado por execution_date, así puedes reprocesar sin volver a pegarle al sitio.

2. Validate

Verifica que el HTML no esté vacío, que el parser haya encontrado al menos N elementos esperados, y que no haya señales de bloqueo (ej. página de captcha).
Aquí es un buen lugar para practicar BranchPythonOperator o @task.branch: si la validación falla, la rama va a una task de retry_with_backoff o alert_scraping_blocked; si pasa, sigue el flujo normal.
Practica también los retries nativos de Airflow (retries, retry_delay, retry_exponential_backoff=True en los default_args) en vez de reintentar manualmente en el código Python.

3. Transform

Limpieza de precios (quitar símbolos de moneda, convertir a float), normalización de fechas, deduplicación.
Usa pandas o polars. Si quieres ir más allá, aquí es donde podrías enganchar dbt más adelante para separar "extracción" de "transformación analítica".

4. Load

Inserta en Postgres usando PostgresHook (mejor que PostgresOperator, que está siendo deprecado a favor de hooks + @task).
Implementa upserts (INSERT ... ON CONFLICT DO UPDATE) para que corridas repetidas no dupliquen filas.
Diseña la tabla con una columna scraped_at o execution_date para mantener el histórico completo, no solo el último valor. Esto te permite después graficar la evolución de precios en el tiempo.

5. Check deltas / Notify

Compara el valor scrapeado hoy contra el de la corrida anterior (puedes traerlo de la misma tabla con una query, o usando XComs si el volumen es chico).
Si hay un cambio significativo (ej. precio bajó más de 10%), dispara una notificación. Aquí puedes usar EmailOperator, un webhook a Slack/Discord, o simplemente loggear con distintos niveles de severidad si no quieres meterte con credenciales externas todavía.

Fase 3: Conceptos de Airflow que se practican explícitamente
ConceptoDónde se aplicaScheduling (schedule, cron)Correr el DAG cada X horasTask dependenciesTodo el flujo extract >> validate >> ...BranchingValidación con rutas de éxito/falloDynamic task mappingUn extract por URL/categoríaXComsPasar rutas de archivos, conteos, deltas entre tasksHooks y ConnectionsConexión a Postgres gestionada desde la UIRetries y backoffManejo de fallos de red al scrapearSensorsEj. FileSensor esperando el archivo crudo antes de transformarPoolsLimitar concurrencia de requests al sitio (evitar rate limiting)SLAs y alertasSi una task tarda más de lo esperadoTemplating con JinjaUso de {{ ds }}, {{ execution_date }} en rutas de archivos
Fase 4: Infraestructura

Levantar Airflow localmente con docker-compose (usando la imagen oficial apache/airflow), incluyendo Postgres como base de datos tanto del metastore de Airflow como de tus datos scrapeados (o separarlos en dos instancias de Postgres para practicar múltiples connections).
Definir la conexión al sitio scrapeado y a la base de datos como Airflow Connections desde la UI, en vez de hardcodear credenciales.
Opcional: usar Airflow Variables para parametrizar cosas como la lista de URLs a scrapear, el umbral de alerta de precio, etc., y así no tocar código para cambiar configuración.

Fase 5: Extensiones opcionales (si quieres ir más allá del DAG básico)

Observabilidad: agregar un dashboard con Streamlit, Metabase o Superset que lea directo de Postgres y muestre la evolución de precios/datos en el tiempo.
Calidad de datos: integrar Great Expectations como task de validación más robusta que un simple chequeo manual.
Transformación seria: enganchar dbt después del load para modelar los datos (staging → marts).
Testing: escribir tests unitarios de las funciones de parsing con pytest, y un test de integración del DAG con airflow dags test.
CI/CD: un pipeline de GitHub Actions que corra los tests del DAG antes de "deployarlo" (aunque sea local).
Anti-scraping: rotación de user-agents, proxies, delays aleatorios entre requests — útil si migras a un sitio real con protecciones.

Tiempo estimado (desglosado)
EtapaTiempo aproximadoSetup de Airflow con Docker Compose2-3 horasDAG básico funcionando end-to-end (con books.toscrape.com)1 díaAgregar branching, retries, dynamic mapping1 díaMigrar a un sitio real + manejo de bloqueos1-2 díasExtensiones opcionales (dashboard, dbt, tests)2-3 días adicionales
En total, un proyecto sólido de portafolio te puede tomar entre una semana (versión completa con extensiones) y un fin de semana (versión núcleo, sin extensiones).
¿Quieres que empecemos armando el docker-compose.yml junto con el DAG inicial usando books.toscrape.com, o prefieres que definamos primero el esquema de la base de datos Postgres?
