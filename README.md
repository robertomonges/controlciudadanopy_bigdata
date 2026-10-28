# controlciudadanopy_bigdata

Scripts del trabajo final de grado *Evaluación de Tecnologías Big Data Aplicadas a Datos Abiertos Utilizando la Metodología QSOS: Caso Control Ciudadano* (Ingeniería en Informática, Facultad Politécnica, Universidad Nacional de Asunción).

Contienen la carga de las fuentes del portal en PostgreSQL y en Apache Iceberg sobre Apache Spark, la medición de almacenamiento y de tiempos de carga, el *benchmark* de consultas y las verificaciones posteriores.

## Estructura

```text
scripts/benchmark/                 carga, medición y benchmark
  setup/                           carga de cada fuente en PostgreSQL y en Iceberg
  queries/                         consultas de los grupos A a D
  measure_storage_and_load.py      almacenamiento y carga en Iceberg
  measure_pg_load.py               carga en PostgreSQL
  run_benchmark_queries.py         benchmark de consultas
  run_benchmark_djbr.py            benchmark de consultas DJBR
  run_full_data_iceberg.py         reconstrucción de la capa curada full_data
scripts/python/local/              procesamiento de fuentes e instrumentación
experimentos/verificacion/         verificaciones M1 a M6
experimentos/QSOS/                 matriz funcional de las 163 tecnologías de la preselección
                                   y cálculo de la evaluación QSOS 2.0 (puntajes, pesos y sensibilidad)
```

## Requisitos

- Python 3.13 y las dependencias de `scripts/benchmark/requirements.txt`.
- Java 17 para Spark.
- El *runtime* de Iceberg para Spark 3.5 (`iceberg-spark-runtime-3.5_2.12-1.5.2.jar`) en la carpeta `jars` de PySpark.
- PostgreSQL 12 con la base del portal.

```bash
python -m venv .venv
.venv/Scripts/pip install -r scripts/benchmark/requirements.txt
```

## Configuración

Los scripts no incluyen credenciales. Se configuran con variables de entorno:

| Variable | Uso |
|---|---|
| `PGPASSWORD` | contraseña de PostgreSQL |
| `CC_ROOT` | raíz del proyecto, donde están `.local/data` y `.local/iceberg_warehouse` (por defecto, el directorio actual) |
| `JAVA17_HOME` | ruta a un JDK 17, si no es el Java por defecto |
| `BENCH_HACIENDA_DOC`, `BENCH_SFP_DOC`, `BENCH_SET_RUC`, `BENCH_PYTYVO_CID`, `BENCH_POLICIA_CED` | valores de búsqueda de las consultas puntuales de `run_benchmark_queries.py`: un documento o RUC existente en cada fuente |

## Datos

Los datos no forman parte del repositorio. Son datos abiertos que publican el Ministerio de Economía y Finanzas, la Secretaría de la Función Pública, la Policía Nacional, la Contraloría General de la República y otras instituciones, y se descargan de sus sitios oficiales en `.local/data`.
