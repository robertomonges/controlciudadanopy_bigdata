from __future__ import annotations

import os
import argparse
import tempfile
import time
from pathlib import Path

import psycopg2

import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent))
from setup.load_iceberg import DEFAULT_WAREHOUSE


CATALOG = "local"
DEFAULT_TABLE = "controlciudadano.full_data"
PG = dict(host="localhost", port=5432, dbname="controlciudadano", user="postgres", password=os.environ.get("PGPASSWORD", ""))
ROOT = Path(os.environ.get("CC_ROOT", "."))
WAREHOUSE_ROOT = ROOT / ".local" / "iceberg_warehouse"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materializa full_data equivalente en Iceberg.")
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument("--codec", default="zstd")
    return parser.parse_args()


def export_query_to_csv(conn, query: str, output_path: Path) -> None:
    copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, handle)


def directory_size_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def iceberg_table_path(table_name: str) -> Path:
    namespace, table = table_name.split(".", 1)
    return WAREHOUSE_ROOT / namespace / table


def build_full_data_spark(warehouse: Path, codec: str, parallelism: int):
    import _spark_windows
    from pyspark.sql import SparkSession

    jdk17 = os.environ.get("JAVA17_HOME", "")

    extra_java_opts = " ".join([
        "-Dhadoop.home.dir=C:/hadoop",
        "-XX:TieredStopAtLevel=1",
        "-XX:CICompilerCount=2",
    ])

    return (
        SparkSession.builder
        .appName("controlciudadano-full-data-iceberg")
        .master(f"local[{parallelism}]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", str(max(8, parallelism * 2)))
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_posix())
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.iceberg.write.parquet.compression-codec", codec)
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.extraJavaOptions", extra_java_opts)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main() -> int:
    args = parse_args()
    full_table = f"{CATALOG}.{args.table}"
    namespace = args.table.rsplit(".", 1)[0]

    export_elapsed = 0.0
    spark_elapsed = 0.0
    total_t0 = time.perf_counter()

    with tempfile.TemporaryDirectory(prefix="full_data_iceberg_") as tmpdir:
        tmpdir_path = Path(tmpdir)
        djbr_csv = tmpdir_path / "analysis_djbr.csv"
        tsje_csv = tmpdir_path / "analysis_tsje_elected.csv"
        nang_csv = tmpdir_path / "staging_nangareko.csv"

        with psycopg2.connect(**PG) as conn:
            t0 = time.perf_counter()
            export_query_to_csv(
                conn,
                """
                SELECT
                    id,
                    document,
                    name,
                    year,
                    version,
                    type,
                    charge,
                    net_worth
                FROM analysis.djbr
                """,
                djbr_csv,
            )
            export_query_to_csv(
                conn,
                """
                SELECT
                    document,
                    full_name,
                    age,
                    charge
                FROM analysis.tsje_elected
                """,
                tsje_csv,
            )
            export_query_to_csv(
                conn,
                """
                SELECT
                    cid,
                    name
                FROM staging.nangareko
                """,
                nang_csv,
            )
            export_elapsed = time.perf_counter() - t0

        spark = build_full_data_spark(Path(args.warehouse), codec=args.codec, parallelism=args.parallelism)
        spark.sparkContext.setLogLevel("WARN")

        try:
            djbr_df = (
                spark.read
                .option("header", "true")
                .option("encoding", "UTF-8")
                .csv(djbr_csv.as_posix())
            )
            tsje_df = (
                spark.read
                .option("header", "true")
                .option("encoding", "UTF-8")
                .csv(tsje_csv.as_posix())
            )
            nang_df = (
                spark.read
                .option("header", "true")
                .option("encoding", "UTF-8")
                .csv(nang_csv.as_posix())
            )

            djbr_df.createOrReplaceTempView("pg_analysis_djbr")
            tsje_df.createOrReplaceTempView("pg_analysis_tsje_elected")
            nang_df.createOrReplaceTempView("pg_staging_nangareko")

            query = """
            WITH sfp_documents AS (
                SELECT
                    documento AS document,
                    MAX(CONCAT(anho, LPAD(mes, 2, '0'))) AS last_month_worked
                FROM local.controlciudadano.sfp
                WHERE documento NOT LIKE 'VAC%'
                GROUP BY documento
            ),
            mh_documents AS (
                SELECT
                    codigopersona AS document,
                    MAX(CONCAT(CAST(anio AS STRING), LPAD(CAST(mes AS STRING), 2, '0'))) AS last_month_worked
                FROM local.controlciudadano.hacienda_funcionarios
                GROUP BY codigopersona
            ),
            police_document AS (
                SELECT
                    cedula AS document,
                    MAX(CONCAT(ano, LPAD(mes, 2, '0'))) AS last_month_worked
                FROM local.controlciudadano.policia
                GROUP BY cedula
            ),
            all_docs AS (
                SELECT
                    'a_quien_elegimos' AS source,
                    UPPER(CONCAT_WS(' ', name, lastname)) AS name,
                    CAST(0.9 AS DOUBLE) AS confidence,
                    REGEXP_REPLACE(identifier, '\\\\.', '') AS document,
                    head_shot AS photo,
                    CAST(NULL AS BIGINT) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    CAST(NULL AS STRING) AS charge
                FROM local.controlciudadano.a_quien_elegimos

                UNION

                SELECT
                    'declarations' AS source,
                    name AS name,
                    CASE
                        WHEN name IS NULL THEN 0.0
                        WHEN CAST(id AS BIGINT) < 0 THEN 0.5
                        WHEN CAST(year AS INT) <= 2014 THEN 0.5
                        WHEN type = 'V1' THEN 0.5
                        ELSE 0.99
                    END AS confidence,
                    document AS document,
                    CAST(NULL AS STRING) AS photo,
                    CAST(NULL AS BIGINT) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(net_worth AS DECIMAL(20,2)) AS net_worth,
                    charge AS charge
                FROM pg_analysis_djbr

                UNION

                SELECT
                    'ande_exonerados' AS source,
                    REGEXP_REPLACE(cliente, '\\\\s+', ' ') AS name,
                    CAST(0.6 AS DOUBLE) AS confidence,
                    documento AS document,
                    CAST(NULL AS STRING) AS photo,
                    CAST(NULL AS BIGINT) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    CAST(NULL AS STRING) AS charge
                FROM local.controlciudadano.ande_exonerados

                UNION

                SELECT
                    'tsje_elected' AS source,
                    full_name AS name,
                    CAST(0.7 AS DOUBLE) AS confidence,
                    document AS document,
                    CAST(NULL AS STRING) AS photo,
                    CAST(NULL AS BIGINT) AS salary,
                    CAST(age AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    charge AS charge
                FROM pg_analysis_tsje_elected

                UNION

                SELECT
                    'sfp' AS source,
                    MAX(CONCAT_WS(' ', raw.nombres, raw.apellidos)) AS name,
                    CAST(0.9 AS DOUBLE) AS confidence,
                    docs.document AS document,
                    CAST(NULL AS STRING) AS photo,
                    SUM(CAST(raw.presupuestado AS BIGINT)) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    raw.cargo AS charge
                FROM local.controlciudadano.sfp raw
                JOIN sfp_documents docs
                  ON docs.document = raw.documento
                 AND CONCAT(raw.anho, LPAD(raw.mes, 2, '0')) = docs.last_month_worked
                GROUP BY docs.document, docs.last_month_worked, raw.cargo

                UNION

                SELECT
                    'mh' AS source,
                    MAX(CONCAT_WS(' ', raw.nombres, raw.apellidos)) AS name,
                    CAST(0.8 AS DOUBLE) AS confidence,
                    docs.document AS document,
                    CAST(NULL AS STRING) AS photo,
                    SUM(CAST(raw.montopresupuestado AS BIGINT)) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    raw.cargo AS charge
                FROM local.controlciudadano.hacienda_funcionarios raw
                JOIN mh_documents docs
                  ON docs.document = raw.codigopersona
                 AND CONCAT(CAST(raw.anio AS STRING), LPAD(CAST(raw.mes AS STRING), 2, '0')) = docs.last_month_worked
                GROUP BY docs.document, docs.last_month_worked, raw.cargo

                UNION

                SELECT
                    'pytyvo' AS source,
                    name AS name,
                    CAST(0.6 AS DOUBLE) AS confidence,
                    cid AS document,
                    CAST(NULL AS STRING) AS photo,
                    CAST(NULL AS BIGINT) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    CAST(NULL AS STRING) AS charge
                FROM local.controlciudadano.pytyvo

                UNION

                SELECT
                    'nangareko' AS source,
                    name AS name,
                    CAST(0.6 AS DOUBLE) AS confidence,
                    cid AS document,
                    CAST(NULL AS STRING) AS photo,
                    CAST(NULL AS BIGINT) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    CAST(NULL AS STRING) AS charge
                FROM pg_staging_nangareko

                UNION

                SELECT
                    'nangareko_transparencia' AS source,
                    nombre_beneficiario AS name,
                    CAST(0.7 AS DOUBLE) AS confidence,
                    numero_documento AS document,
                    CAST(NULL AS STRING) AS photo,
                    CAST(NULL AS BIGINT) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    nombre_subsidio AS charge
                FROM local.controlciudadano.nangareko_transparencia

                UNION

                SELECT
                    'policia' AS source,
                    MAX(CONCAT_WS(' ', raw.nombres, raw.apellidos)) AS name,
                    CAST(0.6 AS DOUBLE) AS confidence,
                    docs.document AS document,
                    CAST(NULL AS STRING) AS photo,
                    SUM(CAST(raw.devengado AS BIGINT)) AS salary,
                    CAST(NULL AS BIGINT) AS age,
                    CAST(NULL AS DECIMAL(20,2)) AS net_worth,
                    raw.cargo_funcion AS charge
                FROM local.controlciudadano.policia raw
                JOIN police_document docs
                  ON docs.document = raw.cedula
                 AND CONCAT(raw.ano, LPAD(raw.mes, 2, '0')) = docs.last_month_worked
                GROUP BY docs.document, docs.last_month_worked, raw.cargo_funcion
            ),
            grouped AS (
                SELECT
                    REGEXP_REPLACE(document, '[^0-9]+', '') AS document,
                    MAX_BY(name, confidence) AS name,
                    COLLECT_LIST(photo) AS photo,
                    COLLECT_LIST(salary) AS salary,
                    COLLECT_LIST(age) AS age,
                    COLLECT_LIST(net_worth) AS net_worth,
                    COLLECT_LIST(source) AS sources,
                    COLLECT_LIST(charge) AS charges
                FROM all_docs
                WHERE document IS NOT NULL
                  AND REGEXP_REPLACE(document, '[^0-9]+', '') <> ''
                GROUP BY REGEXP_REPLACE(document, '[^0-9]+', '')
            )
            SELECT
                document,
                name,
                photo,
                salary,
                age,
                net_worth,
                sources,
                charges
            FROM grouped
            """

            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")

            t0 = time.perf_counter()
            df = spark.sql(query)
            df.writeTo(full_table).using("iceberg").createOrReplace()
            spark_elapsed = time.perf_counter() - t0

            final_rows = spark.table(full_table).count()
            table_path = iceberg_table_path(args.table)
            bytes_used = directory_size_bytes(table_path)
        finally:
            spark.stop()

    total_elapsed = time.perf_counter() - total_t0
    rows_per_second = final_rows / spark_elapsed if spark_elapsed > 0 else 0

    print(f"table|{full_table}")
    print(f"rows|{final_rows}")
    print(f"export_elapsed_s|{export_elapsed:.2f}")
    print(f"spark_elapsed_s|{spark_elapsed:.2f}")
    print(f"total_elapsed_s|{total_elapsed:.2f}")
    print(f"rows_per_second|{rows_per_second:.2f}")
    print(f"bytes|{bytes_used}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
