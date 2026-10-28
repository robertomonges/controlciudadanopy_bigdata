from __future__ import annotations

import os
import argparse
import random
import string
import time
from pathlib import Path

import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent))
import _spark_windows

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


LOCAL_DATA_ROOT = Path(os.environ.get("CC_ROOT", "."), ".local/data")
DEFAULT_WAREHOUSE = Path(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse")
CATALOG = "local"
MULTIPLIERS = [2, 5, 10]

PG_DEFAULTS = dict(
    host="localhost", port=5432,
    database="controlciudadano",
    user="postgres", password=os.environ.get("PGPASSWORD", ""),
)


def build_spark(warehouse: Path) -> SparkSession:
    return (
        SparkSession.builder
        .appName("controlciudadano-scale-gen")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_posix())
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.iceberg.write.parquet.compression-codec", "zstd")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def generate_scale_spark(
    spark: SparkSession,
    multiplier: int,
    warehouse: Path,
) -> None:
    source_table = f"{CATALOG}.controlciudadano.hacienda_funcionarios"
    target_table = f"{CATALOG}.controlciudadano.hacienda_scale_{multiplier}x"

    print(f"[scale] Comprobando existencia de {source_table}...")
    try:
        base_count = spark.table(source_table).count()
    except Exception as e:
        print(f"[scale] ERROR: tabla base no encontrada: {e}")
        print("[scale] Ejecutar load_iceberg.py --source hacienda primero")
        return

    print(f"[scale] Base: {base_count:,} filas  →  objetivo: {base_count * multiplier:,} filas")

    base_df = spark.table(source_table)

    copies = [base_df]
    for i in range(1, multiplier):
        suffix = str(i).zfill(3)
        perturbed = base_df.withColumn(
            "documento",
            F.concat(F.col("documento"), F.lit(suffix)),
        ).withColumn(
            "codigopersona",
            F.concat(F.col("codigopersona"), F.lit(suffix)),
        )
        copies.append(perturbed)

    from functools import reduce
    from pyspark.sql import DataFrame

    combined: DataFrame = reduce(DataFrame.unionByName, copies)

    print(f"[scale] Escribiendo {target_table}  (partición: anio/mes)...")
    (
        combined.writeTo(target_table)
        .using("iceberg")
        .partitionedBy(F.col("anio"), F.col("mes"))
        .createOrReplace()
    )
    final_count = spark.table(target_table).count()
    print(f"[scale] OK → {target_table}  ({final_count:,} filas)")


def generate_scale_postgres(
    multiplier: int,
    pg: dict,
) -> None:
    target_table = f"staging.hacienda_scale_{multiplier}x"
    print(f"\n[pg-scale] Generando {target_table}...")

    conn = psycopg2.connect(**pg)
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {target_table}")
    cur.execute(f"""
        CREATE TABLE {target_table}
        (LIKE staging.hacienda_funcionarios INCLUDING ALL)
    """)
    conn.commit()

    for i in range(multiplier):
        suffix = str(i).zfill(3)
        print(f"  [pg-scale] Inserción {i+1}/{multiplier}  (sufijo={suffix!r})...")
        t0 = time.perf_counter()
        cur.execute(f"""
            INSERT INTO {target_table}
            SELECT
                anio, mes, codigonivel, descripcionnivel,
                codigoentidad, descripcionentidad,
                codigoprograma, descripcionprograma,
                codigosubprograma, descripcionsubprograma,
                codigoproyecto, descripcionproyecto,
                codigounidadresponsable, descripcionunidadresponsable,
                codigoobjetogasto, conceptogasto,
                fuentefinanciamiento, linea,
                codigopersona || %s AS codigopersona,
                nombres, apellidos, sexo, discapacidad,
                codigocategoria, cargo, horascatedra,
                fechaingreso, tipopersonal, lugar,
                montopresupuestado, montodevengado,
                mescorte, aniocorte, fechacorte,
                nivelabr, entidadabr, programaabr,
                subprogramaabr, proyectoabr, unidadabr,
                documento || %s AS documento
            FROM staging.hacienda_funcionarios
        """, (suffix, suffix))
        conn.commit()
        elapsed = time.perf_counter() - t0
        print(f"  [pg-scale] Inserción {i+1} completada en {elapsed:.1f}s")

    print(f"  [pg-scale] Creando índice en documento...")
    cur.execute(f"CREATE INDEX ON {target_table} (documento)")
    cur.execute(f"CREATE INDEX ON {target_table} (anio, mes)")
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {target_table}")
    total = cur.fetchone()[0]
    print(f"[pg-scale] OK → {target_table}  ({total:,} filas)")

    cur.close()
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Genera tablas de escalabilidad para Fase 3 del benchmark."
    )
    parser.add_argument(
        "--multiplier",
        choices=["2", "5", "10", "all"],
        default="all",
        help="Factor de multiplicación (default: all → 2x, 5x, 10x)",
    )
    parser.add_argument(
        "--target",
        choices=["iceberg", "postgres", "both"],
        default="both",
        help="Dónde generar los datos (default: both)",
    )
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    args = parser.parse_args()

    multipliers = MULTIPLIERS if args.multiplier == "all" else [int(args.multiplier)]
    warehouse = Path(args.warehouse)

    pg_config = dict(
        host=args.pg_host, port=args.pg_port,
        database=args.pg_database,
        user=args.pg_user, password=args.pg_password,
    )

    spark = None
    if args.target in ("iceberg", "both"):
        spark = build_spark(warehouse)
        spark.sparkContext.setLogLevel("WARN")

    for m in multipliers:
        print(f"\n{'='*60}")
        print(f"  Multiplicador: {m}x")
        print(f"{'='*60}")

        if args.target in ("iceberg", "both") and spark:
            generate_scale_spark(spark, m, warehouse)

        if args.target in ("postgres", "both"):
            generate_scale_postgres(m, pg_config)

    if spark:
        spark.stop()

    print("\n✓ Generación de datos de escalabilidad completada.")


if __name__ == "__main__":
    main()
