from __future__ import annotations

import argparse
import os
import sys
import time
import zipfile
import io
import csv as csv_mod
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BENCHMARK_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / ".local/data/hacienda"
DEFAULT_WAREHOUSE = ROOT / ".local/iceberg_warehouse"
DEFAULT_TABLE = "controlciudadano.hacienda_funcionarios"


def _project_venv_python() -> Path | None:
    candidate = ROOT / ".venv" / "Scripts" / "python.exe"
    return candidate if candidate.exists() else None


def _ensure_project_python() -> None:
    if os.environ.get("CC_BENCHMARK_VENV_REEXEC") == "1":
        return
    try:
        import pyspark
        return
    except ModuleNotFoundError:
        venv_python = _project_venv_python()
        if not venv_python:
            return
        os.environ["CC_BENCHMARK_VENV_REEXEC"] = "1"
        os.execv(str(venv_python), [str(venv_python), *sys.argv])


_ensure_project_python()

sys.path.insert(0, str(BENCHMARK_DIR))
try:
    import _spark_windows
except ImportError:
    pass


CSV_COLUMNS = [
    "anio", "mes", "codigonivel", "descripcionnivel",
    "codigoentidad", "descripcionentidad",
    "codigoprograma", "descripcionprograma",
    "codigosubprograma", "descripcionsubprograma",
    "codigoproyecto", "descripcionproyecto",
    "codigounidadresponsable", "descripcionunidadresponsable",
    "codigoobjetogasto", "conceptogasto",
    "fuentefinanciamiento", "linea", "codigopersona",
    "nombres", "apellidos", "sexo", "discapacidad",
    "codigocategoria", "cargo", "horascatedra", "fechaingreso",
    "tipopersonal", "lugar", "montopresupuestado", "montodevengado",
    "mescorte", "aniocorte", "fechacorte",
    "nivelabr", "entidadabr", "programaabr", "subprogramaabr",
    "proyectoabr", "unidadabr",
]

ICEBERG_SCHEMA = """
    anio              INT,
    mes               INT,
    codigonivel       INT,
    descripcionnivel  STRING,
    codigoentidad     INT,
    descripcionentidad STRING,
    codigoprograma    INT,
    descripcionprograma STRING,
    codigosubprograma INT,
    descripcionsubprograma STRING,
    codigoproyecto    INT,
    descripcionproyecto STRING,
    codigounidadresponsable INT,
    descripcionunidadresponsable STRING,
    codigoobjetogasto INT,
    conceptogasto     STRING,
    fuentefinanciamiento STRING,
    linea             INT,
    codigopersona     STRING,
    nombres           STRING,
    apellidos         STRING,
    sexo              STRING,
    discapacidad      STRING,
    codigocategoria   STRING,
    cargo             STRING,
    horascatedra      STRING,
    fechaingreso      STRING,
    tipopersonal      STRING,
    lugar             STRING,
    montopresupuestado BIGINT,
    montodevengado    BIGINT,
    mescorte          INT,
    aniocorte         STRING,
    fechacorte        STRING,
    nivelabr          STRING,
    entidadabr        STRING,
    programaabr       STRING,
    subprogramaabr    STRING,
    proyectoabr       STRING,
    unidadabr         STRING
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    p.add_argument("--table", default=DEFAULT_TABLE)
    p.add_argument("--data-dir", default=str(DATA_DIR))
    p.add_argument("--mode", default="append", choices=["append", "overwrite"],
                   help="Modo de escritura Iceberg (default: append)")
    p.add_argument("--codec", default="zstd")
    p.add_argument("--parallelism", type=int, default=4)
    return p.parse_args()


def extract_csv_to_tmp(zip_path: Path, year: int, month: int) -> str:
    csv_name = f"nomina_{year}-{month:02d}.csv"
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(csv_name) as raw:
            text = io.TextIOWrapper(raw, encoding="latin-1")
            reader = csv_mod.DictReader(text, delimiter=",")
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False,
                encoding="utf-8", newline=""
            )
            header_map = {
                "anio": "anio", "mes": "mes",
                "codigoNivel": "codigonivel", "descripcionNivel": "descripcionnivel",
                "codigoEntidad": "codigoentidad", "descripcionEntidad": "descripcionentidad",
                "codigoPrograma": "codigoprograma", "descripcionPrograma": "descripcionprograma",
                "codigoSubprograma": "codigosubprograma", "descripcionSubprograma": "descripcionsubprograma",
                "codigoProyecto": "codigoproyecto", "descripcionProyecto": "descripcionproyecto",
                "codigoUnidadResponsable": "codigounidadresponsable",
                "descripcionUnidadResponsable": "descripcionunidadresponsable",
                "codigoObjetoGasto": "codigoobjetogasto", "conceptoGasto": "conceptogasto",
                "fuenteFinanciamiento": "fuentefinanciamiento", "linea": "linea",
                "codigoPersona": "codigopersona",
                "nombres": "nombres", "apellidos": "apellidos", "sexo": "sexo",
                "discapacidad": "discapacidad", "codigoCategoria": "codigocategoria",
                "cargo": "cargo", "horasCatedra": "horascatedra", "fechaIngreso": "fechaingreso",
                "tipoPersonal": "tipopersonal", "lugar": "lugar",
                "montoPresupuestado": "montopresupuestado", "montoDevengado": "montodevengado",
                "mesCorte": "mescorte", "anioCorte": "aniocorte", "fechaCorte": "fechacorte",
                "nivelAbr": "nivelabr", "entidadAbr": "entidadabr",
                "programaAbr": "programaabr", "subprogramaAbr": "subprogramaabr",
                "proyectoAbr": "proyectoabr", "unidadAbr": "unidadabr",
            }
            writer = csv_mod.DictWriter(
                tmp, fieldnames=CSV_COLUMNS, delimiter=",",
                extrasaction="ignore"
            )
            writer.writeheader()
            count = 0
            for row in reader:
                normalized = {header_map.get(k, k.lower()): (v or "").strip() for k, v in row.items()}
                writer.writerow({k: normalized.get(k, "") for k in CSV_COLUMNS})
                count += 1
            tmp.close()
            return tmp.name, count


def main():
    args = parse_args()
    data_dir = Path(args.data_dir)
    zip_path = data_dir / f"nomina-{args.year}-{args.month:02d}.zip"
    if not zip_path.exists():
        zip_path = data_dir / f"nomina_{args.year}-{args.month:02d}.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    print(f"[Iceberg] Extracting {zip_path.name} to temp CSV...")
    t_extract = time.perf_counter()
    tmp_csv, row_count = extract_csv_to_tmp(zip_path, args.year, args.month)
    extract_elapsed = time.perf_counter() - t_extract
    print(f"[Iceberg] Extracted {row_count:,} rows in {extract_elapsed:.1f}s -> {tmp_csv}")

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField,
        IntegerType, LongType, StringType,
    )

    catalog = "local"
    warehouse = args.warehouse

    spark = (
        SparkSession.builder
        .appName(f"hacienda_iceberg_load_{args.year}_{args.month:02d}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config(f"spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", catalog)
        .config("spark.default.parallelism", str(args.parallelism))
        .config("spark.sql.shuffle.partitions", str(args.parallelism))
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.controlciudadano")

    full_table = f"{catalog}.{args.table}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            {ICEBERG_SCHEMA}
        )
        USING iceberg
        PARTITIONED BY (anio, mes)
        TBLPROPERTIES (
            'write.parquet.compression-codec' = '{args.codec}'
        )
    """)

    print(f"[Iceberg] Starting Spark write ({args.mode})...")
    t0 = time.perf_counter()

    df = spark.read.option("header", "true").option("inferSchema", "false").csv(tmp_csv)
    int_cols = ["anio", "mes", "codigonivel", "codigoentidad", "codigoprograma",
                "codigosubprograma", "codigoproyecto", "codigounidadresponsable",
                "codigoobjetogasto", "mescorte"]
    bigint_cols = ["montopresupuestado", "montodevengado"]
    for c in int_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(IntegerType()))
    for c in bigint_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(LongType()))

    existing = set(df.columns)
    for col in CSV_COLUMNS:
        if col not in existing:
            df = df.withColumn(col, F.lit(None).cast(StringType()))

    df.select(CSV_COLUMNS).writeTo(full_table).option(
        "write.parquet.compression-codec", args.codec
    ).append() if args.mode == "append" else df.select(CSV_COLUMNS).writeTo(full_table).option(
        "write.parquet.compression-codec", args.codec
    ).createOrReplace()

    elapsed = time.perf_counter() - t0

    table_namespace = args.table.replace(".", "/")
    iceberg_data_dir = Path(warehouse) / table_namespace / "data"
    parquet_bytes = sum(
        f.stat().st_size for f in iceberg_data_dir.rglob("*.parquet")
    ) if iceberg_data_dir.exists() else 0

    rows_per_sec = row_count / elapsed if elapsed > 0 else 0
    print(f"[Iceberg] {args.year}-{args.month:02d}: {row_count:,} rows in {elapsed:.1f}s "
          f"({rows_per_sec:,.0f} rows/s) — Parquet: {parquet_bytes/1024/1024:.1f} MB")

    spark.stop()
    Path(tmp_csv).unlink(missing_ok=True)

    return {
        "year": args.year, "month": args.month,
        "rows": row_count, "elapsed_s": round(elapsed, 2),
        "rows_per_sec": round(rows_per_sec, 0),
        "extract_elapsed_s": round(extract_elapsed, 2),
        "parquet_bytes": parquet_bytes,
    }


if __name__ == "__main__":
    main()
