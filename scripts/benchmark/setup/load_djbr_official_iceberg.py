from __future__ import annotations

"""
Cargador de JSON oficial DJBR a Apache Iceberg.

Modos:
  full     (default) - lee archivos + escribe Iceberg en un solo proceso
  prepare  - solo genera el archivo JSONL (sin Spark); soporta workers paralelos
  write    - solo ejecuta el Spark write desde un directorio de chunks JSONL

Uso serial (equivalente al modo anterior):
    python load_djbr_official_iceberg.py

Uso paralelo (coordinado por launch_djbr_official_iceberg.py):
    # 1. Cada worker genera su chunk JSONL
    python load_djbr_official_iceberg.py --mode prepare --worker-index 0 --worker-count 8 --jsonl-dir /tmp/djbr
    python load_djbr_official_iceberg.py --mode prepare --worker-index 1 --worker-count 8 --jsonl-dir /tmp/djbr
    ...
    # 2. Un solo proceso Spark escribe todos los chunks
    python load_djbr_official_iceberg.py --mode write --jsonl-dir /tmp/djbr
"""

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time

import psycopg2

ROOT_DIR = Path(__file__).resolve().parents[3]
BENCHMARK_DIR = Path(__file__).resolve().parents[1]


def project_venv_python() -> Path | None:
    candidate = ROOT_DIR / ".venv" / "Scripts" / "python.exe"
    return candidate if candidate.exists() else None


def ensure_pyspark_from_project_venv() -> None:
    if os.environ.get("CC_BENCHMARK_VENV_REEXEC") == "1":
        return
    try:
        import pyspark
        return
    except ModuleNotFoundError:
        venv_python = project_venv_python()
        if not venv_python:
            return
        os.environ["CC_BENCHMARK_VENV_REEXEC"] = "1"
        os.execv(str(venv_python), [str(venv_python), *sys.argv])


DEFAULT_WAREHOUSE = Path(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse")
DEFAULT_TABLE = "controlciudadano.djbr_official_data"
DEFAULT_OFFICIAL_JSON_DIR = Path(os.environ.get("CC_ROOT", "."), ".local/data/contraloria/json")
CATALOG = "local"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carga JSON oficial DJBR a Apache Iceberg."
    )
    parser.add_argument("--mode", choices=["full", "prepare", "write"], default="full",
                        help="full=todo en uno, prepare=solo JSONL, write=solo Spark (default: full)")
    parser.add_argument("--source-dir", default=str(DEFAULT_OFFICIAL_JSON_DIR))
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument("--codec", default="zstd")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--worker-index", type=int, default=0)
    parser.add_argument("--worker-count", type=int, default=1)
    parser.add_argument("--jsonl-dir", default=None,
                        help="Directorio para chunks JSONL (requerido en modo prepare/write)")
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    return parser.parse_args()


def lookup_raw_data_ids(args: argparse.Namespace) -> dict[str, int]:
    with psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT remote_id::text, id FROM staging.djbr_raw_data WHERE remote_id IS NOT NULL"
            )
            return {remote_id: raw_id for remote_id, raw_id in cur.fetchall()}


def prepare_jsonl(args, jsonl_path: Path) -> int:
    sys.path.insert(0, str(BENCHMARK_DIR / "setup"))
    from djbr_legacy_common import (
        iter_official_json_files,
        official_payload_to_pg_row,
        parse_filename_metadata,
    )

    worker_tag = f"[W{args.worker_index}/{args.worker_count}]" if args.worker_count > 1 else ""
    source_dir = Path(args.source_dir)
    raw_id_by_remote = lookup_raw_data_ids(args)

    prepared = 0
    skipped = 0
    t0 = time.perf_counter()

    with jsonl_path.open("w", encoding="utf-8") as handle:
        use_sorted = args.worker_count == 1
        for index, path in enumerate(iter_official_json_files(source_dir, sorted_order=use_sorted), start=1):
            if args.limit is not None and index > args.limit:
                break
            if index % args.worker_count != args.worker_index:
                continue

            with path.open("r", encoding="utf-8") as src:
                payload = json.load(src)

            meta = parse_filename_metadata(path)
            raw_data_id = raw_id_by_remote.get(str(meta.get("remote_id")))
            if raw_data_id is None:
                skipped += 1
                continue

            row = official_payload_to_pg_row(payload, path, raw_data_id)
            serialized = {
                "raw_data_id": row["raw_data_id"],
                "charge": row["charge"],
                "active": str(row["active"]) if row["active"] is not None else None,
                "passive": str(row["passive"]) if row["passive"] is not None else None,
                "net_worth": str(row["net_worth"]) if row["net_worth"] is not None else None,
                "monthly_income": str(row["monthly_income"]) if row["monthly_income"] is not None else None,
                "anual_income": str(row["anual_income"]) if row["anual_income"] is not None else None,
                "monthly_expenses": str(row["monthly_expenses"]) if row["monthly_expenses"] is not None else None,
                "anual_expenses": str(row["anual_expenses"]) if row["anual_expenses"] is not None else None,
                "declaration_date": row["declaration_date"].strftime("%Y-%m-%d %H:%M:%S")
                if row["declaration_date"] is not None else None,
                "source": row["source"],
                "parsed": row["parsed"],
            }
            handle.write(json.dumps(serialized, ensure_ascii=False))
            handle.write("\n")
            prepared += 1

            if prepared % 10000 == 0:
                elapsed = time.perf_counter() - t0
                rate = prepared / elapsed
                print(f"[iceberg-prepare]{worker_tag} prepared={prepared} skipped={skipped} "
                      f"rate={rate:.0f}/s last={path.name}")

    elapsed = time.perf_counter() - t0
    rate = prepared / elapsed if elapsed > 0 else 0
    print(f"[iceberg-prepare]{worker_tag} done prepared={prepared} skipped={skipped} "
          f"elapsed_s={elapsed:.1f} rate={rate:.0f}/s jsonl={jsonl_path}")
    return prepared


def spark_write(args, jsonl_pattern: str) -> None:
    ensure_pyspark_from_project_venv()
    sys.path.insert(0, str(BENCHMARK_DIR))
    import _spark_windows

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    warehouse = Path(args.warehouse)

    jdk17 = os.environ.get("JAVA17_HOME", "")
    if os.path.isdir(jdk17):
        os.environ["JAVA_HOME"] = jdk17
        os.environ["PATH"] = jdk17 + "/bin;" + os.environ.get("PATH", "")

    spark = (
        SparkSession.builder
        .appName("controlciudadano-djbr-official-iceberg-load")
        .master(f"local[{args.parallelism}]")
        .config("spark.driver.memory", "14g")
        .config("spark.sql.shuffle.partitions", str(args.parallelism * 2))
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_posix())
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.iceberg.write.parquet.compression-codec", args.codec)
        .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        t0 = time.perf_counter()
        raw_df = spark.read.json(jsonl_pattern)
        df = (
            raw_df.select(
                F.col("raw_data_id").cast("long").alias("raw_data_id"),
                F.col("charge").cast("string").alias("charge"),
                F.col("active").cast(DecimalType(20, 2)).alias("active"),
                F.col("passive").cast(DecimalType(20, 2)).alias("passive"),
                F.col("net_worth").cast(DecimalType(20, 2)).alias("net_worth"),
                F.col("monthly_income").cast(DecimalType(20, 2)).alias("monthly_income"),
                F.col("anual_income").cast(DecimalType(20, 2)).alias("anual_income"),
                F.col("monthly_expenses").cast(DecimalType(20, 2)).alias("monthly_expenses"),
                F.col("anual_expenses").cast(DecimalType(20, 2)).alias("anual_expenses"),
                F.to_timestamp(F.col("declaration_date"), "yyyy-MM-dd HH:mm:ss").alias("declaration_date"),
                F.col("source").cast("string").alias("source"),
                F.col("parsed").cast("string").alias("parsed"),
            )
        )
        full_table = f"{CATALOG}.{args.table}"
        namespace = args.table.rsplit(".", 1)[0]
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
        df.writeTo(full_table).using("iceberg").createOrReplace()
        elapsed = time.perf_counter() - t0
        count = spark.table(full_table).count()
        print(f"[iceberg-write] done table={full_table} rows={count} spark_elapsed_s={elapsed:.1f}")
    finally:
        spark.stop()


def main() -> int:
    args = parse_args()

    if args.mode == "prepare":
        jsonl_dir = Path(args.jsonl_dir) if args.jsonl_dir else Path(tempfile.gettempdir()) / "djbr_chunks"
        jsonl_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = jsonl_dir / f"chunk_{args.worker_index:04d}.jsonl"
        prepare_jsonl(args, jsonl_path)
        return 0

    if args.mode == "write":
        jsonl_dir = Path(args.jsonl_dir)
        if not jsonl_dir.exists():
            print(f"[iceberg-write] ERROR: jsonl-dir no existe: {jsonl_dir}")
            return 1
        pattern = (jsonl_dir / "*.jsonl").as_posix()
        spark_write(args, pattern)
        return 0

    with tempfile.TemporaryDirectory() as tmpdir:
        jsonl_path = Path(tmpdir) / "chunk_0000.jsonl"
        t_total = time.perf_counter()
        prepared = prepare_jsonl(args, jsonl_path)
        if prepared == 0:
            print("[iceberg-full] no rows found, abortando")
            return 0
        pattern = (Path(tmpdir) / "*.jsonl").as_posix()
        spark_write(args, pattern)
        elapsed = time.perf_counter() - t_total
        print(f"[iceberg-full] total elapsed_s={elapsed:.1f} ({elapsed/60:.1f} min)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
