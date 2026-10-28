from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

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


ensure_pyspark_from_project_venv()

sys.path.insert(0, str(BENCHMARK_DIR))
import _spark_windows

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
)

from djbr_legacy_common import (
    DEFAULT_LEGACY_JSON_DIR,
    legacy_payload_to_pg_row,
    iter_legacy_json_files,
)


DEFAULT_WAREHOUSE = Path(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse")
DEFAULT_TABLE = "controlciudadano.djbr_legacy_data"
CATALOG = "local"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carga JSON DJBR legacy-compatible en Apache Iceberg."
    )
    parser.add_argument("--source-dir", default=str(DEFAULT_LEGACY_JSON_DIR))
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument("--codec", default="zstd")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    return parser.parse_args()


def build_spark(warehouse: Path, codec: str, parallelism: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName("controlciudadano-djbr-legacy-iceberg-load")
        .master(f"local[{parallelism}]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_posix())
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.iceberg.write.parquet.compression-codec", codec)
        .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


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
                """
                SELECT remote_id::text, id
                FROM staging.djbr_raw_data
                WHERE remote_id IS NOT NULL
                """
            )
            return {remote_id: raw_id for remote_id, raw_id in cur.fetchall()}


def as_text(value):
    if value is None:
        return None
    return str(value)


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    warehouse = Path(args.warehouse)
    spark = build_spark(warehouse, args.codec, args.parallelism)
    raw_id_by_remote = lookup_raw_data_ids(args)

    prepared = 0
    temp_path = None

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".jsonl", delete=False) as handle:
        temp_path = Path(handle.name)
        for index, path in enumerate(iter_legacy_json_files(source_dir), start=1):
            if args.limit is not None and index > args.limit:
                break

            with path.open("r", encoding="utf-8") as source_handle:
                payload = json.load(source_handle)

            meta = payload.get("__meta__") or {}
            raw_data_id = raw_id_by_remote.get(str(meta.get("remote_id")))

            row = legacy_payload_to_pg_row(payload, raw_data_id)
            serialized_row = {
                "raw_data_id": row["raw_data_id"],
                "charge": row["charge"],
                "active": as_text(row["active"]),
                "passive": as_text(row["passive"]),
                "net_worth": as_text(row["net_worth"]),
                "monthly_income": as_text(row["monthly_income"]),
                "anual_income": as_text(row["anual_income"]),
                "monthly_expenses": as_text(row["monthly_expenses"]),
                "anual_expenses": as_text(row["anual_expenses"]),
                "declaration_date": row["declaration_date"].strftime("%Y-%m-%d %H:%M:%S")
                if row["declaration_date"] is not None
                else None,
                "source": row["source"],
                "parsed": row["parsed"],
            }
            handle.write(json.dumps(serialized_row, ensure_ascii=False))
            handle.write("\n")
            prepared += 1

            if prepared % 10000 == 0:
                print(f"[iceberg-load] prepared={prepared} last_file={path.name}")

    if prepared == 0:
        print("[iceberg-load] no rows found")
        spark.stop()
        return 0

    try:
        raw_df = spark.read.json(temp_path.as_posix())
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
        print(f"[iceberg-load] done rows={prepared} table={full_table}")
        return 0
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
