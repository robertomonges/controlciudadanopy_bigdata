from __future__ import annotations

import os
import argparse
import csv
import time
from datetime import datetime
from pathlib import Path

import psycopg2

from measure_pg_load import LOADERS as PG_LOADERS
from measure_pg_load import PG_TABLES
from setup import load_iceberg


ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "scripts" / "benchmark" / "results"
WAREHOUSE = ROOT / ".local" / "iceberg_warehouse"

PG_SOURCES = [
    "pytyvo",
    "nangareko_transparencia",
    "ande_exonerados",
    "tsje_elected",
    "a_quien_elegimos",
]

ICEBERG_SOURCES = [
    "sfp",
    "pytyvo",
    "nangareko_transparencia",
    "ande_exonerados",
    "tsje_elected",
    "a_quien_elegimos",
]

ICEBERG_TABLES = {
    "sfp": "controlciudadano.sfp",
    "pytyvo": "controlciudadano.pytyvo",
    "nangareko_transparencia": "controlciudadano.nangareko_transparencia",
    "ande_exonerados": "controlciudadano.ande_exonerados",
    "tsje_elected": "controlciudadano.tsje_elected",
    "a_quien_elegimos": "controlciudadano.a_quien_elegimos",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark de carga para fuentes chicas/pendientes en PostgreSQL e Iceberg."
    )
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    parser.add_argument("--codec", default="zstd")
    parser.add_argument("--parallelism", type=int, default=4)
    return parser.parse_args()


def connect_pg(args: argparse.Namespace):
    return psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
    )


def pg_storage(cursor, table: str) -> tuple[int, int, int]:
    schema, name = table.split(".")
    cursor.execute(
        """
        SELECT
            pg_relation_size(c.oid) AS heap_bytes,
            pg_indexes_size(c.oid) AS index_bytes,
            pg_total_relation_size(c.oid) AS total_bytes
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, name),
    )
    heap_bytes, index_bytes, total_bytes = cursor.fetchone()
    return int(heap_bytes), int(index_bytes), int(total_bytes)


def run_postgres(args: argparse.Namespace) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with connect_pg(args) as conn:
        for source in PG_SOURCES:
            print(f"[PG] {source}...")
            inserted_rows, seconds = PG_LOADERS[source](conn)
            with conn.cursor() as cur:
                heap_bytes, index_bytes, total_bytes = pg_storage(cur, PG_TABLES[source])
            rows_per_sec = inserted_rows / seconds if seconds else 0
            print(
                f"RESULT|engine=postgres|source={source}|table={PG_TABLES[source]}"
                f"|rows={inserted_rows}|seconds={seconds:.2f}|rows_per_sec={rows_per_sec:.0f}"
                f"|heap_bytes={heap_bytes}|index_bytes={index_bytes}|total_bytes={total_bytes}"
            )
            rows.append(
                {
                    "engine": "postgres",
                    "source": source,
                    "table": PG_TABLES[source],
                    "rows": str(inserted_rows),
                    "seconds": f"{seconds:.2f}",
                    "rows_per_sec": f"{rows_per_sec:.0f}",
                    "heap_bytes": str(heap_bytes),
                    "index_bytes": str(index_bytes),
                    "data_bytes": "",
                    "metadata_bytes": "",
                    "total_bytes": str(total_bytes),
                }
            )
    return rows


def iceberg_storage(warehouse: Path, source: str) -> tuple[int, int, int]:
    namespace, table_name = ICEBERG_TABLES[source].split(".", 1)
    table_root = warehouse / namespace.replace(".", "/") / table_name
    data_dir = table_root / "data"
    metadata_dir = table_root / "metadata"
    data_bytes = sum(p.stat().st_size for p in data_dir.rglob("*.parquet")) if data_dir.exists() else 0
    metadata_bytes = sum(p.stat().st_size for p in metadata_dir.rglob("*") if p.is_file()) if metadata_dir.exists() else 0
    return data_bytes, metadata_bytes, data_bytes + metadata_bytes


def run_iceberg(args: argparse.Namespace) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    spark = load_iceberg.build_spark(WAREHOUSE, codec=args.codec, parallelism=args.parallelism)
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {load_iceberg.CATALOG}.controlciudadano")
        for source in ICEBERG_SOURCES:
            print(f"[Iceberg] {source}...")
            started_at = time.perf_counter()
            load_iceberg.SOURCES[source](spark, load_iceberg.LOCAL_DATA_ROOT / source)
            seconds = time.perf_counter() - started_at
            fq_table = f"{load_iceberg.CATALOG}.{ICEBERG_TABLES[source]}"
            inserted_rows = spark.table(fq_table).count()
            data_bytes, metadata_bytes, total_bytes = iceberg_storage(WAREHOUSE, source)
            rows_per_sec = inserted_rows / seconds if seconds else 0
            print(
                f"RESULT|engine=iceberg|source={source}|table={fq_table}"
                f"|rows={inserted_rows}|seconds={seconds:.2f}|rows_per_sec={rows_per_sec:.0f}"
                f"|data_bytes={data_bytes}|metadata_bytes={metadata_bytes}|total_bytes={total_bytes}"
            )
            rows.append(
                {
                    "engine": "iceberg",
                    "source": source,
                    "table": fq_table,
                    "rows": str(inserted_rows),
                    "seconds": f"{seconds:.2f}",
                    "rows_per_sec": f"{rows_per_sec:.0f}",
                    "heap_bytes": "",
                    "index_bytes": "",
                    "data_bytes": str(data_bytes),
                    "metadata_bytes": str(metadata_bytes),
                    "total_bytes": str(total_bytes),
                }
            )
    finally:
        spark.stop()
    return rows


def write_results(rows: list[dict[str, str]]) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RESULTS_DIR / f"e3_small_sources_load_{datetime.now().strftime('%Y-%m-%d')}.csv"
    fieldnames = [
        "engine",
        "source",
        "table",
        "rows",
        "seconds",
        "rows_per_sec",
        "heap_bytes",
        "index_bytes",
        "data_bytes",
        "metadata_bytes",
        "total_bytes",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def main() -> int:
    args = parse_args()
    rows = run_postgres(args) + run_iceberg(args)
    output_path = write_results(rows)
    print(f"RESULTS_CSV|{output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
