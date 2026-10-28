from __future__ import annotations

import os
import argparse
import time
from pathlib import Path

import psycopg2


DEFAULT_SNAPSHOT = Path(os.environ.get("CC_ROOT", "."), ".local/data/contraloria/raw_data_snapshot.csv")
DEFAULT_SCHEMA = "staging"
DEFAULT_TABLE = "djbr_raw_data"
DEPENDENT_TABLES = [
    "staging.djbr_downloaded_files",
    "analysis.djbr_data",
    "analysis.temp_djbr_scrapped_data",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carga snapshot local de DJBR raw_data en PostgreSQL."
    )
    parser.add_argument("--snapshot-file", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--truncate", action="store_true", default=True)
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    return parser.parse_args()


def connect(args: argparse.Namespace):
    return psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
    )


def table_storage(cursor, schema: str, table: str) -> tuple[int, int, int]:
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
        (schema, table),
    )
    heap_bytes, index_bytes, total_bytes = cursor.fetchone()
    return int(heap_bytes), int(index_bytes), int(total_bytes)


def count_rows_in_snapshot(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def truncate_with_dependencies(cursor, schema: str, table: str) -> None:
    tables = [f"{schema}.{table}", *DEPENDENT_TABLES]
    cursor.execute(
        "TRUNCATE TABLE "
        + ", ".join(tables)
        + " RESTART IDENTITY CASCADE"
    )


def main() -> int:
    args = parse_args()
    if not args.snapshot_file.exists():
        raise SystemExit(f"Snapshot file not found: {args.snapshot_file}")

    started = time.perf_counter()
    snapshot_rows = count_rows_in_snapshot(args.snapshot_file)
    conn = connect(args)
    try:
        with conn:
            with conn.cursor() as cur:
                if args.truncate:
                    truncate_with_dependencies(cur, args.schema, args.table)
                with args.snapshot_file.open("r", encoding="utf-8", newline="") as handle:
                    cur.copy_expert(
                        f"""
                        COPY {args.schema}.{args.table}
                        (remote_id, id_cabecera_djb, nombres, cedula, fecha, nombre_archivo, path, fisico, periodo)
                        FROM STDIN WITH CSV HEADER
                        """,
                        handle,
                    )

        elapsed = time.perf_counter() - started
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {args.schema}.{args.table}")
            inserted_rows = int(cur.fetchone()[0])
            heap_bytes, index_bytes, total_bytes = table_storage(cur, args.schema, args.table)

        rows_per_sec = inserted_rows / elapsed if elapsed else 0
        print(
            "RESULT|engine=postgres"
            f"|table={args.schema}.{args.table}"
            f"|snapshot_rows={snapshot_rows}"
            f"|rows={inserted_rows}"
            f"|seconds={elapsed:.2f}"
            f"|rows_per_sec={rows_per_sec:.0f}"
            f"|heap_bytes={heap_bytes}"
            f"|index_bytes={index_bytes}"
            f"|total_bytes={total_bytes}"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
