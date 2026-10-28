from __future__ import annotations

import os
import argparse
import json
import time
from pathlib import Path

import psycopg2

from djbr_legacy_common import (
    DEFAULT_LEGACY_JSON_DIR,
    iter_legacy_json_files,
    legacy_payload_to_pg_row,
)


DEFAULT_TABLE = "benchmark.djbr_legacy_data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carga JSON DJBR legacy-compatible en PostgreSQL con esquema equivalente al legacy."
    )
    parser.add_argument("--source-dir", default=str(DEFAULT_LEGACY_JSON_DIR))
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--truncate", action="store_true")
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


def ensure_table(conn, table: str) -> None:
    schema, name = table.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id bigserial primary key,
                raw_data_id bigint,
                charge text,
                active numeric(20,2),
                passive numeric(20,2),
                net_worth numeric(20,2),
                monthly_income numeric(20,2),
                anual_income numeric(20,2),
                monthly_expenses numeric(20,2),
                anual_expenses numeric(20,2),
                declaration_date timestamp,
                source text,
                parsed jsonb
            )
            """
        )
    conn.commit()


def truncate(conn, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {table} RESTART IDENTITY")
    conn.commit()


def lookup_raw_data_ids(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT remote_id::text, id
            FROM staging.djbr_raw_data
            WHERE remote_id IS NOT NULL
            """
        )
        return {remote_id: raw_id for remote_id, raw_id in cur.fetchall()}


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)

    started = time.perf_counter()
    with connect(args) as conn:
        ensure_table(conn, args.table)
        if args.truncate:
            truncate(conn, args.table)

        raw_id_by_remote = lookup_raw_data_ids(conn)
        rows = []
        inserted = 0

        insert_sql = f"""
            INSERT INTO {args.table} (
                raw_data_id, charge, active, passive, net_worth,
                monthly_income, anual_income, monthly_expenses, anual_expenses,
                declaration_date, source, parsed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """

        for index, path in enumerate(iter_legacy_json_files(source_dir), start=1):
            if args.limit is not None and index > args.limit:
                break

            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

            meta = payload.get("__meta__") or {}
            raw_data_id = raw_id_by_remote.get(str(meta.get("remote_id")))
            row = legacy_payload_to_pg_row(payload, raw_data_id)
            rows.append(
                (
                    row["raw_data_id"],
                    row["charge"],
                    row["active"],
                    row["passive"],
                    row["net_worth"],
                    row["monthly_income"],
                    row["anual_income"],
                    row["monthly_expenses"],
                    row["anual_expenses"],
                    row["declaration_date"],
                    row["source"],
                    row["parsed"],
                )
            )

            if len(rows) >= args.batch_size:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql, rows)
                conn.commit()
                inserted += len(rows)
                print(f"[pg-load] inserted={inserted} last_file={path.name}")
                rows.clear()

        if rows:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, rows)
            conn.commit()
            inserted += len(rows)

    elapsed = time.perf_counter() - started
    print(
        f"[pg-load] done inserted={inserted} table={args.table} "
        f"elapsed_s={elapsed:.2f} source_dir={source_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
