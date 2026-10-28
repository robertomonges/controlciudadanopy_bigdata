from __future__ import annotations

import os
import argparse
import json
import time
from pathlib import Path

import psycopg2

from djbr_legacy_common import (
    DEFAULT_OFFICIAL_JSON_DIR,
    iter_official_json_files,
    official_payload_to_pg_row,
    parse_filename_metadata,
)


DEFAULT_TABLE = "analysis.djbr_data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carga JSON oficial del portal DJBR en PostgreSQL con el esquema de analysis.djbr_data."
    )
    parser.add_argument("--source-dir", default=str(DEFAULT_OFFICIAL_JSON_DIR))
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true",
                        help="ON CONFLICT DO UPDATE (sobreescribe registros existentes)")
    parser.add_argument("--worker-index", type=int, default=0,
                        help="Indice de este worker (0-based, default 0)")
    parser.add_argument("--worker-count", type=int, default=1,
                        help="Total de workers en paralelo (default 1 = serial)")
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
                id               bigserial primary key,
                raw_data_id      bigint UNIQUE,
                charge           text,
                active           numeric(20,2),
                passive          numeric(20,2),
                net_worth        numeric(20,2),
                monthly_income   numeric(20,2),
                anual_income     numeric(20,2),
                monthly_expenses numeric(20,2),
                anual_expenses   numeric(20,2),
                declaration_date timestamp,
                source           text,
                parsed           jsonb
            )
            """
        )
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


def build_insert_sql(table: str, force: bool) -> str:
    conflict = (
        """
        ON CONFLICT (raw_data_id) DO UPDATE SET
            charge           = EXCLUDED.charge,
            active           = EXCLUDED.active,
            passive          = EXCLUDED.passive,
            net_worth        = EXCLUDED.net_worth,
            monthly_income   = EXCLUDED.monthly_income,
            anual_income     = EXCLUDED.anual_income,
            monthly_expenses = EXCLUDED.monthly_expenses,
            anual_expenses   = EXCLUDED.anual_expenses,
            declaration_date = EXCLUDED.declaration_date,
            source           = EXCLUDED.source,
            parsed           = EXCLUDED.parsed
        """
        if force
        else "ON CONFLICT (raw_data_id) DO NOTHING"
    )
    return f"""
        INSERT INTO {table} (
            raw_data_id, charge, active, passive, net_worth,
            monthly_income, anual_income, monthly_expenses, anual_expenses,
            declaration_date, source, parsed
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        {conflict}
    """


def _flush_batch(conn, insert_sql: str, rows: list, worker_tag: str) -> tuple[int, int]:
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()
        return len(rows), 0
    except Exception:
        conn.rollback()

    inserted = 0
    bad = 0
    for row in rows:
        try:
            with conn.cursor() as cur:
                cur.execute(insert_sql, row)
            conn.commit()
            inserted += 1
        except Exception as e:
            conn.rollback()
            bad += 1
            print(f"[pg-official]{worker_tag} SKIP bad row raw_data_id={row[0]}: {e}")
    return inserted, bad


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    insert_sql = build_insert_sql(args.table, args.force)

    worker_tag = f"[W{args.worker_index}/{args.worker_count}]" if args.worker_count > 1 else ""
    started = time.perf_counter()
    with connect(args) as conn:
        ensure_table(conn, args.table)
        raw_id_by_remote = lookup_raw_data_ids(conn)
        rows = []
        inserted = 0
        skipped = 0

        use_sorted = args.worker_count == 1
        for index, path in enumerate(iter_official_json_files(source_dir, sorted_order=use_sorted), start=1):
            if args.limit is not None and index > args.limit:
                break
            if index % args.worker_count != args.worker_index:
                continue

            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)

            meta = parse_filename_metadata(path)
            raw_data_id = raw_id_by_remote.get(str(meta.get("remote_id")))
            if raw_data_id is None:
                skipped += 1
                continue

            row = official_payload_to_pg_row(payload, path, raw_data_id)
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
                ins, sk = _flush_batch(conn, insert_sql, rows, worker_tag)
                inserted += ins
                skipped += sk
                rows.clear()
                print(f"[pg-official]{worker_tag} inserted={inserted} skipped={skipped} last_file={path.name}")

        if rows:
            ins, sk = _flush_batch(conn, insert_sql, rows, worker_tag)
            inserted += ins
            skipped += sk

    elapsed = time.perf_counter() - started
    print(
        f"[pg-official]{worker_tag} done inserted={inserted} skipped={skipped} "
        f"table={args.table} elapsed_s={elapsed:.2f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
