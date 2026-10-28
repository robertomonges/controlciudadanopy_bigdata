from __future__ import annotations

import os
import argparse
import csv
import re
import time
from pathlib import Path

import psycopg2


DEFAULT_CSV_DIR = Path(os.environ.get("CC_ROOT", "."), ".local/data/policia/procesados")
DEFAULT_SCHEMA = "staging"
DEFAULT_TABLE = "policia"
PG_POLICIA_COLUMNS = {
    "ano",
    "mes",
    "nivel_entidad",
    "ent",
    "oee",
    "linea",
    "cedula",
    "nombres",
    "apellidos",
    "estado",
    "remuneracion",
    "objeto_gasto",
    "ff",
    "categoria",
    "presupuesto",
    "devengado",
    "concepto",
    "movimiento",
    "lugar",
    "cargo",
    "funcion_real",
    "carga",
    "discapacidad",
    "tipo",
    "ingreo",
    "oficina",
    "profesion",
    "correo",
    "fecha_acto_administrativo",
    "movito_movimiento",
    "filename",
}
INTEGER_COLUMNS = {"ano", "mes", "nivel_entidad", "ent", "oee", "linea"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mide carga de CSVs Policía reutilizando staging.policia."
    )
    parser.add_argument("--csv-dir", type=Path, default=DEFAULT_CSV_DIR)
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    return parser.parse_args()


def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return max(sum(1 for _ in csv.reader(handle)) - 1, 0)


def ensure_target_table(cursor, schema: str, table: str) -> None:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        (schema, table),
    )
    exists = cursor.fetchone()[0]
    if not exists:
        raise RuntimeError(f"La tabla destino {schema}.{table} no existe")


def truncate_table(cursor, schema: str, table: str) -> None:
    cursor.execute(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY")


def normalize_integer_text(value: str) -> str:
    text = value.strip()
    if not text:
        return ""

    cleaned = (
        text.replace("Ť", "")
        .replace("ť", "")
        .replace(",", "")
        .replace(".0", "")
    )
    if re.fullmatch(r"-?\d+", cleaned):
        return cleaned

    try:
        return str(int(float(cleaned)))
    except ValueError:
        return ""


def copy_csv(cursor, schema: str, table: str, csv_path: Path) -> None:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            return

        csv_columns = [column.strip().lower() for column in header]
        pg_columns = [column for column in csv_columns if column in PG_POLICIA_COLUMNS]
        column_indexes = [csv_columns.index(column) for column in pg_columns]

        rows_buffer: list[list[str]] = []
        total_csv_columns = len(csv_columns)
        for row in reader:
            if len(row) < total_csv_columns:
                row = row + [""] * (total_csv_columns - len(row))
            normalized_row: list[str] = []
            for column, index in zip(pg_columns, column_indexes):
                value = row[index]
                if column in INTEGER_COLUMNS:
                    value = normalize_integer_text(value)
                normalized_row.append(value)
            rows_buffer.append(normalized_row)

    if not rows_buffer:
        return

    sql = (
        f"COPY {schema}.{table} ({', '.join(pg_columns)}) "
        "FROM STDIN WITH (FORMAT CSV, ENCODING 'UTF8')"
    )
    import io

    stream = io.StringIO()
    writer = csv.writer(stream, quoting=csv.QUOTE_MINIMAL)
    writer.writerows(rows_buffer)
    stream.seek(0)
    cursor.copy_expert(sql, stream)


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


def main() -> int:
    args = parse_args()
    csv_paths = sorted(args.csv_dir.glob("*.csv"))
    if not csv_paths:
        raise SystemExit(f"No se encontraron CSVs en {args.csv_dir}")

    started_at = time.perf_counter()
    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_target_table(cur, args.schema, args.table)
                truncate_table(cur, args.schema, args.table)

        for csv_path in csv_paths:
            file_started_at = time.perf_counter()
            file_rows = count_csv_rows(csv_path)
            with conn:
                with conn.cursor() as cur:
                    copy_csv(cur, args.schema, args.table, csv_path)
            elapsed = time.perf_counter() - file_started_at
            rps = file_rows / elapsed if elapsed else 0
            print(
                f"[PostgreSQL] {csv_path.name}: {file_rows:,} rows in {elapsed:.1f}s "
                f"({rps:,.0f} rows/s)"
            )

        elapsed_total = time.perf_counter() - started_at
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {args.schema}.{args.table}")
            inserted_rows = int(cur.fetchone()[0])
            heap_bytes, index_bytes, total_bytes = table_storage(cur, args.schema, args.table)

        rps_total = inserted_rows / elapsed_total if elapsed_total else 0
        print(
            "RESULT|engine=postgres"
            f"|table={args.schema}.{args.table}"
            f"|rows={inserted_rows}"
            f"|seconds={elapsed_total:.2f}"
            f"|rows_per_sec={rps_total:.0f}"
            f"|heap_bytes={heap_bytes}"
            f"|index_bytes={index_bytes}"
            f"|total_bytes={total_bytes}"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
