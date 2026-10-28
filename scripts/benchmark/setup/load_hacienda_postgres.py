from __future__ import annotations

import os
import argparse
import csv
import io
import time
import zipfile
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT / ".local/data/hacienda"

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

CSV_HEADER_MAP = {
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

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    id                        bigserial PRIMARY KEY,
    anio                      smallint,
    mes                       smallint,
    codigonivel               integer,
    descripcionnivel          text,
    codigoentidad             integer,
    descripcionentidad        text,
    codigoprograma            integer,
    descripcionprograma       text,
    codigosubprograma         integer,
    descripcionsubprograma    text,
    codigoproyecto            integer,
    descripcionproyecto       text,
    codigounidadresponsable   integer,
    descripcionunidadresponsable text,
    codigoobjetogasto         integer,
    conceptogasto             text,
    fuentefinanciamiento      text,
    linea                     integer,
    codigopersona             text,
    nombres                   text,
    apellidos                 text,
    sexo                      text,
    discapacidad              text,
    codigocategoria           text,
    cargo                     text,
    horascatedra              text,
    fechaingreso              text,
    tipopersonal              text,
    lugar                     text,
    montopresupuestado        bigint,
    montodevengado            bigint,
    mescorte                  smallint,
    aniocorte                 text,
    fechacorte                text,
    nivelabr                  text,
    entidadabr                text,
    programaabr               text,
    subprogramaabr            text,
    proyectoabr               text,
    unidadabr                 text
)
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--table", default="benchmark.hacienda_funcionarios")
    p.add_argument("--data-dir", default=str(DATA_DIR))
    p.add_argument("--batch-size", type=int, default=10000)
    p.add_argument("--truncate", action="store_true", help="Truncar tabla antes de cargar")
    p.add_argument("--pg-host", default="localhost")
    p.add_argument("--pg-port", type=int, default=5432)
    p.add_argument("--pg-database", default="controlciudadano")
    p.add_argument("--pg-user", default="postgres")
    p.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    return p.parse_args()


def clean(v: str) -> str | None:
    v = v.strip()
    return v if v else None


def iter_rows(zip_path: Path, year: int, month: int):
    with zipfile.ZipFile(zip_path) as zf:
        csv_name = f"nomina_{year}-{month:02d}.csv"
        with zf.open(csv_name) as raw:
            text = io.TextIOWrapper(raw, encoding="latin-1")
            reader = csv.DictReader(text, delimiter=",")
            for row in reader:
                normalized = {CSV_HEADER_MAP.get(k, k.lower()): v for k, v in row.items()}
                yield tuple(
                    clean(normalized.get(col, "")) for col in CSV_COLUMNS
                )


def main():
    args = parse_args()
    data_dir = Path(args.data_dir)
    zip_path = data_dir / f"nomina-{args.year}-{args.month:02d}.zip"
    if not zip_path.exists():
        zip_path = data_dir / f"nomina_{args.year}-{args.month:02d}.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    schema = args.table.split(".")[0]
    conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname=args.pg_database, user=args.pg_user, password=args.pg_password,
    )
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(CREATE_TABLE_SQL.format(table=args.table))
        if args.truncate:
            cur.execute(f"TRUNCATE {args.table} RESTART IDENTITY")
    conn.commit()

    placeholders = ", ".join(["%s"] * len(CSV_COLUMNS))
    insert_sql = f"INSERT INTO {args.table} ({', '.join(CSV_COLUMNS)}) VALUES ({placeholders})"

    t0 = time.perf_counter()
    batch = []
    total = 0
    with conn.cursor() as cur:
        for row in iter_rows(zip_path, args.year, args.month):
            batch.append(row)
            if len(batch) >= args.batch_size:
                cur.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                batch = []
        if batch:
            cur.executemany(insert_sql, batch)
            conn.commit()
            total += len(batch)

    elapsed = time.perf_counter() - t0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_size_pretty(pg_total_relation_size(%s)), "
            "       pg_total_relation_size(%s)",
            (args.table, args.table),
        )
        size_pretty, size_bytes = cur.fetchone()

    conn.close()

    rows_per_sec = total / elapsed if elapsed > 0 else 0
    print(f"[PG] {args.year}-{args.month:02d}: {total:,} rows in {elapsed:.1f}s "
          f"({rows_per_sec:,.0f} rows/s) — storage: {size_pretty} ({size_bytes:,} bytes)")
    return {
        "year": args.year, "month": args.month,
        "rows": total, "elapsed_s": round(elapsed, 2),
        "rows_per_sec": round(rows_per_sec, 0),
        "storage_bytes": size_bytes,
        "storage_pretty": size_pretty,
    }


if __name__ == "__main__":
    main()
