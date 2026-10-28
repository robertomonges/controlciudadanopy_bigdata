from __future__ import annotations

import os
import argparse
import csv
import io
import shutil
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import psycopg2

DATA_ROOT = Path(os.environ.get("CC_ROOT", "."), ".local/data")

PG_TABLES = {
    "hacienda":                 "staging.hacienda_funcionarios",
    "sfp":                      "staging.sfp",
    "set":                      "staging.set",
    "pytyvo":                   "staging.pytyvo",
    "policia":                  "staging.policia",
    "nangareko_transparencia":  "staging.nangareko_transparencia",
    "ande_exonerados":          "staging.ande_exonerados",
    "tsje_elected":             "staging.tsje_elected",
    "a_quien_elegimos":         "staging.a_quien_elegimos",
}

ALL_SOURCES = list(PG_TABLES.keys())


def connect(args):
    return psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname=args.pg_database, user=args.pg_user, password=args.pg_password,
    )


def truncate(conn, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {table} RESTART IDENTITY")
    conn.commit()


def count_rows(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def count_rows_via_serial(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_get_serial_sequence(%s, 'id')", (table,))
        sequence_name = cur.fetchone()[0]
        if not sequence_name:
            raise RuntimeError(f"No serial sequence found for {table}.id")
        cur.execute(f"SELECT last_value FROM {sequence_name}")
        return int(cur.fetchone()[0])


def copy_stream(conn, table: str, pg_cols: list[str], stream: io.StringIO) -> None:
    col_list = ", ".join(pg_cols)
    sql = f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')"
    stream.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(sql, stream)
    conn.commit()


def copy_file(conn, table: str, pg_cols: list[str],
              csv_path: Path, sep: str = ",",
              encoding: str = "utf-8", skip_header: bool = True) -> None:
    col_list = ", ".join(pg_cols)
    sql = f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv, DELIMITER '{sep}', NULL '')"
    with open(csv_path, "r", encoding=encoding, errors="replace", newline="") as f:
        if skip_header:
            f.readline()
        with conn.cursor() as cur:
            cur.copy_expert(sql, f)
    conn.commit()


def fmt_time(s: float) -> str:
    return f"{s:.1f}s" if s < 60 else f"{s/60:.1f}min"


def load_hacienda(conn) -> tuple[int, float]:
    data_dir = DATA_ROOT / "hacienda"
    csv_paths: list[Path] = []
    tmp_dirs: list[str] = []

    t0 = time.perf_counter()

    for zip_path in sorted(data_dir.glob("nomina_*.zip")):
        tmp_dir = tempfile.mkdtemp(prefix="hacienda_pg_")
        tmp_dirs.append(tmp_dir)
        try:
            with zipfile.ZipFile(zip_path) as zf:
                for member in zf.infolist():
                    try:
                        zf.extract(member, tmp_dir)
                    except Exception as exc:
                        print(f"[hacienda] SKIP entry {member.filename}: {exc}")
        except (zipfile.BadZipFile, OSError) as exc:
            print(f"[hacienda] SKIP {zip_path.name}: {exc}")
            continue
        extracted = list(Path(tmp_dir).glob("**/*.csv"))
        if extracted:
            csv_paths.extend(extracted)
        else:
            print(f"[hacienda] SKIP {zip_path.name}: no contiene CSVs")

    csv_paths.extend(sorted(data_dir.glob("nomina_*.csv")))

    if not csv_paths:
        print("[hacienda] No se encontraron archivos")
        return 0, time.perf_counter() - t0

    with open(csv_paths[0], "r", encoding="utf-8", errors="replace") as f:
        raw_header = f.readline().strip()
    pg_cols = [c.lower().strip('"').strip() for c in raw_header.split(",")]

    print(f"[hacienda] Procesando {len(csv_paths)} archivo(s), {len(pg_cols)} columnas...")
    truncate(conn, PG_TABLES["hacienda"])

    for csv_path in csv_paths:
        copy_file(conn, PG_TABLES["hacienda"], pg_cols, csv_path,
                  sep=",", encoding="utf-8", skip_header=True)

    elapsed = time.perf_counter() - t0
    rows = count_rows_via_serial(conn, PG_TABLES["hacienda"])

    for tmp_dir in tmp_dirs:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return rows, elapsed


def load_sfp(conn) -> tuple[int, float]:
    data_root = DATA_ROOT
    csv_paths: list[Path] = []
    tmp_dirs: list[str] = []

    t0 = time.perf_counter()

    zip_candidates: list[Path] = []
    seen_zip_paths: set[str] = set()
    for zip_root in (DATA_ROOT / "sfp", data_root):
        if not zip_root.exists():
            continue
        for zip_path in sorted(zip_root.glob("sfp_*.zip")):
            normalized = str(zip_path.resolve())
            if normalized in seen_zip_paths:
                continue
            seen_zip_paths.add(normalized)
            zip_candidates.append(zip_path)

    for zip_path in zip_candidates:
        tmp_dir = tempfile.mkdtemp(prefix="sfp_pg_")
        tmp_dirs.append(tmp_dir)
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(tmp_dir)
        except (zipfile.BadZipFile, OSError) as exc:
            print(f"[sfp] SKIP {zip_path.name}: {exc}")
            continue
        extracted = list(Path(tmp_dir).glob("**/*.csv"))
        if extracted:
            print(f"[sfp] Extraido: {extracted[0].name} de {zip_path.name}")
            csv_paths.extend(extracted)

    sfp_dir = DATA_ROOT / "sfp"
    if sfp_dir.exists():
        csv_paths.extend(sorted(sfp_dir.glob("funcionarios_*.csv")))

    if not csv_paths:
        print("[sfp] No se encontraron archivos")
        return 0, time.perf_counter() - t0

    with open(csv_paths[0], "r", encoding="utf-8", errors="replace") as f:
        raw_header = f.readline().strip()
    pg_cols = [c.lower().strip('"').strip() for c in raw_header.split(",")]

    print(f"[sfp] Procesando {len(csv_paths)} archivo(s)...")
    truncate(conn, PG_TABLES["sfp"])

    for csv_path in csv_paths:
        copy_file(conn, PG_TABLES["sfp"], pg_cols, csv_path,
                  sep=",", encoding="utf-8", skip_header=True)

    elapsed = time.perf_counter() - t0
    rows = count_rows_via_serial(conn, PG_TABLES["sfp"])

    for tmp_dir in tmp_dirs:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return rows, elapsed


def load_set(conn) -> tuple[int, float]:
    data_dir = DATA_ROOT / "set"
    txt_paths: list[Path] = []
    tmp_dirs: list[str] = []

    t0 = time.perf_counter()

    for zip_path in sorted(data_dir.glob("*.zip")):
        tmp_dir = tempfile.mkdtemp(prefix="set_pg_")
        tmp_dirs.append(tmp_dir)
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(tmp_dir)
        except (zipfile.BadZipFile, OSError) as exc:
            print(f"[set] SKIP {zip_path.name}: {exc}")
            continue
        txt_paths.extend(Path(tmp_dir).rglob("*.txt"))

    txt_paths.extend(sorted(data_dir.glob("*.txt")))

    if not txt_paths:
        print("[set] No se encontraron archivos .txt")
        return 0, time.perf_counter() - t0

    pg_cols = ["ruc", "nombre", "div", "ruc_viejo"]

    print(f"[set] Procesando {len(txt_paths)} archivo(s)...")
    truncate(conn, PG_TABLES["set"])

    for txt_path in sorted(set(txt_paths)):
        buf = io.StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        encoding = _detect_encoding(txt_path)
        with open(txt_path, "r", encoding=encoding, errors="replace", newline="") as f:
            for line in f:
                line = line.rstrip("\n\r")
                if not line:
                    continue
                parts = line.split("|")
                if len(parts) < 4:
                    continue
                writer.writerow(parts[:4])
        copy_stream(conn, PG_TABLES["set"], pg_cols, buf)

    elapsed = time.perf_counter() - t0
    rows = count_rows(conn, PG_TABLES["set"])

    for tmp_dir in tmp_dirs:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return rows, elapsed


def _detect_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.read(4096)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


PYTYVO_HEADER_ALIASES = {
    "cic no": "cid", "cic nº": "cid", "nro de cic": "cid",
    "nro. de cic": "cid", "n. de cic": "cid",
    "nombre y apellido": "name",
    "departamento": "department",
    "distrito": "district",
}


def load_pytyvo(conn) -> tuple[int, float]:
    data_dir = DATA_ROOT / "pytyvo"
    csv_paths = list(sorted(data_dir.glob("**/*.csv")))

    if not csv_paths:
        print("[pytyvo] No se encontraron archivos")
        return 0, 0.0

    t0 = time.perf_counter()

    pg_cols = ["name", "department", "district", "cid", "file"]
    truncate(conn, PG_TABLES["pytyvo"])

    for csv_path in csv_paths:
        encoding = _detect_encoding(csv_path)
        buf = io.StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        rows_in_file = 0
        with open(csv_path, "r", encoding=encoding, errors="replace", newline="") as f:
            reader = csv.reader(f, delimiter=";")
            header_raw = next(reader, None)
            if header_raw is None:
                continue
            header_norm = [PYTYVO_HEADER_ALIASES.get(h.strip().lower(), h.strip().lower())
                           for h in header_raw]
            try:
                idx_cid  = header_norm.index("cid")
                idx_name = header_norm.index("name")
                idx_dep  = header_norm.index("department")
                idx_dis  = header_norm.index("district")
            except ValueError as exc:
                print(f"[pytyvo] SKIP {csv_path.name}: columna no encontrada ({exc})")
                continue
            file_label = csv_path.name
            for row in reader:
                if len(row) < 4:
                    continue
                writer.writerow([
                    row[idx_name] if idx_name < len(row) else "",
                    row[idx_dep]  if idx_dep  < len(row) else "",
                    row[idx_dis]  if idx_dis  < len(row) else "",
                    row[idx_cid]  if idx_cid  < len(row) else "",
                    file_label,
                ])
                rows_in_file += 1
        copy_stream(conn, PG_TABLES["pytyvo"], pg_cols, buf)

    elapsed = time.perf_counter() - t0
    rows = count_rows(conn, PG_TABLES["pytyvo"])
    return rows, elapsed


def load_policia(conn) -> tuple[int, float]:
    data_dir = DATA_ROOT / "policia"
    csv_paths = list(sorted(data_dir.glob("*.csv")))

    if not csv_paths:
        print("[policia] No se encontraron archivos")
        return 0, 0.0

    PG_POLICIA_COLS = {
        "ano", "mes", "nivel_entidad", "ent", "oee", "linea", "cedula",
        "nombres", "apellidos", "estado", "remuneracion", "objeto_gasto",
        "ff", "categoria", "presupuesto", "devengado", "concepto",
        "movimiento", "lugar", "cargo", "funcion_real", "carga",
        "discapacidad", "tipo", "ingreo", "oficina", "profesion", "correo",
        "fecha_acto_administrativo", "movito_movimiento", "filename",
    }

    t0 = time.perf_counter()
    truncate(conn, PG_TABLES["policia"])

    for csv_path in csv_paths:
        encoding = _detect_encoding(csv_path)
        with open(csv_path, "r", encoding=encoding, errors="replace") as f:
            reader = csv.reader(f)
            header_raw = next(reader, None)
            if header_raw is None:
                continue
            csv_cols = [c.lower().strip('"').strip() for c in header_raw]
            pg_cols = [c for c in csv_cols if c in PG_POLICIA_COLS]
            col_indices = [csv_cols.index(c) for c in pg_cols]
            n_csv = len(csv_cols)

            buf = io.StringIO()
            writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if len(row) < n_csv:
                    row = row + [""] * (n_csv - len(row))
                writer.writerow([row[i] for i in col_indices])

        copy_stream(conn, PG_TABLES["policia"], pg_cols, buf)

    elapsed = time.perf_counter() - t0
    rows = count_rows(conn, PG_TABLES["policia"])
    return rows, elapsed


NANGAREKO_TRANSPARENCIA_HEADER_ALIASES = {
    "\ufeffnivel": "nivel",
    "nivel": "nivel",
    "código entidad": "codigo_entidad",
    "codigo entidad": "codigo_entidad",
    "entidad": "entidad",
    "clase programa": "clase_programa",
    "código programa": "codigo_programa",
    "codigo programa": "codigo_programa",
    "nombre programa": "nombre_programa",
    "código proyecto actividad": "codigo_proyecto_actividad",
    "codigo proyecto actividad": "codigo_proyecto_actividad",
    "nombre actividad proyecto": "nombre_actividad_proyecto",
    "tipo subsidio programa": "tipo_subsidio_programa",
    "número documento": "numero_documento",
    "numero documento": "numero_documento",
    "nombre beneficiario": "nombre_beneficiario",
    "id departamento": "id_departamento",
    "nombre departamento": "nombre_departamento",
    "id municipio": "id_municipio",
    "nombre municipio": "nombre_municipio",
    "característica subsidio": "caracteristica_subsidio",
    "caracteristica subsidio": "caracteristica_subsidio",
    "nombre subsidio": "nombre_subsidio",
    "valor subsidio": "valor_subsidio",
    "cantidad subsidio": "cantidad_subsidio",
    "fecha cargue archivo": "fecha_cargue_archivo",
    "fuente datos": "fuente_datos",
}


def load_nangareko_transparencia(conn) -> tuple[int, float]:
    csv_path = DATA_ROOT / "nangareko" / "subsidios_zip" / "Subsidios.csv"
    if not csv_path.exists():
        print("[nangareko_transparencia] Archivo no encontrado")
        return 0, 0.0

    pg_cols = [
        "nivel",
        "codigo_entidad",
        "entidad",
        "clase_programa",
        "codigo_programa",
        "nombre_programa",
        "codigo_proyecto_actividad",
        "nombre_actividad_proyecto",
        "tipo_subsidio_programa",
        "numero_documento",
        "nombre_beneficiario",
        "id_departamento",
        "nombre_departamento",
        "id_municipio",
        "nombre_municipio",
        "caracteristica_subsidio",
        "nombre_subsidio",
        "valor_subsidio",
        "cantidad_subsidio",
        "fecha_cargue_archivo",
        "fuente_datos",
    ]

    t0 = time.perf_counter()
    truncate(conn, PG_TABLES["nangareko_transparencia"])

    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    with open(csv_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header_raw = next(reader, None)
        if header_raw is None:
            return 0, 0.0
        header_norm = [NANGAREKO_TRANSPARENCIA_HEADER_ALIASES.get(h.strip().lower(), h.strip().lower()) for h in header_raw]
        idx_map = [header_norm.index(col) for col in pg_cols]
        for row in reader:
            if len(row) < len(header_norm):
                row = row + [""] * (len(header_norm) - len(row))
            writer.writerow([row[idx] for idx in idx_map])

    copy_stream(conn, PG_TABLES["nangareko_transparencia"], pg_cols, buf)
    elapsed = time.perf_counter() - t0
    rows = count_rows(conn, PG_TABLES["nangareko_transparencia"])
    return rows, elapsed


def load_ande_exonerados(conn) -> tuple[int, float]:
    csv_path = DATA_ROOT / "ande_exonerados.normalized.csv"
    if not csv_path.exists():
        print("[ande_exonerados] Archivo no encontrado")
        return 0, 0.0

    t0 = time.perf_counter()
    truncate(conn, PG_TABLES["ande_exonerados"])
    copy_file(
        conn,
        PG_TABLES["ande_exonerados"],
        ["agencia", "nis", "tarifa", "cliente", "documento", "fecha_exoneracion"],
        csv_path,
        sep=",",
        encoding="utf-8-sig",
        skip_header=True,
    )
    elapsed = time.perf_counter() - t0
    rows = count_rows(conn, PG_TABLES["ande_exonerados"])
    return rows, elapsed


def load_tsje_elected(conn) -> tuple[int, float]:
    csv_path = DATA_ROOT / "tsje_elected.csv"
    if not csv_path.exists():
        print("[tsje_elected] Archivo no encontrado")
        return 0, 0.0

    t0 = time.perf_counter()
    truncate(conn, PG_TABLES["tsje_elected"])
    copy_file(
        conn,
        PG_TABLES["tsje_elected"],
        [
            "ano", "codeleccion", "departamento", "dep_desc", "candidatura", "cand_desc",
            "lista", "nombre_lista", "siglas_lista", "total_votos", "tit_sup", "desc_tit_sup",
            "orden_lista", "orden_dhont", "nro_div", "cociente", "apellido", "nombre",
            "sexo", "nacionalidad", "edad",
        ],
        csv_path,
        sep=",",
        encoding=_detect_encoding(csv_path),
        skip_header=True,
    )
    elapsed = time.perf_counter() - t0
    rows = count_rows(conn, PG_TABLES["tsje_elected"])
    return rows, elapsed


def load_a_quien_elegimos(conn) -> tuple[int, float]:
    csv_path = DATA_ROOT / "a_quien_elegimos.csv"
    if not csv_path.exists():
        print("[a_quien_elegimos] Archivo no encontrado")
        return 0, 0.0

    t0 = time.perf_counter()
    truncate(conn, PG_TABLES["a_quien_elegimos"])
    copy_file(
        conn,
        PG_TABLES["a_quien_elegimos"],
        [
            "id", "name", "lastname", "alternate_name", "former_name", "identifier",
            "email_address", "gender", "date_of_birth", "date_of_death", "head_shot",
            "one_line_biography", "biography", "national_identity", "phone",
            "contact_detail", "external_links", "decendents", "fb", "tw", "insta",
            "estado_civil", "city_of_residence", "status", "timestamp_creacion",
            "organization", "valoracion", "id_cird", "vistos",
        ],
        csv_path,
        sep=",",
        encoding=_detect_encoding(csv_path),
        skip_header=True,
    )
    elapsed = time.perf_counter() - t0
    rows = count_rows(conn, PG_TABLES["a_quien_elegimos"])
    return rows, elapsed


LOADERS = {
    "hacienda":                 load_hacienda,
    "sfp":                      load_sfp,
    "set":                      load_set,
    "pytyvo":                   load_pytyvo,
    "policia":                  load_policia,
    "nangareko_transparencia":  load_nangareko_transparencia,
    "ande_exonerados":          load_ande_exonerados,
    "tsje_elected":             load_tsje_elected,
    "a_quien_elegimos":         load_a_quien_elegimos,
}

ALL_SCENARIOS = ["PG-A", "PG-B", "PG-C", "PG-D"]


def get_secondary_indexes(conn, table: str) -> list[tuple[str, str, str]]:
    schema, tbl = table.split(".")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT n.nspname, i.relname, pg_get_indexdef(i.oid)
            FROM pg_index x
            JOIN pg_class c ON c.oid = x.indrelid
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
              AND NOT x.indisprimary
        """, (schema, tbl))
        return cur.fetchall()


def drop_indexes(conn, indexes: list[tuple[str, str, str]]) -> None:
    with conn.cursor() as cur:
        for schema, name, _ in indexes:
            full_name = f"{schema}.{name}"
            print(f"    DROP INDEX {full_name}")
            cur.execute(f"DROP INDEX IF EXISTS {full_name}")
    conn.commit()


def rebuild_indexes(conn, indexes: list[tuple[str, str, str]]) -> float:
    t0 = time.perf_counter()
    for schema, name, defn in indexes:
        print(f"    CREATE INDEX {schema}.{name}...")
        with conn.cursor() as cur:
            cur.execute(defn)
        conn.commit()
    return time.perf_counter() - t0


def set_table_logged(conn, table: str, unlogged: bool) -> None:
    mode = "UNLOGGED" if unlogged else "LOGGED"
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {table} SET {mode}")
    conn.commit()


def run_scenario(conn, scenario: str, sources: list[str]) -> dict[str, dict]:
    results = {}

    for source in sources:
        table = PG_TABLES[source]
        print(f"\n  [{scenario}] {source}...")

        saved_indexes: list[tuple[str, str]] = []

        if scenario in ("PG-B", "PG-C"):
            saved_indexes = get_secondary_indexes(conn, table)
            if saved_indexes:
                print(f"    Dropping {len(saved_indexes)} index(es)...")
                drop_indexes(conn, saved_indexes)

        if scenario == "PG-D":
            print(f"    SET UNLOGGED...")
            set_table_logged(conn, table, unlogged=True)

        try:
            rows, copy_elapsed = LOADERS[source](conn)
        except Exception as exc:
            print(f"    [ERROR] {exc}")
            results[source] = {"rows": 0, "copy_s": 0.0, "index_s": 0.0, "total_s": 0.0}
            if scenario in ("PG-B", "PG-C") and saved_indexes:
                rebuild_indexes(conn, saved_indexes)
            if scenario == "PG-D":
                set_table_logged(conn, table, unlogged=False)
            continue

        index_elapsed = 0.0

        if scenario == "PG-B" and saved_indexes:
            print(f"    Rebuilding {len(saved_indexes)} index(es)...")
            index_elapsed = rebuild_indexes(conn, saved_indexes)
            print(f"    Index rebuild: {fmt_time(index_elapsed)}")

        if scenario == "PG-D":
            print(f"    SET LOGGED...")
            set_table_logged(conn, table, unlogged=False)

        if scenario == "PG-C" and saved_indexes:
            print(f"    Restoring {len(saved_indexes)} index(es) (sin contar en tiempo)...")
            rebuild_indexes(conn, saved_indexes)

        total = copy_elapsed + index_elapsed
        rps = rows / total if total else 0
        print(f"    -> {rows:,} filas | COPY={fmt_time(copy_elapsed)}"
              + (f" + idx={fmt_time(index_elapsed)}" if index_elapsed else "")
              + f" | total={fmt_time(total)} | {rps:,.0f} r/s")

        results[source] = {
            "rows":    rows,
            "copy_s":  copy_elapsed,
            "index_s": index_elapsed,
            "total_s": total,
        }

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-host",     default="localhost")
    parser.add_argument("--pg-port",     type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user",     default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    parser.add_argument("--sources", nargs="+", default=ALL_SOURCES,
                        choices=ALL_SOURCES, metavar="SOURCE",
                        help="Fuentes a cargar (default: todas)")
    parser.add_argument("--scenario", nargs="+", default=ALL_SCENARIOS,
                        choices=ALL_SCENARIOS, dest="scenarios", metavar="SCENARIO",
                        help="Escenario(s): PG-A PG-B PG-C PG-D (default: todos)")
    args = parser.parse_args()

    conn = connect(args)

    all_results: dict[str, dict[str, dict]] = {}
    for scenario in args.scenarios:
        print(f"\n{'='*70}")
        print(f"  ESCENARIO {scenario}")
        print(f"{'='*70}")
        all_results[scenario] = run_scenario(conn, scenario, args.sources)

    conn.close()

    print("\n\n" + "="*90)
    print(f"{'FUENTE':<12}", end="")
    for sc in args.scenarios:
        label = f"{sc} total"
        print(f"  {label:>14}", end="")
    print(f"  {'Iceberg ref':>12}")
    print("-"*90)

    ICEBERG_REF = {
        "hacienda": 15.4,
        "sfp": 3.0,
        "set": 2.6,
        "pytyvo": 1.0,
        "policia": 0.9,
        "nangareko_transparencia": 0.0,
        "ande_exonerados": 0.0,
        "tsje_elected": 0.0,
        "a_quien_elegimos": 0.0,
    }

    totals: dict[str, float] = {sc: 0.0 for sc in args.scenarios}
    for source in args.sources:
        print(f"{source:<12}", end="")
        for sc in args.scenarios:
            r = all_results.get(sc, {}).get(source, {})
            t = r.get("total_s", 0.0)
            totals[sc] += t
            print(f"  {fmt_time(t):>14}", end="")
        ice = ICEBERG_REF.get(source, 0.0)
        print(f"  {fmt_time(ice):>12}")
    print("-"*90)
    print(f"{'TOTAL':<12}", end="")
    for sc in args.scenarios:
        print(f"  {fmt_time(totals[sc]):>14}", end="")
    ice_total = sum(ICEBERG_REF[s] for s in args.sources if s in ICEBERG_REF)
    print(f"  {fmt_time(ice_total):>12}")

    print("\n  Escenarios:")
    print("    PG-A: produccion (indices activos durante COPY)")
    print("    PG-B: DBA-bulk (drop indices -> COPY -> CREATE INDEX; tiempo = COPY + rebuild)")
    print("    PG-C: sin indices durante COPY (solo heap, sin rebuild)")
    print("    PG-D: UNLOGGED table (sin WAL; indices activos)")
    print("    Iceberg ref: Spark local[4], Parquet/Zstd, sin indices\n")


if __name__ == "__main__":
    main()
