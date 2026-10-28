from __future__ import annotations

import argparse
import csv as csv_mod
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any

import psycopg2

sys.path.insert(0, str(Path(__file__).parent))


def _project_venv_python() -> Path | None:
    root = Path(__file__).resolve().parents[2]
    candidate = root / ".venv" / "Scripts" / "python.exe"
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

WAREHOUSE = os.path.join(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse")
CATALOG   = "local"

PG_DEFAULTS = dict(
    host="localhost", port=5432,
    dbname="controlciudadano", user="postgres", password=os.environ.get("PGPASSWORD", ""),
)

SAMPLE = {
    "hacienda_doc": os.environ.get("BENCH_HACIENDA_DOC", ""),
    "sfp_doc":      os.environ.get("BENCH_SFP_DOC", ""),
    "set_ruc":      os.environ.get("BENCH_SET_RUC", ""),
    "pytyvo_cid":   os.environ.get("BENCH_PYTYVO_CID", ""),
    "policia_ced":  os.environ.get("BENCH_POLICIA_CED", ""),
    "hacienda_anio": 2020,
    "hacienda_mes":  3,
    "sfp_anho":     2024,
    "sfp_mes":      10,
    "nombre_like":  "%MARTINEZ%",
}


QUERIES: dict[str, dict] = {

    "Q-A1": {
        "group": "A",
        "desc": "Lookup hacienda por codigopersona (idx: codigopersona,anio,mes)",
        "pg_sql": """
            SELECT codigopersona, nombres, apellidos, descripcionentidad,
                   montodevengado, anio, mes
            FROM staging.hacienda_funcionarios
            WHERE codigopersona = %s
        """,
        "pg_params": [SAMPLE["hacienda_doc"]],
        "ice_sql": f"""
            SELECT codigopersona, nombres, apellidos, descripcionentidad,
                   montodevengado, anio, mes
            FROM {CATALOG}.controlciudadano.hacienda_funcionarios
            WHERE codigopersona = '{SAMPLE["hacienda_doc"]}'
        """,
    },

    "Q-A2": {
        "group": "A",
        "desc": "Lookup sfp por documento (idx: documento,anho,mes)",
        "pg_sql": """
            SELECT documento, nombres, apellidos, concepto, devengado, anho, mes
            FROM staging.sfp
            WHERE documento = %s
        """,
        "pg_params": [SAMPLE["sfp_doc"]],
        "ice_sql": f"""
            SELECT documento, nombres, apellidos, concepto, devengado, anho, mes
            FROM {CATALOG}.controlciudadano.sfp
            WHERE documento = '{SAMPLE["sfp_doc"]}'
        """,
    },

    "Q-A3": {
        "group": "A",
        "desc": "Lookup set por ruc (sin indice en ruc -> full scan en ambos)",
        "pg_sql": """
            SELECT ruc, nombre, div, ruc_viejo
            FROM staging.set
            WHERE ruc = %s
        """,
        "pg_params": [SAMPLE["set_ruc"]],
        "ice_sql": f"""
            SELECT ruc, razon_social, tipo_contribuyente, ruc_viejo
            FROM {CATALOG}.controlciudadano.set_ruc
            WHERE ruc = '{SAMPLE["set_ruc"]}'
        """,
    },

    "Q-B1": {
        "group": "B",
        "desc": "Snapshot mensual hacienda (todos los empleados de un mes)",
        "pg_sql": """
            SELECT codigopersona, nombres, apellidos, descripcionentidad,
                   montodevengado
            FROM staging.hacienda_funcionarios
            WHERE anio = %s AND mes = %s
        """,
        "pg_params": [SAMPLE["hacienda_anio"], SAMPLE["hacienda_mes"]],
        "ice_sql": f"""
            SELECT codigopersona, nombres, apellidos, descripcionentidad,
                   montodevengado
            FROM {CATALOG}.controlciudadano.hacienda_funcionarios
            WHERE anio = {SAMPLE["hacienda_anio"]} AND mes = {SAMPLE["hacienda_mes"]}
        """,
        "note": "Iceberg particionado por anio/mes -> partition pruning",
    },

    "Q-B2": {
        "group": "B",
        "desc": "Snapshot mensual sfp (todos los funcionarios de un mes)",
        "pg_sql": """
            SELECT documento, nombres, apellidos, concepto, devengado
            FROM staging.sfp
            WHERE anho = %s AND mes = %s
        """,
        "pg_params": [SAMPLE["sfp_anho"], SAMPLE["sfp_mes"]],
        "ice_sql": f"""
            SELECT documento, nombres, apellidos, concepto, devengado
            FROM {CATALOG}.controlciudadano.sfp
            WHERE anho = {SAMPLE["sfp_anho"]} AND mes = {SAMPLE["sfp_mes"]}
        """,
    },

    "Q-B3": {
        "group": "B",
        "desc": "Busqueda por nombre parcial en set (ILIKE / LIKE)",
        "pg_sql": """
            SELECT ruc, nombre, div
            FROM staging.set
            WHERE nombre ILIKE %s
            LIMIT 200
        """,
        "pg_params": [SAMPLE["nombre_like"]],
        "ice_sql": f"""
            SELECT ruc, razon_social, tipo_contribuyente
            FROM {CATALOG}.controlciudadano.set_ruc
            WHERE upper(razon_social) LIKE upper('{SAMPLE["nombre_like"]}')
            LIMIT 200
        """,
        "note": "PG tiene indice parcial sobre nombre; LIKE con % inicial no lo usa",
    },

    "Q-C1": {
        "group": "C",
        "desc": "Masa salarial por entidad (GROUP BY sobre 1.1M filas)",
        "pg_sql": """
            SELECT descripcionentidad,
                   COUNT(*)               AS empleados,
                   SUM(montodevengado)    AS total_devengado,
                   AVG(montodevengado)    AS promedio
            FROM staging.hacienda_funcionarios
            GROUP BY descripcionentidad
            ORDER BY total_devengado DESC NULLS LAST
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT descripcionentidad,
                   COUNT(*)            AS empleados,
                   SUM(montodevengado) AS total_devengado,
                   AVG(montodevengado) AS promedio
            FROM {CATALOG}.controlciudadano.hacienda_funcionarios
            GROUP BY descripcionentidad
            ORDER BY total_devengado DESC NULLS LAST
        """,
    },

    "Q-C2": {
        "group": "C",
        "desc": "Empleados distintos por mes en sfp (COUNT DISTINCT sobre 820K filas)",
        "pg_sql": """
            SELECT anho, mes,
                   COUNT(DISTINCT documento) AS empleados_distintos,
                   SUM(devengado)            AS masa_salarial
            FROM staging.sfp
            GROUP BY anho, mes
            ORDER BY anho, mes
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT anho, mes,
                   COUNT(DISTINCT documento) AS empleados_distintos,
                   SUM(devengado)            AS masa_salarial
            FROM {CATALOG}.controlciudadano.sfp
            GROUP BY anho, mes
            ORDER BY anho, mes
        """,
    },

    "Q-C3": {
        "group": "C",
        "desc": "JOIN hacienda × sfp por documento (join de 2 tablas grandes)",
        "pg_sql": """
            SELECT h.codigopersona,
                   h.nombres          AS h_nombres,
                   h.montodevengado   AS h_devengado,
                   s.devengado        AS s_devengado,
                   h.descripcionentidad
            FROM staging.hacienda_funcionarios h
            JOIN staging.sfp s
              ON h.codigopersona = s.documento
            LIMIT 1000
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT h.codigopersona,
                   h.nombres        AS h_nombres,
                   h.montodevengado AS h_devengado,
                   s.devengado      AS s_devengado,
                   h.descripcionentidad
            FROM {CATALOG}.controlciudadano.hacienda_funcionarios h
            JOIN {CATALOG}.controlciudadano.sfp s
              ON h.codigopersona = s.documento
            LIMIT 1000
        """,
    },

    "Q-D1": {
        "group": "D",
        "desc": "Conteo total de registros por fuente (UNION ALL 5 tablas)",
        "pg_sql": """
            SELECT fuente, COUNT(*) AS total
            FROM (
                SELECT 'hacienda' AS fuente, codigopersona AS doc
                  FROM staging.hacienda_funcionarios
                UNION ALL
                SELECT 'sfp',     documento FROM staging.sfp
                UNION ALL
                SELECT 'set',     ruc       FROM staging.set
                UNION ALL
                SELECT 'pytyvo',  cid       FROM staging.pytyvo
                UNION ALL
                SELECT 'policia', cedula    FROM staging.policia
            ) t
            GROUP BY fuente
            ORDER BY fuente
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT fuente, COUNT(*) AS total
            FROM (
                SELECT 'hacienda' AS fuente, codigopersona AS doc
                  FROM {CATALOG}.controlciudadano.hacienda_funcionarios
                UNION ALL
                SELECT 'sfp',     documento FROM {CATALOG}.controlciudadano.sfp
                UNION ALL
                SELECT 'set',     ruc       FROM {CATALOG}.controlciudadano.set_ruc
                UNION ALL
                SELECT 'pytyvo',  cid       FROM {CATALOG}.controlciudadano.pytyvo
                UNION ALL
                SELECT 'policia', cedula    FROM {CATALOG}.controlciudadano.policia
            ) t
            GROUP BY fuente
            ORDER BY fuente
        """,
    },

    "Q-D2": {
        "group": "D",
        "desc": "Perfil cruzado: misma persona en todas las fuentes (UNION ALL filtrado)",
        "pg_sql": """
            SELECT 'hacienda' AS fuente, codigopersona AS doc,
                   nombres, apellidos
            FROM staging.hacienda_funcionarios WHERE codigopersona = %s
            UNION ALL
            SELECT 'sfp', documento, nombres, apellidos
            FROM staging.sfp           WHERE documento = %s
            UNION ALL
            SELECT 'pytyvo', cid, name, NULL
            FROM staging.pytyvo        WHERE cid = %s
            UNION ALL
            SELECT 'policia', cedula, nombres, apellidos
            FROM staging.policia       WHERE cedula = %s
        """,
        "pg_params": [
            SAMPLE["hacienda_doc"], SAMPLE["hacienda_doc"],
            SAMPLE["hacienda_doc"], SAMPLE["hacienda_doc"],
        ],
        "ice_sql": f"""
            SELECT 'hacienda' AS fuente, codigopersona AS doc,
                   nombres, apellidos
            FROM {CATALOG}.controlciudadano.hacienda_funcionarios
            WHERE codigopersona = '{SAMPLE["hacienda_doc"]}'
            UNION ALL
            SELECT 'sfp', documento, nombres, apellidos
            FROM {CATALOG}.controlciudadano.sfp
            WHERE documento = '{SAMPLE["hacienda_doc"]}'
            UNION ALL
            SELECT 'pytyvo', cid, name, CAST(NULL AS STRING)
            FROM {CATALOG}.controlciudadano.pytyvo
            WHERE cid = '{SAMPLE["hacienda_doc"]}'
            UNION ALL
            SELECT 'policia', cedula, nombres, apellidos
            FROM {CATALOG}.controlciudadano.policia
            WHERE cedula = '{SAMPLE["hacienda_doc"]}'
        """,
    },
}


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = (len(s) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


def run_pg_query(conn, sql: str, params: list, n: int) -> list[float]:
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql, params or None)
            cur.fetchall()
        times.append(time.perf_counter() - t0)
    return times


class SparkJVMCrash(RuntimeError):
    pass


def _run_single_ice_query_subprocess(qid: str, n: int) -> list[float]:
    import subprocess
    import json as _json
    python = sys.executable
    script = str(Path(__file__).resolve())
    cmd = [python, script, "--_ice-subprocess", qid, str(n)]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True, text=True, timeout=300,
        )
    except subprocess.TimeoutExpired:
        raise SparkJVMCrash(f"Timeout en subproceso Iceberg para {qid}")

    for line in reversed(result.stdout.splitlines()):
        line = line.strip()
        if line.startswith("{") and "times" in line:
            try:
                data = _json.loads(line)
                times_list = data.get("times", [])
                if times_list:
                    return times_list
                err = data.get("error", "sin detalle")
                raise SparkJVMCrash(f"Subproceso {qid} crash en run 1: {err}")
            except (KeyError, ValueError, _json.JSONDecodeError):
                pass

    raise SparkJVMCrash(
        f"Subproceso {qid} fallo (rc={result.returncode}). "
        f"stderr: {result.stderr[-300:] if result.stderr else '(vacio)'}"
    )


def _ice_subprocess_main(qid: str, n: int) -> None:
    import json as _json

    q = QUERIES.get(qid)
    if not q:
        print(_json.dumps({"error": f"query {qid} no encontrada"}))
        sys.exit(1)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    times = []
    try:
        sql = q["ice_sql"]
        for _ in range(n):
            t0 = time.perf_counter()
            spark.sql(sql).count()
            times.append(time.perf_counter() - t0)
    except Exception as e:
        print(_json.dumps({"error": str(e), "times": times}))
        try:
            spark.stop()
        except Exception:
            pass
        sys.exit(2)

    try:
        spark.stop()
    except Exception:
        pass
    print(_json.dumps({"times": times}))


def stats(times: list[float]) -> dict:
    return {
        "min":  min(times),
        "p50":  percentile(times, 50),
        "p95":  percentile(times, 95),
        "p99":  percentile(times, 99),
        "max":  max(times),
        "runs": len(times),
    }


def fmt(s: float) -> str:
    if s < 1:
        return f"{s*1000:.0f}ms"
    return f"{s:.2f}s"


ALL_PG_TABLES = [
    "staging.hacienda_funcionarios",
    "staging.sfp",
    "staging.set",
    "staging.pytyvo",
    "staging.policia",
]


def get_secondary_indexes(conn, table: str) -> list[tuple[str, str, str]]:
    schema, tbl = table.split(".")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT n.nspname, i.relname, pg_get_indexdef(i.oid)
            FROM pg_index x
            JOIN pg_class c ON c.oid = x.indrelid
            JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND NOT x.indisprimary
        """, (schema, tbl))
        return cur.fetchall()


def drop_all_secondary(conn) -> list[tuple[str, str, str]]:
    all_idx = []
    for table in ALL_PG_TABLES:
        idxs = get_secondary_indexes(conn, table)
        all_idx.extend(idxs)
    with conn.cursor() as cur:
        for schema, name, _ in all_idx:
            cur.execute(f"DROP INDEX IF EXISTS {schema}.{name}")
    conn.commit()
    return all_idx


def rebuild_all(conn, indexes: list[tuple[str, str, str]]) -> None:
    for schema, name, defn in indexes:
        with conn.cursor() as cur:
            cur.execute(defn)
        conn.commit()


def build_spark():
    import _spark_windows
    jdk17 = os.environ.get("JAVA17_HOME", "")
    if os.path.isdir(jdk17):
        os.environ["JAVA_HOME"] = jdk17
        os.environ["PATH"] = jdk17 + "/bin;" + os.environ.get("PATH", "")
    from pyspark.sql import SparkSession
    jvm_flags = (
        "-Dhadoop.home.dir=C:/hadoop"
    )
    return (
        SparkSession.builder
        .appName("cc-benchmark-queries")
        .master("local[2]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "4")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.driver.extraJavaOptions", jvm_flags)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main():
    _ensure_project_python()

    parser = argparse.ArgumentParser()
    parser.add_argument("--runs",      type=int, default=10,
                        help="Ejecuciones por query/escenario (default 10)")
    parser.add_argument("--groups",    nargs="+", default=["A", "B", "C", "D"],
                        choices=["A", "B", "C", "D"],
                        help="Grupos a ejecutar (default: todos)")
    parser.add_argument("--no-iceberg", action="store_true",
                        help="Saltar el escenario Iceberg/Spark")
    parser.add_argument("--ice-only", action="store_true",
                        help="Solo ejecutar el escenario Iceberg (saltar PG-idx y PG-noidx)")
    parser.add_argument("--ice-skip", nargs="+", default=[],
                        metavar="QUERY_ID",
                        help="Lista de query IDs a omitir en Iceberg (ej. Q-A3 Q-B3 Q-D1)")
    parser.add_argument("--pg-only-idx", action="store_true",
                        help="Solo ejecutar PG-idx (saltar PG-noidx)")
    parser.add_argument("--output",    default=None,
                        help="Guardar resultados en CSV (ej. results/e2.csv)")
    parser.add_argument("--pg-host",     default="localhost")
    parser.add_argument("--pg-port",     type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user",     default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    parser.add_argument("--_ice-subprocess", nargs=2, metavar=("QID", "N"),
                        help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args._ice_subprocess:
        qid, n_str = args._ice_subprocess
        _ice_subprocess_main(qid, int(n_str))
        return

    conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname=args.pg_database, user=args.pg_user, password=args.pg_password,
    )

    selected = {qid: q for qid, q in QUERIES.items() if q["group"] in args.groups}
    N = args.runs

    all_results: dict[str, dict[str, dict]] = {qid: {} for qid in selected}

    if not args.ice_only:
        print(f"\n{'='*70}")
        print("  ESCENARIO: PG-idx (con indices secundarios)")
        print(f"{'='*70}")
        for qid, q in selected.items():
            print(f"  {qid} - {q['desc'][:55]}...")
            times = run_pg_query(conn, q["pg_sql"], q["pg_params"], N)
            all_results[qid]["PG-idx"] = stats(times)
            s = all_results[qid]["PG-idx"]
            print(f"    p50={fmt(s['p50'])}  p95={fmt(s['p95'])}  p99={fmt(s['p99'])}"
                  f"  min={fmt(s['min'])}")

    if not args.pg_only_idx and not args.ice_only:
        print(f"\n{'='*70}")
        print("  ESCENARIO: PG-noidx (sin indices secundarios)")
        print(f"{'='*70}")
        print("  Dropping secondary indexes...")
        saved = drop_all_secondary(conn)
        print(f"  Dropped {len(saved)} index(es). Running queries...")

        for qid, q in selected.items():
            print(f"  {qid} - {q['desc'][:55]}...")
            times = run_pg_query(conn, q["pg_sql"], q["pg_params"], N)
            all_results[qid]["PG-noidx"] = stats(times)
            s = all_results[qid]["PG-noidx"]
            print(f"    p50={fmt(s['p50'])}  p95={fmt(s['p95'])}  p99={fmt(s['p99'])}"
                  f"  min={fmt(s['min'])}")

        print("  Rebuilding indexes...")
        rebuild_all(conn, saved)
        print("  Indexes restored.")

    if not args.no_iceberg:
        print(f"\n{'='*70}")
        print("  ESCENARIO: Iceberg/Spark (Parquet/Zstd, sin indices)")
        print(f"{'='*70}")
        print("  Modo: subproceso por query (evita crash de JVM en Java 8/Windows)")

        ice_skip_set = set(args.ice_skip)
        for qid, q in selected.items():
            if qid in ice_skip_set:
                print(f"  {qid} - [SKIP] excluido por --ice-skip")
                continue
            print(f"  {qid} - {q['desc'][:55]}...")
            try:
                times = _run_single_ice_query_subprocess(qid, N)
                all_results[qid]["Iceberg"] = stats(times)
                s = all_results[qid]["Iceberg"]
                print(f"    p50={fmt(s['p50'])}  p95={fmt(s['p95'])}  p99={fmt(s['p99'])}"
                      f"  min={fmt(s['min'])}")
            except SparkJVMCrash as e:
                print(f"    [CRASH] {e}")

    conn.close()

    if args.ice_only:
        scenarios = ["Iceberg"]
    elif args.no_iceberg:
        scenarios = ["PG-idx"] if args.pg_only_idx else ["PG-idx", "PG-noidx"]
    elif args.pg_only_idx:
        scenarios = ["PG-idx", "Iceberg"]
    else:
        scenarios = ["PG-idx", "PG-noidx", "Iceberg"]

    _print_report(selected, all_results, scenarios)

    if args.output:
        _save_csv(args.output, selected, all_results, scenarios)
        print(f"\n  Resultados guardados en {args.output}")


def _print_report(selected, all_results, scenarios):
    groups = sorted({q["group"] for q in selected.values()})

    for grp in groups:
        queries_in_group = {qid: q for qid, q in selected.items()
                            if q["group"] == grp}
        print(f"\n{'='*90}")
        print(f"  GRUPO {grp}")
        print(f"{'='*90}")

        hdr = f"{'Query':<8} {'Desc':<42}"
        for sc in scenarios:
            hdr += f"  {sc+' p50':>14} {'p95':>8} {'p99':>8}"
        print(hdr)
        print("-"*90)

        for qid, q in queries_in_group.items():
            note = q.get("note", "")
            desc = q["desc"][:40] + ("…" if len(q["desc"]) > 40 else "")
            line = f"{qid:<8} {desc:<42}"
            for sc in scenarios:
                r = all_results[qid].get(sc)
                if r:
                    line += f"  {fmt(r['p50']):>14} {fmt(r['p95']):>8} {fmt(r['p99']):>8}"
                else:
                    line += f"  {'N/A':>14} {'':>8} {'':>8}"
            print(line)
            if note:
                print(f"         [{note}]")

    if "PG-idx" in scenarios and len(scenarios) > 1:
        print(f"\n{'='*90}")
        print("  VARIACION vs PG-idx (p50) - negativo = mas rapido")
        print(f"{'='*90}")
        hdr2 = f"{'Query':<8} {'Desc':<42}"
        for sc in [s for s in scenarios if s != "PG-idx"]:
            hdr2 += f"  {sc:>20}"
        print(hdr2)
        print("-"*90)
        for qid, q in selected.items():
            base = all_results[qid].get("PG-idx", {}).get("p50", 0)
            if not base:
                continue
            desc = q["desc"][:40] + ("…" if len(q["desc"]) > 40 else "")
            line = f"{qid:<8} {desc:<42}"
            for sc in [s for s in scenarios if s != "PG-idx"]:
                r = all_results[qid].get(sc)
                if r:
                    pct = (r["p50"] - base) / base * 100
                    sign = "+" if pct > 0 else ""
                    winner = " <--" if pct < -20 else (" ^^^" if pct > 20 else "")
                    line += f"  {sign}{pct:+.0f}%{winner:>16}"
                else:
                    line += f"  {'N/A':>20}"
            print(line)

    print()


def _save_csv(path: str, selected, all_results, scenarios):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv_mod.writer(f)
        header = ["query", "group", "scenario", "min_s", "p50_s", "p95_s",
                  "p99_s", "max_s", "runs"]
        w.writerow(header)
        for qid, q in selected.items():
            for sc in scenarios:
                r = all_results[qid].get(sc)
                if r:
                    w.writerow([
                        qid, q["group"], sc,
                        round(r["min"], 4), round(r["p50"], 4),
                        round(r["p95"], 4), round(r["p99"], 4),
                        round(r["max"], 4), r["runs"],
                    ])


if __name__ == "__main__":
    main()
