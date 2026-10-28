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
ICE_TABLE = f"{CATALOG}.controlciudadano.djbr_official_data"
PG_TABLE  = "benchmark.djbr_official_data"

PG_DEFAULTS = dict(
    host="localhost", port=5432,
    dbname="controlciudadano", user="postgres", password=os.environ.get("PGPASSWORD", ""),
)

SECONDARY_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS djbr_official_charge_idx ON {PG_TABLE}(charge)",
    f"CREATE INDEX IF NOT EXISTS djbr_official_decl_date_idx ON {PG_TABLE}(declaration_date)",
    f"CREATE INDEX IF NOT EXISTS djbr_official_active_idx ON {PG_TABLE}(active)",
]
SECONDARY_INDEX_NAMES = [
    "djbr_official_charge_idx",
    "djbr_official_decl_date_idx",
    "djbr_official_active_idx",
]

SAMPLE = {
    "raw_data_id":   1092160,
    "charge":        "DOCENTE (MINISTERIO DE EDUCACION Y CULTURA)",
    "charge2":       "CATEDRATICO (MINISTERIO DE EDUCACION Y CULTURA)",
    "year":          2024,
    "year_start":    "2024-01-01",
    "year_end":      "2025-01-01",
    "active_min":    10_000_000,
    "active_max":    100_000_000,
    "median_active": 35_000_000,
    "json_field":    "totalActivo",
}


QUERIES: dict[str, dict] = {

    "Q-E1": {
        "group": "E",
        "desc": "Lookup por raw_data_id (unique index, ambos escenarios PG)",
        "pg_sql": f"""
            SELECT raw_data_id, charge, active, passive, net_worth,
                   monthly_income, declaration_date, source
            FROM {PG_TABLE}
            WHERE raw_data_id = %s
        """,
        "pg_params": [SAMPLE["raw_data_id"]],
        "ice_sql": f"""
            SELECT raw_data_id, charge, active, passive, net_worth,
                   monthly_income, declaration_date, source
            FROM {ICE_TABLE}
            WHERE raw_data_id = {SAMPLE["raw_data_id"]}
        """,
        "note": "raw_data_id tiene UNIQUE INDEX en ambos escenarios PG",
    },

    "Q-E2": {
        "group": "E",
        "desc": "Lookup por charge exacto (40K filas, idx vs full scan)",
        "pg_sql": f"""
            SELECT raw_data_id, charge, active, net_worth, declaration_date
            FROM {PG_TABLE}
            WHERE charge = %s
        """,
        "pg_params": [SAMPLE["charge"]],
        "ice_sql": f"""
            SELECT raw_data_id, charge, active, net_worth, declaration_date
            FROM {ICE_TABLE}
            WHERE charge = '{SAMPLE["charge"]}'
        """,
        "note": "PG-idx: B-tree en charge; PG-noidx: full scan de 1.58M filas",
    },

    "Q-E3": {
        "group": "E",
        "desc": "Busqueda por rango de fecha (un año, ~205K filas)",
        "pg_sql": f"""
            SELECT raw_data_id, charge, active, declaration_date
            FROM {PG_TABLE}
            WHERE declaration_date >= %s AND declaration_date < %s
            ORDER BY declaration_date
            LIMIT 500
        """,
        "pg_params": [SAMPLE["year_start"], SAMPLE["year_end"]],
        "ice_sql": f"""
            SELECT raw_data_id, charge, active, declaration_date
            FROM {ICE_TABLE}
            WHERE declaration_date >= '{SAMPLE["year_start"]}'
              AND declaration_date <  '{SAMPLE["year_end"]}'
            ORDER BY declaration_date
            LIMIT 500
        """,
        "note": "PG-idx: B-tree en declaration_date; PG-noidx: full scan",
    },

    "Q-F1": {
        "group": "F",
        "desc": "Patrimonio promedio/suma por año (GROUP BY full scan)",
        "pg_sql": f"""
            SELECT EXTRACT(YEAR FROM declaration_date)::int AS yr,
                   COUNT(*)                    AS declaraciones,
                   AVG(net_worth)              AS promedio_patrimonio,
                   SUM(active)                 AS suma_activos
            FROM {PG_TABLE}
            WHERE declaration_date IS NOT NULL
              AND net_worth IS NOT NULL
              AND net_worth > -1e15 AND net_worth < 1e15
            GROUP BY EXTRACT(YEAR FROM declaration_date)::int
            ORDER BY yr
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT EXTRACT(YEAR FROM declaration_date) AS yr,
                   COUNT(*)                            AS declaraciones,
                   AVG(net_worth)                      AS promedio_patrimonio,
                   SUM(active)                         AS suma_activos
            FROM {ICE_TABLE}
            WHERE declaration_date IS NOT NULL
              AND net_worth IS NOT NULL
              AND net_worth > -1e15 AND net_worth < 1e15
            GROUP BY EXTRACT(YEAR FROM declaration_date)
            ORDER BY yr
        """,
    },

    "Q-F2": {
        "group": "F",
        "desc": "Top 20 cargos por activo promedio (GROUP BY + ORDER BY)",
        "pg_sql": f"""
            SELECT charge,
                   COUNT(*)       AS declaraciones,
                   AVG(active)    AS promedio_activo,
                   MAX(active)    AS max_activo
            FROM {PG_TABLE}
            WHERE charge IS NOT NULL
              AND active IS NOT NULL
              AND active > 0 AND active < 1e12
            GROUP BY charge
            ORDER BY AVG(active) DESC
            LIMIT 20
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT charge,
                   COUNT(*)    AS declaraciones,
                   AVG(active) AS promedio_activo,
                   MAX(active) AS max_activo
            FROM {ICE_TABLE}
            WHERE charge IS NOT NULL
              AND active IS NOT NULL
              AND active > 0 AND active < 1e12
            GROUP BY charge
            ORDER BY AVG(active) DESC
            LIMIT 20
        """,
    },

    "Q-F3": {
        "group": "F",
        "desc": "Conteo filas con activo en rango [10M, 100M] (range filter)",
        "pg_sql": f"""
            SELECT COUNT(*),
                   MIN(active), MAX(active), AVG(active)
            FROM {PG_TABLE}
            WHERE active >= %s AND active <= %s
        """,
        "pg_params": [SAMPLE["active_min"], SAMPLE["active_max"]],
        "ice_sql": f"""
            SELECT COUNT(*),
                   MIN(active), MAX(active), AVG(active)
            FROM {ICE_TABLE}
            WHERE active >= {SAMPLE["active_min"]}
              AND active <= {SAMPLE["active_max"]}
        """,
        "note": "PG-idx: index on active; columnar Iceberg: column pruning",
    },

    "Q-F4": {
        "group": "F",
        "desc": "Conteo total de filas (COUNT(*) full scan)",
        "pg_sql": f"""
            SELECT COUNT(*) FROM {PG_TABLE}
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT COUNT(*) FROM {ICE_TABLE}
        """,
        "note": "Iceberg: estadisticas de metadata (sin scan); PG: scan o pg_class",
    },

    "Q-G1": {
        "group": "G",
        "desc": "Extraccion scalar de JSONB para una fila (PK + JSONB access)",
        "pg_sql": f"""
            SELECT raw_data_id,
                   parsed->>'{SAMPLE["json_field"]}' AS total_activo_json,
                   parsed->>'fecha'                   AS fecha_json
            FROM {PG_TABLE}
            WHERE raw_data_id = %s
        """,
        "pg_params": [SAMPLE["raw_data_id"]],
        "ice_sql": f"""
            SELECT raw_data_id,
                   get_json_object(parsed, '$.{SAMPLE["json_field"]}') AS total_activo_json,
                   get_json_object(parsed, '$.fecha')                   AS fecha_json
            FROM {ICE_TABLE}
            WHERE raw_data_id = {SAMPLE["raw_data_id"]}
        """,
        "note": "PK lookup + JSONB field extraction. Iceberg: full scan por raw_data_id.",
    },

    "Q-G2": {
        "group": "G",
        "desc": "Conteo por año extraido de JSONB (scan + JSON extract)",
        "pg_sql": f"""
            SELECT (parsed->>'anioDeclaracion')::int AS anio,
                   COUNT(*)                          AS total
            FROM {PG_TABLE}
            WHERE parsed->>'anioDeclaracion' IS NOT NULL
            GROUP BY (parsed->>'anioDeclaracion')::int
            ORDER BY anio
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT CAST(get_json_object(parsed, '$.anioDeclaracion') AS INT) AS anio,
                   COUNT(*)                                                    AS total
            FROM {ICE_TABLE}
            WHERE get_json_object(parsed, '$.anioDeclaracion') IS NOT NULL
            GROUP BY CAST(get_json_object(parsed, '$.anioDeclaracion') AS INT)
            ORDER BY anio
        """,
        "note": "Full scan con extraccion y GROUP BY sobre campo de JSONB/string",
    },

    "Q-G3": {
        "group": "G",
        "desc": "Filtro por contenido JSONB (PG: @> operador; Iceberg: LIKE)",
        "pg_sql": f"""
            SELECT COUNT(*)
            FROM {PG_TABLE}
            WHERE parsed @> '{{"tipoPresentacion": "DIGITAL"}}'
        """,
        "pg_params": [],
        "ice_sql": f"""
            SELECT COUNT(*)
            FROM {ICE_TABLE}
            WHERE parsed LIKE '%"tipoPresentacion":"DIGITAL"%'
               OR parsed LIKE '%"tipoPresentacion": "DIGITAL"%'
        """,
        "note": "PG: @> containment en JSONB; Iceberg: LIKE full scan en string",
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
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
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
        f"stderr: {result.stderr[-400:] if result.stderr else '(vacio)'}"
    )


def _ice_subprocess_main(qid: str, n: int) -> None:
    import json as _json
    q = QUERIES.get(qid)
    if not q:
        print(_json.dumps({"error": f"query {qid} no encontrada"}))
        sys.exit(1)

    spark = _build_spark()
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


def create_secondary_indexes(conn) -> None:
    print("  Creando indices secundarios (charge, declaration_date, active)...")
    conn.rollback()
    for ddl in SECONDARY_INDEXES:
        idx_name = ddl.split("NOT EXISTS ")[1].split(" ON")[0]
        t0 = time.perf_counter()
        try:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        except Exception:
            conn.rollback()
        elapsed = time.perf_counter() - t0
        print(f"    {idx_name}: {elapsed:.1f}s")


def drop_secondary_indexes(conn) -> None:
    print("  Eliminando indices secundarios...")
    conn.rollback()
    for name in SECONDARY_INDEX_NAMES:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP INDEX IF EXISTS benchmark.{name}")
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"    WARN: no se pudo eliminar {name}: {e}")
    print(f"  {len(SECONDARY_INDEX_NAMES)} index(es) eliminados.")


def secondary_indexes_exist(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM pg_indexes
            WHERE schemaname = 'benchmark'
              AND tablename = 'djbr_official_data'
              AND indexname = ANY(%s)
        """, (SECONDARY_INDEX_NAMES,))
        return cur.fetchone()[0] > 0


def _build_spark():
    import _spark_windows
    jdk17 = os.environ.get("JAVA17_HOME", "")
    if os.path.isdir(jdk17):
        os.environ["JAVA_HOME"] = jdk17
        os.environ["PATH"] = jdk17 + "/bin;" + os.environ.get("PATH", "")
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("cc-benchmark-djbr-queries")
        .master("local[2]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "4")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main():
    _ensure_project_python()

    parser = argparse.ArgumentParser()
    parser.add_argument("--runs",      type=int, default=10)
    parser.add_argument("--groups",    nargs="+", default=["E", "F", "G"],
                        choices=["E", "F", "G"])
    parser.add_argument("--no-iceberg", action="store_true")
    parser.add_argument("--ice-only",   action="store_true")
    parser.add_argument("--ice-skip",   nargs="+", default=[], metavar="QUERY_ID")
    parser.add_argument("--pg-only-idx", action="store_true")
    parser.add_argument("--output",    default=None)
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
        create_secondary_indexes(conn)
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
        drop_secondary_indexes(conn)

        for qid, q in selected.items():
            print(f"  {qid} - {q['desc'][:55]}...")
            times = run_pg_query(conn, q["pg_sql"], q["pg_params"], N)
            all_results[qid]["PG-noidx"] = stats(times)
            s = all_results[qid]["PG-noidx"]
            print(f"    p50={fmt(s['p50'])}  p95={fmt(s['p95'])}  p99={fmt(s['p99'])}"
                  f"  min={fmt(s['min'])}")

        print("  Restaurando indices secundarios...")
        create_secondary_indexes(conn)
        print("  Indices restaurados.")

    if not args.no_iceberg:
        print(f"\n{'='*70}")
        print("  ESCENARIO: Iceberg/Spark (Parquet/zstd, sin indices)")
        print(f"{'='*70}")
        print("  Modo: subproceso por query (evita crash JVM en Windows)")

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
        print("-" * 90)

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
        print("-" * 90)
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
        w.writerow(["query", "group", "scenario", "min_s", "p50_s", "p95_s",
                    "p99_s", "max_s", "runs"])
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
