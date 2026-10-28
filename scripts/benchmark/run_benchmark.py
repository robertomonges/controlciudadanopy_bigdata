from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from statistics import median, stdev
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
import _spark_windows

_spark = None

RESULTS_DIR = Path(__file__).parent / "results"
QUERIES_DIR = Path(__file__).parent / "queries"
DEFAULT_WAREHOUSE = Path(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse")
CATALOG = "local"

QUERY_PARAMS = {
    "documento":      "1234567",
    "ruc":            "80000001-0",
    "anio":           2024,
    "mes":            10,
    "periodo_desde":  2020,
    "periodo_hasta":  2024,
    "nombre_patron":  "%EMPRESA%",
}

QUERY_CATALOG: dict[str, dict] = {
    "Q-A1": {"group": "A", "file": "group_a_point.sql",   "label": "lookup documento hacienda"},
    "Q-A2": {"group": "A", "file": "group_a_point.sql",   "label": "lookup documento full_data"},
    "Q-A3": {"group": "A", "file": "group_a_point.sql",   "label": "lookup RUC en SET"},
    "Q-B1": {"group": "B", "file": "group_b_range.sql",   "label": "snapshot mensual hacienda"},
    "Q-B2": {"group": "B", "file": "group_b_range.sql",   "label": "rango temporal DJBR"},
    "Q-B3": {"group": "B", "file": "group_b_range.sql",   "label": "búsqueda nombre parcial SET"},
    "Q-C1": {"group": "C", "file": "group_c_agg.sql",     "label": "masa salarial por institución"},
    "Q-C2": {"group": "C", "file": "group_c_agg.sql",     "label": "evolución empleados SFP"},
    "Q-C3": {"group": "C", "file": "group_c_agg.sql",     "label": "ranking proveedores OCDS"},
    "Q-C4": {"group": "C", "file": "group_c_agg.sql",     "label": "join hacienda-SFP por documento"},
    "Q-D1": {"group": "D", "file": "group_d_complex.sql", "label": "reconstrucción full_data"},
    "Q-D2": {"group": "D", "file": "group_d_complex.sql", "label": "evolución patrimonial DJBR+hacienda"},
    "Q-D3": {"group": "D", "file": "group_d_complex.sql", "label": "triple nómina hacienda+SFP+policía"},
}


def load_queries_from_file(sql_file: Path, engine: str) -> dict[str, str]:
    raw = sql_file.read_text(encoding="utf-8")
    queries: dict[str, str] = {}

    blocks = re.split(r"\n(?=-- Q-[A-Z]\d+:)", raw)
    for block in blocks:
        match = re.search(r"-- (Q-[A-Z]\d+):", block)
        if not match:
            continue
        qid = match.group(1)

        if engine == "postgres":
            lines = []
            for line in block.splitlines():
                stripped = line.lstrip()
                if stripped.startswith("-- FROM local.") or stripped.startswith("-- JOIN local."):
                    continue
                lines.append(line)
            query = "\n".join(lines)
        else:
            lines = []
            for line in block.splitlines():
                stripped = line.lstrip()
                lines.append(line)
            spark_lines = []
            skip_next = False
            for i, line in enumerate(lines):
                stripped = line.lstrip()
                if stripped.startswith("-- FROM local.") or stripped.startswith("-- JOIN local."):
                    spark_lines.append(line.replace("-- ", "", 1))
                    continue
                if i + 1 < len(lines):
                    next_stripped = lines[i + 1].lstrip()
                    if (
                        next_stripped.startswith("-- FROM local.")
                        or next_stripped.startswith("-- JOIN local.")
                    ) and ("FROM" in stripped or "JOIN" in stripped):
                        continue
                spark_lines.append(line)
            query = "\n".join(spark_lines)

        for key, val in QUERY_PARAMS.items():
            query = query.replace(f":{key}", str(val))

        queries[qid] = query.strip()

    return queries


def run_pg_query(conn, query: str) -> float:
    cur = conn.cursor()
    t0 = time.perf_counter()
    try:
        cur.execute(query)
        cur.fetchall()
    except psycopg2.Error as e:
        cur.close()
        raise RuntimeError(f"PG error: {e}") from e
    elapsed = time.perf_counter() - t0
    cur.close()
    return elapsed


def reset_pg_caches(pg_config: dict) -> None:
    conn = psycopg2.connect(**pg_config)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DISCARD ALL")
    cur.close()
    conn.close()


def get_or_create_spark(warehouse: Path) -> Any:
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = (
            SparkSession.builder
            .appName("controlciudadano-benchmark")
            .master("local[4]")
            .config("spark.driver.memory", "6g")
            .config("spark.sql.shuffle.partitions", "8")
            .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_posix())
            .config("spark.sql.defaultCatalog", CATALOG)
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("WARN")
    return _spark


def run_spark_query(spark, query: str) -> float:
    t0 = time.perf_counter()
    try:
        spark.sql(query).collect()
    except Exception as e:
        raise RuntimeError(f"Spark error: {e}") from e
    return time.perf_counter() - t0


def reset_spark_cache(spark) -> None:
    spark.catalog.clearCache()


def measure_query(
    run_fn,
    reset_fn,
    query: str,
    n_cold: int = 3,
    n_warm: int = 7,
) -> dict:
    times_cold = []
    times_warm = []

    total_runs = n_cold + n_warm
    for i in range(total_runs):
        is_cold = i < n_cold
        if is_cold:
            reset_fn()

        try:
            elapsed = run_fn(query)
        except RuntimeError as e:
            return {"error": str(e), "times_cold": [], "times_warm": []}

        if is_cold:
            times_cold.append(elapsed)
        else:
            times_warm.append(elapsed)

    all_times = times_cold + times_warm
    warm_arr = np.array(times_warm) if times_warm else np.array(all_times)

    return {
        "times_cold": times_cold,
        "times_warm": times_warm,
        "p50_s":  float(np.percentile(warm_arr, 50)),
        "p95_s":  float(np.percentile(warm_arr, 95)),
        "p99_s":  float(np.percentile(warm_arr, 99)),
        "min_s":  float(warm_arr.min()),
        "max_s":  float(warm_arr.max()),
        "stddev": float(warm_arr.std()),
        "cold_p50_s": float(np.percentile(times_cold, 50)) if times_cold else None,
        "error": None,
    }


def run_benchmark(args) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    results_path = RESULTS_DIR / f"results_{timestamp}.csv"

    pg_config = dict(
        host=args.pg_host, port=args.pg_port,
        database=args.pg_database,
        user=args.pg_user, password=args.pg_password,
    )
    warehouse = Path(args.warehouse)

    groups_filter = set(args.groups.upper().split(",")) if args.groups else None
    query_ids = [
        qid for qid, meta in QUERY_CATALOG.items()
        if groups_filter is None or meta["group"] in groups_filter
    ]

    engines = []
    if args.engine in ("postgres", "both"):
        engines.append("postgres")
    if args.engine in ("spark", "both"):
        engines.append("spark")

    rows = []

    for engine in engines:
        print(f"\n{'='*60}")
        print(f"  Motor: {engine.upper()}")
        print(f"{'='*60}")

        if engine == "postgres":
            pg_conn = psycopg2.connect(**pg_config)
            pg_conn.autocommit = True
            run_fn   = lambda q, c=pg_conn: run_pg_query(c, q)
            reset_fn = lambda: reset_pg_caches(pg_config)
        else:
            spark = get_or_create_spark(warehouse)
            run_fn   = lambda q, sp=spark: run_spark_query(sp, q)
            reset_fn = lambda sp=spark: reset_spark_cache(sp)

        for qid in query_ids:
            meta = QUERY_CATALOG[qid]
            sql_path = QUERIES_DIR / meta["file"]

            if not sql_path.exists():
                print(f"  [{qid}] SKIP — archivo no encontrado: {sql_path}")
                continue

            queries_in_file = load_queries_from_file(sql_path, engine)
            if qid not in queries_in_file:
                print(f"  [{qid}] SKIP — no se encontró el bloque en {sql_path}")
                continue

            query = queries_in_file[qid]
            print(f"\n  [{qid}] {meta['label']}")
            print(f"  Runs: {args.cold_runs} cold + {args.warm_runs} warm")

            stats = measure_query(
                run_fn=run_fn,
                reset_fn=reset_fn,
                query=query,
                n_cold=args.cold_runs,
                n_warm=args.warm_runs,
            )

            if stats.get("error"):
                print(f"  ERROR: {stats['error']}")
                row = {
                    "timestamp": timestamp,
                    "engine": engine,
                    "query_id": qid,
                    "group": meta["group"],
                    "label": meta["label"],
                    "error": stats["error"],
                    "p50_s": None, "p95_s": None, "p99_s": None,
                    "min_s": None, "max_s": None, "stddev": None,
                    "cold_p50_s": None,
                }
            else:
                print(
                    f"  p50={stats['p50_s']*1000:.0f}ms  "
                    f"p95={stats['p95_s']*1000:.0f}ms  "
                    f"p99={stats['p99_s']*1000:.0f}ms  "
                    f"cold_p50={stats['cold_p50_s']*1000:.0f}ms"
                )
                row = {
                    "timestamp": timestamp,
                    "engine": engine,
                    "query_id": qid,
                    "group": meta["group"],
                    "label": meta["label"],
                    "error": None,
                    "p50_s":     stats["p50_s"],
                    "p95_s":     stats["p95_s"],
                    "p99_s":     stats["p99_s"],
                    "min_s":     stats["min_s"],
                    "max_s":     stats["max_s"],
                    "stddev":    stats["stddev"],
                    "cold_p50_s": stats["cold_p50_s"],
                }
            rows.append(row)

        if engine == "postgres":
            pg_conn.close()

    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(results_path, index=False)
        print(f"\n✓ Resultados guardados en: {results_path}")
        print(df[["engine", "query_id", "p50_s", "p95_s", "p99_s", "error"]].to_string(index=False))
    else:
        print("\n! No se generaron resultados.")

    if _spark:
        _spark.stop()


def run_scale_benchmark(args) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    results_path = RESULTS_DIR / f"scale_results_{timestamp}.csv"

    SCALE_QUERIES = {
        "Q-C1-scale": """
            SELECT descripcionentidad, anio, mes, SUM(montodevengado) AS masa_salarial
            FROM {table}
            GROUP BY descripcionentidad, anio, mes
            ORDER BY anio, mes
        """,
        "Q-C4-scale": """
            SELECT COUNT(*) FROM {table}
            WHERE anio = 2024 AND mes = 10
        """,
    }
    MULTIPLIERS = [1, 2, 5, 10]

    pg_config = dict(
        host=args.pg_host, port=args.pg_port,
        database=args.pg_database,
        user=args.pg_user, password=args.pg_password,
    )
    warehouse = Path(args.warehouse)
    rows = []

    for multiplier in MULTIPLIERS:
        suffix = "" if multiplier == 1 else f"_scale_{multiplier}x"
        pg_table    = f"staging.hacienda_funcionarios{suffix}"
        spark_table = f"local.controlciudadano.hacienda_funcionarios{suffix}"

        for qid, query_template in SCALE_QUERIES.items():
            try:
                pg_conn = psycopg2.connect(**pg_config)
                pg_conn.autocommit = True
                query = query_template.format(table=pg_table)
                stats = measure_query(
                    run_fn=lambda q, c=pg_conn: run_pg_query(c, q),
                    reset_fn=lambda: reset_pg_caches(pg_config),
                    query=query, n_cold=2, n_warm=3,
                )
                pg_conn.close()
                rows.append({
                    "multiplier": multiplier,
                    "engine": "postgres",
                    "query_id": qid,
                    **{k: stats.get(k) for k in ["p50_s","p95_s","p99_s","error"]},
                })
                print(f"  PG {qid} {multiplier}x → p50={stats.get('p50_s',0)*1000:.0f}ms")
            except Exception as e:
                rows.append({"multiplier": multiplier, "engine": "postgres", "query_id": qid, "error": str(e)})

            try:
                spark = get_or_create_spark(warehouse)
                query = query_template.format(table=spark_table)
                stats = measure_query(
                    run_fn=lambda q, sp=spark: run_spark_query(sp, q),
                    reset_fn=lambda sp=spark: reset_spark_cache(sp),
                    query=query, n_cold=2, n_warm=3,
                )
                rows.append({
                    "multiplier": multiplier,
                    "engine": "spark",
                    "query_id": qid,
                    **{k: stats.get(k) for k in ["p50_s","p95_s","p99_s","error"]},
                })
                print(f"  Spark {qid} {multiplier}x → p50={stats.get('p50_s',0)*1000:.0f}ms")
            except Exception as e:
                rows.append({"multiplier": multiplier, "engine": "spark", "query_id": qid, "error": str(e)})

    df = pd.DataFrame(rows)
    df.to_csv(results_path, index=False)
    print(f"\n✓ Resultados de escalabilidad guardados en: {results_path}")

    if _spark:
        _spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark PostgreSQL vs Spark+Iceberg — Control Ciudadano."
    )
    parser.add_argument(
        "--mode",
        choices=["benchmark", "scale"],
        default="benchmark",
        help="'benchmark' = Fases 2 (queries), 'scale' = Fase 3 (escalabilidad)",
    )
    parser.add_argument(
        "--engine",
        choices=["postgres", "spark", "both"],
        default="both",
        help="Motor(es) a evaluar (default: both)",
    )
    parser.add_argument(
        "--groups",
        default=None,
        help="Grupos de queries a correr, ej: 'A,C' (default: todos)",
    )
    parser.add_argument("--cold-runs",  type=int, default=3)
    parser.add_argument("--warm-runs",  type=int, default=7)
    parser.add_argument("--warehouse",  default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--pg-host",     default="localhost")
    parser.add_argument("--pg-port",     type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user",     default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    args = parser.parse_args()

    if args.mode == "benchmark":
        run_benchmark(args)
    else:
        run_scale_benchmark(args)


if __name__ == "__main__":
    main()
