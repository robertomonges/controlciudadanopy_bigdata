from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
SETUP_DIR = Path(__file__).resolve().parent / "setup"
DATA_DIR = ROOT / ".local/data/hacienda"
WAREHOUSE = ROOT / ".local/iceberg_warehouse"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "results" / "e4_hacienda_load.csv"

PG_DEFAULTS = dict(
    host="localhost", port=5432,
    dbname="controlciudadano", user="postgres", password=os.environ.get("PGPASSWORD", ""),
)
PG_BENCH_TABLE = "benchmark.hacienda_funcionarios"
ICE_TABLE = "controlciudadano.hacienda_funcionarios"

DEFAULT_MONTHS = [
    (2019, 1),
    (2022, 6),
    (2024, 10),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark de carga Hacienda: PG vs Iceberg")
    p.add_argument(
        "--months", nargs="+", metavar="YYYY-MM",
        help="Meses a benchmarkear (default: 3 representativos)",
    )
    p.add_argument("--no-iceberg", action="store_true", help="Solo ejecutar benchmark PG")
    p.add_argument("--no-pg", action="store_true", help="Solo ejecutar benchmark Iceberg")
    p.add_argument("--output", default=str(DEFAULT_OUTPUT))
    p.add_argument("--pg-host", default="localhost")
    p.add_argument("--pg-port", type=int, default=5432)
    p.add_argument("--pg-database", default="controlciudadano")
    p.add_argument("--pg-user", default="postgres")
    p.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    return p.parse_args()


def parse_months(month_strs: list[str]) -> list[tuple[int, int]]:
    result = []
    for s in month_strs:
        y, m = s.split("-")
        result.append((int(y), int(m)))
    return result


def truncate_pg_table(args: argparse.Namespace) -> None:
    conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname=args.pg_database, user=args.pg_user, password=args.pg_password,
    )
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS benchmark")
        cur.execute(f"DROP TABLE IF EXISTS {PG_BENCH_TABLE}")
    conn.commit()
    conn.close()


def pg_table_size(args: argparse.Namespace) -> tuple[int, str]:
    conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname=args.pg_database, user=args.pg_user, password=args.pg_password,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_total_relation_size(%s), pg_size_pretty(pg_total_relation_size(%s))",
            (PG_BENCH_TABLE, PG_BENCH_TABLE),
        )
        size_bytes, size_pretty = cur.fetchone()
    conn.close()
    return size_bytes, size_pretty


def iceberg_parquet_size() -> int:
    table_path = WAREHOUSE / ICE_TABLE.replace(".", "/") / "data"
    if not table_path.exists():
        return 0
    return sum(f.stat().st_size for f in table_path.rglob("*.parquet"))


def run_pg_load(year: int, month: int, args: argparse.Namespace) -> dict:
    cmd = [
        sys.executable,
        str(SETUP_DIR / "load_hacienda_postgres.py"),
        "--year", str(year),
        "--month", str(month),
        "--table", PG_BENCH_TABLE,
        "--truncate",
        "--batch-size", "10000",
        "--pg-host", args.pg_host,
        "--pg-port", str(args.pg_port),
        "--pg-database", args.pg_database,
        "--pg-user", args.pg_user,
        "--pg-password", args.pg_password,
    ]
    t0 = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.perf_counter() - t0

    print(result.stdout.strip())
    if result.returncode != 0:
        print("STDERR:", result.stderr[-500:])
        return {"year": year, "month": month, "error": result.stderr[-200:]}

    out = result.stdout.strip()
    rows = int(out.split("rows in")[0].split(": ")[-1].replace(",", "").strip())
    elapsed_s = float(out.split("rows in")[1].split("s")[0].strip())
    rps = float(out.split("rows/s")[0].split("(")[-1].replace(",", "").strip())
    storage_bytes = int(out.split("(")[-1].replace("bytes)", "").replace(",", "").strip())
    storage_pretty = out.split("storage: ")[1].split(" (")[0].strip()

    return {
        "scenario": "PG",
        "year": year, "month": month,
        "rows": rows, "elapsed_s": elapsed_s,
        "rows_per_sec": rps,
        "storage_bytes": storage_bytes, "storage_pretty": storage_pretty,
    }


def run_iceberg_load(year: int, month: int) -> dict:
    cmd = [
        sys.executable,
        str(SETUP_DIR / "load_hacienda_iceberg.py"),
        "--year", str(year),
        "--month", str(month),
        "--warehouse", str(WAREHOUSE),
        "--table", ICE_TABLE,
        "--mode", "overwrite",
    ]
    t0 = time.perf_counter()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.perf_counter() - t0

    print(result.stdout.strip())
    if result.returncode != 0:
        print("STDERR:", result.stderr[-1000:])
        return {"year": year, "month": month, "scenario": "Iceberg", "error": result.stderr[-200:]}

    import re as _re
    out = result.stdout.strip()
    for line in out.splitlines():
        if _re.search(r'\[Iceberg\] \d{4}-\d{2}:', line) and "rows in" in line:
            rows = int(line.split("rows in")[0].split(": ")[-1].replace(",", "").strip())
            elapsed_s = float(line.split("rows in")[1].split("s")[0].strip())
            rps = float(line.split("rows/s")[0].split("(")[-1].replace(",", "").strip())
            parquet_mb = float(line.split("Parquet: ")[1].replace(" MB", "").strip())
            return {
                "scenario": "Iceberg",
                "year": year, "month": month,
                "rows": rows, "elapsed_s": elapsed_s,
                "rows_per_sec": rps,
                "storage_bytes": int(parquet_mb * 1024 * 1024),
                "storage_pretty": f"{parquet_mb:.1f} MB",
            }

    return {"year": year, "month": month, "scenario": "Iceberg", "error": "parse failed"}


def print_summary(results: list[dict]) -> None:
    print("\n" + "=" * 80)
    print(f"{'Scenario':<12} {'Month':<10} {'Rows':>12} {'Time(s)':>10} {'Rows/s':>12} {'Storage':>12}")
    print("-" * 80)
    for r in results:
        if "error" in r:
            print(f"  ERROR {r.get('year')}-{r.get('month',0):02d}: {r['error'][:60]}")
            continue
        print(f"{r['scenario']:<12} {r['year']}-{r['month']:02d}  "
              f"{r['rows']:>12,} {r['elapsed_s']:>10.1f} {r['rows_per_sec']:>12,.0f} {r['storage_pretty']:>12}")

    print("-" * 80)
    for scenario in ["PG", "Iceberg"]:
        subset = [r for r in results if r.get("scenario") == scenario and "error" not in r]
        if not subset:
            continue
        total_rows = sum(r["rows"] for r in subset)
        total_time = sum(r["elapsed_s"] for r in subset)
        avg_rps = total_rows / total_time if total_time > 0 else 0
        total_storage = sum(r["storage_bytes"] for r in subset)
        print(f"{scenario:<12} {'TOTAL':<10} {total_rows:>12,} {total_time:>10.1f} "
              f"{avg_rps:>12,.0f} {total_storage/1024/1024:>10.1f} MB")

    pg_results = [r for r in results if r.get("scenario") == "PG" and "error" not in r]
    ice_results = [r for r in results if r.get("scenario") == "Iceberg" and "error" not in r]
    if pg_results and ice_results:
        pg_total = sum(r["storage_bytes"] for r in pg_results)
        ice_total = sum(r["storage_bytes"] for r in ice_results)
        if pg_total > 0:
            print(f"\nCompresión Iceberg vs PG: {ice_total/pg_total:.2f}x "
                  f"(Iceberg {ice_total/1024/1024:.1f} MB vs PG {pg_total/1024/1024:.1f} MB)")
    print("=" * 80)


def save_csv(results: list[dict], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fields = ["scenario", "year", "month", "rows", "elapsed_s", "rows_per_sec",
              "storage_bytes", "storage_pretty"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            if "error" not in r:
                w.writerow(r)
    print(f"\nResultados guardados en: {path}")


def main():
    args = parse_args()
    months = parse_months(args.months) if args.months else DEFAULT_MONTHS

    print(f"Benchmark E4-Hacienda: {len(months)} mes(es) — "
          f"{'PG' if not args.no_pg else ''}"
          f"{' + ' if not args.no_pg and not args.no_iceberg else ''}"
          f"{'Iceberg' if not args.no_iceberg else ''}")
    print(f"Meses: {', '.join(f'{y}-{m:02d}' for y, m in months)}\n")

    results = []

    for year, month in months:
        zip_path = DATA_DIR / f"nomina-{year}-{month:02d}.zip"
        if not zip_path.exists():
            zip_path = DATA_DIR / f"nomina_{year}-{month:02d}.zip"
        if not zip_path.exists():
            print(f"SKIP {year}-{month:02d}: ZIP not found")
            continue

        if not args.no_pg:
            print(f"\n--- PG load: {year}-{month:02d} ---")
            r = run_pg_load(year, month, args)
            results.append(r)

        if not args.no_iceberg:
            print(f"\n--- Iceberg load: {year}-{month:02d} ---")
            r = run_iceberg_load(year, month)
            results.append(r)

    print_summary(results)
    save_csv(results, args.output)


if __name__ == "__main__":
    main()
