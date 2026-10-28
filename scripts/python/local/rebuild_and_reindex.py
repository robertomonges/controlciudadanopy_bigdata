from __future__ import annotations

import os
import argparse
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[3]
LOG_FILE = ROOT / ".local/data/rebuild_reindex_timing.json"

PG = dict(host="localhost", port=5432, dbname="controlciudadano",
          user="postgres", password=os.environ.get("PGPASSWORD", ""))


def pg_count(query: str) -> int:
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()[0]
    conn.close()
    return int(result) if result is not None else 0


def es_count(index: str) -> int:
    try:
        with urllib.request.urlopen(f"http://localhost:9200/{index}/_count", timeout=10) as r:
            return json.loads(r.read())["count"]
    except Exception:
        return -1


def run_ps1(script: str) -> float:
    cmd = ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", script]
    t0 = time.perf_counter()
    result = subprocess.run(cmd, capture_output=False, text=True)
    elapsed = time.perf_counter() - t0
    if result.returncode != 0:
        raise RuntimeError(f"Script failed: {script}")
    return elapsed


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--skip-reindex", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    results = {"timestamp": datetime.now().isoformat()}

    print("=" * 70)
    print("REBUILD analysis.full_data + REINDEX Elasticsearch")
    print(f"Iniciado: {results['timestamp']}")
    print("=" * 70)

    print("\n[1/4] Contando filas en tablas fuente...")
    large_tables = {"staging.sfp", "staging.hacienda_funcionarios", "staging.policia"}
    sources = [
        "staging.sfp",
        "staging.hacienda_funcionarios",
        "staging.policia",
        "staging.a_quien_elegimos",
        "staging.ande_exonerados",
        "staging.pytyvo",
        "staging.nangareko",
        "staging.nangareko_transparencia",
        "analysis.djbr",
        "staging.tsje_elected",
    ]
    source_counts = {}
    for table in sources:
        try:
            schema, tname = table.split(".")
            if table in large_tables:
                count = pg_count(f"""
                    SELECT reltuples::bigint FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = '{schema}' AND c.relname = '{tname}'
                """)
                label = f"{count:>15,} (est.)"
            else:
                count = pg_count(f"SELECT COUNT(*) FROM {table}")
                label = f"{count:>15,}"
            source_counts[table] = count
            print(f"  {table:<45} {label}")
        except Exception as e:
            source_counts[table] = f"ERROR: {e}"
            print(f"  {table:<45} ERROR: {e}")

    results["source_counts"] = source_counts
    total_source_rows = sum(v for v in source_counts.values() if isinstance(v, int))
    print(f"\n  {'TOTAL filas en fuentes':<45} {total_source_rows:>15,}")

    print("\n[2/4] Rebuild analysis.full_data (refresh-derived-local.ps1)...")
    t0 = time.perf_counter()
    refresh_script = str(ROOT / "scripts/powershell/refresh-derived-local.ps1")
    result = subprocess.run(
        ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", refresh_script],
        capture_output=True, text=True
    )
    rebuild_elapsed = time.perf_counter() - t0

    if result.returncode != 0:
        print(f"  ERROR: {result.stderr[-300:]}")
        sys.exit(1)

    fd_count    = pg_count("SELECT COUNT(*) FROM analysis.full_data")
    tsje_count  = pg_count("SELECT COUNT(*) FROM analysis.tsje_elected")
    auth_count  = pg_count("SELECT COUNT(*) FROM analysis.authorities_with_ddjj")

    print(f"  Tiempo rebuild:                  {rebuild_elapsed:.1f}s ({rebuild_elapsed/60:.1f} min)")
    print(f"  analysis.tsje_elected:           {tsje_count:,}")
    print(f"  analysis.authorities_with_ddjj:  {auth_count:,}")
    print(f"  analysis.full_data:              {fd_count:,}")

    results["rebuild"] = {
        "elapsed_s": round(rebuild_elapsed, 1),
        "analysis_tsje_elected": tsje_count,
        "analysis_authorities_with_ddjj": auth_count,
        "analysis_full_data": fd_count,
    }

    if args.skip_reindex:
        print("\n[SKIP] Reindex omitido (--skip-reindex)")
        results["reindex"] = {"skipped": True}
    else:
        print("\n[3/4] Reindex Elasticsearch (reindex-elasticsearch-local.ps1)...")
        reindex_script = str(ROOT / "scripts/powershell/reindex-elasticsearch-local.ps1")
        result = subprocess.run(
            ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", reindex_script],
            capture_output=True, text=True
        )
        reindex_elapsed = time.perf_counter() - (t0 + rebuild_elapsed)

        if result.returncode != 0:
            print(f"  ERROR: {result.stderr[-300:]}")
        else:
            fts_count  = es_count("fts_full_data")
            auth_es    = es_count("fts_authorities_ddjj")
            print(f"  Tiempo reindex:                  {reindex_elapsed:.1f}s ({reindex_elapsed/60:.1f} min)")
            print(f"  ES fts_full_data:                {fts_count:,}")
            print(f"  ES fts_authorities_ddjj:         {auth_es:,}")

            results["reindex"] = {
                "elapsed_s": round(reindex_elapsed, 1),
                "fts_full_data": fts_count,
                "fts_authorities_ddjj": auth_es,
            }

    total_elapsed = rebuild_elapsed + results.get("reindex", {}).get("elapsed_s", 0)
    results["total_elapsed_s"] = round(total_elapsed, 1)

    print("\n" + "=" * 70)
    print("RESUMEN")
    print(f"  Rebuild analysis.full_data:  {rebuild_elapsed:.1f}s ({rebuild_elapsed/60:.1f} min)")
    if not args.skip_reindex and "elapsed_s" in results.get("reindex", {}):
        print(f"  Reindex Elasticsearch:       {results['reindex']['elapsed_s']:.1f}s ({results['reindex']['elapsed_s']/60:.1f} min)")
    print(f"  TOTAL:                       {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
    print("=" * 70)

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResultados guardados en: {LOG_FILE}")


if __name__ == "__main__":
    main()
