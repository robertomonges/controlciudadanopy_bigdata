from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import _spark_windows

import psycopg2
from pyspark.sql import SparkSession

WAREHOUSE = Path(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse")
DATA_ROOT  = Path(os.environ.get("CC_ROOT", "."), ".local/data")
CATALOG    = "local"

PG_TABLES = {
    "hacienda": "staging.hacienda_funcionarios",
    "sfp":      "staging.sfp",
    "set":      "staging.set",
    "pytyvo":   "staging.pytyvo",
    "policia":  "staging.policia",
}

ICE_TABLES = {
    "hacienda": "controlciudadano.hacienda_funcionarios",
    "sfp":      "controlciudadano.sfp",
    "set":      "controlciudadano.set_ruc",
    "pytyvo":   "controlciudadano.pytyvo",
    "policia":  "controlciudadano.policia",
}


def pg_table_stats(conn, pg_table: str) -> dict:
    schema, table = pg_table.split(".")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                pg_total_relation_size(c.oid)           AS total_bytes,
                pg_relation_size(c.oid)                  AS heap_bytes,
                pg_indexes_size(c.oid)                   AS index_bytes,
                c.reltuples::bigint                      AS row_estimate
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """, (schema, table))
        row = cur.fetchone()
        if not row:
            return {}
        return {
            "total_bytes":  row[0],
            "heap_bytes":   row[1],
            "index_bytes":  row[2],
            "row_estimate": row[3],
        }


def pg_exact_count(conn, pg_table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {pg_table}")
        return cur.fetchone()[0]


def iceberg_data_size_bytes(source: str) -> int:
    table_dir = WAREHOUSE / ICE_TABLES[source].replace(".", "/") / "data"
    if not table_dir.exists():
        return 0
    return sum(f.stat().st_size for f in table_dir.rglob("*.parquet"))


def iceberg_metadata_size_bytes(source: str) -> int:
    meta_dir = WAREHOUSE / ICE_TABLES[source].replace(".", "/") / "metadata"
    if not meta_dir.exists():
        return 0
    return sum(f.stat().st_size for f in meta_dir.rglob("*") if f.is_file())


def reload_iceberg_source(spark: SparkSession, source: str) -> tuple[int, float]:
    sys.path.insert(0, str(Path(__file__).parent / "setup"))
    from load_iceberg import SOURCES

    t0 = time.perf_counter()
    source_data_dir = DATA_ROOT / source
    SOURCES[source](spark, source_data_dir)
    elapsed = time.perf_counter() - t0

    ice_table = f"{CATALOG}.{ICE_TABLES[source]}"
    rows = spark.table(ice_table).count()
    return rows, elapsed


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("cc-measure-storage")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE.as_posix())
        .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

def fmt_time(s: float) -> str:
    if s < 60:
        return f"{s:.1f}s"
    return f"{s/60:.1f}min"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reload-iceberg", action="store_true",
                        help="Re-carga Iceberg y mide tiempo (tarda ~5-15 min)")
    parser.add_argument("--pg-host",     default="localhost")
    parser.add_argument("--pg-port",     type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user",     default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    args = parser.parse_args()

    print("\n=== Conectando a PostgreSQL ===")
    conn = psycopg2.connect(
        host=args.pg_host, port=args.pg_port,
        dbname=args.pg_database, user=args.pg_user, password=args.pg_password
    )

    pg_stats = {}
    for source, pg_table in PG_TABLES.items():
        print(f"  Midiendo {pg_table}...")
        stats = pg_table_stats(conn, pg_table)
        if stats:
            stats["rows_exact"] = pg_exact_count(conn, pg_table)
            pg_stats[source] = stats
        else:
            print(f"  [WARN] {pg_table} no encontrada en PostgreSQL")

    print("\n=== Midiendo storage Iceberg ===")
    ice_stats = {}
    for source in ICE_TABLES:
        data_bytes = iceberg_data_size_bytes(source)
        meta_bytes = iceberg_metadata_size_bytes(source)
        ice_stats[source] = {
            "data_bytes": data_bytes,
            "meta_bytes": meta_bytes,
            "total_bytes": data_bytes + meta_bytes,
        }
        print(f"  {source}: data={fmt_bytes(data_bytes)}  meta={fmt_bytes(meta_bytes)}")

    ice_load_times = {}
    ice_load_rows  = {}
    if args.reload_iceberg:
        print("\n=== Re-cargando Iceberg (timing) ===")
        spark = build_spark()
        spark.sparkContext.setLogLevel("ERROR")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.controlciudadano")

        for source in ICE_TABLES:
            print(f"  Cargando {source}...")
            rows, elapsed = reload_iceberg_source(spark, source)
            ice_load_times[source] = elapsed
            ice_load_rows[source]  = rows
            print(f"  {source}: {rows:,} filas en {fmt_time(elapsed)}")

        spark.stop()

        if all(v["total_bytes"] == 0 for v in ice_stats.values()):
            print("\n=== Re-midiendo storage Iceberg post-carga ===")
            for source in ICE_TABLES:
                data_bytes = iceberg_data_size_bytes(source)
                meta_bytes = iceberg_metadata_size_bytes(source)
                ice_stats[source] = {
                    "data_bytes": data_bytes,
                    "meta_bytes": meta_bytes,
                    "total_bytes": data_bytes + meta_bytes,
                }
                print(f"  {source}: data={fmt_bytes(data_bytes)}  meta={fmt_bytes(meta_bytes)}")

    print("\n" + "="*90)
    print(f"{'FUENTE':<12} {'PG heap':>10} {'PG idx':>10} {'PG total':>10} {'PG filas':>10} "
          f"{'ICE data':>10} {'ICE total':>10} {'ratio':>7}")
    print("-"*90)

    totals = {"pg_total": 0, "pg_heap": 0, "pg_idx": 0, "pg_rows": 0,
              "ice_data": 0, "ice_total": 0}

    for source in PG_TABLES:
        pg  = pg_stats.get(source, {})
        ice = ice_stats.get(source, {})

        pg_heap  = pg.get("heap_bytes", 0)
        pg_idx   = pg.get("index_bytes", 0)
        pg_total = pg.get("total_bytes", 0)
        pg_rows  = pg.get("rows_exact", 0)
        ice_data = ice.get("data_bytes", 0)
        ice_tot  = ice.get("total_bytes", 0)
        ratio    = (ice_tot / pg_total * 100) if pg_total else 0

        print(f"{source:<12} {fmt_bytes(pg_heap):>10} {fmt_bytes(pg_idx):>10} "
              f"{fmt_bytes(pg_total):>10} {pg_rows:>10,} "
              f"{fmt_bytes(ice_data):>10} {fmt_bytes(ice_tot):>10} {ratio:>6.1f}%")

        totals["pg_total"] += pg_total
        totals["pg_heap"]  += pg_heap
        totals["pg_idx"]   += pg_idx
        totals["pg_rows"]  += pg_rows
        totals["ice_data"] += ice_data
        totals["ice_total"]+= ice_tot

    print("-"*90)
    ratio_total = (totals["ice_total"] / totals["pg_total"] * 100) if totals["pg_total"] else 0
    print(f"{'TOTAL':<12} {fmt_bytes(totals['pg_heap']):>10} {fmt_bytes(totals['pg_idx']):>10} "
          f"{fmt_bytes(totals['pg_total']):>10} {totals['pg_rows']:>10,} "
          f"{fmt_bytes(totals['ice_data']):>10} {fmt_bytes(totals['ice_total']):>10} "
          f"{ratio_total:>6.1f}%")

    print(f"\n  Reduccion de almacenamiento Iceberg vs PG: {100-ratio_total:.1f}%")

    if ice_load_times:
        print("\n" + "="*60)
        print(f"{'FUENTE':<12} {'Filas':>12} {'Iceberg (s)':>14} {'filas/s':>12}")
        print("-"*60)
        total_rows = 0
        total_time = 0
        for source, elapsed in ice_load_times.items():
            rows = ice_load_rows.get(source, 0)
            rps  = rows / elapsed if elapsed else 0
            print(f"{source:<12} {rows:>12,} {fmt_time(elapsed):>14} {rps:>12,.0f}")
            total_rows += rows
            total_time += elapsed
        print("-"*60)
        rps_total = total_rows / total_time if total_time else 0
        print(f"{'TOTAL':<12} {total_rows:>12,} {fmt_time(total_time):>14} {rps_total:>12,.0f}")
        print("\n  Nota: tiempos Iceberg incluyen lectura CSV + transformacion + escritura Parquet")
        print("  Para comparar con PG, ver tiempos de los ETLs individuales en scripts/python/local/")

    conn.close()
    print()


if __name__ == "__main__":
    main()
