from __future__ import annotations

import os
import argparse
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BENCHMARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_DIR))
sys.path.insert(0, str(BENCHMARK_DIR / "setup"))

from load_iceberg import CATALOG, DEFAULT_WAREHOUSE, build_spark


DEFAULT_SNAPSHOT = Path(os.environ.get("CC_ROOT", "."), ".local/data/contraloria/raw_data_snapshot.csv")
DEFAULT_TABLE = "controlciudadano.djbr_raw_data"
WAREHOUSE_ROOT = ROOT / ".local" / "iceberg_warehouse"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Carga snapshot local de DJBR raw_data en Apache Iceberg."
    )
    parser.add_argument("--snapshot-file", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--warehouse", default=str(DEFAULT_WAREHOUSE))
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument("--codec", default="zstd")
    return parser.parse_args()


def iceberg_table_path(table_name: str) -> Path:
    namespace, table = table_name.split(".", 1)
    return WAREHOUSE_ROOT / namespace / table


def directory_size_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main() -> int:
    args = parse_args()
    if not args.snapshot_file.exists():
        raise SystemExit(f"Snapshot file not found: {args.snapshot_file}")

    started = time.perf_counter()
    full_table = f"{CATALOG}.{args.table}"
    namespace = args.table.rsplit(".", 1)[0]
    spark = build_spark(Path(args.warehouse), codec=args.codec, parallelism=args.parallelism)
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("sep", ",")
            .option("nullValue", "")
            .csv(args.snapshot_file.as_posix())
        )

        typed = (
            df.selectExpr(
                "CAST(remote_id AS BIGINT) AS remote_id",
                "CAST(id_cabecera_djb AS BIGINT) AS id_cabecera_djb",
                "CAST(nombres AS STRING) AS nombres",
                "CAST(cedula AS STRING) AS cedula",
                "TO_TIMESTAMP(fecha) AS fecha",
                "CAST(nombre_archivo AS STRING) AS nombre_archivo",
                "CAST(path AS STRING) AS path",
                "CAST(fisico AS BOOLEAN) AS fisico",
                "CAST(periodo AS INT) AS periodo",
            )
        )

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
        typed.writeTo(full_table).using("iceberg").createOrReplace()
        row_count = spark.table(full_table).count()
    finally:
        spark.stop()

    elapsed = time.perf_counter() - started
    bytes_total = directory_size_bytes(iceberg_table_path(args.table))
    rows_per_sec = row_count / elapsed if elapsed else 0
    print(
        "RESULT|engine=iceberg"
        f"|table={full_table}"
        f"|rows={row_count}"
        f"|seconds={elapsed:.2f}"
        f"|rows_per_sec={rows_per_sec:.0f}"
        f"|bytes={bytes_total}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
