from __future__ import annotations

import os
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import _spark_windows

from pyspark.sql import SparkSession


DEFAULT_CSV_DIR = Path(os.environ.get("CC_ROOT", "."), ".local/data/policia/procesados")
DEFAULT_WAREHOUSE = Path(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse")
CATALOG = "local"
NAMESPACE = "controlciudadano"
TABLE = "policia"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mide carga de CSVs Policía reutilizando local.controlciudadano.policia."
    )
    parser.add_argument("--csv-dir", type=Path, default=DEFAULT_CSV_DIR)
    parser.add_argument("--warehouse", type=Path, default=DEFAULT_WAREHOUSE)
    parser.add_argument("--codec", default="zstd")
    parser.add_argument("--parallelism", type=int, default=4)
    return parser.parse_args()


def build_spark(warehouse: Path, codec: str, parallelism: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName("cc-policia-iceberg-benchmark")
        .master(f"local[{parallelism}]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_posix())
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.iceberg.write.parquet.compression-codec", codec)
        .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def table_dir(warehouse: Path) -> Path:
    return warehouse / NAMESPACE.replace(".", "/") / TABLE


def storage_bytes(warehouse: Path) -> tuple[int, int, int]:
    base = table_dir(warehouse)
    data_dir = base / "data"
    meta_dir = base / "metadata"
    data_bytes = sum(p.stat().st_size for p in data_dir.rglob("*.parquet")) if data_dir.exists() else 0
    meta_bytes = sum(p.stat().st_size for p in meta_dir.rglob("*") if p.is_file()) if meta_dir.exists() else 0
    return data_bytes, meta_bytes, data_bytes + meta_bytes


def main() -> int:
    args = parse_args()
    csv_paths = [p.as_posix() for p in sorted(args.csv_dir.glob("*.csv"))]
    if not csv_paths:
        raise SystemExit(f"No se encontraron CSVs en {args.csv_dir}")

    spark = build_spark(args.warehouse, args.codec, args.parallelism)
    spark.sparkContext.setLogLevel("ERROR")
    fq_namespace = f"{CATALOG}.{NAMESPACE}"
    fq_table = f"{fq_namespace}.{TABLE}"

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fq_namespace}")
        started_at = time.perf_counter()
        df = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("sep", ",")
            .option("nullValue", "")
            .csv(csv_paths)
        )
        df = df.toDF(*[c.lower() for c in df.columns])
        row_count = df.count()
        print(f"[Iceberg] policia: {row_count:,} rows read from {len(csv_paths)} CSVs")

        df.writeTo(fq_table).using("iceberg").createOrReplace()
        elapsed = time.perf_counter() - started_at
        data_bytes, meta_bytes, total_bytes = storage_bytes(args.warehouse)
        rps = row_count / elapsed if elapsed else 0
        print(
            "RESULT|engine=iceberg"
            f"|table={fq_table}"
            f"|rows={row_count}"
            f"|seconds={elapsed:.2f}"
            f"|rows_per_sec={rps:.0f}"
            f"|data_bytes={data_bytes}"
            f"|metadata_bytes={meta_bytes}"
            f"|total_bytes={total_bytes}"
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
