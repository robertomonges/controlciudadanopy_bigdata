from __future__ import annotations

import os
import argparse
import csv
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = ROOT / "scripts" / "benchmark" / "results"
PG_LOADER = ROOT / "scripts" / "benchmark" / "setup" / "load_policia_postgres.py"
ICE_LOADER = ROOT / "scripts" / "benchmark" / "setup" / "load_policia_iceberg.py"
PROJECT_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"

RESULT_RE = re.compile(r"^RESULT\|(.+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark de carga Policía: PostgreSQL vs Iceberg.")
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    parser.add_argument("--codec", default="zstd")
    parser.add_argument("--parallelism", type=int, default=4)
    return parser.parse_args()


def run_command(command: list[str]) -> dict[str, str]:
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=True)
    print(result.stdout, end="")
    for line in result.stdout.splitlines():
        match = RESULT_RE.match(line.strip())
        if not match:
            continue
        payload: dict[str, str] = {}
        for item in match.group(1).split("|"):
            key, value = item.split("=", 1)
            payload[key] = value
        return payload
    raise RuntimeError("No se encontró línea RESULT en la salida del loader")


def write_results(rows: list[dict[str, str]]) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RESULTS_DIR / f"e4_policia_load_{datetime.now().strftime('%Y-%m-%d')}.csv"
    fieldnames = [
        "engine",
        "table",
        "rows",
        "seconds",
        "rows_per_sec",
        "heap_bytes",
        "index_bytes",
        "data_bytes",
        "metadata_bytes",
        "total_bytes",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def main() -> int:
    args = parse_args()
    rows: list[dict[str, str]] = []

    rows.append(
        run_command(
            [
                sys.executable,
                str(PG_LOADER),
                "--pg-host", args.pg_host,
                "--pg-port", str(args.pg_port),
                "--pg-database", args.pg_database,
                "--pg-user", args.pg_user,
                "--pg-password", args.pg_password,
            ]
        )
    )
    rows.append(
        run_command(
            [
                str(PROJECT_PYTHON if PROJECT_PYTHON.exists() else sys.executable),
                str(ICE_LOADER),
                "--codec", args.codec,
                "--parallelism", str(args.parallelism),
            ]
        )
    )

    output_path = write_results(rows)
    print(f"RESULTS_CSV|{output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
