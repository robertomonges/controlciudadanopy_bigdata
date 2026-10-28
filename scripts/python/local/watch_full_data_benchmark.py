from __future__ import annotations

import os
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2


ROOT = Path(os.environ.get("CC_ROOT", "."))
LOG_JSON = ROOT / ".local" / "data" / "full_data_benchmark_timing.json"
ICEBERG_SCRIPT = ROOT / "scripts" / "benchmark" / "run_full_data_iceberg.py"
VENV_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
PG = dict(host="localhost", port=5432, dbname="controlciudadano", user="postgres", password=os.environ.get("PGPASSWORD", ""))


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def query_active_rebuild():
    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pid, query_start
                FROM pg_stat_activity
                WHERE query ILIKE 'CREATE TABLE analysis.full_data AS%%'
                  AND state = 'active'
                ORDER BY query_start
                """
            )
            return cur.fetchall()


def read_full_data_state() -> dict[str, int]:
    with psycopg2.connect(**PG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM analysis.full_data")
            rows = int(cur.fetchone()[0])
            cur.execute("SELECT pg_total_relation_size('analysis.full_data')")
            bytes_used = int(cur.fetchone()[0])
            return {"rows": rows, "bytes": bytes_used}


def run_iceberg() -> dict[str, str | int | float]:
    cmd = [str(VENV_PYTHON), str(ICEBERG_SCRIPT)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            "Iceberg full_data failed\nSTDOUT:\n"
            + result.stdout[-4000:]
            + "\nSTDERR:\n"
            + result.stderr[-4000:]
        )

    parsed: dict[str, str | int | float] = {}
    for line in result.stdout.splitlines():
        if "|" not in line:
            continue
        key, value = line.split("|", 1)
        key = key.strip()
        value = value.strip()
        if key in {"rows", "bytes"}:
            parsed[key] = int(float(value))
        elif key in {"export_elapsed_s", "spark_elapsed_s", "total_elapsed_s", "rows_per_second"}:
            parsed[key] = float(value)
        else:
            parsed[key] = value
    parsed["stdout"] = result.stdout[-4000:]
    return parsed


def main() -> int:
    print(f"[full_data_watch] iniciado {now_iso()}")
    rebuild_rows = query_active_rebuild()
    if not rebuild_rows:
        print("[full_data_watch] no hay CREATE TABLE analysis.full_data activo; abortando")
        return 1

    start_ts = rebuild_rows[0][1]
    start_epoch = start_ts.timestamp()
    print(f"[full_data_watch] rebuild activo desde {start_ts.isoformat()}")

    while True:
        active = query_active_rebuild()
        if not active:
            break
        print(f"[full_data_watch] rebuild sigue activo; workers={len(active)}")
        time.sleep(15)

    end_dt = datetime.now().astimezone()
    pg_elapsed = end_dt.timestamp() - start_epoch
    pg_state = read_full_data_state()
    pg_rows_per_second = pg_state["rows"] / pg_elapsed if pg_elapsed > 0 else 0.0

    print(
        "[full_data_watch] rebuild PG terminado "
        f"rows={pg_state['rows']} bytes={pg_state['bytes']} elapsed_s={pg_elapsed:.2f}"
    )

    iceberg_state = run_iceberg()
    print(
        "[full_data_watch] iceberg terminado "
        f"rows={iceberg_state.get('rows')} bytes={iceberg_state.get('bytes')} "
        f"spark_elapsed_s={iceberg_state.get('spark_elapsed_s')}"
    )

    payload = {
        "timestamp": now_iso(),
        "postgres": {
            "started_at": start_ts.isoformat(),
            "finished_at": end_dt.isoformat(),
            "elapsed_s": round(pg_elapsed, 2),
            "rows": pg_state["rows"],
            "rows_per_second": round(pg_rows_per_second, 2),
            "bytes": pg_state["bytes"],
        },
        "iceberg": iceberg_state,
    }

    LOG_JSON.parent.mkdir(parents=True, exist_ok=True)
    LOG_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[full_data_watch] resultados guardados en {LOG_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
