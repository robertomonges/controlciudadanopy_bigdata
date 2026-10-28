from __future__ import annotations

"""
Launcher paralelo para load_djbr_official_postgres.py.

Lanza N workers en paralelo, cada uno procesa 1/N de los archivos JSON.
Mide y reporta el tiempo total de carga.

Uso:
    python launch_djbr_official_postgres.py --workers 8
    python launch_djbr_official_postgres.py --workers 1          # serial (benchmark baseline)
    python launch_djbr_official_postgres.py --workers 4 --limit 100000
"""

import os
import argparse
import subprocess
import sys
import time
from pathlib import Path


WORKER_SCRIPT = Path(__file__).parent / "load_djbr_official_postgres.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lanza N workers en paralelo para cargar JSON DJBR oficial a PostgreSQL."
    )
    parser.add_argument("--workers", type=int, default=8,
                        help="Numero de workers en paralelo (default 8)")
    parser.add_argument("--table", default="benchmark.djbr_official_data")
    parser.add_argument("--source-dir",
                        default=os.path.join(os.environ.get("CC_ROOT", "."), ".local/data/contraloria/json"))
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--limit", type=int, default=None,
                        help="Limitar archivos totales (se divide entre workers)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    return parser.parse_args()


def build_worker_cmd(args: argparse.Namespace, worker_index: int) -> list[str]:
    cmd = [
        sys.executable, str(WORKER_SCRIPT),
        "--worker-index", str(worker_index),
        "--worker-count", str(args.workers),
        "--table", args.table,
        "--source-dir", args.source_dir,
        "--batch-size", str(args.batch_size),
        "--pg-host", args.pg_host,
        "--pg-port", str(args.pg_port),
        "--pg-database", args.pg_database,
        "--pg-user", args.pg_user,
        "--pg-password", args.pg_password,
    ]
    if args.limit is not None:
        cmd += ["--limit", str(args.limit)]
    if args.force:
        cmd.append("--force")
    return cmd


def main() -> int:
    args = parse_args()
    n = args.workers

    print(f"[launcher] mode=parallel workers={n} table={args.table}")
    print(f"[launcher] source={args.source_dir}")
    if args.limit:
        print(f"[launcher] limit={args.limit} (~{args.limit // n} por worker)")
    print(f"[launcher] iniciando {n} worker(s)...")

    started = time.perf_counter()
    procs = []
    for i in range(n):
        cmd = build_worker_cmd(args, i)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs.append((i, p))
        print(f"[launcher] worker {i} pid={p.pid}")

    outputs = [""] * n
    for i, p in procs:
        stdout, _ = p.communicate()
        outputs[i] = stdout.strip()

    elapsed = time.perf_counter() - started

    total_inserted = 0
    total_skipped = 0
    print(f"\n{'='*60}")
    print(f"  RESUMEN workers={n}  elapsed={elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"{'='*60}")
    for i, p in procs:
        last_line = outputs[i].split("\n")[-1] if outputs[i] else "(sin output)"
        rc = p.returncode
        status = "OK" if rc == 0 else f"ERROR rc={rc}"
        print(f"  W{i}: {status}  {last_line}")
        for token in last_line.split():
            if token.startswith("inserted="):
                try:
                    total_inserted += int(token.split("=")[1])
                except ValueError:
                    pass
            if token.startswith("skipped="):
                try:
                    total_skipped += int(token.split("=")[1])
                except ValueError:
                    pass

    rate = total_inserted / elapsed if elapsed > 0 else 0
    print(f"\n  total_inserted={total_inserted} total_skipped={total_skipped}")
    print(f"  throughput={rate:.0f} rows/s  elapsed={elapsed:.1f}s ({elapsed/60:.1f} min)")

    failed = sum(1 for _, p in procs if p.returncode != 0)
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
