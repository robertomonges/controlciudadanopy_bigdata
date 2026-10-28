from __future__ import annotations

"""
Launcher paralelo para carga de JSON oficial DJBR a Apache Iceberg.

Fase 1 (paralela): N workers generan chunks JSONL en paralelo.
Fase 2 (serial):   Un solo proceso Spark lee todos los chunks y escribe Iceberg.

Uso:
    python launch_djbr_official_iceberg.py --workers 8
    python launch_djbr_official_iceberg.py --workers 1          # serial (benchmark baseline)
    python launch_djbr_official_iceberg.py --workers 4 --limit 100000
"""

import os
import argparse
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


WORKER_SCRIPT = Path(__file__).parent / "load_djbr_official_iceberg.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Launcher paralelo para carga DJBR oficial a Iceberg."
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--table", default="controlciudadano.djbr_official_data")
    parser.add_argument("--source-dir",
                        default=os.path.join(os.environ.get("CC_ROOT", "."), ".local/data/contraloria/json"))
    parser.add_argument("--warehouse",
                        default=os.path.join(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse"))
    parser.add_argument("--parallelism", type=int, default=4,
                        help="Paralelismo de Spark para la fase de escritura")
    parser.add_argument("--codec", default="zstd")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--jsonl-dir", default=None,
                        help="Directorio para chunks JSONL (default: temp dir autogenerado)")
    parser.add_argument("--keep-jsonl", action="store_true",
                        help="No borrar el directorio JSONL al finalizar")
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-database", default="controlciudadano")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default=os.environ.get("PGPASSWORD"))
    return parser.parse_args()


def build_prepare_cmd(args: argparse.Namespace, worker_index: int, jsonl_dir: Path) -> list[str]:
    cmd = [
        sys.executable, str(WORKER_SCRIPT),
        "--mode", "prepare",
        "--worker-index", str(worker_index),
        "--worker-count", str(args.workers),
        "--source-dir", args.source_dir,
        "--jsonl-dir", str(jsonl_dir),
        "--pg-host", args.pg_host,
        "--pg-port", str(args.pg_port),
        "--pg-database", args.pg_database,
        "--pg-user", args.pg_user,
        "--pg-password", args.pg_password,
    ]
    if args.limit is not None:
        cmd += ["--limit", str(args.limit)]
    return cmd


def build_write_cmd(args: argparse.Namespace, jsonl_dir: Path) -> list[str]:
    return [
        sys.executable, str(WORKER_SCRIPT),
        "--mode", "write",
        "--jsonl-dir", str(jsonl_dir),
        "--table", args.table,
        "--warehouse", args.warehouse,
        "--parallelism", str(args.parallelism),
        "--codec", args.codec,
        "--pg-host", args.pg_host,
        "--pg-port", str(args.pg_port),
        "--pg-database", args.pg_database,
        "--pg-user", args.pg_user,
        "--pg-password", args.pg_password,
    ]


def main() -> int:
    args = parse_args()
    n = args.workers

    own_tmpdir = args.jsonl_dir is None
    jsonl_dir = Path(args.jsonl_dir) if args.jsonl_dir else Path(tempfile.mkdtemp(prefix="djbr_chunks_"))
    jsonl_dir.mkdir(parents=True, exist_ok=True)

    print(f"[launcher] mode=parallel workers={n} table={args.table}")
    print(f"[launcher] source={args.source_dir}")
    print(f"[launcher] jsonl_dir={jsonl_dir}")
    if args.limit:
        print(f"[launcher] limit={args.limit} (~{args.limit // n} por worker)")

    print(f"\n[launcher] FASE 1: generando {n} chunks JSONL en paralelo...")
    t_prepare = time.perf_counter()

    procs = []
    for i in range(n):
        cmd = build_prepare_cmd(args, i, jsonl_dir)
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        procs.append((i, p))
        print(f"[launcher] worker {i} pid={p.pid}")

    outputs = [""] * n
    total_prepared = 0
    for i, p in procs:
        stdout, _ = p.communicate()
        outputs[i] = stdout.strip()
        last = outputs[i].split("\n")[-1] if outputs[i] else ""
        for token in last.split():
            if token.startswith("prepared="):
                try:
                    total_prepared += int(token.split("=")[1])
                except ValueError:
                    pass

    t_prepare_elapsed = time.perf_counter() - t_prepare
    print(f"\n[launcher] FASE 1 completada: total_prepared={total_prepared} "
          f"elapsed={t_prepare_elapsed:.1f}s ({t_prepare_elapsed/60:.1f} min)")

    failed_prepare = sum(1 for _, p in procs if p.returncode != 0)
    if failed_prepare > 0:
        print(f"[launcher] ERROR: {failed_prepare} worker(s) fallaron en FASE 1")
        for i, p in procs:
            if p.returncode != 0:
                print(f"  W{i}: rc={p.returncode}")
                print(f"  {outputs[i][-300:]}")
        return 1

    if total_prepared == 0:
        print("[launcher] No hay filas para escribir. Abortando.")
        return 0

    chunks = sorted(jsonl_dir.glob("chunk_*.jsonl"))
    total_size_mb = sum(c.stat().st_size for c in chunks) / 1024 / 1024
    print(f"[launcher] {len(chunks)} chunk(s) generados, {total_size_mb:.1f} MB total")

    print(f"\n[launcher] FASE 2: escribiendo a Iceberg con Spark (parallelism={args.parallelism})...")
    t_write = time.perf_counter()

    write_cmd = build_write_cmd(args, jsonl_dir)
    result = subprocess.run(write_cmd, capture_output=False, text=True)

    t_write_elapsed = time.perf_counter() - t_write
    print(f"[launcher] FASE 2 completada: elapsed={t_write_elapsed:.1f}s ({t_write_elapsed/60:.1f} min)")

    if result.returncode != 0:
        print(f"[launcher] ERROR en FASE 2: rc={result.returncode}")
        return 1

    t_total = t_prepare_elapsed + t_write_elapsed
    print(f"\n{'='*60}")
    print(f"  RESUMEN ICEBERG  workers={n}  codec={args.codec}")
    print(f"{'='*60}")
    print(f"  Fase 1 (JSONL): {t_prepare_elapsed:.1f}s ({t_prepare_elapsed/60:.1f} min)")
    print(f"  Fase 2 (Spark): {t_write_elapsed:.1f}s ({t_write_elapsed/60:.1f} min)")
    print(f"  TOTAL          : {t_total:.1f}s ({t_total/60:.1f} min)")
    print(f"  Filas preparadas: {total_prepared}")
    rate = total_prepared / t_total if t_total > 0 else 0
    print(f"  Throughput total: {rate:.0f} rows/s")

    if own_tmpdir and not args.keep_jsonl:
        shutil.rmtree(jsonl_dir, ignore_errors=True)
        print(f"  JSONL dir eliminado: {jsonl_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
