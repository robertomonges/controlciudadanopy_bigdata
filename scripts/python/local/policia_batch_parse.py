from __future__ import annotations

import os
import argparse
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DOWNLOADS_DIR = Path(os.environ.get("CC_ROOT", "."), ".local/data/policia/downloads")
OUTPUT_DIR    = Path(os.environ.get("CC_ROOT", "."), ".local/data/policia/procesados")
PARSER_SCRIPT = Path(os.environ.get("CC_ROOT", "."), "scripts/python/local/policia_pdf_parser.py")

MONTH_MAP = {
    "ENERO": "01", "FEBRERO": "02", "MARZO": "03", "ABRIL": "04",
    "MAYO": "05", "JUNIO": "06", "JULIO": "07", "AGOSTO": "08",
    "SETIEMBRE": "09", "OCTUBRE": "10", "NOVIEMBRE": "11", "DICIEMBRE": "12",
}


def pdf_to_output_name(pdf: Path) -> str | None:
    parts = pdf.stem.upper().split("_")
    for i, part in enumerate(parts):
        if part in MONTH_MAP and i + 1 < len(parts) and parts[i + 1].isdigit():
            return f"{parts[i + 1]}-{MONTH_MAP[part]}.csv"
    return None


def process_pdf(pdf: Path, out_path: Path, workers: int) -> tuple[bool, float, int, str]:
    t0 = time.perf_counter()
    result = subprocess.run(
        [sys.executable, str(PARSER_SCRIPT), str(pdf), "-o", str(out_path),
         "--allow-empty-page", "--workers", str(workers)],
        capture_output=True, text=True
    )
    elapsed = time.perf_counter() - t0
    if result.returncode == 0 and out_path.exists():
        rows = sum(1 for _ in out_path.open(encoding='utf-8')) - 1
        return True, elapsed, rows, ""
    return False, elapsed, 0, result.stderr[-300:].strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--parallel", type=int, default=2,
                        help="PDFs en paralelo (default: 2)")
    parser.add_argument("--workers-per-pdf", type=int, default=8,
                        help="Workers pdfplumber por PDF (default: 8)")
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(DOWNLOADS_DIR.rglob("*.pdf"))
    if not pdfs:
        print(f"No se encontraron PDFs en {DOWNLOADS_DIR}")
        return

    print(f"PDFs encontrados: {len(pdfs)}")
    print(f"Paralelo: {args.parallel} PDFs × {args.workers_per_pdf} workers = "
          f"{args.parallel * args.workers_per_pdf} workers totales")
    print(f"Salida en: {OUTPUT_DIR}\n")

    pending = []
    for i, pdf in enumerate(pdfs, 1):
        out_name = pdf_to_output_name(pdf)
        if out_name is None:
            print(f"[{i}/{len(pdfs)}] SKIP (nombre no reconocido): {pdf.name}")
            continue
        out_path = OUTPUT_DIR / out_name
        if args.skip_existing and out_path.exists():
            rows = sum(1 for _ in out_path.open(encoding='utf-8')) - 1
            print(f"[{i}/{len(pdfs)}] EXISTE {out_name} ({rows:,} filas) — saltando")
        else:
            pending.append((i, pdf, out_path))

    print(f"\nPendientes: {len(pending)}\n")

    if args.dry_run or not pending:
        return

    ok = errors = 0
    t_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        futures = {
            executor.submit(process_pdf, pdf, out_path, args.workers_per_pdf): (i, pdf, out_path)
            for i, pdf, out_path in pending
        }
        for future in as_completed(futures):
            i, pdf, out_path = futures[future]
            success, elapsed, rows, stderr = future.result()
            if success:
                print(f"[{i}/{len(pdfs)}] OK {pdf.name} -> {out_path.name} | {rows:,} filas en {elapsed:.1f}s", flush=True)
                ok += 1
            else:
                print(f"[{i}/{len(pdfs)}] ERROR {pdf.name} ({elapsed:.1f}s)", flush=True)
                if stderr:
                    print(f"  {stderr[:200]}", flush=True)
                errors += 1

    total_time = time.perf_counter() - t_start
    print(f"\n=== RESUMEN ===")
    print(f"OK:      {ok}")
    print(f"Errores: {errors}")
    print(f"Tiempo:  {total_time:.1f}s ({total_time/60:.1f} min)")
    print(f"Salida:  {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
