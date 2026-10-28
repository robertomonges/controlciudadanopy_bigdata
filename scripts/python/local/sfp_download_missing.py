from __future__ import annotations

import os
import argparse
import time
from pathlib import Path

import requests
import urllib3
urllib3.disable_warnings()

BASE_URL = "https://datos.sfp.gov.py/data"
CACHE_DIR = Path(os.environ.get("CC_ROOT", "."), ".local/data/sfp")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2015-01")
    p.add_argument("--end", default="2025-02")
    return p.parse_args()


def zip_url(year: int, month: int) -> str:
    return f"{BASE_URL}/funcionarios_{year}_{month}.csv.zip"


def zip_path(year: int, month: int) -> Path:
    return CACHE_DIR / f"sfp_{year}_{month:02d}.zip"


def download(year: int, month: int) -> tuple[bool, float, int]:
    url = zip_url(year, month)
    dest = zip_path(year, month)
    t0 = time.perf_counter()
    try:
        r = requests.get(url, verify=False, timeout=120, stream=True)
        r.raise_for_status()
        data = r.content
        if len(data) < 10_000:
            return False, 0, 0
        dest.write_bytes(data)
        return True, time.perf_counter() - t0, len(data)
    except Exception as e:
        print(f"  ERROR {year}-{month:02d}: {e}")
        return False, 0, 0


def main():
    args = parse_args()
    sy, sm = map(int, args.start.split("-"))
    ey, em = map(int, args.end.split("-"))

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    missing = [(y, m) for y, m in months if not zip_path(y, m).exists()]
    already = len(months) - len(missing)

    print(f"Rango: {args.start} a {args.end} ({len(months)} meses)")
    print(f"Ya disponibles: {already} | A descargar: {len(missing)}\n")

    total_bytes = 0
    downloaded = 0
    errors = 0

    for i, (y, m) in enumerate(missing, 1):
        print(f"[{i}/{len(missing)}] {y}-{m:02d} ... ", end="", flush=True)
        ok, elapsed, size = download(y, m)
        if ok:
            downloaded += 1
            total_bytes += size
            print(f"OK {size/1e6:.1f} MB en {elapsed:.1f}s")
        else:
            errors += 1
            print("FAIL")

    print(f"\n=== RESUMEN ===")
    print(f"Descargados: {downloaded}/{len(missing)}")
    print(f"Errores:     {errors}")
    print(f"Total:       {total_bytes/1e9:.2f} GB")
    print(f"Directorio:  {CACHE_DIR}")


if __name__ == "__main__":
    main()
