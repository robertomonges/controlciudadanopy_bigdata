from __future__ import annotations

import argparse
import json
from pathlib import Path

from djbr_legacy_common import (
    DEFAULT_LEGACY_JSON_DIR,
    DEFAULT_OFFICIAL_JSON_DIR,
    iter_official_json_files,
    normalize_portal_json_to_legacy,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transforma JSON oficiales DJBR en JSON legacy-compatible para benchmark."
    )
    parser.add_argument("--source-dir", default=str(DEFAULT_OFFICIAL_JSON_DIR))
    parser.add_argument("--target-dir", default=str(DEFAULT_LEGACY_JSON_DIR))
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Sobrescribe archivos legacy ya existentes.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    target_dir = Path(args.target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    transformed = 0
    skipped = 0

    for index, source_path in enumerate(iter_official_json_files(source_dir), start=1):
        if args.limit is not None and index > args.limit:
            break

        target_path = target_dir / source_path.name
        if target_path.exists() and not args.force:
            skipped += 1
            continue

        with source_path.open("r", encoding="utf-8") as handle:
            portal_payload = json.load(handle)

        legacy_payload = normalize_portal_json_to_legacy(portal_payload, source_path)
        with target_path.open("w", encoding="utf-8") as handle:
            json.dump(legacy_payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        transformed += 1

        if transformed % 1000 == 0:
            print(f"[transform] transformed={transformed} skipped={skipped} last={target_path.name}")

    print(
        f"[transform] done transformed={transformed} skipped={skipped} "
        f"source_dir={source_dir} target_dir={target_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
