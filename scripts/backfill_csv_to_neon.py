"""
Backfill inverter CSV history into Neon.
"""

import csv
import glob
import json
import logging
import os
import sys
from typing import Any, Dict

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from services.neon_store import NeonStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_inverters(config_path: str) -> Dict[str, Dict[str, Any]]:
    with open(config_path, "r", encoding="utf-8") as fp:
        rows = json.load(fp)
    return {str(row.get("serial_number")): row for row in rows}


def main() -> int:
    load_dotenv()

    database_url = os.getenv("NEON_DATABASE_URL")
    timezone_name = os.getenv("WATCHPOWER_TIMEZONE", "Europe/Istanbul")
    if not database_url:
        logger.error("NEON_DATABASE_URL is required")
        return 1

    config_path = os.path.join(BASE_DIR, "config", "inverters.json")
    data_dir = os.path.join(BASE_DIR, "data")
    inverter_map = load_inverters(config_path)

    store = NeonStore(database_url=database_url, timezone_name=timezone_name)

    csv_paths = sorted(glob.glob(os.path.join(data_dir, "inverter_*.csv")))
    if not csv_paths:
        logger.info("No CSV files found in %s", data_dir)
        return 0

    total_inserted = 0
    total_skipped = 0
    total_errors = 0

    for csv_path in csv_paths:
        file_name = os.path.basename(csv_path)
        serial_from_name = file_name.removeprefix("inverter_").removesuffix(".csv")
        inverter_config = inverter_map.get(serial_from_name, {"serial_number": serial_from_name})

        try:
            store.upsert_inverter(inverter_config)
        except Exception as exc:
            logger.error("Failed to upsert inverter metadata for %s: %s", serial_from_name, exc)
            total_errors += 1
            continue

        inserted = 0
        skipped = 0
        errors = 0

        with open(csv_path, "r", encoding="utf-8") as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                serial = str(row.get("serial_number") or serial_from_name)
                try:
                    was_inserted = store.persist_reading(
                        serial_number=serial,
                        raw_data=row,
                        source="csv_backfill",
                    )
                    if was_inserted:
                        inserted += 1
                    else:
                        skipped += 1
                except Exception as exc:
                    logger.error("Failed to import row for %s from %s: %s", serial, file_name, exc)
                    errors += 1

        total_inserted += inserted
        total_skipped += skipped
        total_errors += errors
        logger.info(
            "Backfill %s complete: inserted=%s skipped=%s errors=%s",
            file_name,
            inserted,
            skipped,
            errors,
        )

    logger.info(
        "Backfill complete: inserted=%s skipped=%s errors=%s",
        total_inserted,
        total_skipped,
        total_errors,
    )
    return 0 if total_errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
