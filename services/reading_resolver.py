from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from utils.telemetry_time import parse_watchpower_timestamp


def _resolve_cache_entry(
    cache_entry: Optional[Dict[str, Any]], timezone_name: str
) -> Optional[Dict[str, Any]]:
    if not cache_entry:
        return None

    raw_data = cache_entry.get("data")
    if not isinstance(raw_data, dict):
        return None

    reading_at = cache_entry.get("reading_at")
    if not isinstance(reading_at, datetime):
        reading_at = parse_watchpower_timestamp(raw_data.get("Data E Hora"), timezone_name)

    return {
        "data": raw_data,
        "reading_at": reading_at,
        "cached_at": cache_entry.get("timestamp"),
        "polled_at": cache_entry.get("polled_at"),
        "inverter_config": cache_entry.get("inverter_config"),
        "data_source": "cache",
    }


def resolve_latest_reading(
    serial_number: str,
    timezone_name: str,
    neon_store=None,
    csv_writer=None,
    cache_entry: Optional[Dict[str, Any]] = None,
    prefer_cache: bool = False,
) -> Optional[Dict[str, Any]]:
    if prefer_cache:
        cached = _resolve_cache_entry(cache_entry=cache_entry, timezone_name=timezone_name)
        if cached:
            return cached

    if neon_store and getattr(neon_store, "enabled", False):
        latest = neon_store.fetch_latest_reading(serial_number)
        if latest:
            polled_at = latest.get("polled_at")
            return {
                "data": latest["data"],
                "reading_at": latest.get("reading_at"),
                "cached_at": polled_at.isoformat() if polled_at else None,
                "polled_at": polled_at,
                "inverter_config": None,
                "data_source": "neon",
            }

    if csv_writer:
        latest = csv_writer.read_freshest(
            serial_number=serial_number,
            timezone_name=timezone_name,
        )
        if latest:
            reading_at = latest.get("reading_at")
            return {
                "data": latest["data"],
                "reading_at": reading_at,
                "cached_at": reading_at.isoformat() if reading_at else None,
                "polled_at": None,
                "inverter_config": None,
                "data_source": "csv",
            }

    return _resolve_cache_entry(cache_entry=cache_entry, timezone_name=timezone_name)
