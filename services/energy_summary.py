"""
Energy summary helpers for inverter history and persisted telemetry samples.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional


def _to_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _first_number(data: Dict[str, Any], candidates: List[str]) -> float:
    for key in candidates:
        if key in data:
            return _to_float(data.get(key))
    return 0.0


def _iso_day_key(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def _iso_month_key(value: datetime) -> str:
    return value.strftime("%Y-%m")


def _zero_bucket(period: str) -> Dict[str, Any]:
    return {
        "period": period,
        "loadKwh": 0.0,
        "solarPvKwh": 0.0,
        "batteryChargedKwh": 0.0,
        "batteryDischargedKwh": 0.0,
        "gridUsedKwh": 0.0,
        "gridExportedKwh": 0.0,
    }


def _round_energy(value: float) -> float:
    return round(value + 1e-12, 3)


def _create_recent_day_keys(days: int) -> List[str]:
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return [_iso_day_key(today - timedelta(days=offset)) for offset in range(days - 1, -1, -1)]


def _create_recent_month_keys(months: int) -> List[str]:
    now = datetime.now()
    keys: List[str] = []
    year = now.year
    month = now.month
    for offset in range(months - 1, -1, -1):
        target_month = month - offset
        target_year = year
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        keys.append(f"{target_year:04d}-{target_month:02d}")
    return keys


def _finalize_buckets(
    order: Iterable[str], source: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    buckets: List[Dict[str, Any]] = []
    for period in order:
        bucket = dict(source.get(period) or _zero_bucket(period))
        bucket["loadKwh"] = _round_energy(bucket["loadKwh"])
        bucket["solarPvKwh"] = _round_energy(bucket["solarPvKwh"])
        bucket["batteryChargedKwh"] = _round_energy(bucket["batteryChargedKwh"])
        bucket["batteryDischargedKwh"] = _round_energy(bucket["batteryDischargedKwh"])
        bucket["gridUsedKwh"] = _round_energy(bucket["gridUsedKwh"])
        bucket["gridExportedKwh"] = _round_energy(bucket["gridExportedKwh"])
        buckets.append(bucket)
    return buckets


def _normalize_payload(raw_payload: Any) -> Dict[str, Any]:
    if isinstance(raw_payload, dict):
        return raw_payload
    if isinstance(raw_payload, str):
        try:
            parsed = json.loads(raw_payload)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def _build_point(sample: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    timestamp = sample.get("reading_at")
    if not isinstance(timestamp, datetime):
        return None

    payload = _normalize_payload(sample.get("raw_payload"))
    load_w = _to_float(sample.get("load_power_w"))
    solar_w = _to_float(sample.get("pv_power_w"))
    grid_used_w = _to_float(sample.get("grid_power_w"))

    battery_voltage = _first_number(payload, ["Battery Voltage", "battery_voltage"])
    battery_charge_current = _first_number(
        payload, ["Battery Charging Current", "battery_charging_current"]
    )
    battery_discharge_current = _first_number(
        payload, ["Battery Discharge Current", "battery_discharge_current"]
    )

    return {
        "timestamp": timestamp,
        "load_w": load_w,
        "solar_w": solar_w,
        "grid_used_w": max(grid_used_w, 0.0),
        "battery_charged_w": max(battery_voltage * battery_charge_current, 0.0),
        "battery_discharged_w": max(battery_voltage * battery_discharge_current, 0.0),
    }


def _add_interval_energy(
    bucket: Dict[str, Any], previous: Dict[str, Any], current: Dict[str, Any], dt_hours: float
) -> None:
    def to_kwh(prev_w: float, curr_w: float) -> float:
        return ((prev_w + curr_w) / 2.0) * dt_hours / 1000.0

    bucket["loadKwh"] += to_kwh(previous["load_w"], current["load_w"])
    bucket["solarPvKwh"] += to_kwh(previous["solar_w"], current["solar_w"])
    bucket["batteryChargedKwh"] += to_kwh(
        previous["battery_charged_w"], current["battery_charged_w"]
    )
    bucket["batteryDischargedKwh"] += to_kwh(
        previous["battery_discharged_w"], current["battery_discharged_w"]
    )
    bucket["gridUsedKwh"] += to_kwh(
        previous["grid_used_w"], current["grid_used_w"]
    )


def build_energy_summary(
    inverter_id: str, samples: Iterable[Dict[str, Any]]
) -> Dict[str, Any]:
    points = [
        point
        for point in (
            _build_point(sample) for sample in samples
        )
        if point is not None
    ]
    points.sort(key=lambda item: item["timestamp"])

    day_buckets: Dict[str, Dict[str, Any]] = {}
    month_buckets: Dict[str, Dict[str, Any]] = {}

    for index in range(1, len(points)):
        previous = points[index - 1]
        current = points[index]
        dt_hours = (
            current["timestamp"].timestamp() - previous["timestamp"].timestamp()
        ) / 3600.0
        if dt_hours <= 0:
            continue

        day_key = _iso_day_key(current["timestamp"])
        month_key = _iso_month_key(current["timestamp"])
        day_bucket = day_buckets.setdefault(day_key, _zero_bucket(day_key))
        month_bucket = month_buckets.setdefault(month_key, _zero_bucket(month_key))

        _add_interval_energy(day_bucket, previous, current, dt_hours)
        _add_interval_energy(month_bucket, previous, current, dt_hours)

    return {
        "inverterId": inverter_id,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "daily30d": _finalize_buckets(_create_recent_day_keys(30), day_buckets),
        "monthly12m": _finalize_buckets(_create_recent_month_keys(12), month_buckets),
    }
