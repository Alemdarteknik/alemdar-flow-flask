"""
Neon persistence service for inverter metadata and readings.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


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


class NeonStore:
    """Encapsulates all DB operations used by the Flask backend."""

    def __init__(self, database_url: Optional[str], timezone_name: str) -> None:
        self.database_url = database_url
        self.enabled = bool(database_url)
        self.timezone_name = timezone_name or "Europe/Istanbul"
        self._tz = ZoneInfo(self.timezone_name)

    def _connect(self):
        if not self.database_url:
            raise RuntimeError("NEON_DATABASE_URL is not configured")
        return psycopg.connect(self.database_url, row_factory=dict_row)

    def _parse_reading_at(self, raw_data: Dict[str, Any]) -> datetime:
        raw_timestamp = raw_data.get("Data E Hora")
        if isinstance(raw_timestamp, str) and raw_timestamp.strip():
            text = raw_timestamp.strip()
            try:
                parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
                return parsed.replace(tzinfo=self._tz).astimezone(timezone.utc)
            except ValueError:
                try:
                    parsed = datetime.fromisoformat(text)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=self._tz)
                    return parsed.astimezone(timezone.utc)
                except ValueError:
                    logger.warning("Failed to parse timestamp '%s', using now()", text)
        return datetime.now(timezone.utc)

    def _source_hash(self, serial_number: str, reading_at: datetime, raw_data: Dict[str, Any]) -> str:
        canonical = {
            "serial_number": serial_number,
            "reading_at": reading_at.isoformat(),
            "raw_payload": raw_data,
        }
        encoded = json.dumps(canonical, sort_keys=True, ensure_ascii=True, default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _derive_metrics(self, raw_data: Dict[str, Any]) -> Dict[str, float]:
        pv1 = _first_number(raw_data, ["PV1 Charging Power", "PV1 Charging power"])
        pv2 = _first_number(raw_data, ["PV2 Charging Power", "PV2 Charging power"])
        load = _first_number(raw_data, ["AC Output Active Power"])

        battery_voltage = _first_number(raw_data, ["Battery Voltage"])
        battery_charge_current = _first_number(raw_data, ["Battery Charging Current"])
        battery_discharge_current = _first_number(raw_data, ["Battery Discharge Current"])
        battery_power = battery_voltage * (
            battery_charge_current + battery_discharge_current
        )

        pv_power = pv1 + pv2
        grid_power = max(load - pv_power - battery_power, 0.0)

        return {
            "pv_power_w": pv_power,
            "load_power_w": load,
            "grid_power_w": grid_power,
            "grid_voltage_v": _first_number(raw_data, ["Grid Voltage"]),
            "grid_frequency_hz": _first_number(raw_data, ["Grid Frequency"]),
        }

    def upsert_inverter(self, inverter_config: Dict[str, Any]) -> None:
        if not self.enabled:
            return

        serial_number = str(inverter_config.get("serial_number") or "").strip()
        if not serial_number:
            return

        sql = """
            INSERT INTO public.inverters (
                serial_number,
                alias,
                description,
                system_type,
                watchpower_username,
                wifi_pn,
                device_code,
                device_address
            )
            VALUES (
                %(serial_number)s,
                %(alias)s,
                %(description)s,
                %(system_type)s,
                %(watchpower_username)s,
                %(wifi_pn)s,
                %(device_code)s,
                %(device_address)s
            )
            ON CONFLICT (serial_number)
            DO UPDATE SET
                alias = EXCLUDED.alias,
                description = EXCLUDED.description,
                system_type = EXCLUDED.system_type,
                watchpower_username = EXCLUDED.watchpower_username,
                wifi_pn = EXCLUDED.wifi_pn,
                device_code = EXCLUDED.device_code,
                device_address = EXCLUDED.device_address,
                updated_at = now()
        """

        params = {
            "serial_number": serial_number,
            "alias": inverter_config.get("alias"),
            "description": inverter_config.get("description"),
            "system_type": inverter_config.get("system_type"),
            "watchpower_username": inverter_config.get("username"),
            "wifi_pn": inverter_config.get("wifi_pn"),
            "device_code": inverter_config.get("device_code"),
            "device_address": inverter_config.get("device_address"),
        }

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()

    def ensure_poll_audit_table(self) -> None:
        """Create scheduler poll audit table if it doesn't exist."""
        if not self.enabled:
            return

        sql = """
            CREATE TABLE IF NOT EXISTS public.inverter_poll_audit (
                id BIGSERIAL PRIMARY KEY,
                serial_number TEXT NOT NULL,
                alias TEXT,
                polled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                error_text TEXT,
                source TEXT NOT NULL DEFAULT 'scheduler'
            );

            CREATE INDEX IF NOT EXISTS inverter_poll_audit_serial_polled_idx
                ON public.inverter_poll_audit (serial_number, polled_at DESC);

            CREATE INDEX IF NOT EXISTS inverter_poll_audit_status_polled_idx
                ON public.inverter_poll_audit (status, polled_at DESC);
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

    def persist_reading(
        self,
        serial_number: str,
        raw_data: Dict[str, Any],
        source: str,
    ) -> bool:
        if not self.enabled:
            return False

        reading_at = self._parse_reading_at(raw_data)
        metrics = self._derive_metrics(raw_data)
        row_hash = self._source_hash(serial_number, reading_at, raw_data)

        sql = """
            INSERT INTO public.inverter_readings (
                serial_number,
                reading_at,
                pv_power_w,
                load_power_w,
                grid_power_w,
                grid_voltage_v,
                grid_frequency_hz,
                raw_payload,
                source,
                source_row_hash
            )
            VALUES (
                %(serial_number)s,
                %(reading_at)s,
                %(pv_power_w)s,
                %(load_power_w)s,
                %(grid_power_w)s,
                %(grid_voltage_v)s,
                %(grid_frequency_hz)s,
                %(raw_payload)s,
                %(source)s,
                %(source_row_hash)s
            )
            ON CONFLICT (serial_number, reading_at, source_row_hash) DO NOTHING
        """

        params = {
            "serial_number": serial_number,
            "reading_at": reading_at,
            "pv_power_w": metrics["pv_power_w"],
            "load_power_w": metrics["load_power_w"],
            "grid_power_w": metrics["grid_power_w"],
            "grid_voltage_v": metrics["grid_voltage_v"],
            "grid_frequency_hz": metrics["grid_frequency_hz"],
            "raw_payload": json.dumps(raw_data, ensure_ascii=True, default=str),
            "source": source,
            "source_row_hash": row_hash,
        }

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                inserted = cur.rowcount > 0
            conn.commit()
        return inserted

    def record_poll_outcome(
        self,
        serial_number: str,
        alias: Optional[str],
        status: str,
        attempts: int,
        error_text: Optional[str] = None,
    ) -> None:
        if not self.enabled:
            return

        sql = """
            INSERT INTO public.inverter_poll_audit (
                serial_number,
                alias,
                status,
                attempts,
                error_text,
                source
            )
            VALUES (%s, %s, %s, %s, %s, 'scheduler')
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        serial_number,
                        alias,
                        status,
                        attempts,
                        error_text,
                    ),
                )
            conn.commit()

    def fetch_history(self, serial_number: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        with self._connect() as conn:
            with conn.cursor() as cur:
                if limit and limit > 0:
                    cur.execute(
                        """
                        SELECT raw_payload, reading_at, polled_at
                        FROM (
                            SELECT raw_payload, reading_at, polled_at
                            FROM public.inverter_readings
                            WHERE serial_number = %s
                            ORDER BY reading_at DESC
                            LIMIT %s
                        ) t
                        ORDER BY reading_at ASC
                        """,
                        (serial_number, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT raw_payload, reading_at, polled_at
                        FROM public.inverter_readings
                        WHERE serial_number = %s
                        ORDER BY reading_at ASC
                        """,
                        (serial_number,),
                    )

                rows = cur.fetchall()

        history: List[Dict[str, Any]] = []
        for row in rows:
            payload = row.get("raw_payload")
            reading_at = row.get("reading_at")
            polled_at = row.get("polled_at")
            if isinstance(payload, dict):
                normalized = dict(payload)
                if isinstance(reading_at, datetime):
                    normalized["reading_at"] = reading_at.isoformat()
                    normalized["timestamp"] = reading_at.isoformat()
                    normalized["time"] = reading_at.isoformat()
                if isinstance(polled_at, datetime):
                    normalized["polled_at"] = polled_at.isoformat()
                history.append(normalized)
            elif isinstance(payload, str):
                try:
                    parsed = json.loads(payload)
                    if isinstance(parsed, dict):
                        if isinstance(reading_at, datetime):
                            parsed["reading_at"] = reading_at.isoformat()
                            parsed["timestamp"] = reading_at.isoformat()
                            parsed["time"] = reading_at.isoformat()
                        if isinstance(polled_at, datetime):
                            parsed["polled_at"] = polled_at.isoformat()
                        history.append(parsed)
                except json.JSONDecodeError:
                    continue
        return history
