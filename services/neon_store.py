"""
Neon persistence service for inverter metadata and readings.
"""

import hashlib
import json
import logging
import os
import threading
from contextlib import contextmanager, nullcontext
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - fallback for environments without pool package
    ConnectionPool = None

from utils.telemetry_time import parse_watchpower_timestamp

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
        self._pool = None
        self._pool_lock = threading.Lock()

        # Per-process pool sizing. With multiple Gunicorn workers each worker
        # gets its own pool, so total connections = workers * max_size. Keep
        # this small to stay under the Neon plan limit. Override via env.
        try:
            self._pool_min_size = max(0, int(os.getenv("NEON_POOL_MIN_SIZE", "0")))
        except ValueError:
            self._pool_min_size = 0
        try:
            self._pool_max_size = max(1, int(os.getenv("NEON_POOL_MAX_SIZE", "2")))
        except ValueError:
            self._pool_max_size = 2
        if self._pool_min_size > self._pool_max_size:
            self._pool_min_size = self._pool_max_size

    def _get_pool(self):
        if not self.enabled or not self.database_url or ConnectionPool is None:
            return None

        with self._pool_lock:
            if self._pool is None:
                self._pool = ConnectionPool(
                    self.database_url,
                    min_size=self._pool_min_size,
                    max_size=self._pool_max_size,
                    kwargs={"row_factory": dict_row},
                )
            return self._pool

    @contextmanager
    def connection(self):
        pool = self._get_pool()
        if pool is not None:
            with pool.connection() as conn:
                yield conn
            return

        if not self.database_url:
            raise RuntimeError("NEON_DATABASE_URL is not configured")

        with psycopg.connect(self.database_url, row_factory=dict_row) as conn:
            yield conn

    def _parse_reading_at(self, raw_data: Dict[str, Any]) -> datetime:
        parsed = parse_watchpower_timestamp(raw_data.get("Data E Hora"), self._tz)
        if parsed is not None:
            return parsed
        raw_timestamp = raw_data.get("Data E Hora")
        if isinstance(raw_timestamp, str) and raw_timestamp.strip():
            logger.warning("Failed to parse timestamp '%s', using now()", raw_timestamp)
        return datetime.now(timezone.utc)

    def _source_hash(
        self, serial_number: str, reading_at: datetime, raw_data: Dict[str, Any]
    ) -> str:
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
        battery_discharge_current = _first_number(
            raw_data, ["Battery Discharge Current"]
        )
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

    def upsert_inverter(self, inverter_config: Dict[str, Any], conn=None) -> None:
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

        with (
            nullcontext(conn) if conn is not None else self.connection()
        ) as active_conn:
            with active_conn.cursor() as cur:
                cur.execute(sql, params)
            if conn is None:
                active_conn.commit()

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

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

    def ensure_inverter_readings_indexes(self) -> None:
        """Ensure hot-path read indexes exist for persisted telemetry."""
        if not self.enabled:
            return

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT to_regclass('public.inverter_readings') AS table_name"
                )
                row = cur.fetchone() or {}
                if not row.get("table_name"):
                    return

                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS inverter_readings_serial_reading_hash_idx
                    ON public.inverter_readings (serial_number, reading_at, source_row_hash)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS inverter_readings_serial_reading_desc_idx
                    ON public.inverter_readings (serial_number, reading_at DESC)
                    """
                )
            conn.commit()

    def persist_reading(
        self,
        serial_number: str,
        raw_data: Dict[str, Any],
        source: str,
        conn=None,
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

        with (
            nullcontext(conn) if conn is not None else self.connection()
        ) as active_conn:
            with active_conn.cursor() as cur:
                cur.execute(sql, params)
                inserted = cur.rowcount > 0
            if conn is None:
                active_conn.commit()
        return inserted

    def record_poll_outcome(
        self,
        serial_number: str,
        alias: Optional[str],
        status: str,
        attempts: int,
        error_text: Optional[str] = None,
        conn=None,
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

        with (
            nullcontext(conn) if conn is not None else self.connection()
        ) as active_conn:
            with active_conn.cursor() as cur:
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
            if conn is None:
                active_conn.commit()

    def fetch_history(
        self, serial_number: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        with self.connection() as conn:
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

    def _normalize_reading_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        payload = row.get("raw_payload")
        reading_at = row.get("reading_at")
        polled_at = row.get("polled_at")

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return None

        if not isinstance(payload, dict):
            return None

        normalized = dict(payload)
        if isinstance(reading_at, datetime):
            normalized["reading_at"] = reading_at.isoformat()
            normalized["timestamp"] = reading_at.isoformat()
            normalized["time"] = reading_at.isoformat()
        if isinstance(polled_at, datetime):
            normalized["polled_at"] = polled_at.isoformat()

        return {
            "data": normalized,
            "reading_at": reading_at,
            "polled_at": polled_at,
        }

    def fetch_latest_reading(self, serial_number: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT raw_payload, reading_at, polled_at
                    FROM public.inverter_readings
                    WHERE serial_number = %s
                    ORDER BY reading_at DESC
                    LIMIT 1
                    """,
                    (serial_number,),
                )
                row = cur.fetchone()

        if not row:
            return None

        return self._normalize_reading_row(row)

    def fetch_latest_readings_batch(
        self, serial_numbers: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Bulk-fetch the latest reading for each serial in a single query.

        Returns a dict keyed by serial_number. Serials with no rows are absent.
        Use this instead of calling fetch_latest_reading() in a loop to avoid
        N+1 round-trips against Neon.
        """
        if not self.enabled:
            return {}

        unique_serials = [s for s in {str(sn).strip() for sn in serial_numbers} if s]
        if not unique_serials:
            return {}

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (serial_number)
                           serial_number, raw_payload, reading_at, polled_at
                    FROM public.inverter_readings
                    WHERE serial_number = ANY(%s)
                    ORDER BY serial_number, reading_at DESC
                    """,
                    (unique_serials,),
                )
                rows = cur.fetchall() or []

        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            serial = row.get("serial_number")
            if not serial:
                continue
            result[str(serial)] = self._normalize_reading_row(row)
        return result

    def fetch_energy_summary_samples(
        self,
        serial_number: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []

        sql = """
            SELECT reading_at, load_power_w, pv_power_w, grid_power_w, raw_payload
            FROM public.inverter_readings
            WHERE serial_number = %s
              AND (%s IS NULL OR reading_at >= %s)
              AND (%s IS NULL OR reading_at <= %s)
            ORDER BY reading_at ASC
        """

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (serial_number, since, since, until, until))
                rows = cur.fetchall()

        return [dict(row) for row in rows]

    def fetch_energy_summary_available_months(
        self,
        serial_number: str,
    ) -> List[str]:
        if not self.enabled:
            return []

        sql = """
            SELECT DISTINCT TO_CHAR(DATE_TRUNC('month', reading_at), 'YYYY-MM') AS month_key
            FROM public.inverter_readings
            WHERE serial_number = %s
            ORDER BY month_key DESC
        """

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (serial_number,))
                rows = cur.fetchall() or []

        months: List[str] = []
        for row in rows:
            month_key = row.get("month_key") if isinstance(row, dict) else None
            if isinstance(month_key, str) and month_key.strip():
                months.append(month_key.strip())

        return months

    # ------------------------------------------------------------------
    # Daily history (date-bound) for dashboard charting
    # ------------------------------------------------------------------

    DAILY_TITLES: List[str] = [
        "Data E Hora",
        "PV1 Charging Power",
        "PV2 Charging Power",
        "AC Output Active Power",
        "Battery Voltage",
        "Battery Discharge Current",
        "Battery Charging Current",
    ]

    def _build_daily_payload_from_rows(
        self,
        rows: List[Dict[str, Any]],
        day: date,
        tz: ZoneInfo,
    ) -> Optional[Dict[str, Any]]:
        if not rows:
            return None

        titles = list(self.DAILY_TITLES)
        title_keys = titles[1:]  # skip timestamp slot
        built_rows: List[List[Any]] = []

        for row in rows:
            payload = row.get("raw_payload")
            reading_at = row.get("reading_at")

            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = None

            if not isinstance(payload, dict):
                payload = {}

            timestamp_label = ""
            if isinstance(reading_at, datetime):
                local_reading = reading_at.astimezone(tz)
                timestamp_label = local_reading.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(payload.get("Data E Hora"), str):
                timestamp_label = payload["Data E Hora"]

            row_values: List[Any] = [timestamp_label]
            for key in title_keys:
                value = payload.get(key)
                if value is None:
                    # Try a couple of common alternative spellings
                    for alt in (key.lower(), key.replace(" ", "_")):
                        if alt in payload and payload[alt] is not None:
                            value = payload[alt]
                            break
                row_values.append(value if value is not None else 0)

            built_rows.append(row_values)

        return {
            "titles": titles,
            "rows": built_rows,
            "date": day.isoformat(),
            "row_count": len(built_rows),
        }

    def fetch_daily_payload(
        self,
        serial_number: str,
        day: date,
        timezone_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return WatchPower-compatible {titles, rows} for the given local day.

        Day boundaries are interpreted in the store's configured timezone.
        Returns None when the store is disabled or the DB has no rows for the
        requested day. Never falls back to live API data.
        """
        if not self.enabled:
            return None

        tz = self._tz
        if timezone_name:
            try:
                tz = ZoneInfo(timezone_name)
            except Exception:
                logger.warning(
                    "Unknown dashboard timezone %r, falling back to %s",
                    timezone_name,
                    self.timezone_name,
                )

        start_local = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
        end_local = start_local + timedelta(days=1)

        sql = """
            SELECT raw_payload, reading_at
            FROM public.inverter_readings
            WHERE serial_number = %s
              AND reading_at >= %s
              AND reading_at < %s
            ORDER BY reading_at ASC
        """

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (serial_number, start_local, end_local),
                )
                rows = cur.fetchall()

        return self._build_daily_payload_from_rows(rows=rows, day=day, tz=tz)

    def fetch_daily_payload_batch(
        self,
        serial_numbers: List[str],
        day: date,
        timezone_name: Optional[str] = None,
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Return {serial -> daily payload} for all requested serials using one query."""
        if not self.enabled:
            return {}

        normalized_serials = sorted(
            {str(serial).strip() for serial in serial_numbers if str(serial).strip()}
        )
        if not normalized_serials:
            return {}

        tz = self._tz
        if timezone_name:
            try:
                tz = ZoneInfo(timezone_name)
            except Exception:
                logger.warning(
                    "Unknown dashboard timezone %r, falling back to %s",
                    timezone_name,
                    self.timezone_name,
                )

        start_local = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
        end_local = start_local + timedelta(days=1)

        sql = """
            SELECT serial_number, raw_payload, reading_at
            FROM public.inverter_readings
            WHERE serial_number = ANY(%s)
              AND reading_at >= %s
              AND reading_at < %s
            ORDER BY serial_number ASC, reading_at ASC
        """

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (normalized_serials, start_local, end_local))
                rows = cur.fetchall() or []

        rows_by_serial: Dict[str, List[Dict[str, Any]]] = {
            serial: [] for serial in normalized_serials
        }
        for row in rows:
            serial_number = str(row.get("serial_number") or "").strip()
            if serial_number and serial_number in rows_by_serial:
                rows_by_serial[serial_number].append(row)

        return {
            serial: self._build_daily_payload_from_rows(serial_rows, day=day, tz=tz)
            for serial, serial_rows in rows_by_serial.items()
        }

    def inspect_daily_payload_window(
        self,
        serial_number: str,
        day: date,
        timezone_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return DB-level diagnostics for a local-day history query window."""
        tz = self._tz
        resolved_timezone = self.timezone_name
        if timezone_name:
            try:
                tz = ZoneInfo(timezone_name)
                resolved_timezone = timezone_name
            except Exception:
                logger.warning(
                    "Unknown dashboard timezone %r, falling back to %s",
                    timezone_name,
                    self.timezone_name,
                )

        start_local = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
        end_local = start_local + timedelta(days=1)

        diagnostics: Dict[str, Any] = {
            "serial_number": serial_number,
            "date": day.isoformat(),
            "timezone": resolved_timezone,
            "window_start": start_local.isoformat(),
            "window_end": end_local.isoformat(),
            "db_enabled": self.enabled,
            "row_count": 0,
            "first_reading_at": None,
            "last_reading_at": None,
        }

        if not self.enabled:
            return diagnostics

        sql = """
            SELECT
                COUNT(*) AS row_count,
                MIN(reading_at) AS first_reading_at,
                MAX(reading_at) AS last_reading_at
            FROM public.inverter_readings
            WHERE serial_number = %s
              AND reading_at >= %s
              AND reading_at < %s
        """

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (serial_number, start_local, end_local))
                row = cur.fetchone() or {}

        row_count = row.get("row_count") if isinstance(row, dict) else None
        diagnostics["row_count"] = int(row_count or 0)

        first_reading_at = (
            row.get("first_reading_at") if isinstance(row, dict) else None
        )
        if isinstance(first_reading_at, datetime):
            diagnostics["first_reading_at"] = first_reading_at.isoformat()

        last_reading_at = row.get("last_reading_at") if isinstance(row, dict) else None
        if isinstance(last_reading_at, datetime):
            diagnostics["last_reading_at"] = last_reading_at.isoformat()

        return diagnostics
