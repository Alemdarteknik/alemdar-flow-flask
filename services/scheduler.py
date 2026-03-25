"""
Polling Scheduler
Handles periodic data fetching from WatchPower API.
"""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from services.reading_resolver import resolve_latest_reading

logger = logging.getLogger(__name__)


class PollingScheduler:
    """Scheduler for durable polling and live snapshot refreshes."""

    def __init__(
        self,
        watchpower_service,
        csv_writer,
        neon_store=None,
        poll_interval_minutes: int = 5,
        poll_retry_attempts: int = 3,
        poll_retry_backoff_seconds: int = 2,
        tick_seconds: int = 15,
        timezone_name: str = "Europe/Nicosia",
        live_status_refresh_seconds: int = 60,
    ):
        self.watchpower_service = watchpower_service
        self.csv_writer = csv_writer
        self.neon_store = neon_store
        self.poll_interval_minutes = poll_interval_minutes
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.poll_retry_attempts = max(1, int(poll_retry_attempts))
        self.poll_retry_backoff_seconds = max(0, int(poll_retry_backoff_seconds))
        self.tick_seconds = max(1, int(tick_seconds))
        self.timezone_name = timezone_name
        self.live_status_refresh_seconds = max(15, int(live_status_refresh_seconds))
        self.live_snapshot_grace_seconds = self.live_status_refresh_seconds * 2

        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.state_lock = threading.RLock()

        self.cache: Dict[str, Any] = {}
        self.inverter_states: Dict[str, Dict[str, Any]] = {}
        self.last_poll_time: Optional[datetime] = None

    def _compute_next_due_at(
        self, reading_at: Optional[datetime], reference_time: Optional[datetime] = None
    ) -> datetime:
        now = reference_time or datetime.now(timezone.utc)
        if reading_at is None:
            return now + timedelta(seconds=self.poll_interval_seconds)

        due_at = reading_at + timedelta(seconds=self.poll_interval_seconds)
        while due_at <= now:
            due_at += timedelta(seconds=self.poll_interval_seconds)
        return due_at

    def _compute_next_live_due_at(
        self, reference_time: Optional[datetime] = None
    ) -> datetime:
        now = reference_time or datetime.now(timezone.utc)
        return now + timedelta(seconds=self.live_status_refresh_seconds)

    def _initial_live_snapshot_due_at(
        self, now: datetime, index: int, total: int
    ) -> datetime:
        if total <= 1:
            return now
        spread_seconds = self.live_status_refresh_seconds / total
        return now + timedelta(seconds=index * spread_seconds)

    def _ensure_inverter_state(self, serial_number: str) -> Dict[str, Any]:
        with self.state_lock:
            return self.inverter_states.setdefault(
                serial_number,
                {
                    "last_successful_reading_at": None,
                    "last_successful_poll_at": None,
                    "last_polled_at": None,
                    "next_poll_due_at": None,
                    "last_status": "pending",
                    "last_error": None,
                    "last_live_checked_at": None,
                    "last_live_telemetry_at": None,
                    "last_live_status": "pending",
                    "last_live_error": None,
                    "next_live_snapshot_due_at": None,
                    "is_polling": False,
                },
            )

    def initialize_runtime_state(self) -> None:
        now = datetime.now(timezone.utc)
        total = max(1, len(self.watchpower_service.inverters))
        for index, inverter in enumerate(self.watchpower_service.inverters):
            serial_number = str(inverter.get("serial_number") or "unknown")
            state = self._ensure_inverter_state(serial_number)
            latest = resolve_latest_reading(
                serial_number=serial_number,
                timezone_name=self.timezone_name,
                neon_store=self.neon_store,
                csv_writer=self.csv_writer,
                cache_entry=None,
            )
            if latest and isinstance(latest.get("data"), dict):
                reading_at = latest.get("reading_at")
                cached_at = latest.get("cached_at")
                polled_at = latest.get("polled_at")
                with self.state_lock:
                    self.cache[serial_number] = {
                        "data": latest["data"],
                        "timestamp": cached_at or now.isoformat(),
                        "polled_at": polled_at.isoformat() if polled_at else cached_at,
                        "reading_at": reading_at,
                        "csv_written": None,
                        "inverter_config": inverter,
                        "source": latest.get("data_source"),
                    }
                    state["last_successful_reading_at"] = reading_at
                    state["last_successful_poll_at"] = polled_at or reading_at
                    state["last_status"] = "ready"
                    state["next_poll_due_at"] = self._compute_next_due_at(
                        reading_at=reading_at,
                        reference_time=now,
                    )
                    state["last_error"] = None
                    state["next_live_snapshot_due_at"] = self._initial_live_snapshot_due_at(
                        now=now,
                        index=index,
                        total=total,
                    )
            else:
                with self.state_lock:
                    state["next_poll_due_at"] = now
                    state["next_live_snapshot_due_at"] = self._initial_live_snapshot_due_at(
                        now=now,
                        index=index,
                        total=total,
                    )

    def _fetch_with_retries(self, serial_number: str) -> Dict[str, Any]:
        attempts = 0
        for attempt in range(1, self.poll_retry_attempts + 1):
            attempts = attempt
            data = self.watchpower_service.get_latest_data(serial_number)
            if data and "data" in data:
                return {
                    "status": "success",
                    "attempts": attempts,
                    "data": data,
                    "error": None,
                }

            if attempt < self.poll_retry_attempts and self.poll_retry_backoff_seconds > 0:
                sleep_for = self.poll_retry_backoff_seconds * attempt
                logger.warning(
                    "No data for %s on attempt %s/%s. Retrying in %ss.",
                    serial_number,
                    attempt,
                    self.poll_retry_attempts,
                    sleep_for,
                )
                time.sleep(sleep_for)

        return {
            "status": "no_data",
            "attempts": attempts,
            "data": None,
            "error": f"No data after {attempts} attempt(s)",
        }

    def _update_cache_entry(
        self,
        serial_number: str,
        data_dict: Dict[str, Any],
        checked_at: datetime,
        reading_at: Optional[datetime],
        inverter_config: Dict[str, Any],
        csv_written: Optional[bool],
        source: str,
    ) -> None:
        with self.state_lock:
            existing = self.cache.get(serial_number, {})
            existing_reading_at = existing.get("reading_at")
            if isinstance(existing_reading_at, datetime) and reading_at is not None:
                if existing_reading_at > reading_at:
                    return

            self.cache[serial_number] = {
                "data": data_dict,
                "timestamp": checked_at.isoformat(),
                "polled_at": checked_at.isoformat(),
                "reading_at": reading_at,
                "csv_written": csv_written,
                "inverter_config": inverter_config,
                "source": source,
            }

    def _record_refresh_success(
        self,
        serial_number: str,
        inverter_data: Dict[str, Any],
        checked_at: datetime,
        persist_reading: bool,
        csv_written: Optional[bool],
    ) -> None:
        reading_at = inverter_data.get("reading_at")
        data_dict = inverter_data["data"]
        inverter_config = inverter_data.get("inverter_config") or {}

        with self.state_lock:
            state = self._ensure_inverter_state(serial_number)
            state["last_live_checked_at"] = checked_at
            state["last_live_telemetry_at"] = reading_at
            state["last_live_status"] = "success"
            state["last_live_error"] = None
            state["next_live_snapshot_due_at"] = self._compute_next_live_due_at(
                reference_time=checked_at
            )
            state["last_polled_at"] = checked_at

            if persist_reading:
                state["last_successful_poll_at"] = checked_at
                state["last_successful_reading_at"] = reading_at
                state["next_poll_due_at"] = self._compute_next_due_at(
                    reading_at=reading_at,
                    reference_time=checked_at,
                )
                state["last_status"] = "success"
                state["last_error"] = None
                self.last_poll_time = checked_at

            state["is_polling"] = False

        self._update_cache_entry(
            serial_number=serial_number,
            data_dict=data_dict,
            checked_at=checked_at,
            reading_at=reading_at,
            inverter_config=inverter_config,
            csv_written=csv_written,
            source="poll" if persist_reading else "live_snapshot",
        )

    def _record_refresh_failure(
        self,
        serial_number: str,
        checked_at: datetime,
        error_text: Optional[str],
        persist_reading: bool,
        status: str,
    ) -> None:
        with self.state_lock:
            state = self._ensure_inverter_state(serial_number)
            state["last_live_checked_at"] = checked_at
            state["last_live_status"] = status
            state["last_live_error"] = error_text
            state["next_live_snapshot_due_at"] = self._compute_next_live_due_at(
                reference_time=checked_at
            )
            state["last_polled_at"] = checked_at

            if persist_reading:
                state["last_status"] = status
                state["last_error"] = error_text
                state["next_poll_due_at"] = self._compute_next_due_at(
                    reading_at=state.get("last_successful_reading_at"),
                    reference_time=checked_at,
                )
                self.last_poll_time = checked_at

            state["is_polling"] = False

    def refresh_inverter(
        self,
        serial_number: str,
        persist_reading: bool,
        update_live_snapshot: bool,
    ) -> Dict[str, Any]:
        state = self._ensure_inverter_state(serial_number)
        with self.state_lock:
            if state["is_polling"]:
                return {"status": "busy", "attempts": 0, "data": None, "error": None}
            state["is_polling"] = True

        checked_at = datetime.now(timezone.utc)
        try:
            fetch_result = self._fetch_with_retries(serial_number)
            inverter_data = fetch_result.get("data")
            inverter_config = next(
                (
                    inverter
                    for inverter in self.watchpower_service.inverters
                    if str(inverter.get("serial_number")) == serial_number
                ),
                {},
            )
            alias = inverter_config.get("alias")

            if fetch_result["status"] == "success" and inverter_data:
                csv_written = None
                if persist_reading:
                    data_dict = inverter_data["data"]
                    csv_written = self.csv_writer.write_with_deduplication(
                        serial_number=serial_number,
                        data=data_dict,
                    )

                    if self.neon_store and self.neon_store.enabled:
                        try:
                            with self.neon_store.connection() as conn:
                                self.neon_store.persist_reading(
                                    serial_number=serial_number,
                                    raw_data=data_dict,
                                    source="poll",
                                    conn=conn,
                                )
                                self.neon_store.record_poll_outcome(
                                    serial_number=serial_number,
                                    alias=alias,
                                    status="success",
                                    attempts=fetch_result["attempts"],
                                    error_text=None,
                                    conn=conn,
                                )
                                conn.commit()
                        except Exception as neon_error:
                            logger.error(
                                "Failed to persist poll data to Neon for %s: %s",
                                serial_number,
                                neon_error,
                            )

                self._record_refresh_success(
                    serial_number=serial_number,
                    inverter_data=inverter_data,
                    checked_at=checked_at,
                    persist_reading=persist_reading,
                    csv_written=csv_written,
                )

                logger.info(
                    "Refreshed %s successfully (persist=%s, live=%s)",
                    serial_number,
                    persist_reading,
                    update_live_snapshot,
                )
                return {
                    **fetch_result,
                    "csv_written": csv_written,
                    "checked_at": checked_at,
                    "persisted": persist_reading,
                    "updated_live_snapshot": update_live_snapshot,
                }

            if persist_reading and self.neon_store and self.neon_store.enabled:
                try:
                    with self.neon_store.connection() as conn:
                        self.neon_store.record_poll_outcome(
                            serial_number=serial_number,
                            alias=alias,
                            status="no_data",
                            attempts=fetch_result["attempts"],
                            error_text=fetch_result["error"],
                            conn=conn,
                        )
                        conn.commit()
                except Exception as neon_error:
                    logger.error(
                        "Failed to persist poll audit to Neon for %s: %s",
                        serial_number,
                        neon_error,
                    )

            self._record_refresh_failure(
                serial_number=serial_number,
                checked_at=checked_at,
                error_text=fetch_result["error"],
                persist_reading=persist_reading,
                status="no_data",
            )
            logger.warning(
                "No data received for %s after %s attempt(s)",
                serial_number,
                fetch_result["attempts"],
            )
            return {
                **fetch_result,
                "checked_at": checked_at,
                "persisted": persist_reading,
                "updated_live_snapshot": update_live_snapshot,
            }
        except Exception as error:
            self._record_refresh_failure(
                serial_number=serial_number,
                checked_at=checked_at,
                error_text=str(error),
                persist_reading=persist_reading,
                status="error",
            )
            logger.error("Error processing %s: %s", serial_number, error)
            return {
                "status": "error",
                "attempts": 0,
                "data": None,
                "error": str(error),
                "checked_at": checked_at,
                "persisted": persist_reading,
                "updated_live_snapshot": update_live_snapshot,
            }

    def poll_once(self) -> Dict[str, Any]:
        logger.info("Starting manual polling cycle...")
        results = {}
        for inverter in self.watchpower_service.inverters:
            serial_number = str(inverter.get("serial_number") or "unknown")
            results[serial_number] = self.refresh_inverter(
                serial_number=serial_number,
                persist_reading=True,
                update_live_snapshot=True,
            )["status"]
        logger.info("Manual polling cycle completed. Results: %s", results)
        return results

    def refresh_due_inverters(
        self, now: Optional[datetime] = None
    ) -> Dict[str, Dict[str, Any]]:
        reference_time = now or datetime.now(timezone.utc)
        due_serials: List[Dict[str, Any]] = []

        with self.state_lock:
            for inverter in self.watchpower_service.inverters:
                serial_number = str(inverter.get("serial_number") or "unknown")
                state = self._ensure_inverter_state(serial_number)
                next_poll_due_at = state.get("next_poll_due_at")
                next_live_snapshot_due_at = state.get("next_live_snapshot_due_at")
                poll_due = next_poll_due_at is None or next_poll_due_at <= reference_time
                live_due = (
                    next_live_snapshot_due_at is None
                    or next_live_snapshot_due_at <= reference_time
                )

                if (poll_due or live_due) and not state.get("is_polling"):
                    due_serials.append(
                        {
                            "serial_number": serial_number,
                            "persist_reading": poll_due,
                            "update_live_snapshot": live_due or poll_due,
                        }
                    )

        results: Dict[str, Dict[str, Any]] = {}
        for entry in due_serials:
            results[entry["serial_number"]] = self.refresh_inverter(
                serial_number=entry["serial_number"],
                persist_reading=entry["persist_reading"],
                update_live_snapshot=entry["update_live_snapshot"],
            )
        return results

    def _polling_loop(self):
        logger.info(
            "Polling loop started. Interval: %s minutes, tick: %ss, live snapshot: %ss",
            self.poll_interval_minutes,
            self.tick_seconds,
            self.live_status_refresh_seconds,
        )
        self.initialize_runtime_state()
        self.refresh_due_inverters()

        while not self.stop_event.is_set():
            try:
                self.refresh_due_inverters()
            except Exception as error:
                logger.error("Error in polling loop: %s", error)
            self.stop_event.wait(self.tick_seconds)

    def start(self):
        if self.is_running:
            logger.warning("Polling scheduler is already running")
            return

        self.stop_event.clear()
        self.is_running = True
        self.thread = threading.Thread(target=self._polling_loop, daemon=True)
        self.thread.start()
        logger.info("Polling scheduler started")

    def stop(self):
        if not self.is_running:
            logger.warning("Polling scheduler is not running")
            return

        self.is_running = False
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Polling scheduler stopped")

    def get_cached_data(self, serial_number: str) -> Optional[Dict[str, Any]]:
        with self.state_lock:
            cached = self.cache.get(serial_number)
            return dict(cached) if cached else None

    def get_all_cached_data(self) -> Dict[str, Any]:
        with self.state_lock:
            return {serial: dict(payload) for serial, payload in self.cache.items()}

    def get_inverter_state(self, serial_number: str) -> Optional[Dict[str, Any]]:
        with self.state_lock:
            state = self.inverter_states.get(serial_number)
            if not state:
                return None
            return {
                "last_successful_reading_at": (
                    state["last_successful_reading_at"].isoformat()
                    if state["last_successful_reading_at"]
                    else None
                ),
                "last_successful_poll_at": (
                    state["last_successful_poll_at"].isoformat()
                    if state["last_successful_poll_at"]
                    else None
                ),
                "last_polled_at": (
                    state["last_polled_at"].isoformat()
                    if state["last_polled_at"]
                    else None
                ),
                "next_poll_due_at": (
                    state["next_poll_due_at"].isoformat()
                    if state["next_poll_due_at"]
                    else None
                ),
                "last_status": state["last_status"],
                "last_error": state["last_error"],
                "last_live_checked_at": (
                    state["last_live_checked_at"].isoformat()
                    if state["last_live_checked_at"]
                    else None
                ),
                "last_live_telemetry_at": (
                    state["last_live_telemetry_at"].isoformat()
                    if state["last_live_telemetry_at"]
                    else None
                ),
                "last_live_status": state["last_live_status"],
                "last_live_error": state["last_live_error"],
                "next_live_snapshot_due_at": (
                    state["next_live_snapshot_due_at"].isoformat()
                    if state["next_live_snapshot_due_at"]
                    else None
                ),
                "is_polling": state["is_polling"],
            }

    def force_poll(self) -> Dict[str, Any]:
        logger.info("Force polling triggered")
        return self.poll_once()

    def get_status(self) -> Dict[str, Any]:
        with self.state_lock:
            states = {
                serial: {
                    "last_successful_reading_at": (
                        state["last_successful_reading_at"].isoformat()
                        if state["last_successful_reading_at"]
                        else None
                    ),
                    "last_successful_poll_at": (
                        state["last_successful_poll_at"].isoformat()
                        if state["last_successful_poll_at"]
                        else None
                    ),
                    "last_polled_at": (
                        state["last_polled_at"].isoformat()
                        if state["last_polled_at"]
                        else None
                    ),
                    "next_poll_due_at": (
                        state["next_poll_due_at"].isoformat()
                        if state["next_poll_due_at"]
                        else None
                    ),
                    "last_status": state["last_status"],
                    "last_live_checked_at": (
                        state["last_live_checked_at"].isoformat()
                        if state["last_live_checked_at"]
                        else None
                    ),
                    "last_live_telemetry_at": (
                        state["last_live_telemetry_at"].isoformat()
                        if state["last_live_telemetry_at"]
                        else None
                    ),
                    "last_live_status": state["last_live_status"],
                    "next_live_snapshot_due_at": (
                        state["next_live_snapshot_due_at"].isoformat()
                        if state["next_live_snapshot_due_at"]
                        else None
                    ),
                }
                for serial, state in self.inverter_states.items()
            }

        return {
            "is_running": self.is_running,
            "poll_interval_minutes": self.poll_interval_minutes,
            "tick_seconds": self.tick_seconds,
            "live_status_refresh_seconds": self.live_status_refresh_seconds,
            "live_snapshot_grace_seconds": self.live_snapshot_grace_seconds,
            "last_poll_time": (
                self.last_poll_time.isoformat() if self.last_poll_time else None
            ),
            "cached_inverters": list(self.cache.keys()),
            "cache_size": len(self.cache),
            "inverters": states,
        }
