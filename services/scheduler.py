"""
Polling Scheduler
Handles periodic data fetching from WatchPower API
"""

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class PollingScheduler:
    """Scheduler for periodic data polling"""

    def __init__(
        self,
        watchpower_service,
        csv_writer,
        neon_store=None,
        poll_interval_minutes: int = 5,
        poll_retry_attempts: int = 3,
        poll_retry_backoff_seconds: int = 2,
    ):
        """
        Initialize polling scheduler

        Args:
            watchpower_service: WatchPowerService instance
            csv_writer: CSVWriter instance
            poll_interval_minutes: Polling interval in minutes
        """
        self.watchpower_service = watchpower_service
        self.csv_writer = csv_writer
        self.neon_store = neon_store
        self.poll_interval_minutes = poll_interval_minutes
        self.poll_interval_seconds = poll_interval_minutes * 60
        self.poll_retry_attempts = max(1, int(poll_retry_attempts))
        self.poll_retry_backoff_seconds = max(0, int(poll_retry_backoff_seconds))

        self.is_running = False
        self.thread: Optional[threading.Thread] = None

        # In-memory cache for latest data
        self.cache: Dict[str, Any] = {}
        self.last_poll_time: Optional[datetime] = None

    def _fetch_with_retries(self, serial_number: str) -> Dict[str, Any]:
        """
        Fetch inverter data with bounded retries.

        Returns:
            {
                "status": "success" | "no_data",
                "attempts": int,
                "data": Optional[Dict[str, Any]],
                "error": Optional[str],
            }
        """
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

    def poll_once(self) -> Dict[str, Any]:
        """
        Execute a single polling cycle for all inverters

        Returns:
            Dictionary of results for all inverters
        """
        logger.info("Starting polling cycle...")
        results = {}

        try:
            for inverter in self.watchpower_service.inverters:
                serial_number = str(inverter.get("serial_number") or "unknown")
                alias = inverter.get("alias")
                try:
                    fetch_result = self._fetch_with_retries(serial_number)
                    inverter_data = fetch_result.get("data")

                    if fetch_result["status"] == "success" and inverter_data:
                        data_dict = inverter_data["data"]

                        # Write to CSV with deduplication
                        success = self.csv_writer.write_with_deduplication(
                            serial_number=serial_number, data=data_dict
                        )

                        # Update cache regardless of CSV write
                        self.cache[serial_number] = {
                            "data": data_dict,
                            "timestamp": datetime.now().isoformat(),
                            "csv_written": success,
                            "inverter_config": inverter_data.get("inverter_config"),
                        }

                        if self.neon_store and self.neon_store.enabled:
                            try:
                                inverter_config = inverter_data.get("inverter_config") or {}
                                self.neon_store.upsert_inverter(inverter_config)
                                self.neon_store.persist_reading(
                                    serial_number=serial_number,
                                    raw_data=data_dict,
                                    source="poll",
                                )
                                self.neon_store.record_poll_outcome(
                                    serial_number=serial_number,
                                    alias=alias,
                                    status="success",
                                    attempts=fetch_result["attempts"],
                                    error_text=None,
                                )
                            except Exception as neon_error:
                                logger.error(
                                    "Failed to persist poll data to Neon for %s: %s",
                                    serial_number,
                                    neon_error,
                                )

                        results[serial_number] = "success"
                        logger.info(f"Polled {serial_number} successfully")
                    else:
                        if self.neon_store and self.neon_store.enabled:
                            try:
                                self.neon_store.record_poll_outcome(
                                    serial_number=serial_number,
                                    alias=alias,
                                    status="no_data",
                                    attempts=fetch_result["attempts"],
                                    error_text=fetch_result["error"],
                                )
                            except Exception as neon_error:
                                logger.error(
                                    "Failed to persist poll audit to Neon for %s: %s",
                                    serial_number,
                                    neon_error,
                                )

                        results[serial_number] = "no_data"
                        logger.warning(
                            "No data received for %s after %s attempt(s)",
                            serial_number,
                            fetch_result["attempts"],
                        )

                except Exception as e:
                    results[serial_number] = f"error: {str(e)}"
                    logger.error(f"Error processing {serial_number}: {e}")

            self.last_poll_time = datetime.now()
            logger.info(f"Polling cycle completed. Results: {results}")

        except Exception as e:
            logger.error(f"Polling cycle failed: {e}")

        return results

    def _polling_loop(self):
        """Internal polling loop that runs in a separate thread"""
        logger.info(
            f"Polling loop started. Interval: {self.poll_interval_minutes} minutes"
        )

        # Do initial poll immediately
        self.poll_once()

        while self.is_running:
            try:
                # Wait for the specified interval
                time.sleep(self.poll_interval_seconds)

                if self.is_running:
                    self.poll_once()

            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                # Continue loop even if there's an error

    def start(self):
        """Start the polling scheduler in a background thread"""
        if self.is_running:
            logger.warning("Polling scheduler is already running")
            return

        self.is_running = True
        self.thread = threading.Thread(target=self._polling_loop, daemon=True)
        self.thread.start()
        logger.info("Polling scheduler started")

    def stop(self):
        """Stop the polling scheduler"""
        if not self.is_running:
            logger.warning("Polling scheduler is not running")
            return

        self.is_running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Polling scheduler stopped")

    def get_cached_data(self, serial_number: str) -> Optional[Dict[str, Any]]:
        """
        Get cached data for a specific inverter

        Args:
            serial_number: Inverter serial number

        Returns:
            Cached data dictionary or None
        """
        return self.cache.get(serial_number)

    def get_all_cached_data(self) -> Dict[str, Any]:
        """
        Get all cached data

        Returns:
            Dictionary of all cached inverter data
        """
        return self.cache.copy()

    def force_poll(self) -> Dict[str, Any]:
        """
        Force an immediate polling cycle (outside of the schedule)

        Returns:
            Results dictionary
        """
        logger.info("Force polling triggered")
        return self.poll_once()

    def get_status(self) -> Dict[str, Any]:
        """
        Get scheduler status information

        Returns:
            Status dictionary
        """
        return {
            "is_running": self.is_running,
            "poll_interval_minutes": self.poll_interval_minutes,
            "last_poll_time": (
                self.last_poll_time.isoformat() if self.last_poll_time else None
            ),
            "cached_inverters": list(self.cache.keys()),
            "cache_size": len(self.cache),
        }
