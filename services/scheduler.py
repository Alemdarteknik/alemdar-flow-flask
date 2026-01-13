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

    def __init__(self, watchpower_service, csv_writer, poll_interval_minutes: int = 5):
        """
        Initialize polling scheduler

        Args:
            watchpower_service: WatchPowerService instance
            csv_writer: CSVWriter instance
            poll_interval_minutes: Polling interval in minutes
        """
        self.watchpower_service = watchpower_service
        self.csv_writer = csv_writer
        self.poll_interval_minutes = poll_interval_minutes
        self.poll_interval_seconds = poll_interval_minutes * 60

        self.is_running = False
        self.thread: Optional[threading.Thread] = None

        # In-memory cache for latest data
        self.cache: Dict[str, Any] = {}
        self.last_poll_time: Optional[datetime] = None

    def poll_once(self) -> Dict[str, Any]:
        """
        Execute a single polling cycle for all inverters

        Returns:
            Dictionary of results for all inverters
        """
        logger.info("Starting polling cycle...")
        results = {}

        try:
            # Get data for all inverters
            all_data = self.watchpower_service.get_all_inverters_data()

            for serial_number, inverter_data in all_data.items():
                try:
                    if inverter_data and "data" in inverter_data:
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
                        }

                        results[serial_number] = "success"
                        logger.info(f"Polled {serial_number} successfully")
                    else:
                        results[serial_number] = "no_data"
                        logger.warning(f"No data received for {serial_number}")

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
