"""
CSV Writer Utility
Handles writing inverter data to CSV files
"""

import csv
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class CSVWriter:
    """Utility class for writing inverter data to CSV files"""

    def __init__(self, data_dir: str = "data"):
        """
        Initialize CSV writer

        Args:
            data_dir: Directory to store CSV files
        """
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def get_csv_path(self, serial_number: str) -> str:
        """
        Get the CSV file path for a specific inverter

        Args:
            serial_number: Inverter serial number

        Returns:
            Full path to CSV file
        """
        filename = f"inverter_{serial_number}.csv"
        return os.path.join(self.data_dir, filename)

    def write_data(self, serial_number: str, data: Dict[str, Any]) -> bool:
        """
        Write inverter data to CSV file

        Args:
            serial_number: Inverter serial number
            data: Dictionary containing inverter data

        Returns:
            True if successful, False otherwise
        """
        csv_path = self.get_csv_path(serial_number)

        try:
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.exists(csv_path)

            # Get all keys from data as fieldnames
            fieldnames = list(data.keys())

            # Open file in append mode
            with open(csv_path, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header if file is new
                if not file_exists:
                    writer.writeheader()
                    logger.info(f"Created new CSV file: {csv_path}")

                # Write data row
                writer.writerow(data)
                logger.debug(f"Wrote data to {csv_path}")

            return True

        except Exception as e:
            logger.error(f"Failed to write CSV for {serial_number}: {e}")
            return False

    def check_duplicate(
        self, serial_number: str, timestamp_field: str, timestamp_value: str
    ) -> bool:
        """
        Check if a timestamp already exists in the CSV file

        Args:
            serial_number: Inverter serial number
            timestamp_field: Name of the timestamp field
            timestamp_value: Timestamp value to check

        Returns:
            True if duplicate found, False otherwise
        """
        csv_path = self.get_csv_path(serial_number)

        if not os.path.exists(csv_path):
            return False

        try:
            with open(csv_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if row.get(timestamp_field) == timestamp_value:
                        return True
            return False
        except Exception as e:
            logger.error(f"Error checking duplicate for {serial_number}: {e}")
            return False

    def write_with_deduplication(
        self,
        serial_number: str,
        data: Dict[str, Any],
        timestamp_field: str = "Data E Hora",
    ) -> bool:
        """
        Write data to CSV only if timestamp doesn't already exist

        Args:
            serial_number: Inverter serial number
            data: Dictionary containing inverter data
            timestamp_field: Field name to use for deduplication

        Returns:
            True if data was written, False if duplicate or error
        """
        if timestamp_field not in data:
            logger.warning(f"Timestamp field '{timestamp_field}' not found in data")
            return self.write_data(serial_number, data)

        timestamp_value = data[timestamp_field]

        if self.check_duplicate(serial_number, timestamp_field, timestamp_value):
            logger.debug(
                f"Duplicate timestamp {timestamp_value} for {serial_number}, skipping"
            )
            return False

        return self.write_data(serial_number, data)

    def read_latest(
        self, serial_number: str, num_rows: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Read the latest N rows from a CSV file

        Args:
            serial_number: Inverter serial number
            num_rows: Number of latest rows to read

        Returns:
            List of dictionaries containing the latest rows
        """
        csv_path = self.get_csv_path(serial_number)

        if not os.path.exists(csv_path):
            logger.warning(f"CSV file not found for {serial_number}")
            return []

        try:
            with open(csv_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                return rows[-num_rows:] if rows else []
        except Exception as e:
            logger.error(f"Failed to read CSV for {serial_number}: {e}")
            return []

    def get_all_data(self, serial_number: str) -> List[Dict[str, Any]]:
        """
        Read all data from a CSV file

        Args:
            serial_number: Inverter serial number

        Returns:
            List of all rows as dictionaries
        """
        csv_path = self.get_csv_path(serial_number)

        if not os.path.exists(csv_path):
            logger.warning(f"CSV file not found for {serial_number}")
            return []

        try:
            with open(csv_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                return list(reader)
        except Exception as e:
            logger.error(f"Failed to read CSV for {serial_number}: {e}")
            return []
