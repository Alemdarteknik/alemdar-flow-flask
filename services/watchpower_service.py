"""
WatchPower API Service Wrapper
Handles authentication and data fetching from WatchPower API
"""

import json
import logging
import os
from datetime import date
from typing import Any, Dict, List, Optional

from watchpower_api import WatchPowerAPI
from watchpower_api.models import DeviceIdentifier

logger = logging.getLogger(__name__)


class WatchPowerService:
    """Service class to manage WatchPower API interactions"""

    def __init__(self, username: str, password: str):
        """
        Initialize WatchPower service

        Args:
            username: default WatchPower account username
            password: default WatchPower account password
        """
        self.username = username
        self.password = password
        self.api = WatchPowerAPI()
        self.authenticated = False
        self.inverters: List[Dict[str, Any]] = []
        self.api_sessions: Dict[tuple[str, str], WatchPowerAPI] = {}

    def authenticate(self) -> bool:
        """
        Authenticate with WatchPower API

        Returns:
            bool: True if authentication successful
        """
        try:
            self.api.login(self.username, self.password)
            self.authenticated = True
            self.api_sessions[(self.username, self.password)] = self.api
            logger.info("Successfully authenticated with WatchPower API")
            return True
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            self.authenticated = False
            return False

    def _get_api_for_inverter(
        self, inverter_config: Dict[str, Any]
    ) -> Optional[WatchPowerAPI]:
        """Return an authenticated API client for the given inverter credentials."""
        username = inverter_config.get("username") or self.username
        password = inverter_config.get("password") or self.password

        if not username or not password:
            logger.error(
                f"Missing credentials for inverter {inverter_config.get('serial_number', 'unknown')}"
            )
            return None

        creds = (username, password)
        if creds in self.api_sessions:
            return self.api_sessions[creds]

        api = WatchPowerAPI()
        try:
            api.login(username, password)
            self.api_sessions[creds] = api
            logger.info(
                "Authenticated WatchPower API session for inverter %s with user %s",
                inverter_config.get("serial_number"),
                username,
            )
            return api
        except Exception as e:
            logger.error(
                "Authentication failed for inverter %s with user %s: %s",
                inverter_config.get("serial_number"),
                username,
                e,
            )
            return None

    def load_inverters_config(self, config_path: str) -> List[Dict[str, Any]]:
        """
        Load inverter configurations from JSON file

        Args:
            config_path: Path to inverters.json configuration file

        Returns:
            List of inverter configurations
        """
        try:
            with open(config_path, "r") as f:
                self.inverters = json.load(f)
            logger.info(f"Loaded {len(self.inverters)} inverter configurations")
            return self.inverters
        except Exception as e:
            logger.error(f"Failed to load inverters config: {e}")
            return []

    def get_latest_data(self, serial_number: str) -> Optional[Dict[str, Any]]:
        """
        Get latest data for a specific inverter

        Args:
            serial_number: Inverter serial number

        Returns:
            Dictionary containing latest inverter data or None if failed
        """
        # Find inverter config
        inverter_config = next(
            (inv for inv in self.inverters if inv["serial_number"] == serial_number),
            None,
        )

        if not inverter_config:
            logger.error(f"No configuration found for serial number: {serial_number}")
            return None

        api_client = self._get_api_for_inverter(inverter_config)
        if not api_client:
            return None

        try:
            # Get today's data
            today = date.today()
            logger.info(f"Fetching daily data for {serial_number} from WatchPower API")
            raw_data = api_client.get_daily_data(
                day=today,
                serial_number=inverter_config["serial_number"],
                wifi_pn=inverter_config["wifi_pn"],
                dev_code=inverter_config["device_code"],
                dev_addr=inverter_config["device_address"],
            )
            logger.info(
                f"WatchPower API response for {serial_number}: {raw_data is not None}"
            )

            # Extract latest reading (last row)
            if raw_data and "dat" in raw_data and "row" in raw_data["dat"]:
                rows = raw_data["dat"]["row"]
                titles_data = raw_data["dat"].get("title", [])

                # Extract title strings from title objects
                titles = []
                if titles_data:
                    if isinstance(titles_data[0], dict) and "title" in titles_data[0]:
                        titles = [t["title"] for t in titles_data]
                    else:
                        titles = titles_data

                if rows:
                    latest_row_obj = rows[-1]  # Get last entry

                    # Extract values from 'field' array if present, otherwise use the row directly
                    latest_values = (
                        latest_row_obj.get("field", latest_row_obj)
                        if isinstance(latest_row_obj, dict)
                        else latest_row_obj
                    )

                    # Create dictionary mapping titles to values
                    data_dict = {}
                    for i, title in enumerate(titles):
                        if i < len(latest_values):
                            data_dict[title] = latest_values[i]

                    # Add metadata
                    data_dict["serial_number"] = serial_number
                    data_dict["alias"] = inverter_config.get("alias", serial_number)
                    data_dict["system_type"] = inverter_config.get(
                        "system_type", "unknown"
                    )

                    logger.info(f"Successfully fetched latest data for {serial_number}")
                    return {
                        "data": data_dict,
                        "raw": raw_data,
                        "inverter_config": inverter_config,
                    }

            logger.warning(f"No data rows found for {serial_number}")
            return None

        except Exception as e:
            logger.error(
                f"Failed to fetch data for {serial_number}: {e}", exc_info=True
            )
            return None

    def get_all_inverters_data(self) -> Dict[str, Any]:
        """
        Get latest data for all configured inverters

        Returns:
            Dictionary mapping serial numbers to their latest data
        """
        results = {}

        for inverter in self.inverters:
            serial = inverter["serial_number"]
            data = self.get_latest_data(serial)
            if data:
                results[serial] = data
            else:
                logger.warning(f"Failed to get data for {serial}")

        return results

    def get_daily_raw(
        self, serial_number: str, day: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get full daily raw data (all rows and titles) for a specific inverter

        Args:
            serial_number: Inverter serial number
            day: Optional date, defaults to today

        Returns:
            Dictionary with titles and rows, or None if failed
        """
        inverter_config = next(
            (inv for inv in self.inverters if inv["serial_number"] == serial_number),
            None,
        )

        if not inverter_config:
            logger.error(f"No configuration found for serial number: {serial_number}")
            return None

        api_client = self._get_api_for_inverter(inverter_config)
        if not api_client:
            return None

        try:
            target_day = day or date.today()
            logger.info(f"Fetching daily raw data for {serial_number} on {target_day}")
            raw_data = api_client.get_daily_data(
                day=target_day,
                serial_number=inverter_config["serial_number"],
                wifi_pn=inverter_config["wifi_pn"],
                dev_code=inverter_config["device_code"],
                dev_addr=inverter_config["device_address"],
            )
            logger.info(
                f"WatchPower API daily response for {serial_number}: {raw_data is not None}"
            )
            if raw_data:
                logger.debug(
                    f"Raw data keys: {raw_data.keys() if isinstance(raw_data, dict) else type(raw_data)}"
                )

            if raw_data and "dat" in raw_data:
                titles_data = raw_data["dat"].get("title", [])
                rows_data = raw_data["dat"].get("row", [])

                # Extract title strings from title objects
                titles = []
                if titles_data:
                    if isinstance(titles_data[0], dict) and "title" in titles_data[0]:
                        titles = [t["title"] for t in titles_data]
                    else:
                        titles = titles_data

                # Extract values from 'field' arrays in each row
                rows = []
                for row in rows_data:
                    if isinstance(row, dict) and "field" in row:
                        rows.append(row["field"])
                    else:
                        rows.append(row)

                return {
                    "serial_number": serial_number,
                    "alias": inverter_config.get("alias", serial_number),
                    "system_type": inverter_config.get("system_type", "unknown"),
                    "titles": titles,
                    "rows": rows,
                    "raw": raw_data,
                    "date": target_day.isoformat(),
                }

            logger.warning(f"No daily data found for {serial_number}")
            return None

        except Exception as e:
            logger.error(
                f"Failed to fetch daily data for {serial_number}: {e}", exc_info=True
            )
            return None

    def get_inverters_list(self) -> List[Dict[str, Any]]:
        """
        Get list of all configured inverters (metadata only)

        Returns:
            List of inverter metadata
        """
        return [
            {
                "serial_number": inv["serial_number"],
                "alias": inv.get("alias", inv["serial_number"]),
                "system_type": inv.get("system_type", "unknown"),
                "description": inv.get("description", ""),
            }
            for inv in self.inverters
        ]
