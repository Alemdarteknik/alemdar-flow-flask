"""
WatchPower API Service Wrapper
Handles authentication and data fetching from WatchPower API
"""

import json
import logging
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from requests.exceptions import Timeout as RequestsTimeout
from watchpower_api import WatchPowerAPI
from watchpower_api.models import DeviceIdentifier

from utils.telemetry_time import parse_watchpower_timestamp

logger = logging.getLogger(__name__)


class WatchPowerService:
    """Service class to manage WatchPower API interactions"""

    def __init__(
        self,
        config_path: Optional[str] = None,
        timezone_name: Optional[str] = None,
    ):
        """
        Initialize WatchPower service

        Args:
            config_path: Path to inverters.json configuration file.
                        If None, will look for config/inverters.json relative to this file.
        """
        self.api = WatchPowerAPI()
        self.authenticated = False
        self.inverters: List[Dict[str, Any]] = []
        self.api_sessions: Dict[tuple[str, str], WatchPowerAPI] = {}
        self.timezone_name = timezone_name or os.getenv(
            "WATCHPOWER_TIMEZONE", "Europe/Nicosia"
        )
        self.daily_retry_attempts = max(
            1, int(os.getenv("WATCHPOWER_DAILY_RETRY_ATTEMPTS", 2))
        )
        self.daily_retry_backoff_seconds = max(
            0, int(os.getenv("WATCHPOWER_DAILY_RETRY_BACKOFF_SECONDS", 2))
        )

        # Load inverter configuration
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "config", "inverters.json"
            )
        self.load_inverters_config(config_path)

    def authenticate(self) -> bool:
        """
        Authenticate with WatchPower API for all inverters
        Creates API sessions for each unique set of credentials

        Returns:
            bool: True if at least one authentication successful
        """
        if not self.inverters:
            logger.error("No inverters loaded. Cannot authenticate.")
            return False

        success_count = 0
        unique_creds = set()

        # Extract unique credentials from inverters
        for inv in self.inverters:
            username = inv.get("username")
            password = inv.get("password")
            if username and password:
                unique_creds.add((username, password))

        # Authenticate each unique credential set
        for username, password in unique_creds:
            try:
                api = WatchPowerAPI()
                logger.info(
                    "Attempting to authenticate with WatchPower API for user: %s",
                    username,
                )
                api.login(username, password)
                self.api_sessions[(username, password)] = api
                logger.info(
                    f"Successfully authenticated with WatchPower API for user: {username}"
                )
                success_count += 1
            except Exception as e:
                logger.error(f"Authentication failed for user {username}: {e}")

        self.authenticated = success_count > 0
        if self.authenticated:
            logger.info(
                f"Authenticated {success_count}/{len(unique_creds)} credential sets"
            )
        else:
            logger.error("Failed to authenticate any credentials")

        return self.authenticated

    def _get_api_for_inverter(
        self, inverter_config: Dict[str, Any]
    ) -> Optional[WatchPowerAPI]:
        """Return an authenticated API client for the given inverter credentials."""
        username = inverter_config.get("username")
        password = inverter_config.get("password")

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

    def _is_timeout_error(self, err: Exception) -> bool:
        if isinstance(err, RequestsTimeout):
            return True
        message = str(err).lower()
        return "read timed out" in message or "timed out" in message

    def _fetch_daily_data_with_retries(
        self,
        api_client: WatchPowerAPI,
        inverter_config: Dict[str, Any],
        target_day: date,
        serial_number: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch WatchPower daily data with bounded retries."""
        attempts = self.daily_retry_attempts
        for attempt in range(1, attempts + 1):
            try:
                return api_client.get_daily_data(
                    day=target_day,
                    serial_number=inverter_config["serial_number"],
                    wifi_pn=inverter_config["wifi_pn"],
                    dev_code=inverter_config["device_code"],
                    dev_addr=inverter_config["device_address"],
                )
            except Exception as e:
                timeout_error = self._is_timeout_error(e)
                if timeout_error:
                    logger.warning(
                        "WatchPower timeout for %s (attempt %s/%s): %s",
                        serial_number,
                        attempt,
                        attempts,
                        e,
                    )
                else:
                    logger.error(
                        "WatchPower error for %s (attempt %s/%s): %s",
                        serial_number,
                        attempt,
                        attempts,
                        e,
                    )

                if attempt >= attempts:
                    return None

                if self.daily_retry_backoff_seconds > 0:
                    backoff = self.daily_retry_backoff_seconds * attempt
                    logger.warning(
                        "Retrying WatchPower fetch for %s in %ss",
                        serial_number,
                        backoff,
                    )
                    import time

                    time.sleep(backoff)

        return None

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
            raw_data = self._fetch_daily_data_with_retries(
                api_client=api_client,
                inverter_config=inverter_config,
                target_day=today,
                serial_number=serial_number,
            )
            logger.info(
                f"WatchPower API response for {serial_number}: {raw_data is not None}"
            )

            if raw_data and "dat" in raw_data and "row" in raw_data["dat"]:
                rows = raw_data["dat"]["row"]
                titles = self._extract_titles(raw_data["dat"].get("title", []))

                if rows:
                    data_dict, reading_at = self._select_latest_row_payload(
                        rows=rows,
                        titles=titles,
                        serial_number=serial_number,
                        inverter_config=inverter_config,
                    )
                    if data_dict is not None:
                        data_dict["serial_number"] = serial_number
                        data_dict["alias"] = inverter_config.get("alias", serial_number)
                        data_dict["system_type"] = inverter_config.get(
                            "system_type", "unknown"
                        )

                        logger.info(
                            "Successfully fetched latest data for %s at %s",
                            serial_number,
                            reading_at.isoformat() if reading_at else "unknown time",
                        )
                        return {
                            "data": data_dict,
                            "raw": raw_data,
                            "inverter_config": inverter_config,
                            "reading_at": reading_at,
                        }

            logger.warning(f"No data rows found for {serial_number}")
            return None

        except Exception as e:
            logger.error(f"Failed to fetch data for {serial_number}: {e}")
            return None

    def _extract_titles(self, titles_data: List[Any]) -> List[str]:
        if not titles_data:
            return []
        if isinstance(titles_data[0], dict) and "title" in titles_data[0]:
            return [str(title.get("title", "")).strip() for title in titles_data]
        return [str(title).strip() for title in titles_data]

    def _build_row_payload(self, row_obj: Any, titles: List[str]) -> Dict[str, Any]:
        values = (
            row_obj.get("field", row_obj) if isinstance(row_obj, dict) else row_obj
        )
        if not isinstance(values, list):
            return {}

        payload: Dict[str, Any] = {}
        for index, title in enumerate(titles):
            if index < len(values):
                payload[title] = values[index]
        return payload

    def _accepted_serials_for_inverter(
        self, inverter_config: Optional[Dict[str, Any]]
    ) -> List[str]:
        if not inverter_config:
            return []

        accepted_serials: List[str] = []
        for candidate in [
            inverter_config.get("serial_number"),
            *(inverter_config.get("accepted_telemetry_serials") or []),
        ]:
            normalized = str(candidate or "").strip()
            if normalized and normalized not in accepted_serials:
                accepted_serials.append(normalized)
        return accepted_serials

    def _row_matches_serial(
        self,
        payload: Dict[str, Any],
        inverter_config: Optional[Dict[str, Any]],
    ) -> bool:
        accepted_serials = self._accepted_serials_for_inverter(inverter_config)
        if not accepted_serials:
            return True

        candidates = [
            str(payload.get("SN") or "").strip(),
            str(payload.get("serial_number") or "").strip(),
        ]

        return any(
            candidate in accepted_serials for candidate in candidates if candidate
        )

    def _filter_rows_for_serial(
        self,
        rows: List[Any],
        titles: List[str],
        serial_number: str,
        inverter_config: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        matching_rows: List[Any] = []
        saw_serial_marker = False
        accepted_serials = self._accepted_serials_for_inverter(inverter_config)

        for row_obj in rows:
            payload = self._build_row_payload(row_obj, titles)
            if not payload:
                continue
            if str(payload.get("SN") or "").strip() or str(
                payload.get("serial_number") or ""
            ).strip():
                saw_serial_marker = True
            if self._row_matches_serial(payload, inverter_config):
                matching_rows.append(row_obj)

        if matching_rows:
            return matching_rows

        if saw_serial_marker:
            logger.error(
                "WatchPower returned rows for %s, but none matched accepted serials %s. Refusing to use mismatched rows.",
                serial_number,
                accepted_serials or [serial_number],
            )
            return []

        logger.warning(
            "WatchPower returned no serial markers for %s; falling back to unfiltered rows",
            serial_number,
        )
        return rows

    def _select_latest_row_payload(
        self,
        rows: List[Any],
        titles: List[str],
        serial_number: str,
        inverter_config: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[datetime]]:
        fallback_payload: Optional[Dict[str, Any]] = None
        fallback_reading_at: Optional[datetime] = None
        latest_payload: Optional[Dict[str, Any]] = None
        latest_reading_at: Optional[datetime] = None

        filtered_rows = self._filter_rows_for_serial(
            rows=rows,
            titles=titles,
            serial_number=serial_number,
            inverter_config=inverter_config,
        )

        for row_obj in filtered_rows:
            payload = self._build_row_payload(row_obj, titles)
            if not payload:
                continue

            if fallback_payload is None:
                fallback_payload = payload

            parsed_timestamp = parse_watchpower_timestamp(
                payload.get("Data E Hora"), self.timezone_name
            )
            if parsed_timestamp is None:
                continue

            if latest_reading_at is None or parsed_timestamp > latest_reading_at:
                latest_reading_at = parsed_timestamp
                latest_payload = payload

        if latest_payload is not None:
            return latest_payload, latest_reading_at

        if filtered_rows:
            fallback_payload = self._build_row_payload(filtered_rows[-1], titles)
            fallback_reading_at = parse_watchpower_timestamp(
                fallback_payload.get("Data E Hora") if fallback_payload else None,
                self.timezone_name,
            )
            logger.warning(
                "WatchPower returned rows without valid timestamps for %s; falling back to last row",
                serial_number,
            )
        else:
            logger.error(
                "No WatchPower rows matched serial %s after filtering; skipping payload.",
                serial_number,
            )

        return fallback_payload, fallback_reading_at

    def _sort_rows_by_timestamp(self, rows: List[List[Any]], titles: List[str]) -> List[List[Any]]:
        time_index = next(
            (
                index
                for index, title in enumerate(titles)
                if isinstance(title, str) and title.strip().lower() == "data e hora"
            ),
            -1,
        )
        if time_index < 0:
            return rows

        def sort_key(row: List[Any]) -> Tuple[int, datetime]:
            candidate = row[time_index] if time_index < len(row) else None
            parsed = parse_watchpower_timestamp(candidate, self.timezone_name)
            return (
                0 if parsed is not None else 1,
                parsed or datetime.max.replace(tzinfo=timezone.utc),
            )

        return sorted(rows, key=sort_key)

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
            raw_data = self._fetch_daily_data_with_retries(
                api_client=api_client,
                inverter_config=inverter_config,
                target_day=target_day,
                serial_number=serial_number,
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

                rows = self._filter_rows_for_serial(
                    rows=rows,
                    titles=titles,
                    serial_number=serial_number,
                    inverter_config=inverter_config,
                )
                rows = self._sort_rows_by_timestamp(rows=rows, titles=titles)

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
            logger.error(f"Failed to fetch daily data for {serial_number}: {e}")
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
                "location": inv.get("location", ""),
            }
            for inv in self.inverters
        ]
