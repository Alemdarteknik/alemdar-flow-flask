import unittest
from datetime import datetime, timezone

import app as flask_app_module


class StubScheduler:
    def __init__(
        self,
        cache_entry,
        state,
        last_poll_time=None,
        live_snapshot_grace_seconds=120,
    ):
        self._cache_entry = cache_entry
        self._state = state
        self.last_poll_time = last_poll_time
        self.live_snapshot_grace_seconds = live_snapshot_grace_seconds

    def get_cached_data(self, serial_number):
        return self._cache_entry

    def get_inverter_state(self, serial_number):
        return self._state


class StubNeonStore:
    def __init__(self, latest):
        self.enabled = True
        self._latest = latest
        self.fetch_latest_calls = 0
        self.inverters_list = []
        self.summary_samples = []
        self.available_months = []
        self.summary_calls = []
        self.daily_payload = None
        self.daily_payload_by_serial = {}
        self.daily_calls = []
        self.daily_diagnostics = {}

    def fetch_latest_reading(self, serial_number):
        self.fetch_latest_calls += 1
        return self._latest

    def fetch_inverters_list(self):
        return list(self.inverters_list)

    def fetch_energy_summary_samples(self, serial_number, since=None, until=None):
        self.summary_calls.append(
            {"serial_number": serial_number, "since": since, "until": until}
        )
        return list(self.summary_samples)

    def fetch_energy_summary_available_months(self, serial_number):
        return list(self.available_months)

    def fetch_daily_payload(self, serial_number, day, timezone_name=None):
        self.daily_calls.append(
            {
                "serial_number": serial_number,
                "day": day,
                "timezone_name": timezone_name,
            }
        )
        if serial_number in self.daily_payload_by_serial:
            return self.daily_payload_by_serial[serial_number]
        return self.daily_payload

    def inspect_daily_payload_window(self, serial_number, day, timezone_name=None):
        default_payload = {
            "serial_number": serial_number,
            "date": day.isoformat(),
            "timezone": timezone_name,
            "window_start": f"{day.isoformat()}T00:00:00+00:00",
            "window_end": f"{day.isoformat()}T23:59:59+00:00",
            "row_count": 0,
            "first_reading_at": None,
            "last_reading_at": None,
        }
        return {
            **default_payload,
            **self.daily_diagnostics.get(serial_number, {}),
        }


class StubCsvWriter:
    def __init__(self, latest=None):
        self._latest = latest

    def read_freshest(
        self,
        serial_number,
        timestamp_field="Data E Hora",
        timezone_name="Europe/Nicosia",
    ):
        return self._latest

    def get_all_data(self, serial_number):
        return []


class StubWatchPowerService:
    def __init__(self, serial_numbers, latest_by_serial=None):
        if isinstance(serial_numbers, str):
            serial_numbers = [serial_numbers]
        self.inverters = [
            {"serial_number": serial_number, "alias": f"Alias {index + 1}"}
            for index, serial_number in enumerate(serial_numbers)
        ]
        self.latest_by_serial = latest_by_serial or {}

    def get_latest_data(self, serial_number):
        return self.latest_by_serial.get(serial_number)


class AppReadPathTests(unittest.TestCase):
    def setUp(self):
        self.client = flask_app_module.app.test_client()
        self.original_scheduler = flask_app_module.scheduler
        self.original_neon_store = flask_app_module.neon_store
        self.original_csv_writer = flask_app_module.csv_writer
        self.original_watchpower_service = flask_app_module.watchpower_service
        self.original_threshold = flask_app_module.INVERTER_STALE_THRESHOLD_MINUTES

    def tearDown(self):
        flask_app_module.scheduler = self.original_scheduler
        flask_app_module.neon_store = self.original_neon_store
        flask_app_module.csv_writer = self.original_csv_writer
        flask_app_module.watchpower_service = self.original_watchpower_service
        flask_app_module.INVERTER_STALE_THRESHOLD_MINUTES = self.original_threshold

    def test_get_inverter_prefers_cache_for_warm_reads(self):
        serial_number = "INV-001"
        cache_entry = {
            "data": {"Data E Hora": "2026-03-19 11:10:00", "Load Status": "Load on"},
            "timestamp": "2026-03-19T09:10:30+00:00",
            "reading_at": datetime(2026, 3, 19, 9, 10, tzinfo=timezone.utc),
        }
        persisted = {
            "data": {"Data E Hora": "2026-03-19 11:15:00", "Load Status": "Load on"},
            "reading_at": datetime(2026, 3, 19, 9, 15, tzinfo=timezone.utc),
            "polled_at": datetime(2026, 3, 19, 9, 16, tzinfo=timezone.utc),
        }
        scheduler_state = {
            "last_successful_poll_at": "2026-03-19T09:16:00+00:00",
            "last_polled_at": "2026-03-19T09:16:00+00:00",
            "next_poll_due_at": "2026-03-19T09:20:00+00:00",
            "last_live_checked_at": None,
            "last_live_telemetry_at": None,
        }

        flask_app_module.scheduler = StubScheduler(
            cache_entry=cache_entry,
            state=scheduler_state,
            last_poll_time=datetime(2026, 3, 19, 9, 16, tzinfo=timezone.utc),
        )
        flask_app_module.neon_store = StubNeonStore(persisted)
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)

        first = self.client.get(f"/api/inverter/{serial_number}")
        second = self.client.get(f"/api/inverter/{serial_number}")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        first_payload = first.get_json()
        second_payload = second.get_json()

        self.assertEqual(first_payload["data"]["Data E Hora"], "2026-03-19 11:10:00")
        self.assertEqual(first_payload["data_source"], "cache")
        self.assertEqual(
            first_payload["latest_reading_at"], "2026-03-19T09:10:00+00:00"
        )
        self.assertEqual(first_payload["cached_at"], "2026-03-19T09:10:30+00:00")
        self.assertEqual(first_payload["status_source"], "persisted")
        self.assertEqual(second_payload["data"]["Data E Hora"], "2026-03-19 11:10:00")
        self.assertEqual(flask_app_module.neon_store.fetch_latest_calls, 0)

    def test_get_inverter_falls_back_to_persisted_reading_when_cache_empty(self):
        serial_number = "INV-001"
        persisted = {
            "data": {"Data E Hora": "2026-03-19 11:15:00", "Load Status": "Load on"},
            "reading_at": datetime(2026, 3, 19, 9, 15, tzinfo=timezone.utc),
            "polled_at": datetime(2026, 3, 19, 9, 16, tzinfo=timezone.utc),
        }
        scheduler_state = {
            "last_successful_poll_at": "2026-03-19T09:16:00+00:00",
            "last_polled_at": "2026-03-19T09:16:00+00:00",
            "next_poll_due_at": "2026-03-19T09:20:00+00:00",
            "last_live_checked_at": None,
            "last_live_telemetry_at": None,
        }

        flask_app_module.scheduler = StubScheduler(
            cache_entry=None,
            state=scheduler_state,
            last_poll_time=datetime(2026, 3, 19, 9, 16, tzinfo=timezone.utc),
        )
        flask_app_module.neon_store = StubNeonStore(persisted)
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)

        response = self.client.get(f"/api/inverter/{serial_number}")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["data"]["Data E Hora"], "2026-03-19 11:15:00")
        self.assertEqual(payload["data_source"], "neon")
        self.assertEqual(flask_app_module.neon_store.fetch_latest_calls, 1)

    def test_get_inverter_prefers_live_snapshot_for_status_when_fresher(self):
        serial_number = "INV-001"
        persisted = {
            "data": {
                "Data E Hora": "2026-03-19 11:45:00",
                "Load Status": "Load on",
                "Battery Capacity": "80",
                "Battery Voltage": "52",
                "AC Output Active Power": "1200",
                "PV1 Charging Power": "800",
                "PV2 Charging Power": "400",
            },
            "reading_at": datetime(2026, 3, 19, 9, 45, tzinfo=timezone.utc),
            "polled_at": datetime(2026, 3, 19, 9, 46, tzinfo=timezone.utc),
        }
        scheduler_state = {
            "last_successful_poll_at": "2026-03-19T09:46:00+00:00",
            "last_polled_at": "2026-03-19T09:52:00+00:00",
            "next_poll_due_at": "2026-03-19T09:50:00+00:00",
            "last_live_checked_at": datetime.now(timezone.utc).isoformat(),
            "last_live_telemetry_at": "2026-03-19T09:50:00+00:00",
        }

        flask_app_module.scheduler = StubScheduler(
            cache_entry=None,
            state=scheduler_state,
            last_poll_time=datetime.now(timezone.utc),
        )
        flask_app_module.neon_store = StubNeonStore(persisted)
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)

        response = self.client.get(f"/api/inverter/{serial_number}")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status_source"], "live_snapshot")
        self.assertEqual(
            payload["live_telemetry_timestamp"], "2026-03-19T09:50:00+00:00"
        )
        self.assertEqual(
            payload["persisted_telemetry_timestamp"], "2026-03-19T09:45:00+00:00"
        )
        self.assertEqual(payload["persistence_lag_minutes"], 5)
        self.assertEqual(
            payload["telemetry_health"]["telemetry_timestamp"],
            "2026-03-19T09:50:00+00:00",
        )

    def test_bulk_status_endpoint_returns_all_configured_inverters(self):
        now = datetime.now(timezone.utc)
        persisted = {
            "data": {
                "Data E Hora": "2026-03-19 11:45:00",
                "Load Status": "Load on",
                "Battery Capacity": "80",
                "Battery Voltage": "52",
                "AC Output Active Power": "1200",
                "PV1 Charging Power": "800",
                "PV2 Charging Power": "400",
            },
            "reading_at": datetime(2026, 3, 19, 9, 45, tzinfo=timezone.utc),
            "polled_at": datetime(2026, 3, 19, 9, 46, tzinfo=timezone.utc),
        }
        scheduler_state = {
            "last_successful_poll_at": "2026-03-19T09:46:00+00:00",
            "last_polled_at": "2026-03-19T09:46:00+00:00",
            "next_poll_due_at": "2026-03-19T09:50:00+00:00",
            "last_live_checked_at": now.isoformat(),
            "last_live_telemetry_at": "2026-03-19T09:50:00+00:00",
        }

        flask_app_module.scheduler = StubScheduler(
            cache_entry=None,
            state=scheduler_state,
            last_poll_time=now,
        )
        flask_app_module.neon_store = StubNeonStore(persisted)
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(
            ["INV-001", "INV-002"]
        )

        response = self.client.get("/api/inverters/status")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["count"], 2)
        self.assertEqual(
            sorted(entry["serial_number"] for entry in payload["inverters"]),
            ["INV-001", "INV-002"],
        )
        self.assertTrue(
            all(
                entry["status_source"] == "live_snapshot"
                for entry in payload["inverters"]
            )
        )

    def test_inverters_endpoint_prefers_neon_roster_and_merges_config_fields(self):
        flask_app_module.watchpower_service = StubWatchPowerService(["INV-001"])
        flask_app_module.neon_store = StubNeonStore(latest=None)
        flask_app_module.neon_store.inverters_list = [
            {
                "serial_number": "INV-001",
                "alias": "OG-002",
                "description": "Baskent",
                "system_type": "offgrid",
                "username": "Baskent",
                "location": "",
                "wifi_pn": "WIFI-001",
                "device_code": 2449,
                "device_address": 1,
            }
        ]

        original_loader = flask_app_module._load_inverters_config
        flask_app_module._load_inverters_config = lambda: [
            {
                "serial_number": "INV-001",
                "alias": "OG-003",
                "description": "Baskent",
                "system_type": "offgrid",
                "username": "Baskent",
                "location": "Lefkosa",
                "wifi_pn": "WIFI-001",
                "device_code": 2449,
                "device_address": 1,
                "password": "secret",
            }
        ]

        try:
            response = self.client.get("/api/inverters")
        finally:
            flask_app_module._load_inverters_config = original_loader

        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["inverters"][0]["alias"], "OG-002")
        self.assertEqual(payload["inverters"][0]["description"], "Baskent")
        self.assertEqual(payload["inverters"][0]["location"], "Lefkosa")
        self.assertEqual(payload["inverters"][0]["password"], "secret")

    def test_build_telemetry_health_obeys_staleness_threshold_boundaries(self):
        flask_app_module.INVERTER_STALE_THRESHOLD_MINUTES = 9
        now = datetime(2026, 3, 19, 9, 21, tzinfo=timezone.utc)

        online = flask_app_module._build_telemetry_health(
            {"Data E Hora": "2026-03-19 11:15:00"},
            now_utc=now,
        )
        offline = flask_app_module._build_telemetry_health(
            {"Data E Hora": "2026-03-19 11:10:00"},
            now_utc=now,
        )

        self.assertEqual(online["state"], "online")
        self.assertEqual(online["stale_minutes"], 6)
        self.assertEqual(offline["state"], "offline")
        self.assertEqual(offline["stale_minutes"], 11)

    def test_energy_summary_endpoint_returns_server_side_summary(self):
        serial_number = "INV-001"
        now = datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc)
        neon_store = StubNeonStore(latest=None)
        neon_store.summary_samples = [
            {
                "reading_at": now,
                "load_power_w": 1000,
                "pv_power_w": 600,
                "grid_power_w": 400,
                "raw_payload": {
                    "Battery Voltage": "50",
                    "Battery Charging Current": "0",
                    "Battery Discharge Current": "0",
                },
            },
            {
                "reading_at": now.replace(minute=30),
                "load_power_w": 1000,
                "pv_power_w": 600,
                "grid_power_w": 400,
                "raw_payload": {
                    "Battery Voltage": "50",
                    "Battery Charging Current": "0",
                    "Battery Discharge Current": "0",
                },
            },
        ]

        flask_app_module.scheduler = None
        flask_app_module.neon_store = neon_store
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)

        response = self.client.get(
            f"/api/inverter/{serial_number}/energy-summary",
            query_string={
                "from": "2026-03-01T00:00:00+00:00",
                "to": "2026-03-31T23:59:59+00:00",
            },
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["data"]["inverterId"], serial_number)
        self.assertEqual(payload["data"]["monthKey"], "2026-03")
        self.assertEqual(payload["sampleCount"], 2)
        self.assertEqual(payload["intervalCount"], 1)
        self.assertTrue(payload["hasHistory"])
        self.assertEqual(payload["sourceUsed"], "neon")
        self.assertEqual(payload["insufficientReason"], None)
        self.assertEqual(len(payload["data"]["dailyRows"]), 31)
        self.assertEqual(
            payload["data"]["dailyRows"][18]["period"], "2026-03-19"
        )
        self.assertEqual(payload["data"]["dailyRows"][18]["loadKwh"], 0.5)
        self.assertEqual(payload["data"]["dailyRows"][18]["solarPvKwh"], 0.3)
        self.assertEqual(payload["data"]["dailyRows"][18]["gridUsedKwh"], 0.2)
        self.assertEqual(neon_store.summary_calls[0]["serial_number"], serial_number)

    def test_energy_summary_endpoint_requires_timeframe(self):
        serial_number = "INV-001"
        flask_app_module.scheduler = None
        flask_app_module.neon_store = StubNeonStore(latest=None)
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)

        response = self.client.get(f"/api/inverter/{serial_number}/energy-summary")
        payload = response.get_json()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["error"], "Missing required query parameter: from")

    def test_energy_summary_months_endpoint_returns_available_months(self):
        serial_number = "INV-001"
        neon_store = StubNeonStore(latest=None)
        neon_store.available_months = ["2026-05", "2026-04", "2026-02"]
        flask_app_module.scheduler = None
        flask_app_module.neon_store = neon_store
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)

        response = self.client.get(
            f"/api/inverter/{serial_number}/energy-summary/months"
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["data"]["inverterId"], serial_number)
        self.assertEqual(payload["data"]["months"], neon_store.available_months)
        self.assertEqual(payload["count"], 3)

    def test_dashboard_bootstrap_uses_requested_timezone_for_daily_history(self):
        serial_number = "INV-001"
        persisted = {
            "data": {
                "Data E Hora": "2026-04-19 11:15:00",
                "Load Status": "Load on",
            },
            "reading_at": datetime(2026, 4, 19, 9, 15, tzinfo=timezone.utc),
            "polled_at": datetime(2026, 4, 19, 9, 16, tzinfo=timezone.utc),
        }
        neon_store = StubNeonStore(persisted)
        neon_store.daily_payload = {
            "titles": ["Data E Hora", "PV1 Charging Power"],
            "rows": [["2026-04-18 00:30:00", "800"]],
            "date": "2026-04-18",
            "row_count": 1,
        }

        flask_app_module.scheduler = None
        flask_app_module.neon_store = neon_store
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)
        flask_app_module.watchpower_service.inverters = [
            {
                "serial_number": serial_number,
                "alias": "Demo User",
                "description": "Demo User",
                "system_type": "Hybrid",
            }
        ]

        response = self.client.get(
            "/api/dashboard/user/demo-user/bootstrap",
            query_string={
                "date": "2026-04-18",
                "timezone": "America/Los_Angeles",
            },
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["overview"]["date"], "2026-04-18")
        self.assertEqual(payload["overview"]["timezone"], "America/Los_Angeles")
        self.assertEqual(len(neon_store.daily_calls), 1)
        self.assertEqual(neon_store.daily_calls[0]["serial_number"], serial_number)
        self.assertEqual(neon_store.daily_calls[0]["day"].isoformat(), "2026-04-18")
        self.assertEqual(
            neon_store.daily_calls[0]["timezone_name"],
            "America/Los_Angeles",
        )

    def test_dashboard_chart_history_requires_date(self):
        serial_number = "INV-001"
        flask_app_module.scheduler = None
        flask_app_module.neon_store = StubNeonStore(latest=None)
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)
        flask_app_module.watchpower_service.inverters = [
            {
                "serial_number": serial_number,
                "alias": "Demo User",
                "description": "Demo User",
                "system_type": "Hybrid",
            }
        ]

        response = self.client.get("/api/dashboard/user/demo-user/chart-history")
        payload = response.get_json()

        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["error"], "Missing required query parameter: date")

    def test_dashboard_chart_history_uses_requested_timezone_for_daily_history(self):
        serial_number = "INV-001"
        neon_store = StubNeonStore(latest=None)
        neon_store.daily_payload = {
            "titles": ["Data E Hora", "PV1 Charging Power"],
            "rows": [["2026-04-18 00:30:00", "800"]],
            "date": "2026-04-18",
            "row_count": 1,
        }

        flask_app_module.scheduler = None
        flask_app_module.neon_store = neon_store
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)
        flask_app_module.watchpower_service.inverters = [
            {
                "serial_number": serial_number,
                "alias": "Demo User",
                "description": "Demo User",
                "system_type": "Hybrid",
            }
        ]

        response = self.client.get(
            "/api/dashboard/user/demo-user/chart-history",
            query_string={
                "date": "2026-04-18",
                "timezone": "America/Los_Angeles",
            },
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["success"])
        self.assertEqual(payload["history"]["date"], "2026-04-18")
        self.assertEqual(payload["history"]["timezone"], "America/Los_Angeles")
        self.assertNotIn("overview", payload)
        self.assertEqual(len(neon_store.daily_calls), 1)
        self.assertEqual(neon_store.daily_calls[0]["serial_number"], serial_number)
        self.assertEqual(neon_store.daily_calls[0]["day"].isoformat(), "2026-04-18")
        self.assertEqual(
            neon_store.daily_calls[0]["timezone_name"],
            "America/Los_Angeles",
        )

    def test_dashboard_chart_history_rejects_out_of_range_dates(self):
        serial_number = "INV-001"
        flask_app_module.scheduler = None
        flask_app_module.neon_store = StubNeonStore(latest=None)
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)
        flask_app_module.watchpower_service.inverters = [
            {
                "serial_number": serial_number,
                "alias": "Demo User",
                "description": "Demo User",
                "system_type": "Hybrid",
            }
        ]

        future_response = self.client.get(
            "/api/dashboard/user/demo-user/chart-history",
            query_string={"date": "3026-04-18"},
        )
        past_response = self.client.get(
            "/api/dashboard/user/demo-user/chart-history",
            query_string={"date": "2020-04-18"},
        )

        self.assertEqual(future_response.status_code, 400)
        self.assertEqual(past_response.status_code, 400)
        self.assertIn("Date out of range", future_response.get_json()["error"])
        self.assertIn("Date out of range", past_response.get_json()["error"])

    def test_dashboard_chart_history_falls_back_on_invalid_timezone(self):
        serial_number = "INV-001"
        neon_store = StubNeonStore(latest=None)
        neon_store.daily_payload = {
            "titles": ["Data E Hora", "PV1 Charging Power"],
            "rows": [["2026-04-18 00:30:00", "800"]],
            "date": "2026-04-18",
            "row_count": 1,
        }

        flask_app_module.scheduler = None
        flask_app_module.neon_store = neon_store
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_number)
        flask_app_module.watchpower_service.inverters = [
            {
                "serial_number": serial_number,
                "alias": "Demo User",
                "description": "Demo User",
                "system_type": "Hybrid",
            }
        ]

        response = self.client.get(
            "/api/dashboard/user/demo-user/chart-history",
            query_string={
                "date": "2026-04-18",
                "timezone": "Mars/Olympus",
            },
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            payload["history"]["timezone"], flask_app_module.WATCHPOWER_TIMEZONE
        )
        self.assertEqual(
            neon_store.daily_calls[0]["timezone_name"],
            flask_app_module.WATCHPOWER_TIMEZONE,
        )

    def test_dashboard_chart_history_returns_mixed_daily_availability(self):
        serial_numbers = ["INV-001", "INV-002"]
        neon_store = StubNeonStore(latest=None)
        neon_store.daily_payload_by_serial = {
            "INV-001": {
                "titles": ["Data E Hora", "PV1 Charging Power"],
                "rows": [["2026-04-18 00:30:00", "800"]],
                "date": "2026-04-18",
                "row_count": 1,
            },
            "INV-002": None,
        }
        neon_store.daily_diagnostics = {
            "INV-001": {
                "row_count": 1,
                "first_reading_at": "2026-04-18T00:30:00+00:00",
                "last_reading_at": "2026-04-18T00:30:00+00:00",
            },
            "INV-002": {
                "row_count": 0,
            },
        }

        flask_app_module.scheduler = None
        flask_app_module.neon_store = neon_store
        flask_app_module.csv_writer = StubCsvWriter()
        flask_app_module.watchpower_service = StubWatchPowerService(serial_numbers)
        flask_app_module.watchpower_service.inverters = [
            {
                "serial_number": "INV-001",
                "alias": "Demo User",
                "description": "Demo User",
                "system_type": "Hybrid",
            },
            {
                "serial_number": "INV-002",
                "alias": "Demo User",
                "description": "Demo User",
                "system_type": "Hybrid",
            },
        ]

        response = self.client.get(
            "/api/dashboard/user/demo-user/chart-history",
            query_string={"date": "2026-04-18"},
        )
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(payload["history"]["dailyById"]["INV-001"])
        self.assertIsNone(payload["history"]["dailyById"]["INV-002"])
        self.assertEqual(
            payload["history"]["diagnosticsById"]["INV-001"]["rowCount"], 1
        )
        self.assertTrue(payload["history"]["diagnosticsById"]["INV-001"]["hasPayload"])
        self.assertEqual(
            payload["history"]["diagnosticsById"]["INV-002"]["rowCount"], 0
        )
        self.assertFalse(payload["history"]["diagnosticsById"]["INV-002"]["hasPayload"])
        self.assertEqual(
            payload["history"]["dailyErrorsById"]["INV-002"],
            "No telemetry recorded for 2026-04-18.",
        )


if __name__ == "__main__":
    unittest.main()
