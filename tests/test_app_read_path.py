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

    def fetch_latest_reading(self, serial_number):
        return self._latest


class StubCsvWriter:
    def __init__(self, latest=None):
        self._latest = latest

    def read_freshest(self, serial_number, timestamp_field="Data E Hora", timezone_name="Europe/Nicosia"):
        return self._latest


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

    def test_get_inverter_prefers_persisted_latest_reading_over_cache(self):
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

        self.assertEqual(first_payload["data"]["Data E Hora"], "2026-03-19 11:15:00")
        self.assertEqual(first_payload["data_source"], "neon")
        self.assertEqual(first_payload["latest_reading_at"], "2026-03-19T09:15:00+00:00")
        self.assertEqual(first_payload["cached_at"], "2026-03-19T09:16:00+00:00")
        self.assertEqual(first_payload["status_source"], "persisted")
        self.assertEqual(second_payload["data"]["Data E Hora"], "2026-03-19 11:15:00")

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
        self.assertEqual(payload["live_telemetry_timestamp"], "2026-03-19T09:50:00+00:00")
        self.assertEqual(
            payload["persisted_telemetry_timestamp"], "2026-03-19T09:45:00+00:00"
        )
        self.assertEqual(payload["persistence_lag_minutes"], 5)
        self.assertEqual(payload["telemetry_health"]["telemetry_timestamp"], "2026-03-19T09:50:00+00:00")

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
            all(entry["status_source"] == "live_snapshot" for entry in payload["inverters"])
        )

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

if __name__ == "__main__":
    unittest.main()
