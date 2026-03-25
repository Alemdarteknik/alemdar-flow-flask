import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from services.scheduler import PollingScheduler
from services.watchpower_service import WatchPowerService


class StubCsvWriter:
    def __init__(self, freshest_by_serial=None):
        self.freshest_by_serial = freshest_by_serial or {}
        self.writes = []

    def write_with_deduplication(self, serial_number, data):
        self.writes.append((serial_number, data))
        return True

    def read_freshest(self, serial_number, timestamp_field="Data E Hora", timezone_name="Europe/Nicosia"):
        reading_at = self.freshest_by_serial.get(serial_number)
        if reading_at is None:
            return None
        return {
            "data": {
                "Data E Hora": reading_at.astimezone(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "serial_number": serial_number,
            },
            "reading_at": reading_at,
        }


class StubWatchPowerService:
    def __init__(self, responses):
        self.responses = responses
        self.inverters = [
            {"serial_number": serial_number, "alias": serial_number}
            for serial_number in responses.keys()
        ]
        self.calls = []

    def get_latest_data(self, serial_number):
        self.calls.append(serial_number)
        return self.responses[serial_number]


class StubConnection:
    def __init__(self):
        self.commit_calls = 0

    def commit(self):
        self.commit_calls += 1


class StubConnectionContext:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False


class StubNeonStore:
    def __init__(self):
        self.enabled = True
        self.connection_object = StubConnection()
        self.persist_calls = []
        self.audit_calls = []
        self.connection_calls = 0
        self.upsert_calls = []

    def connection(self):
        self.connection_calls += 1
        return StubConnectionContext(self.connection_object)

    def persist_reading(self, serial_number, raw_data, source, conn=None):
        self.persist_calls.append(
            {
                "serial_number": serial_number,
                "raw_data": raw_data,
                "source": source,
                "conn": conn,
            }
        )
        return True

    def record_poll_outcome(
        self, serial_number, alias, status, attempts, error_text=None, conn=None
    ):
        self.audit_calls.append(
            {
                "serial_number": serial_number,
                "alias": alias,
                "status": status,
                "attempts": attempts,
                "error_text": error_text,
                "conn": conn,
            }
        )

    def upsert_inverter(self, inverter_config, conn=None):
        self.upsert_calls.append({"inverter_config": inverter_config, "conn": conn})


class WatchPowerLatestDataTests(unittest.TestCase):
    def test_get_latest_data_uses_max_timestamp_not_last_row(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as config_file:
            json.dump(
                [
                    {
                        "serial_number": "INV-001",
                        "wifi_pn": "wifi",
                        "device_code": 1,
                        "device_address": 1,
                        "system_type": "offgrid",
                        "alias": "Alpha",
                        "username": "user",
                        "password": "pass",
                    }
                ],
                config_file,
            )
            config_path = config_file.name

        try:
            service = WatchPowerService(
                config_path=config_path, timezone_name="Europe/Nicosia"
            )
            service._get_api_for_inverter = lambda inverter_config: object()
            service._fetch_daily_data_with_retries = lambda **kwargs: {
                "dat": {
                    "title": [
                        {"title": "Data E Hora"},
                        {"title": "AC Output Active Power"},
                    ],
                    "row": [
                        {"field": ["2026-03-19 11:10:00", "100"]},
                        {"field": ["2026-03-19 11:15:00", "120"]},
                        {"field": ["2026-03-19 11:12:00", "110"]},
                    ],
                }
            }

            result = service.get_latest_data("INV-001")

            self.assertIsNotNone(result)
            self.assertEqual(result["data"]["Data E Hora"], "2026-03-19 11:15:00")
            self.assertEqual(result["data"]["AC Output Active Power"], "120")
            self.assertEqual(
                result["reading_at"].isoformat(), "2026-03-19T09:15:00+00:00"
            )
        finally:
            os.unlink(config_path)

    def test_get_daily_raw_sorts_rows_by_timestamp(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as config_file:
            json.dump(
                [
                    {
                        "serial_number": "INV-001",
                        "wifi_pn": "wifi",
                        "device_code": 1,
                        "device_address": 1,
                        "system_type": "offgrid",
                        "alias": "Alpha",
                        "username": "user",
                        "password": "pass",
                    }
                ],
                config_file,
            )
            config_path = config_file.name

        try:
            service = WatchPowerService(
                config_path=config_path, timezone_name="Europe/Nicosia"
            )
            service._get_api_for_inverter = lambda inverter_config: object()
            service._fetch_daily_data_with_retries = lambda **kwargs: {
                "dat": {
                    "title": [
                        {"title": "Data E Hora"},
                        {"title": "AC Output Active Power"},
                    ],
                    "row": [
                        {"field": ["2026-03-19 11:48:00", "120"]},
                        {"field": ["2026-03-19 11:45:00", "100"]},
                        {"field": ["2026-03-19 11:50:00", "130"]},
                    ],
                }
            }

            result = service.get_daily_raw("INV-001")

            self.assertIsNotNone(result)
            self.assertEqual(
                [row[0] for row in result["rows"]],
                [
                    "2026-03-19 11:45:00",
                    "2026-03-19 11:48:00",
                    "2026-03-19 11:50:00",
                ],
            )
        finally:
            os.unlink(config_path)


class PollingSchedulerTests(unittest.TestCase):
    def test_initialize_runtime_state_preserves_per_inverter_offsets(self):
        now = datetime.now(timezone.utc)
        csv_writer = StubCsvWriter(
            freshest_by_serial={
                "INV-A": now - timedelta(minutes=6),
                "INV-B": now - timedelta(minutes=3),
            }
        )
        scheduler = PollingScheduler(
            watchpower_service=StubWatchPowerService(
                {"INV-A": None, "INV-B": None}
            ),
            csv_writer=csv_writer,
            poll_interval_minutes=5,
            poll_retry_attempts=1,
            poll_retry_backoff_seconds=0,
            tick_seconds=1,
        )

        scheduler.initialize_runtime_state()

        state_a = scheduler.get_inverter_state("INV-A")
        state_b = scheduler.get_inverter_state("INV-B")

        self.assertNotEqual(state_a["next_poll_due_at"], state_b["next_poll_due_at"])
        self.assertLess(state_b["next_poll_due_at"], state_a["next_poll_due_at"])
        self.assertIsNotNone(state_a["next_live_snapshot_due_at"])
        self.assertIsNotNone(state_b["next_live_snapshot_due_at"])

    def test_poll_due_inverters_only_polls_due_serials(self):
        now = datetime.now(timezone.utc)
        response_a = {
            "data": {"Data E Hora": (now - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")},
            "reading_at": now - timedelta(minutes=1),
            "inverter_config": {"serial_number": "INV-A"},
        }
        response_b = {
            "data": {"Data E Hora": (now - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")},
            "reading_at": now - timedelta(minutes=1),
            "inverter_config": {"serial_number": "INV-B"},
        }
        watchpower_service = StubWatchPowerService(
            {"INV-A": response_a, "INV-B": response_b}
        )
        scheduler = PollingScheduler(
            watchpower_service=watchpower_service,
            csv_writer=StubCsvWriter(),
            poll_interval_minutes=5,
            poll_retry_attempts=1,
            poll_retry_backoff_seconds=0,
            tick_seconds=1,
        )
        scheduler.inverter_states = {
            "INV-A": {
                "last_successful_reading_at": now - timedelta(minutes=10),
                "last_successful_poll_at": None,
                "last_polled_at": None,
                "next_poll_due_at": now - timedelta(seconds=1),
                "last_status": "ready",
                "last_error": None,
                "last_live_checked_at": None,
                "last_live_telemetry_at": None,
                "last_live_status": "pending",
                "last_live_error": None,
                "next_live_snapshot_due_at": now + timedelta(minutes=1),
                "is_polling": False,
            },
            "INV-B": {
                "last_successful_reading_at": now - timedelta(minutes=2),
                "last_successful_poll_at": None,
                "last_polled_at": None,
                "next_poll_due_at": now + timedelta(minutes=3),
                "last_status": "ready",
                "last_error": None,
                "last_live_checked_at": None,
                "last_live_telemetry_at": None,
                "last_live_status": "pending",
                "last_live_error": None,
                "next_live_snapshot_due_at": now + timedelta(minutes=3),
                "is_polling": False,
            },
        }

        results = scheduler.refresh_due_inverters(now=now)

        self.assertIn("INV-A", results)
        self.assertNotIn("INV-B", results)
        self.assertEqual(watchpower_service.calls, ["INV-A"])

    def test_combined_due_work_fetches_once_for_poll_and_live_snapshot(self):
        now = datetime.now(timezone.utc)
        response = {
            "data": {"Data E Hora": now.strftime("%Y-%m-%d %H:%M:%S")},
            "reading_at": now,
            "inverter_config": {"serial_number": "INV-A"},
        }
        watchpower_service = StubWatchPowerService({"INV-A": response})
        scheduler = PollingScheduler(
            watchpower_service=watchpower_service,
            csv_writer=StubCsvWriter(),
            poll_interval_minutes=5,
            poll_retry_attempts=1,
            poll_retry_backoff_seconds=0,
            tick_seconds=1,
        )
        scheduler.inverter_states = {
            "INV-A": {
                "last_successful_reading_at": now - timedelta(minutes=10),
                "last_successful_poll_at": None,
                "last_polled_at": None,
                "next_poll_due_at": now - timedelta(seconds=1),
                "last_status": "ready",
                "last_error": None,
                "last_live_checked_at": None,
                "last_live_telemetry_at": None,
                "last_live_status": "pending",
                "last_live_error": None,
                "next_live_snapshot_due_at": now - timedelta(seconds=1),
                "is_polling": False,
            }
        }

        results = scheduler.refresh_due_inverters(now=now)
        state = scheduler.get_inverter_state("INV-A")

        self.assertEqual(watchpower_service.calls, ["INV-A"])
        self.assertEqual(results["INV-A"]["status"], "success")
        self.assertEqual(state["last_status"], "success")
        self.assertEqual(state["last_live_status"], "success")

    def test_no_data_refresh_keeps_anchor_based_next_due(self):
        anchor = datetime.now(timezone.utc) - timedelta(minutes=8)
        watchpower_service = StubWatchPowerService({"INV-A": None})
        scheduler = PollingScheduler(
            watchpower_service=watchpower_service,
            csv_writer=StubCsvWriter(),
            poll_interval_minutes=5,
            poll_retry_attempts=1,
            poll_retry_backoff_seconds=0,
            tick_seconds=1,
        )
        scheduler.inverter_states = {
            "INV-A": {
                "last_successful_reading_at": anchor,
                "last_successful_poll_at": None,
                "last_polled_at": None,
                "next_poll_due_at": anchor + timedelta(minutes=5),
                "last_status": "ready",
                "last_error": None,
                "last_live_checked_at": None,
                "last_live_telemetry_at": None,
                "last_live_status": "pending",
                "last_live_error": None,
                "next_live_snapshot_due_at": anchor + timedelta(minutes=1),
                "is_polling": False,
            }
        }

        result = scheduler.refresh_inverter(
            "INV-A", persist_reading=True, update_live_snapshot=True
        )
        state = scheduler.get_inverter_state("INV-A")

        self.assertEqual(result["status"], "no_data")
        self.assertEqual(
            state["last_successful_reading_at"], anchor.isoformat()
        )
        self.assertEqual(state["last_status"], "no_data")
        self.assertGreater(
            datetime.fromisoformat(state["next_poll_due_at"]),
            result["checked_at"],
        )

    def test_successful_poll_persists_reading_and_audit_in_one_connection(self):
        now = datetime.now(timezone.utc)
        response = {
            "data": {"Data E Hora": now.strftime("%Y-%m-%d %H:%M:%S")},
            "reading_at": now,
            "inverter_config": {"serial_number": "INV-A"},
        }
        neon_store = StubNeonStore()
        scheduler = PollingScheduler(
            watchpower_service=StubWatchPowerService({"INV-A": response}),
            csv_writer=StubCsvWriter(),
            neon_store=neon_store,
            poll_interval_minutes=5,
            poll_retry_attempts=1,
            poll_retry_backoff_seconds=0,
            tick_seconds=1,
        )

        result = scheduler.refresh_inverter(
            "INV-A", persist_reading=True, update_live_snapshot=True
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(neon_store.connection_calls, 1)
        self.assertEqual(len(neon_store.persist_calls), 1)
        self.assertEqual(len(neon_store.audit_calls), 1)
        self.assertEqual(neon_store.connection_object.commit_calls, 1)
        self.assertIs(neon_store.persist_calls[0]["conn"], neon_store.connection_object)
        self.assertIs(neon_store.audit_calls[0]["conn"], neon_store.connection_object)
        self.assertEqual(neon_store.upsert_calls, [])


if __name__ == "__main__":
    unittest.main()
