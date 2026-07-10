import unittest
from datetime import datetime, timezone

from services.energy_summary import build_energy_summary


class EnergySummaryTests(unittest.TestCase):
    def test_empty_samples_return_no_history(self):
        result = build_energy_summary(
            "INV-001",
            [],
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 4, 1, tzinfo=timezone.utc),
        )

        self.assertFalse(result.has_history)
        self.assertEqual(result.reason, "no_samples")
        self.assertIsNone(result.summary)

    def test_single_timestamped_point_returns_no_history(self):
        point = {
            "reading_at": datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc),
            "load_power_w": 1000,
            "pv_power_w": 500,
            "grid_power_w": 500,
            "raw_payload": {},
        }

        result = build_energy_summary(
            "INV-001",
            [point],
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 4, 1, tzinfo=timezone.utc),
        )
        self.assertFalse(result.has_history)
        self.assertEqual(result.reason, "only_one_point")
        self.assertEqual(result.timestamped_point_count, 1)

    def test_non_positive_intervals_return_no_history(self):
        timestamp = datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc)
        samples = [
            {
                "reading_at": timestamp,
                "load_power_w": 1000,
                "pv_power_w": 500,
                "grid_power_w": 500,
                "raw_payload": {},
            },
            {
                "reading_at": timestamp,
                "load_power_w": 1200,
                "pv_power_w": 600,
                "grid_power_w": 600,
                "raw_payload": {},
            },
        ]

        result = build_energy_summary(
            "INV-001",
            samples,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 4, 1, tzinfo=timezone.utc),
        )

        self.assertFalse(result.has_history)
        self.assertEqual(result.reason, "no_positive_intervals")
        self.assertIsNone(result.summary)

    def test_valid_samples_return_summary(self):
        samples = [
            {
                "reading_at": datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc),
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
                "reading_at": datetime(2026, 3, 19, 9, 30, tzinfo=timezone.utc),
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

        result = build_energy_summary(
            "INV-001",
            samples,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 4, 1, tzinfo=timezone.utc),
        )

        self.assertTrue(result.has_history)
        self.assertIsNone(result.reason)
        self.assertEqual(result.interval_count, 1)
        self.assertIsNotNone(result.summary)
        self.assertEqual(result.summary["inverterId"], "INV-001")
        self.assertEqual(result.summary["monthKey"], "2026-03")
        self.assertEqual(len(result.summary["dailyRows"]), 31)


if __name__ == "__main__":
    unittest.main()
