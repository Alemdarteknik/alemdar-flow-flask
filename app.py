"""
Alemdar Flow Flask API
Backend service for WatchPower API integration
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from services.energy_summary import build_energy_summary
from services.neon_store import NeonStore
from services.reading_resolver import resolve_latest_reading
from services.scheduler import PollingScheduler
from services.watchpower_service import WatchPowerService
from utils.csv_writer import CSVWriter
from utils.telemetry_time import parse_watchpower_timestamp

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _read_positive_int_env(name, default):
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        return max(1, int(raw_value))
    except ValueError:
        logger.warning("Invalid %s=%r. Falling back to %s.", name, raw_value, default)
        return default


WATCHPOWER_TIMEZONE = os.getenv("WATCHPOWER_TIMEZONE", "Europe/Nicosia")
INVERTER_STALE_THRESHOLD_MINUTES = _read_positive_int_env(
    "INVERTER_STALE_THRESHOLD_MINUTES", 8
)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Global service instances
watchpower_service = None
csv_writer = None
scheduler = None
neon_store = None
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "inverters.json")


def _build_telemetry_health_from_timestamp(parsed_timestamp, now_utc=None):
    base_payload = {
        "threshold_minutes": INVERTER_STALE_THRESHOLD_MINUTES,
        "timezone": WATCHPOWER_TIMEZONE,
        "telemetry_timestamp": (
            parsed_timestamp.isoformat() if parsed_timestamp is not None else None
        ),
    }

    if parsed_timestamp is None:
        return {
            **base_payload,
            "state": "offline",
            "reason": (
                "This inverter is not connected to the internet. "
                "No valid recent inverter data is available."
            ),
            "stale_minutes": None,
        }

    current_time = now_utc or datetime.now(timezone.utc)
    elapsed_seconds = max(0, int((current_time - parsed_timestamp).total_seconds()))
    stale_minutes = elapsed_seconds // 60

    if elapsed_seconds >= INVERTER_STALE_THRESHOLD_MINUTES * 60:
        return {
            **base_payload,
            "state": "offline",
            "reason": (
                "This inverter is not connected to the internet. "
                f"No new data has been received for {stale_minutes} minutes."
            ),
            "stale_minutes": stale_minutes,
        }
    return {
        **base_payload,
        "state": "online",
        "reason": "Telemetry is current.",
        "stale_minutes": stale_minutes,
    }


def _build_telemetry_health(raw_data, now_utc=None):
    telemetry_timestamp = (
        raw_data.get("Data E Hora") if isinstance(raw_data, dict) else None
    )
    parsed_timestamp = parse_watchpower_timestamp(
        telemetry_timestamp, WATCHPOWER_TIMEZONE
    )
    return _build_telemetry_health_from_timestamp(
        parsed_timestamp=parsed_timestamp,
        now_utc=now_utc,
    )


def _parse_optional_timestamp(value):
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _calculate_persistence_lag_minutes(live_telemetry_at, persisted_telemetry_at):
    if live_telemetry_at is None or persisted_telemetry_at is None:
        return None
    lag_seconds = int((live_telemetry_at - persisted_telemetry_at).total_seconds())
    if lag_seconds <= 0:
        return 0
    return lag_seconds // 60


def _resolve_status_timestamp(latest, scheduler_state):
    now_utc = datetime.now(timezone.utc)
    persisted_telemetry_at = latest.get("reading_at") if latest else None
    live_checked_at = _parse_optional_timestamp(
        scheduler_state.get("last_live_checked_at")
    )
    live_telemetry_at = _parse_optional_timestamp(
        scheduler_state.get("last_live_telemetry_at")
    )
    live_grace_seconds = (
        scheduler.live_snapshot_grace_seconds if scheduler else 120
    )
    live_is_recent = (
        live_checked_at is not None
        and int((now_utc - live_checked_at).total_seconds()) <= live_grace_seconds
    )

    if live_is_recent and live_telemetry_at is not None:
        return {
            "status_source": "live_snapshot",
            "status_timestamp": live_telemetry_at,
            "live_checked_at": live_checked_at,
            "live_telemetry_at": live_telemetry_at,
            "persisted_telemetry_at": persisted_telemetry_at,
        }

    return {
        "status_source": "persisted",
        "status_timestamp": persisted_telemetry_at,
        "live_checked_at": live_checked_at,
        "live_telemetry_at": live_telemetry_at,
        "persisted_telemetry_at": persisted_telemetry_at,
    }


def _build_inverter_response(serial_number, inverter_config):
    latest = resolve_latest_reading(
        serial_number=serial_number,
        timezone_name=WATCHPOWER_TIMEZONE,
        neon_store=neon_store,
        csv_writer=csv_writer,
        cache_entry=scheduler.get_cached_data(serial_number) if scheduler else None,
        prefer_cache=True,
    )
    scheduler_state = scheduler.get_inverter_state(serial_number) if scheduler else {}
    scheduler_state = scheduler_state or {}
    resolved_status = _resolve_status_timestamp(latest=latest, scheduler_state=scheduler_state)

    latest_data = latest.get("data") if latest else None
    status_timestamp = resolved_status["status_timestamp"]
    if latest_data is None and status_timestamp is None:
        return None

    cached_at = None
    if latest:
        cached_at = latest.get("cached_at")
    if not cached_at:
        cached_at = (
            scheduler_state.get("last_successful_poll_at")
            or scheduler_state.get("last_polled_at")
            or (
                resolved_status["live_checked_at"].isoformat()
                if resolved_status["live_checked_at"]
                else None
            )
        )

    latest_reading_at = latest.get("reading_at") if latest else None
    live_telemetry_at = resolved_status["live_telemetry_at"]
    persisted_telemetry_at = resolved_status["persisted_telemetry_at"]

    return {
        "success": True,
        "serial_number": serial_number,
        "data": latest_data,
        "telemetry_health": _build_telemetry_health_from_timestamp(
            parsed_timestamp=status_timestamp
        ),
        "cached_at": cached_at,
        "last_poll": (
            scheduler.last_poll_time.isoformat() if scheduler and scheduler.last_poll_time else None
        ),
        "latest_reading_at": (
            latest_reading_at.isoformat() if latest_reading_at else None
        ),
        "last_successful_poll_at": scheduler_state.get("last_successful_poll_at"),
        "next_poll_due_at": scheduler_state.get("next_poll_due_at"),
        "status_source": resolved_status["status_source"],
        "data_source": latest.get("data_source") if latest else None,
        "live_telemetry_timestamp": (
            live_telemetry_at.isoformat() if live_telemetry_at else None
        ),
        "live_checked_at": (
            resolved_status["live_checked_at"].isoformat()
            if resolved_status["live_checked_at"]
            else None
        ),
        "persisted_telemetry_timestamp": (
            persisted_telemetry_at.isoformat() if persisted_telemetry_at else None
        ),
        "persistence_lag_minutes": _calculate_persistence_lag_minutes(
            live_telemetry_at=live_telemetry_at,
            persisted_telemetry_at=persisted_telemetry_at,
        ),
        "inverter_config": (latest.get("inverter_config") if latest else None)
        or inverter_config,
    }

def _load_inverters_config():
    """Load inverter configs from disk"""
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.error(f"Failed to load inverters config: {e}")
        raise


def _write_inverters_config(inverters):
    """Write inverter configs to disk"""
    with open(CONFIG_PATH, "w") as f:
        json.dump(inverters, f, indent=2)


def _coerce_float(value):
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def init_services():
    """Initialize all services"""
    global watchpower_service, csv_writer, scheduler, neon_store

    logger.info("Initializing services...")

    # Initialize services
    watchpower_service = WatchPowerService(
        CONFIG_PATH, timezone_name=WATCHPOWER_TIMEZONE
    )
    csv_writer = CSVWriter(data_dir="data")
    neon_store = NeonStore(
        database_url=os.getenv("NEON_DATABASE_URL"),
        timezone_name=WATCHPOWER_TIMEZONE,
    )

    # Authenticate
    if not watchpower_service.authenticate():
        logger.error("Failed to authenticate with WatchPower API")
        raise RuntimeError("Authentication failed")

    # Create and start scheduler
    poll_interval = int(os.getenv("POLL_INTERVAL_MINUTES", 5))
    poll_retry_attempts = int(os.getenv("POLL_RETRY_ATTEMPTS", 3))
    poll_retry_backoff_seconds = int(os.getenv("POLL_RETRY_BACKOFF_SECONDS", 2))
    scheduler = PollingScheduler(
        watchpower_service=watchpower_service,
        csv_writer=csv_writer,
        neon_store=neon_store,
        poll_interval_minutes=poll_interval,
        poll_retry_attempts=poll_retry_attempts,
        poll_retry_backoff_seconds=poll_retry_backoff_seconds,
        tick_seconds=int(os.getenv("POLL_TICK_SECONDS", 15)),
        timezone_name=WATCHPOWER_TIMEZONE,
        live_status_refresh_seconds=int(
            os.getenv("LIVE_STATUS_REFRESH_SECONDS", 60)
        ),
    )

    if neon_store.enabled:
        try:
            neon_store.ensure_poll_audit_table()
            neon_store.ensure_inverter_readings_indexes()
        except Exception as e:
            logger.error("Failed to ensure Neon runtime objects: %s", e)

        for inverter in watchpower_service.inverters:
            try:
                neon_store.upsert_inverter(inverter)
            except Exception as e:
                logger.error(
                    "Failed to upsert inverter %s in Neon: %s",
                    inverter.get("serial_number"),
                    e,
                )

    scheduler.start()

    logger.info("All services initialized successfully")


# API Routes


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify(
        {
            "status": "healthy",
            "service": "alemdar-flow-flask",
            "scheduler_status": scheduler.get_status() if scheduler else None,
        }
    )


@app.route("/api/inverters", methods=["GET"])
def get_inverters():
    """Get list of all configured inverters"""
    try:
        if not watchpower_service:
            return jsonify({"error": "Service not initialized"}), 503

        inverters = watchpower_service.get_inverters_list()
        return jsonify(
            {"success": True, "count": len(inverters), "inverters": inverters}
        )
    except Exception as e:
        logger.error(f"Error fetching inverters list: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/inverters/status", methods=["GET"])
def get_inverters_status():
    """Get current status payloads for all configured inverters."""
    try:
        if not watchpower_service or not scheduler:
            return jsonify({"error": "Service not initialized"}), 503

        payloads = []
        for inverter in watchpower_service.inverters:
            serial_number = str(inverter.get("serial_number") or "")
            if not serial_number:
                continue

            response_payload = _build_inverter_response(
                serial_number=serial_number,
                inverter_config=inverter,
            )
            if response_payload is not None:
                payloads.append(response_payload)

        return jsonify(
            {
                "success": True,
                "count": len(payloads),
                "inverters": payloads,
            }
        )
    except Exception as e:
        logger.error(f"Error fetching inverter status list: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/inverters/<serial_number>/config", methods=["GET"])
def get_inverter_config(serial_number):
    """Get full inverter configuration by serial number"""
    try:
        inverters = (
            watchpower_service.inverters
            if watchpower_service
            else _load_inverters_config()
        )

        inverter = next(
            (inv for inv in inverters if inv.get("serial_number") == serial_number),
            None,
        )

        if not inverter:
            return (
                jsonify(
                    {
                        "error": "Inverter not found",
                        "serial_number": serial_number,
                    }
                ),
                404,
            )

        return jsonify({"success": True, "inverter": inverter})
    except Exception as e:
        logger.error(f"Error fetching inverter config: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/inverters", methods=["POST"])
def upsert_inverter():
    """Create or override inverter configuration"""
    try:
        payload = request.get_json(silent=True) or {}
        override = bool(payload.get("override", False))

        required_fields = [
            "serial_number",
            "wifi_pn",
            "device_code",
            "device_address",
            "system_type",
            "username",
            "password",
        ]

        missing = [
            field for field in required_fields if payload.get(field) in (None, "")
        ]

        if missing:
            return (
                jsonify({"error": "Missing required fields", "fields": missing}),
                400,
            )

        try:
            device_code = int(payload.get("device_code"))
            device_address = int(payload.get("device_address"))
        except (TypeError, ValueError):
            return (
                jsonify(
                    {
                        "error": "Invalid device_code or device_address",
                    }
                ),
                400,
            )

        inverter = {
            "serial_number": str(payload.get("serial_number")).strip(),
            "wifi_pn": str(payload.get("wifi_pn")).strip(),
            "device_code": device_code,
            "device_address": device_address,
            "system_type": str(payload.get("system_type")).strip(),
            "alias": str(payload.get("alias") or "").strip(),
            "description": str(payload.get("description") or "").strip(),
            "username": str(payload.get("username")).strip(),
            "password": str(payload.get("password")).strip(),
        }

        inverters = _load_inverters_config()
        existing_index = next(
            (
                index
                for index, inv in enumerate(inverters)
                if inv.get("serial_number") == inverter["serial_number"]
            ),
            None,
        )

        if existing_index is not None and not override:
            return (
                jsonify(
                    {
                        "error": "Inverter with this serial already exists",
                        "serial_number": inverter["serial_number"],
                        "inverter": inverters[existing_index],
                    }
                ),
                409,
            )

        if existing_index is not None:
            inverters[existing_index] = inverter
        else:
            inverters.append(inverter)

        _write_inverters_config(inverters)

        if watchpower_service:
            watchpower_service.load_inverters_config(CONFIG_PATH)
            watchpower_service.authenticate()
        if neon_store and neon_store.enabled:
            try:
                neon_store.upsert_inverter(inverter)
            except Exception as e:
                logger.error(
                    "Failed to upsert inverter %s in Neon after config change: %s",
                    inverter["serial_number"],
                    e,
                )

        return jsonify(
            {
                "success": True,
                "inverter": inverter,
                "overridden": existing_index is not None,
            }
        )
    except Exception as e:
        logger.error(f"Error saving inverter: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/inverter/<serial_number>", methods=["GET"])
def get_inverter_data(serial_number):
    """Get latest data for a specific inverter."""
    try:
        if not scheduler:
            return jsonify({"error": "Scheduler not initialized"}), 503

        # Get inverter config for metadata
        inverter_config = next(
            (
                inv
                for inv in watchpower_service.inverters
                if inv["serial_number"] == serial_number
            ),
            None,
        )

        response_payload = _build_inverter_response(
            serial_number=serial_number,
            inverter_config=inverter_config,
        )
        if not response_payload or not response_payload.get("data"):
            return (
                jsonify(
                    {
                        "error": "No data available for this inverter",
                        "serial_number": serial_number,
                    }
                ),
                404,
            )
        return jsonify(response_payload)
    except Exception as e:
        logger.error(f"Error fetching data for {serial_number}: {e}")
        return jsonify({"error": str(e)}), 500



@app.route("/api/inverter/<serial_number>/history", methods=["GET"])
def get_inverter_history(serial_number):
    """Get historical data for a specific inverter from CSV"""
    try:
        # Get optional query parameters
        limit = request.args.get("limit", type=int)

        if neon_store and neon_store.enabled:
            try:
                data = neon_store.fetch_history(serial_number=serial_number, limit=limit)
                if data:
                    return jsonify(
                        {
                            "success": True,
                            "serial_number": serial_number,
                            "count": len(data),
                            "data": data,
                        }
                    )
            except Exception as e:
                logger.error(
                    "Failed to read history from Neon for %s: %s. Falling back to CSV.",
                    serial_number,
                    e,
                )

        if not csv_writer:
            return jsonify({"error": "CSV writer not initialized"}), 503

        if limit:
            data = csv_writer.read_latest(serial_number, num_rows=limit)
        else:
            data = csv_writer.get_all_data(serial_number)

        return jsonify(
            {
                "success": True,
                "serial_number": serial_number,
                "count": len(data),
                "data": data,
            }
        )
    except Exception as e:
        logger.error(f"Error fetching history for {serial_number}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/inverter/<serial_number>/energy-summary", methods=["GET"])
def get_inverter_energy_summary(serial_number):
    """Get server-side energy summaries for a specific inverter."""
    try:
        samples = []
        since = datetime.now(timezone.utc) - timedelta(days=400)
        if neon_store and neon_store.enabled:
            try:
                samples = neon_store.fetch_energy_summary_samples(
                    serial_number=serial_number,
                    since=since,
                )
            except Exception as e:
                logger.error(
                    "Failed to read energy summary samples from Neon for %s: %s. Falling back to CSV.",
                    serial_number,
                    e,
                )

        if not samples:
            if not csv_writer:
                return jsonify({"error": "CSV writer not initialized"}), 503

            rows = csv_writer.get_all_data(serial_number)
            samples = []
            for row in rows:
                parsed_timestamp = parse_watchpower_timestamp(
                    row.get("Data E Hora"), WATCHPOWER_TIMEZONE
                )
                if parsed_timestamp is None:
                    continue
                samples.append(
                    {
                        "reading_at": parsed_timestamp,
                        "load_power_w": row.get("AC Output Active Power"),
                        "pv_power_w": (
                            _coerce_float(
                                row.get("PV1 Charging Power")
                                or row.get("PV1 Charging power")
                            )
                            + _coerce_float(
                                row.get("PV2 Charging Power")
                                or row.get("PV2 Charging power")
                            )
                        ),
                        "grid_power_w": 0,
                        "raw_payload": row,
                    }
                )

        summary = build_energy_summary(serial_number, samples)
        return jsonify({"success": True, "data": summary})
    except Exception as e:
        logger.error(f"Error fetching energy summary for {serial_number}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/inverter/<serial_number>/daily", methods=["GET"])
def get_inverter_daily(serial_number):
    """Get full daily data (rows + titles) for charting"""
    try:
        if not watchpower_service:
            return jsonify({"error": "Service not initialized"}), 503

        daily = watchpower_service.get_daily_raw(serial_number)
        if not daily:
            return (
                jsonify(
                    {
                        "error": "No daily data available for this inverter",
                        "serial_number": serial_number,
                    }
                ),
                404,
            )

        return jsonify({"success": True, **daily})
    except Exception as e:
        logger.error(f"Error fetching daily data for {serial_number}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/poll/force", methods=["POST"])
def force_poll():
    """Force an immediate polling cycle"""
    try:
        if not scheduler:
            return jsonify({"error": "Scheduler not initialized"}), 503

        results = scheduler.force_poll()

        return jsonify(
            {"success": True, "message": "Polling cycle completed", "results": results}
        )
    except Exception as e:
        logger.error(f"Error during forced poll: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/status", methods=["GET"])
def get_status():
    """Get service status and statistics"""
    try:
        status = {
            "service": "alemdar-flow-flask",
            "authenticated": (
                watchpower_service.authenticated if watchpower_service else False
            ),
            "inverters_configured": (
                len(watchpower_service.inverters) if watchpower_service else 0
            ),
        }

        if scheduler:
            status["scheduler"] = scheduler.get_status()

        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return jsonify({"error": str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    try:
        # Initialize all services
        init_services()

        # Get port from environment
        port = int(os.getenv("FLASK_PORT", 5000))
        debug = os.getenv("FLASK_ENV") == "development"

        logger.info(f"Starting Flask server on port {port}")
        app.run(host="0.0.0.0", port=port, debug=debug)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        if scheduler:
            scheduler.stop()
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        raise
