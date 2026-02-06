"""
Alemdar Flow Flask API
Backend service for WatchPower API integration
"""

import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from services.scheduler import PollingScheduler
from services.watchpower_service import WatchPowerService
from utils.csv_writer import CSVWriter

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for Next.js frontend

# Global service instances
watchpower_service = None
csv_writer = None
scheduler = None
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "inverters.json")


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


def init_services():
    """Initialize all services"""
    global watchpower_service, csv_writer, scheduler

    logger.info("Initializing services...")

    # Initialize services
    watchpower_service = WatchPowerService(CONFIG_PATH)
    csv_writer = CSVWriter(data_dir="data")

    # Authenticate
    if not watchpower_service.authenticate():
        logger.error("Failed to authenticate with WatchPower API")
        raise RuntimeError("Authentication failed")

    # Create and start scheduler
    poll_interval = int(os.getenv("POLL_INTERVAL_MINUTES", 5))
    scheduler = PollingScheduler(
        watchpower_service=watchpower_service,
        csv_writer=csv_writer,
        poll_interval_minutes=poll_interval,
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
    """Get latest data for a specific inverter (from cache)"""
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

        # Get cached data
        cached_data = scheduler.get_cached_data(serial_number)

        # If not cached yet, try to fetch directly and populate cache
        if not cached_data:
            logger.info(
                f"Cache miss for {serial_number}. Fetching latest data directly from WatchPower API"
            )
            latest = watchpower_service.get_latest_data(serial_number)
            if latest and "data" in latest:
                # Store in cache for future requests
                scheduler.cache[serial_number] = {
                    "data": latest["data"],
                    "timestamp": latest["data"].get("Data E Hora")
                    or datetime.now().isoformat(),
                    "csv_written": False,
                    "inverter_config": latest.get("inverter_config"),
                }
                cached_data = scheduler.cache[serial_number]
            else:
                return (
                    jsonify(
                        {
                            "error": "No data available for this inverter",
                            "serial_number": serial_number,
                        }
                    ),
                    404,
                )

        return jsonify(
            {
                "success": True,
                "serial_number": serial_number,
                "data": cached_data["data"],
                "cached_at": cached_data["timestamp"],
                "last_poll": (
                    scheduler.last_poll_time.isoformat()
                    if scheduler.last_poll_time
                    else None
                ),
                "inverter_config": cached_data.get("inverter_config")
                or inverter_config,
            }
        )
    except Exception as e:
        logger.error(f"Error fetching data for {serial_number}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/inverter/<serial_number>/history", methods=["GET"])
def get_inverter_history(serial_number):
    """Get historical data for a specific inverter from CSV"""
    try:
        if not csv_writer:
            return jsonify({"error": "CSV writer not initialized"}), 503

        # Get optional query parameters
        limit = request.args.get("limit", type=int)

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
