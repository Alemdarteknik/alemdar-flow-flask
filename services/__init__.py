"""
Alemdar Flow Flask API
Backend service for WatchPower API integration
"""

import logging
import os

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
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for Next.js frontend

# Configuration
WATCHPOWER_USERNAME = os.getenv("WATCHPOWER_USERNAME")
WATCHPOWER_PASSWORD = os.getenv("WATCHPOWER_PASSWORD")
FLASK_PORT = int(os.getenv("FLASK_PORT", 5000))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_MINUTES", 5))
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "inverters.json")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Initialize services
watchpower_service = None
csv_writer = None
scheduler = None


def init_services():
    """Initialize all services"""
    global watchpower_service, csv_writer, scheduler

    logger.info("Initializing services...")

    # Get credentials from environment
    username = os.getenv("WATCHPOWER_USERNAME")
    password = os.getenv("WATCHPOWER_PASSWORD")

    if not username or not password:
        logger.error("WatchPower credentials not found in environment variables")
        raise ValueError("WATCHPOWER_USERNAME and WATCHPOWER_PASSWORD must be set")

    # Initialize services
    watchpower_service = WatchPowerService(username, password)
    csv_writer = CSVWriter(data_dir="data")

    # Load inverters configuration
    config_path = os.path.join(os.path.dirname(__file__), "config", "inverters.json")
    watchpower_service.load_inverters_config(config_path)

    # Authenticate
    if not watchpower_service.authenticate():
        logger.error("Failed to authenticate with WatchPower API")
        raise RuntimeError("Authentication failed")

    # Create and start scheduler
    poll_interval = int(os.getenv("POLL_INTERVAL_MINUTES", 5))
    scheduler = PollingScheduler(
        watchpower_service=watchpower_service,
        csv_writer=csv_writer,
        poll_interval_minutes=poll_interval_minutes,
    )

    scheduler.start()

    return watchpower_service, scheduler, csv_writer


# Initialize services (will be set up in app.py)
watchpower_service = None
csv_writer = None
scheduler = None
