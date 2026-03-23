# Alemdar Flow Flask API

Backend service for WatchPower API integration with Alemdar Flow dashboard.

## Features

- ✅ Fetches real-time inverter data from WatchPower API
- ✅ Polls data every 5 minutes (configurable)
- ✅ Caches latest readings in-memory for fast access
- ✅ Writes historical data to CSV files per inverter
- ✅ Supports multiple inverters (currently configured for 1, scalable to 90+)
- ✅ RESTful API for frontend integration
- ✅ Automatic timestamp deduplication

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` to `.env` and update with your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```env
WATCHPOWER_USERNAME=your_username
WATCHPOWER_PASSWORD=your_password
FLASK_PORT=5000
POLL_INTERVAL_MINUTES=5
INVERTER_STALE_THRESHOLD_MINUTES=8
```

### 3. Configure Inverters

Edit `config/inverters.json` to add your inverter details:

```json
[
  {
    "serial_number": "00202507001060",
    "wifi_pn": "W0068107329284",
    "device_code": 2449,
    "device_address": 1,
    "system_type": "offgrid",
    "alias": "OG-001",
    "description": "Off-Grid System 001"
  }
]
```

### 4. Run the Server

```bash
python app.py
```

Server will start on `http://localhost:5000`

## API Endpoints

### Health Check

```
GET /health
```

Returns service health status.

### List All Inverters

```
GET /api/inverters
```

Returns list of all configured inverters.

### Get Inverter Data

```
GET /api/inverter/<serial_number>
```

Returns latest cached data for specific inverter.
Response also includes `telemetry_health`, which marks the inverter offline when no
new inverter data has been received within `INVERTER_STALE_THRESHOLD_MINUTES`.

### Get Inverter History

```
GET /api/inverter/<serial_number>/history?limit=100
```

Returns historical data from CSV file. Optional `limit` parameter.

### Force Poll

```
POST /api/poll/force
```

Triggers immediate polling cycle outside of schedule.

### Get Status

```
GET /api/status
```

Returns service status and statistics.

## Data Flow

1. **Flask Scheduler** polls WatchPower API every 5 minutes
2. **Extracts** latest reading from today's data
3. **Caches** in-memory for instant access
4. **Writes** to CSV files (`data/inverter_<serial>.csv`)
5. **Next.js frontend** polls Flask API every 5-10 seconds
6. **Dashboard** displays cached data with real-time feel

## CSV Files

CSV files are stored in `data/` directory with format:

```
data/inverter_00202507001060.csv
```

Each file contains all readings with automatic deduplication by timestamp.

## Scaling to Multiple Inverters

To add more inverters, simply add entries to `config/inverters.json`. The system automatically:

- Polls all configured inverters
- Creates separate CSV files
- Maintains independent caches
- Handles errors gracefully per inverter

## Development

### Project Structure

```
alemdar-flow-flask/
├── app.py                 # Flask application
├── requirements.txt       # Python dependencies
├── .env                   # Environment configuration
├── config/
│   └── inverters.json     # Inverter configurations
├── data/                  # CSV output files
├── services/
│   ├── watchpower_service.py  # WatchPower API wrapper
│   └── scheduler.py           # Polling scheduler
└── utils/
    └── csv_writer.py          # CSV file management
```

### Logging

Logs are output to console with timestamps. Adjust logging level in `app.py`:

```python
logging.basicConfig(level=logging.DEBUG)  # For verbose logs
```

## Troubleshooting

### Authentication Fails

- Verify credentials in `.env` file
- Check WatchPower account is active
- Ensure username/password are correct

### No Data for Inverter

- Verify inverter configuration in `config/inverters.json`
- Check serial number, wifi_pn, device_code, device_address are correct
- Test with WatchPower mobile app to ensure device is online

### Polling Not Working

- Check logs for error messages
- Verify WatchPower API is accessible
- Try force polling: `POST /api/poll/force`

## License

MIT
