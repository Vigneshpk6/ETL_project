"""
config.py — Central configuration for the Weather ETL Pipeline
"""

# ── Cities to monitor ────────────────────────────────────────────────────────
# Uses Open-Meteo geocoordinates. Add/remove cities freely.
CITIES = [
    {"name": "New York",    "lat": 40.7128,  "lon": -74.0060, "timezone": "America/New_York"},
    {"name": "London",      "lat": 51.5074,  "lon":  -0.1278, "timezone": "Europe/London"},
    {"name": "Tokyo",       "lat": 35.6762,  "lon": 139.6503, "timezone": "Asia/Tokyo"},
    {"name": "Sydney",      "lat": -33.8688, "lon": 151.2093, "timezone": "Australia/Sydney"},
    {"name": "Dubai",       "lat": 25.2048,  "lon":  55.2708, "timezone": "Asia/Dubai"},
]

# ── MySQL connection ─────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "root",        # ← change before running
    "database": "weather_db",
    "charset":  "utf8mb4",
}

# ── Open-Meteo API settings ──────────────────────────────────────────────────
API_CONFIG = {
    "base_url":    "https://api.open-meteo.com/v1/forecast",
    "past_days":   7,           # fetch last N days of historical hourly data
    "forecast_days": 7,         # fetch next N days of forecast data
    "hourly_vars": [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
        "surface_pressure",
        "cloud_cover",
        "visibility",
        "uv_index",
        "weather_code",
    ],
    "timeout_seconds": 30,
}

# ── Pipeline behaviour ────────────────────────────────────────────────────────
PIPELINE_CONFIG = {
    "api_delay_seconds": 0.5,    # polite delay between city API calls
    "batch_size":        500,    # rows per MySQL INSERT batch
    "log_level":         "INFO",
    "log_file":          "etl_pipeline.log",
}
