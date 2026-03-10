# 🌦️ Weather Data ETL Pipeline

A production-ready Python ETL pipeline that **extracts** hourly weather data
from the free [Open-Meteo API](https://open-meteo.com/), **transforms** it
with rich cleaning and enrichment, and **loads** it into a MySQL database for
analysis — **no API key required**.

---

## Architecture

```
Open-Meteo REST API
        │
        ▼
┌──────────────┐     Raw JSON      ┌─────────────────┐    Clean dicts    ┌──────────────┐
│  extractor   │  ──────────────►  │   transformer   │  ─────────────►  │    loader    │
│ (requests +  │                   │  (clean, enrich,│                   │ (MySQL batch │
│  retries)    │                   │   derive, tag)  │                   │   upsert)    │
└──────────────┘                   └─────────────────┘                   └──────────────┘
        ▲                                                                         │
        │                                                                         ▼
   config.py                                                          weather_observations
  (cities, API,                                                        + 3 analytical views
   DB settings)
```

---

## File Structure

```
weather_etl/
├── etl_pipeline.py      # Orchestrator — run this
├── config.py            # Cities, DB credentials, API settings
├── extractor.py         # EXTRACT: Open-Meteo API client
├── transformer.py       # TRANSFORM: cleaning, enrichment, derived fields
├── loader.py            # LOAD: MySQL schema init + batch upsert
├── logger.py            # Dual console + rotating-file logging
├── analysis_queries.sql # 10 ready-to-run analytical SQL queries
└── requirements.txt     # Python dependencies
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Set up MySQL
```sql
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'etl_password';
GRANT ALL PRIVILEGES ON weather_db.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;
```

### 3. Edit `config.py`
Update `DB_CONFIG` with your MySQL credentials. Optionally add/remove cities.

### 4. Run the pipeline
```bash
# Full ETL (extract + transform + load)
python etl_pipeline.py

# Single city
python etl_pipeline.py --city "Tokyo"

# Dry run — skips MySQL (great for testing)
python etl_pipeline.py --dry-run

# Web UI (Streamlit dashboard)
streamlit run app.py
```

---

## What Gets Extracted

The pipeline fetches **14 hourly variables** from Open-Meteo for each city:
temperature, apparent temperature, humidity, precipitation, rain, snowfall,
wind speed/direction/gusts, surface pressure, cloud cover, visibility,
UV index, and WMO weather code.

Data covers **7 days past + 7 days forecast** per run (configurable in `config.py`).

---

## Transform — Derived Fields

| Category | Fields Added |
|---|---|
| Temperature | temp_f, dew_point_c, heat_index_c, wind_chill_c |
| Unit conversions | wind_speed_mph, wind_speed_kph, pressure_inhg, visibility_miles |
| Wind | wind_cardinal (N/NE/…), beaufort_scale (0–12) |
| UV | uv_category (Low/Moderate/High/Very High/Extreme) |
| Categorical | weather_desc (human-readable WMO code description) |
| Temporal | hour_of_day, day_of_week, month, season, is_daytime, is_weekend |

---

## Database Schema

**Table:** `weather_observations`
- 42 typed columns with proper indexes
- `UNIQUE KEY (city, observed_at)` — re-running the pipeline safely upserts

**Analytical Views** (auto-created):

| View | Description |
|---|---|
| `vw_daily_summary` | Daily high/low/avg temp + precip per city |
| `vw_city_comparison` | Aggregate stats across all cities |
| `vw_weather_alerts` | Rows matching extreme condition thresholds |

---

## Scheduling (Cron)

To run the pipeline every 6 hours automatically:
```bash
crontab -e
# Add:
0 */6 * * * /usr/bin/python3 /path/to/weather_etl/etl_pipeline.py >> /var/log/weather_etl.log 2>&1
```

---

## Configuration Reference (`config.py`)

| Setting | Default | Description |
|---|---|---|
| `CITIES` | 5 global cities | List of `{name, lat, lon, timezone}` dicts |
| `DB_CONFIG` | localhost:3306 | MySQL connection parameters |
| `API_CONFIG.past_days` | 7 | Historical days to fetch per run |
| `API_CONFIG.forecast_days` | 7 | Forecast days to fetch per run |
| `PIPELINE_CONFIG.batch_size` | 500 | Rows per MySQL INSERT batch |
| `PIPELINE_CONFIG.api_delay_seconds` | 0.5 | Delay between city API calls |

---

## Sample Output

```
2025-01-15 09:00:01  INFO      etl.pipeline          🚀 Weather ETL Pipeline starting [FULL ETL] — 5 city/cities
2025-01-15 09:00:01  INFO      etl.pipeline          🗄️  Initialising database schema...
2025-01-15 09:00:01  INFO      etl.loader            Schema initialised (table + 3 analytical views)
2025-01-15 09:00:01  INFO      etl.pipeline          ━━━ Processing: New York ━━━
2025-01-15 09:00:01  INFO      etl.pipeline          [EXTRACT] Fetching weather data for New York
2025-01-15 09:00:02  INFO      etl.pipeline          [EXTRACT] ✓  336 hourly records
2025-01-15 09:00:02  INFO      etl.pipeline          [TRANSFORM] Cleaning and enriching data for New York
2025-01-15 09:00:02  INFO      etl.pipeline          [TRANSFORM] ✓  336 records ready
2025-01-15 09:00:02  INFO      etl.pipeline          [LOAD] Inserting records into MySQL for New York
2025-01-15 09:00:02  INFO      etl.pipeline          [LOAD] ✓  336 rows upserted
...
═══════════════════════════════════════════════════════
📊 PIPELINE SUMMARY
   Status             : SUCCESS
   Cities processed   : 5/5
   Records extracted  : 1680
   Records transformed: 1680
   Records loaded     : 1680
═══════════════════════════════════════════════════════
```
