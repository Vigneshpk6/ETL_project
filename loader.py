"""
loader.py — LOAD layer
Initialises the MySQL schema and bulk-upserts transformed records
using efficient batched INSERT ... ON DUPLICATE KEY UPDATE statements.
"""

import logging
from typing import Any

import mysql.connector
from mysql.connector import Error as MySQLError

from config import PIPELINE_CONFIG

logger = logging.getLogger("etl.loader")

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_DB_SQL = "CREATE DATABASE IF NOT EXISTS {db} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_observations (
    id                  BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

    -- Identity
    city                VARCHAR(100)    NOT NULL,
    latitude            DECIMAL(9,6)    NOT NULL,
    longitude           DECIMAL(9,6)    NOT NULL,
    timezone            VARCHAR(60)     NOT NULL,
    observed_at         DATETIME        NOT NULL,
    ingested_at         DATETIME        NOT NULL,
    elevation_m         FLOAT,

    -- Temperature
    temp_c              FLOAT,
    temp_f              FLOAT,
    apparent_temp_c     FLOAT,
    apparent_temp_f     FLOAT,
    dew_point_c         FLOAT,
    heat_index_c        FLOAT,
    wind_chill_c        FLOAT,

    -- Humidity & Precipitation
    humidity_pct        TINYINT UNSIGNED,
    precipitation_mm    FLOAT,
    rain_mm             FLOAT,
    snowfall_cm         FLOAT,

    -- Wind
    wind_speed_ms       FLOAT,
    wind_speed_mph      FLOAT,
    wind_speed_kph      FLOAT,
    wind_direction_deg  SMALLINT UNSIGNED,
    wind_cardinal       VARCHAR(4),
    wind_gust_ms        FLOAT,
    beaufort_scale      TINYINT UNSIGNED,

    -- Atmosphere
    pressure_hpa        FLOAT,
    pressure_inhg       FLOAT,
    cloud_cover_pct     TINYINT UNSIGNED,
    visibility_m        FLOAT,
    visibility_miles    FLOAT,
    uv_index            FLOAT,
    uv_category         VARCHAR(12),

    -- Categorical
    weather_code        SMALLINT,
    weather_desc        VARCHAR(60),

    -- Temporal
    hour_of_day         TINYINT UNSIGNED,
    day_of_week         VARCHAR(10),
    month               TINYINT UNSIGNED,
    season              VARCHAR(10),
    is_daytime          BOOLEAN,
    is_weekend          BOOLEAN,

    -- Unique constraint prevents duplicate hourly rows
    UNIQUE KEY uq_city_time (city, observed_at),
    INDEX idx_city          (city),
    INDEX idx_observed_at   (observed_at),
    INDEX idx_temp_c        (temp_c),
    INDEX idx_precip        (precipitation_mm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ── Analytical VIEWs ──────────────────────────────────────────────────────────

VIEWS_SQL = [
    """
    CREATE OR REPLACE VIEW vw_daily_summary AS
    SELECT
        city,
        DATE(observed_at)           AS obs_date,
        ROUND(AVG(temp_c), 2)       AS avg_temp_c,
        ROUND(MAX(temp_c), 2)       AS max_temp_c,
        ROUND(MIN(temp_c), 2)       AS min_temp_c,
        ROUND(AVG(humidity_pct), 1) AS avg_humidity_pct,
        ROUND(SUM(precipitation_mm), 2) AS total_precip_mm,
        ROUND(MAX(wind_speed_kph), 2)   AS max_wind_kph,
        ROUND(AVG(uv_index), 2)         AS avg_uv_index,
        COUNT(*)                        AS hourly_records
    FROM weather_observations
    GROUP BY city, DATE(observed_at);
    """,
    """
    CREATE OR REPLACE VIEW vw_city_comparison AS
    SELECT
        city,
        ROUND(AVG(temp_c), 2)           AS avg_temp_c,
        ROUND(AVG(humidity_pct), 1)     AS avg_humidity_pct,
        ROUND(SUM(precipitation_mm), 2) AS total_precip_mm,
        ROUND(AVG(wind_speed_kph), 2)   AS avg_wind_kph,
        MIN(observed_at)                AS data_from,
        MAX(observed_at)                AS data_to,
        COUNT(*)                        AS total_records
    FROM weather_observations
    GROUP BY city;
    """,
    """
    CREATE OR REPLACE VIEW vw_weather_alerts AS
    SELECT
        city, observed_at, temp_c, wind_speed_kph,
        precipitation_mm, uv_index, weather_desc,
        CASE
            WHEN temp_c > 35                THEN 'Extreme Heat'
            WHEN temp_c < -10               THEN 'Extreme Cold'
            WHEN wind_speed_kph > 90        THEN 'Storm-Force Wind'
            WHEN precipitation_mm > 10      THEN 'Heavy Precipitation'
            WHEN uv_index >= 11             THEN 'Extreme UV'
            ELSE 'Normal'
        END AS alert_type
    FROM weather_observations
    WHERE temp_c > 35
       OR temp_c < -10
       OR wind_speed_kph > 90
       OR precipitation_mm > 10
       OR uv_index >= 11;
    """,
]


class WeatherLoader:
    """
    Handles all MySQL interactions for the pipeline.

    Supports:
        - Schema / table creation
        - Batch upsert with ON DUPLICATE KEY UPDATE
        - Analytical view creation
    """

    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.batch_size = PIPELINE_CONFIG["batch_size"]

    # ── Public ────────────────────────────────────────────────────────────────

    def init_schema(self):
        """Create database, table, and analytical views if they don't exist."""
        # Connect without a database first to run CREATE DATABASE
        base_cfg = {k: v for k, v in self.db_config.items() if k != "database"}
        with self._connect(base_cfg) as conn:
            cursor = conn.cursor()
            cursor.execute(CREATE_DB_SQL.format(db=self.db_config["database"]))
            conn.commit()

        # Now connect to the target database
        with self._connect(self.db_config) as conn:
            cursor = conn.cursor()
            cursor.execute(CREATE_TABLE_SQL)
            for view_sql in VIEWS_SQL:
                cursor.execute(view_sql)
            conn.commit()
            logger.info("Schema initialised (table + 3 analytical views)")

    def load(self, records: list[dict]) -> int:
        """
        Upsert a list of transformed records into MySQL.

        Returns:
            Number of rows affected (inserted + updated)
        """
        if not records:
            return 0

        columns = list(records[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(f"`{c}`" for c in columns)

        # ON DUPLICATE KEY UPDATE all non-identity columns
        skip = {"city", "observed_at", "id"}
        updates = ", ".join(
            f"`{c}` = VALUES(`{c}`)" for c in columns if c not in skip
        )

        sql = (
            f"INSERT INTO weather_observations ({col_names}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {updates};"
        )

        total_affected = 0
        with self._connect(self.db_config) as conn:
            cursor = conn.cursor()
            for batch in self._batches(records, self.batch_size):
                values = [tuple(r[c] for c in columns) for r in batch]
                cursor.executemany(sql, values)
                total_affected += cursor.rowcount
            conn.commit()

        return total_affected

    def fetch_records(
        self,
        city: str | None = None,
        limit: int = 200,
        days_back: int | None = 7,
    ) -> list[dict]:
        """
        Fetch records from weather_observations for display/editing.

        Args:
            city: Filter by city name (None = all cities)
            limit: Max rows to return
            days_back: Only fetch records from last N days (None = no date filter)

        Returns:
            List of record dicts
        """
        conditions = []
        params = []
        if city:
            conditions.append("city = %s")
            params.append(city)
        if days_back is not None:
            conditions.append("observed_at >= DATE_SUB(NOW(), INTERVAL %s DAY)")
            params.append(days_back)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)

        sql = f"SELECT * FROM weather_observations{where} ORDER BY observed_at DESC LIMIT %s"
        with self._connect(self.db_config) as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params)
            rows = cursor.fetchall()

        # Convert datetime objects to strings for Streamlit
        for row in rows:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat() if v else None
        return rows

    def update_record(self, record_id: int, updates: dict) -> bool:
        """
        Update a single record by id.

        Args:
            record_id: Primary key
            updates: Dict of column -> value (only updatable columns)

        Returns:
            True if row was updated
        """
        skip = {"id", "city", "observed_at", "ingested_at"}
        cols = [c for c in updates.keys() if c not in skip]
        if not cols:
            return False

        set_clause = ", ".join(f"`{c}` = %s" for c in cols)
        values = [updates[c] for c in cols]
        values.append(record_id)

        sql = f"UPDATE weather_observations SET {set_clause} WHERE id = %s"
        with self._connect(self.db_config) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, values)
            conn.commit()
            return cursor.rowcount > 0

    # ── Private ───────────────────────────────────────────────────────────────

    def _connect(self, cfg: dict):
        try:
            return mysql.connector.connect(**cfg)
        except MySQLError as exc:
            logger.error(f"MySQL connection failed: {exc}")
            raise

    @staticmethod
    def _batches(lst: list, size: int):
        for i in range(0, len(lst), size):
            yield lst[i : i + size]
